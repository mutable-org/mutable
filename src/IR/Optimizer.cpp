#include "mutable/IR/Optimizer.hpp"

#include "mutable/IR/Operator.hpp"
#include "mutable/storage/Store.hpp"
#include <algorithm>
#include <vector>


using namespace m;


/*======================================================================================================================
 * Helper functions
 *====================================================================================================================*/

/** Returns `true` iff the given join predicate in `cnf::CNF` formula is an equi-join. */
bool is_equi_join(const cnf::CNF &cnf)
{
    if (cnf.size() != 1) return false;
    auto &clause = cnf[0];
    if (clause.size() != 1) return false;
    auto &literal = clause[0];
    if (literal.negative()) return false;
    auto expr = literal.expr();
    auto binary = cast<const BinaryExpr>(expr);
    if (not binary or binary->tok != TK_EQUAL) return false;
    return is<const Designator>(binary->lhs) and is<const Designator>(binary->rhs);
}

struct WeightFilterClauses : ConstASTExprVisitor
{
    private:
    std::vector<unsigned> clause_costs_;
    unsigned weight_;

    public:
    void operator()(const cnf::CNF &cnf) {
        for (auto &clause : cnf) {
            weight_ = 0;
            for (auto &pred : clause)
                (*this)(*pred.expr());
            clause_costs_.push_back(weight_);
        }
    }
    unsigned operator[](std::size_t idx) const { insist(idx < clause_costs_.size()); return clause_costs_[idx]; }

    private:
    using ConstASTExprVisitor::operator();
    void operator()(const ErrorExpr &e) { unreachable("no errors at this stage"); }

    void operator()(const Designator &e) {
        if (auto cs = cast<const CharacterSequence>(e.type()))
            weight_ += cs->length;
        else
            weight_ += 1;
    }

    void operator()(const Constant &e) {
        if (auto cs = cast<const CharacterSequence>(e.type()))
            weight_ += cs->length;
        else
            weight_ += 1;
    }

    void operator()(const FnApplicationExpr &e) {
        weight_ += 1;
        for (auto arg : e.args)
            (*this)(*arg);
    }

    void operator()(const UnaryExpr &e) { weight_ += 1; (*this)(*e.expr); }

    void operator()(const BinaryExpr &e) { weight_ += 1; (*this)(*e.lhs); (*this)(*e.rhs); }

    void operator()(const QueryExpr &e) { weight_ += 1000; }
};

std::unique_ptr<FilterOperator> optimize_filter(std::unique_ptr<FilterOperator> filter)
{
    WeightFilterClauses W;
    W(filter->filter());
    std::vector<std::pair<cnf::Clause, unsigned>> weighted_clauses;
    weighted_clauses.reserve(filter->filter().size());
    for (std::size_t i = 0; i != filter->filter().size(); ++i)
        weighted_clauses.emplace_back(filter->filter()[i], W[i]);

    /* Sort clauses by weight in descending order. */
    std::sort(weighted_clauses.begin(), weighted_clauses.end(), [](auto &left, auto &right) -> bool {
        return left.second > right.second;
    });

    filter->filter(cnf::CNF{});
    constexpr unsigned MAX_WEIGHT = 12; // equals to 4 comparisons of fixed-length values
    unsigned w = 0;
    cnf::CNF cnf;
    {
        auto wc = weighted_clauses.back();
        weighted_clauses.pop_back();
        cnf.push_back(wc.first);
        w = wc.second;
    }
    while (not weighted_clauses.empty()) {
        auto wc = weighted_clauses.back();
        weighted_clauses.pop_back();

        if (w + wc.second <= MAX_WEIGHT) {
            cnf.push_back(wc.first);
            w += wc.second;
        } else {
            filter->filter(std::move(cnf));
            cnf = cnf::CNF();
            auto tmp = std::make_unique<FilterOperator>(cnf::CNF{});
            tmp->add_child(filter.release());
            filter = std::move(tmp);
            cnf.push_back(wc.first);
            w = wc.second;
        }
    }
    insist(not cnf.empty());
    filter->filter(std::move(cnf));

    return filter;
}


/*======================================================================================================================
 * Optimizer
 *====================================================================================================================*/

std::unique_ptr<Producer> Optimizer::operator()(const QueryGraph &G) const
{
    return std::move(optimize(G).first);
}

std::pair<std::unique_ptr<Producer>, PlanTable> Optimizer::optimize(const QueryGraph &G) const
{
    PlanTable plan_table(G);
    const auto num_sources = G.sources().size();
    auto &C = Catalog::Get();
    auto &DB = C.get_database_in_use();
    auto &CE = DB.cardinality_estimator();

    if (num_sources == 0)
        return { std::make_unique<ProjectionOperator>(G.projections()), std::move(plan_table) };

    /*----- Initialize plan table and compute plans for data sources. ------------------------------------------------*/
    Producer **source_plans = new Producer*[num_sources];
    for (auto ds : G.sources()) {
        Subproblem s(1UL << ds->id());
        if (auto bt = cast<const BaseTable>(ds)) {
            /* Produce a scan for base tables. */
            plan_table[s].cost = 0;
            plan_table[s].model = CE.estimate_scan(G, s);
            auto &store = bt->table().store();
            source_plans[ds->id()] = new ScanOperator(store, bt->alias());
        }
        else {
            /* Recursively solve nested queries. */
            auto Q = as<const Query>(ds);
            auto [sub_plan, sub_table] = optimize(*Q->query_graph());
            auto &sub = sub_table.get_final();

            /* Prefix every attribute of the nested query with the nested query's alias. */
            Schema S;
            for (auto &e : sub_plan->schema())
                S.add({Q->alias(), e.id.name}, e.type);
            sub_plan->schema() = S;

            /* Update the plan table with the `DataModel` and cost of the nested query and save the plan in the array of
             * source plans. */
            plan_table[s].cost = sub.cost;
            plan_table[s].model = std::move(sub.model);
            source_plans[ds->id()] = sub_plan.release();
        }
        /* Apply filter, if any. */
        if (ds->filter().size()) {
            auto filter = std::make_unique<FilterOperator>(ds->filter());
            // auto filter = new FilterOperator(ds->filter());
            filter->add_child(source_plans[ds->id()]);
            filter = optimize_filter(std::move(filter));
            auto new_model = CE.estimate_filter(*plan_table[s].model, filter->filter());
            source_plans[ds->id()] = filter.release();
            plan_table[s].model = std::move(new_model);
        }
    }

    optimize_locally(G, plan_table);
    auto plan = construct_plan(G, plan_table, source_plans).release();
    auto &entry = plan_table.get_final();

    /* Perform grouping */
    if (not G.group_by().empty()) {
        /* Compute `DataModel` after grouping. */
        auto new_model = CE.estimate_grouping(*entry.model, G.group_by()); // TODO provide aggregates
        entry.model = std::move(new_model);
        // TODO pick "best" algorithm
        auto group_by = new GroupingOperator(G.group_by(), G.aggregates(), GroupingOperator::G_Hashing);
        group_by->add_child(plan);
        plan = group_by;
    } else if (not G.aggregates().empty()) {
        /* Compute `DataModel` after grouping. */
        // TODO compute data model
        auto agg = new AggregationOperator(G.aggregates());
        agg->add_child(plan);
        plan = agg;
    }

    /* Perform ordering */
    if (not G.order_by().empty()) {
        // TODO estimate data model
        auto order_by = new SortingOperator(G.order_by());
        order_by->add_child(plan);
        plan = order_by;
    }

    /* Perform projection */
    if (not G.projections().empty() or G.projection_is_anti()) {
        // TODO estimate data model
        auto projection = new ProjectionOperator(G.projections(), G.projection_is_anti());
        projection->add_child(plan);
        plan = projection;
    }

    /* Limit. */
    if (G.limit().limit or G.limit().offset) {
        /* Compute `DataModel` after limit. */
        auto new_model = CE.estimate_limit(*entry.model, G.limit().limit, G.limit().offset);
        entry.model = std::move(new_model);
        // TODO estimate data model
        auto limit = new LimitOperator(G.limit().limit, G.limit().offset);
        limit->add_child(plan);
        plan = limit;
    }

    plan->minimize_schema();
    delete[] source_plans;
    return std::make_pair(std::unique_ptr<Producer>(plan), std::move(plan_table));
}

void Optimizer::optimize_locally(const QueryGraph &G, PlanTable &plan_table) const
{
    plan_enumerator()(G, cost_function(), plan_table);
}

std::unique_ptr<Producer>
Optimizer::construct_plan(const QueryGraph &G, PlanTable &plan_table, Producer **source_plans) const
{
    auto joins = G.joins(); // create a "set" of all joins

    /* Use nested lambdas to implement recursive lambda using CPS. */
    const auto construct_recursive = [&](Subproblem s) -> Producer* {
        auto construct_plan_impl = [&](Subproblem s, auto &construct_plan_rec) -> Producer* {
            auto subproblems = plan_table[s].get_subproblems();
            if (subproblems.empty()) {
                insist(s.size() == 1);
                return source_plans[*s.begin()];
            } else {
                /* Compute plan for each sub problem.  Must happen *before* calculating the join predicate. */
                std::vector<Producer*> sub_plans;
                for (auto sub : subproblems)
                    sub_plans.push_back(construct_plan_rec(sub, construct_plan_rec));

                /* Calculate the join predicate. */
                cnf::CNF join_condition;
                for (auto it = joins.begin(); it != joins.end(); ) {
                    Subproblem join_sources;
                    /* Compute subproblem of sources to join. */
                    for (auto ds : (*it)->sources())
                        join_sources.set(ds->id());

                    if (join_sources.is_subset(s)) { // possible join
                        join_condition = join_condition and (*it)->condition();
                        it = joins.erase(it);
                    } else {
                        ++it;
                    }
                }

                /* Construct the join. */
                if (sub_plans.size() == 2 and is_equi_join(join_condition)) {
                    auto join = new JoinOperator(join_condition, JoinOperator::J_SimpleHashJoin);
                    for (auto sub_plan : sub_plans)
                        join->add_child(sub_plan);
                    return join;
                } else {
                    auto join = new JoinOperator(join_condition, JoinOperator::J_NestedLoops);
                    for (auto sub_plan : sub_plans)
                        join->add_child(sub_plan);
                    return join;
                }
            }
        };
        return construct_plan_impl(s, construct_plan_impl);
    };

    return std::unique_ptr<Producer>(construct_recursive(Subproblem((1UL << G.sources().size()) - 1)));
}

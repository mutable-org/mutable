#include <mutable/IR/Optimizer.hpp>

#include <algorithm>
#include <mutable/catalog/Catalog.hpp>
#include <mutable/IR/Operator.hpp>
#include <mutable/IR/Operator.hpp>
#include <mutable/Options.hpp>
#include <mutable/parse/AST.hpp>
#include <mutable/storage/Store.hpp>
#include <mutable/storage/Store.hpp>
#include <vector>


using namespace m;
using namespace m::ast;


/*======================================================================================================================
 * Helper functions
 *====================================================================================================================*/

struct WeightFilterClauses : ConstASTExprVisitor
{
    private:
    std::vector<unsigned> clause_costs_;
    unsigned weight_;

    public:
    void operator()(const cnf::CNF &cnf) {
        for (auto &clause : cnf) {
            weight_ = 0;
            for (auto pred : clause)
                (*this)(*pred);
            clause_costs_.push_back(weight_);
        }
    }
    unsigned operator[](std::size_t idx) const { M_insist(idx < clause_costs_.size()); return clause_costs_[idx]; }

    private:
    using ConstASTExprVisitor::operator();
    void operator()(const ErrorExpr&) { M_unreachable("no errors at this stage"); }

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
        for (auto &arg : e.args)
            (*this)(*arg);
    }

    void operator()(const UnaryExpr &e) { weight_ += 1; (*this)(*e.expr); }

    void operator()(const BinaryExpr &e) { weight_ += 1; (*this)(*e.lhs); (*this)(*e.rhs); }

    void operator()(const QueryExpr&) { weight_ += 1000; }
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
    M_insist(not cnf.empty());
    filter->filter(std::move(cnf));

    return filter;
}


/*======================================================================================================================
 * Optimizer
 *====================================================================================================================*/

std::pair<std::unique_ptr<Producer>, PlanTableEntry>
Optimizer::optimize(const QueryGraph &G) const
{
    return optimize_recursive(G);
}

std::pair<std::unique_ptr<Producer>, PlanTableEntry>
Optimizer::optimize_recursive(const QueryGraph &G) const
{
    switch (Options::Get().plan_table_type)
    {
        case Options::PT_auto: {
            /* Select most suitable type of plan table depending on the query graph structure.
             * Currently a simple heuristic based on the number of data sources.
             * TODO: Consider join edges too.  Eventually consider #CSGs. */
            if (G.num_sources() <= 15) {
                auto [plan, PT] = optimize_with_plantable<PlanTableSmallOrDense>(G);
                return { std::move(plan), std::move(PT.get_final()) };
            } else {
                auto [plan, PT] = optimize_with_plantable<PlanTableLargeAndSparse>(G);
                return { std::move(plan), std::move(PT.get_final()) };
            }
        }

        case Options::PT_SmallOrDense: {
            auto [plan, PT] = optimize_with_plantable<PlanTableSmallOrDense>(G);
            return { std::move(plan), std::move(PT.get_final()) };
        }

        case Options::PT_LargeAndSparse: {
            auto [plan, PT] = optimize_with_plantable<PlanTableLargeAndSparse>(G);
            return { std::move(plan), std::move(PT.get_final()) };
        }
    }
}

template<typename PlanTable>
std::pair<std::unique_ptr<Producer>, PlanTable>
Optimizer::optimize_with_plantable(const QueryGraph &G) const
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
    for (auto &ds : G.sources()) {
        Subproblem s(1UL << ds->id());
        if (auto bt = cast<const BaseTable>(ds.get())) {
            /* Produce a scan for base tables. */
            plan_table[s].cost = 0;
            plan_table[s].model = CE.estimate_scan(G, s);
            auto &store = bt->table().store();
            auto source = new ScanOperator(store, bt->name());
            source_plans[ds->id()] = source;

            /* Set operator information. */
            auto source_info = std::make_unique<OperatorInformation>();
            source_info->subproblem = s;
            source_info->estimated_cardinality = CE.predict_cardinality(*plan_table[s].model);
            source->info(std::move(source_info));
        } else {
            /* Recursively solve nested queries. */
            auto &Q = as<const Query>(*ds);
            const bool old = std::exchange(needs_projection_, bool(Q.alias())); // aliased nested queries need projection
            auto [sub_plan, sub] = optimize(Q.query_graph());
            needs_projection_ = old;

            /* If an alias for the nested query is given, prefix every attribute with the alias. */
            if (Q.alias()) {
                M_insist(is<ProjectionOperator>(sub_plan), "only projection may rename attributes");
                Schema S;
                for (auto &e : sub_plan->schema())
                    S.add({ Q.alias(), e.id.name }, e.type, e.constraints);
                sub_plan->schema() = S;
            }

            /* Update the plan table with the `DataModel` and cost of the nested query and save the plan in the array of
             * source plans. */
            plan_table[s].cost = sub.cost;
            sub.model->assign_to(s); // adapt model s.t. it describes the result of the current subproblem
            plan_table[s].model = std::move(sub.model);
            source_plans[ds->id()] = sub_plan.release();
        }

        /* Apply filter, if any. */
        if (ds->filter().size()) {
            auto filter = std::make_unique<FilterOperator>(ds->filter());
            filter->add_child(source_plans[ds->id()]);
            filter = optimize_filter(std::move(filter));
            auto new_model = CE.estimate_filter(G, *plan_table[s].model, filter->filter());
            source_plans[ds->id()] = filter.release();
            plan_table[s].model = std::move(new_model);
        }

        /* Set operator information. */
        auto source = source_plans[ds->id()];
        auto source_info = std::make_unique<OperatorInformation>();
        source_info->subproblem = s;
        source_info->estimated_cardinality = CE.predict_cardinality(*plan_table[s].model); // includes filters, if any
        source->info(std::move(source_info));
    }

    optimize_locally(G, plan_table);
    std::unique_ptr<Producer> plan = construct_plan(G, plan_table, source_plans);
    auto &entry = plan_table.get_final();

    /* Perform grouping. */
    if (not G.group_by().empty()) {
        /* Compute `DataModel` after grouping. */
        auto new_model = CE.estimate_grouping(G, *entry.model, G.group_by()); // TODO provide aggregates
        entry.model = std::move(new_model);
        // TODO pick "best" algorithm
        auto group_by = std::make_unique<GroupingOperator>(G.group_by(), G.aggregates());
        group_by->add_child(plan.release());

        /* Set operator information. */
        auto info = std::make_unique<OperatorInformation>();
        info->subproblem = Subproblem((1UL << G.sources().size()) - 1UL);
        info->estimated_cardinality = CE.predict_cardinality(*entry.model);

        group_by->info(std::move(info));
        plan = std::move(group_by);
    } else if (not G.aggregates().empty()) {
        /* Compute `DataModel` after grouping. */
        auto new_model = CE.estimate_grouping(G, *entry.model, std::vector<GroupingOperator::group_type>());
        entry.model = std::move(new_model);
        auto agg = std::make_unique<AggregationOperator>(G.aggregates());
        agg->add_child(plan.release());

        /* Set operator information. */
        auto info = std::make_unique<OperatorInformation>();
        info->subproblem = Subproblem((1UL << G.sources().size()) - 1UL);
        info->estimated_cardinality = CE.predict_cardinality(*entry.model);

        agg->info(std::move(info));
        plan = std::move(agg);
    }

    auto additional_projections = compute_projections_required_for_order_by(G.projections(), G.order_by());
    const bool requires_post_projection = not additional_projections.empty();

    /* Perform projection. */
    if (not additional_projections.empty() or not G.projections().empty()) {
        /* Merge original projections with additional projections. */
        additional_projections.insert(additional_projections.end(), G.projections().begin(), G.projections().end());
        auto projection = std::make_unique<ProjectionOperator>(std::move(additional_projections));
        projection->add_child(plan.release());

        /* Set operator information. */
        auto info = std::make_unique<OperatorInformation>();
        info->subproblem = Subproblem((1UL << G.sources().size()) - 1UL);
        info->estimated_cardinality = projection->child(0)->info().estimated_cardinality;

        projection->info(std::move(info));
        plan = std::move(projection);
    }

    /* Perform ordering. */
    if (not G.order_by().empty()) {
        // TODO estimate data model
        auto order_by = std::make_unique<SortingOperator>(G.order_by());
        order_by->add_child(plan.release());

        /* Set operator information. */
        auto info = std::make_unique<OperatorInformation>();
        info->subproblem = Subproblem((1UL << G.sources().size()) - 1UL);
        info->estimated_cardinality = order_by->child(0)->info().estimated_cardinality;

        order_by->info(std::move(info));
        plan = std::move(order_by);
    }

    /* Limit. */
    if (G.limit().limit or G.limit().offset) {
        /* Compute `DataModel` after limit. */
        auto new_model = CE.estimate_limit(G, *entry.model, G.limit().limit, G.limit().offset);
        entry.model = std::move(new_model);
        // TODO estimate data model
        auto limit = std::make_unique<LimitOperator>(G.limit().limit, G.limit().offset);
        limit->add_child(plan.release());

        /* Set operator information. */
        auto info = std::make_unique<OperatorInformation>();
        info->subproblem = Subproblem((1UL << G.sources().size()) - 1UL);
        info->estimated_cardinality = CE.predict_cardinality(*entry.model);

        limit->info(std::move(info));
        plan = std::move(limit);
    }

    /* Perform post-ordering projection. */
    if (requires_post_projection or (not is<ProjectionOperator>(plan) and needs_projection_)) {
        // TODO estimate data model
        /* Change aliased projections in designators with the alias as name since original projection is
         * performed beforehand. */
        std::vector<projection_type> adapted_projections;
        for (auto [expr, alias] : G.projections()) {
            if (alias) {
                Token name(expr.get().tok.pos, alias, TK_IDENTIFIER);
                auto d = std::make_unique<const Designator>(Token(), Token(), name, expr.get().type(), &expr.get());
                adapted_projections.emplace_back(*d, nullptr);
                created_exprs_.emplace_back(std::move(d));
            } else {
                adapted_projections.emplace_back(expr, nullptr);
            }
        }
        auto projection = std::make_unique<ProjectionOperator>(std::move(adapted_projections));
        projection->add_child(plan.release());

        /* Set operator information. */
        auto info = std::make_unique<OperatorInformation>();
        info->subproblem = Subproblem((1UL << G.sources().size()) - 1UL);
        info->estimated_cardinality = projection->child(0)->info().estimated_cardinality;

        projection->info(std::move(info));
        plan = std::move(projection);
    }

    // std::cerr << "Plan before minimizing:\n";
    // plan->dump();
    plan->minimize_schema();
    delete[] source_plans;
    return { std::move(plan), std::move(plan_table) };
}

template<typename PlanTable>
void Optimizer::optimize_locally(const QueryGraph &G, PlanTable &PT) const
{
    Catalog &C = Catalog::Get();
    auto &DB = C.get_database_in_use();
    auto &CE = DB.cardinality_estimator();

#ifndef NDEBUG
    if (Options::Get().statistics) {
        std::size_t num_CSGs = 0, num_CCPs = 0;
        const SmallBitset All((1UL << G.num_sources()) - 1UL);
        auto inc_CSGs = [&num_CSGs](SmallBitset) { ++num_CSGs; };
        auto inc_CCPs = [&num_CCPs](SmallBitset, SmallBitset) { ++num_CCPs; };
        G.adjacency_matrix().for_each_CSG_undirected(All, inc_CSGs);
        G.adjacency_matrix().for_each_CSG_pair_undirected(All, inc_CCPs);
        std::cout << num_CSGs << " CSGs, " << num_CCPs << " CCPs" << std::endl;
    }
#endif

    M_TIME_EXPR(plan_enumerator()(G, cost_function(), PT), "Plan enumeration", C.timer());

    if (Options::Get().statistics) {
        std::cout << "Est. total cost: " << PT.get_final().cost
                  << "\nEst. result set size: " << CE.predict_cardinality(*PT.get_final().model)
                  << "\nPlan cost: " << PT[PT.get_final().left].cost + PT[PT.get_final().right].cost
                  << std::endl;
    }
}

template<typename PlanTable>
std::unique_ptr<Producer>
Optimizer::construct_plan(const QueryGraph &G, const PlanTable &plan_table, Producer * const *source_plans) const
{
    auto &C = Catalog::Get();
    auto &DB = C.get_database_in_use();
    auto &CE = DB.cardinality_estimator();

    std::vector<std::reference_wrapper<Join>> joins;
    for (auto &J : G.joins()) joins.emplace_back(*J);

    /* Use nested lambdas to implement recursive lambda using CPS. */
    const auto construct_recursive = [&](Subproblem s) -> Producer* {
        auto construct_plan_impl = [&](Subproblem s, auto &construct_plan_rec) -> Producer* {
            auto subproblems = plan_table[s].get_subproblems();
            if (subproblems.empty()) {
                M_insist(s.size() == 1);
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
                    for (auto ds : it->get().sources())
                        join_sources(ds.get().id()) = true;

                    if (join_sources.is_subset(s)) { // possible join
                        join_condition = join_condition and it->get().condition();
                        it = joins.erase(it);
                    } else {
                        ++it;
                    }
                }

                /* Construct the join. */
                auto join = new JoinOperator(join_condition);
                for (auto sub_plan : sub_plans)
                    join->add_child(sub_plan);
                auto join_info = std::make_unique<OperatorInformation>();
                join_info->subproblem = s;
                join_info->estimated_cardinality = CE.predict_cardinality(*plan_table[s].model);
                join->info(std::move(join_info));
                return join;
            }
        };
        return construct_plan_impl(s, construct_plan_impl);
    };

    return std::unique_ptr<Producer>(construct_recursive(Subproblem((1UL << G.sources().size()) - 1)));
}

std::vector<Optimizer::projection_type>
Optimizer::compute_projections_required_for_order_by(const std::vector<projection_type> &projections,
                                                     const std::vector<order_type> &order_by) const
{
    std::vector<Optimizer::projection_type> required_projections;

    /* Collect all required `Designator`s which are not included in the projection. */
    auto get_required_designator = overloaded {
        [&](const ast::Designator &d) -> void {
            auto target = d.target();
            if (auto t = std::get_if<const Expr*>(&target)) { // refers to another expression?
                /*----- Find `t` in projections. -----*/
                for (auto &[expr, alias] : projections) {
                    if (*t == &expr.get() or d == expr.get()) // refers to or is identical to an `Expr` in SELECT clause
                        return; // found
                }
            } else {
                /*----- Find `d` in projections. -----*/
                for (auto &[expr, _] : projections) {
                    if (expr.get() == d)
                        return; // found
                }
            }
            required_projections.emplace_back(d, nullptr);
        },
        [](auto&&) -> void { /* nothing to be done */ },
    };
    /* Process the ORDER BY clause. */
    for (auto [expr, _] : order_by)
        visit(get_required_designator, expr.get(), m::tag<m::ast::ConstPreOrderExprVisitor>());

    return required_projections;
}

#define DEFINE(PLANTABLE) \
template \
std::pair<std::unique_ptr<Producer>, PLANTABLE> \
Optimizer::optimize_with_plantable(const QueryGraph &G) const; \
template \
void \
Optimizer::optimize_locally(const QueryGraph &G, PLANTABLE &plan_table) const; \
template \
std::unique_ptr<Producer> \
Optimizer::construct_plan(const QueryGraph &G, const PLANTABLE &plan_table, Producer * const *source_plans) const
DEFINE(PlanTableSmallOrDense);
DEFINE(PlanTableLargeAndSparse);
#undef DEFINE

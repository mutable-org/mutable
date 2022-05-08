#include <mutable/IR/Optimizer.hpp>

#include <algorithm>
#include <mutable/catalog/Catalog.hpp>
#include <mutable/IR/Operator.hpp>
#include <mutable/IR/Operator.hpp>
#include <mutable/Options.hpp>
#include <mutable/storage/Store.hpp>
#include <mutable/storage/Store.hpp>
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
        for (auto arg : e.args)
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
    switch (Options::Get().plan_table_type)
    {
        case Options::PT_auto: {
            /* Select most suitable type of plan table depending on the query graph structure.
             * Currently a simple heuristic based on the number of data sources.
             * TODO: Consider join edges too.  Eventually consider #CSGs. */
            if (G.num_sources() <= 15) {
                auto res = optimize_with_plantable<PlanTableSmallOrDense>(G);
                return { std::move(res.first), std::move(res.second.get_final()) };
            } else {
                auto res = optimize_with_plantable<PlanTableLargeAndSparse>(G);
                return { std::move(res.first), std::move(res.second.get_final()) };
            }
        }

        case Options::PT_SmallOrDense: {
            auto res = optimize_with_plantable<PlanTableSmallOrDense>(G);
            return { std::move(res.first), std::move(res.second.get_final()) };
        }

        case Options::PT_LargeAndSparse: {
            auto res = optimize_with_plantable<PlanTableLargeAndSparse>(G);
            return { std::move(res.first), std::move(res.second.get_final()) };
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
    for (auto ds : G.sources()) {
        Subproblem s(1UL << ds->id());
        if (auto bt = cast<const BaseTable>(ds)) {
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
            auto Q = as<const Query>(ds);
            auto [sub_plan, sub] = optimize(*Q->query_graph());

            /* If an alias for the nested query is given, prefix every attribute with the alias. */
            if (Q->alias()) {
                Schema S;
                for (auto &e : sub_plan->schema())
                    S.add({Q->alias(), e.id.name}, e.type);
                sub_plan->schema() = S;
            }

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
        auto group_by = std::make_unique<GroupingOperator>(G.group_by(), G.aggregates(), GroupingOperator::G_Hashing);
        group_by->add_child(plan.release());

        /* Set operator information. */
        auto info = std::make_unique<OperatorInformation>();
        info->subproblem = Subproblem((1UL << G.sources().size()) - 1UL);
        info->estimated_cardinality = CE.predict_cardinality(*entry.model);

        group_by->info(std::move(info));
        plan = std::move(group_by);
    } else if (not G.aggregates().empty()) {
        /* Compute `DataModel` after grouping. */
        auto new_model = CE.estimate_grouping(G, *entry.model, std::vector<const Expr*>());
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

    bool additional_projection = false; // indicate whether an additional projection must be performed before ordering

    /* Perform ordering. */
    if (not G.order_by().empty()) {
        /* Perform additional projection to provide all attributes needed to perform ordering. */
        if (projection_needed(G.projections(), G.order_by())) {
            // TODO estimate data model
            additional_projection = true;
            auto projection = std::make_unique<ProjectionOperator>(G.projections()); // projection on all attributes
            projection->add_child(plan.release());
            plan = std::move(projection);
        }
        // TODO estimate data model
        auto order_by = std::make_unique<SortingOperator>(G.order_by());
        order_by->add_child(plan.release());
        plan = std::move(order_by);
    }

    /* Perform projection. */
    if (not additional_projection and not G.projections().empty()) {
        // TODO estimate data model
        auto projection = std::make_unique<ProjectionOperator>(G.projections());
        projection->add_child(plan.release());
        plan = std::move(projection);
    }

    /* Limit. */
    if (G.limit().limit or G.limit().offset) {
        /* Compute `DataModel` after limit. */
        auto new_model = CE.estimate_limit(G, *entry.model, G.limit().limit, G.limit().offset);
        entry.model = std::move(new_model);
        // TODO estimate data model
        auto limit = std::make_unique<LimitOperator>(G.limit().limit, G.limit().offset);
        limit->add_child(plan.release());
        plan = std::move(limit);
    }

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
    M_TIME_EXPR(plan_enumerator()(G, cost_function(), PT), "Plan enumeration", C.timer());
// #ifdef NDEBUG
    std::cout << "Est. total cost: " << PT.get_final().cost
              << "\nEst. result set size: " << CE.predict_cardinality(*PT.get_final().model)
              << "\nPlan cost: " << PT[PT.get_final().left].cost + PT[PT.get_final().right].cost
              << std::endl;
// #endif
}

template<typename PlanTable>
std::unique_ptr<Producer>
Optimizer::construct_plan(const QueryGraph &G, const PlanTable &plan_table, Producer * const *source_plans) const
{
    auto &C = Catalog::Get();
    auto &DB = C.get_database_in_use();
    auto &CE = DB.cardinality_estimator();

    auto joins = G.joins(); // create a "set" of all joins

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
                    for (auto ds : (*it)->sources())
                        join_sources(ds->id()) = true;

                    if (join_sources.is_subset(s)) { // possible join
                        join_condition = join_condition and (*it)->condition();
                        it = joins.erase(it);
                    } else {
                        ++it;
                    }
                }

                /* Construct the join. */
                JoinOperator *join;
                if (sub_plans.size() == 2 and is_equi_join(join_condition)) {
                    join = new JoinOperator(join_condition, JoinOperator::J_SimpleHashJoin);
                    for (auto sub_plan : sub_plans)
                        join->add_child(sub_plan);
                } else {
                    join = new JoinOperator(join_condition, JoinOperator::J_NestedLoops);
                    for (auto sub_plan : sub_plans)
                        join->add_child(sub_plan);
                }
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

bool Optimizer::projection_needed(const std::vector<projection_type> &projections,
                                  const std::vector<order_type> &order_by) const
{
    struct GetDesignatorTargets : ConstASTExprVisitor
    {
        private:
        std::vector<std::pair<const Designator*, const Expr*>> designator_targets;

        public:
        GetDesignatorTargets() { }

        std::vector<std::pair<const Designator*, const Expr*>> get() { return std::move(designator_targets); }

        void compute(const std::vector<order_type> &order_by) {
            for (auto &p : order_by)
                (*this)(*p.first);
        }

        private:
        using ConstASTExprVisitor::operator();
        void operator()(Const<ErrorExpr>&) override { M_unreachable("order by must not contain errors"); }
        void operator()(Const<Designator> &e) override {
            if (not e.table_name.text) {
                auto target = e.target();
                if (auto t = std::get_if<const Expr*>(&target))
                    designator_targets.emplace_back(&e, *t);
            }
        }
        void operator()(Const<Constant>&) override { /* nothing to be done */ }
        void operator()(Const<FnApplicationExpr>&) override { M_unreachable("unexpected FnApplicationExpr"); }
        void operator()(Const<UnaryExpr> &e) override { (*this)(*e.expr); }
        void operator()(Const<BinaryExpr> &e) override { (*this)(*e.lhs); (*this)(*e.rhs); }
        void operator()(Const<QueryExpr>&) override { M_unreachable("unexpected QueryExpr"); }
    };

    /* Compute all expression targets of designators without table name in `order_by`. */
    GetDesignatorTargets GDT;
    GDT.compute(order_by);
    auto designator_targets = GDT.get();

    /* Check if there is an element of `projections` which is needed by `order_by`, i.e. which is target and creates
     * the needed attribute name by renaming. */
    for (auto &p : projections) {
        auto pred = [&p](const std::pair<const Designator*, const Expr*> &des_target) -> bool {
            return des_target.second == p.first and des_target.first->attr_name.text == p.second;
        };
        if (std::find_if(designator_targets.begin(), designator_targets.end(), pred) != designator_targets.end())
            return true;
    }

    return false;
}

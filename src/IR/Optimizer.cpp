#include <mutable/IR/Optimizer.hpp>

#include <algorithm>
#include <mutable/catalog/Catalog.hpp>
#include <mutable/IR/Operator.hpp>
#include <mutable/Options.hpp>
#include <mutable/parse/AST.hpp>
#include <mutable/storage/Store.hpp>
#include <numeric>
#include <vector>


using namespace m;
using namespace m::ast;


/*======================================================================================================================
 * Helper functions
 *====================================================================================================================*/

struct WeighExpr
{
    private:
    unsigned weight_ = 0;

    public:
    operator unsigned() const { return weight_; }

    void operator()(const cnf::CNF &cnf) {
        for (auto &clause : cnf)
            (*this)(clause);
    }

    void operator()(const cnf::Clause &clause) {
        for (auto pred : clause)
            (*this)(*pred);
    }

    void operator()(const ast::Expr &e) {
        visit(overloaded {
            [](const ErrorExpr&) { M_unreachable("no errors at this stage"); },
            [this](const Designator &d) {
                if (auto cs = cast<const CharacterSequence>(d.type()))
                    weight_ += cs->length;
                else
                    weight_ += 1;
            },
            [this](const Constant &e) {
                if (auto cs = cast<const CharacterSequence>(e.type()))
                    weight_ += cs->length;
                // fixed-size constants are considered free, as they may be encoded as immediate constants in the instr
            },
            [this](const FnApplicationExpr&) {
                weight_ += 1;
            },
            [this](const UnaryExpr&) { weight_ += 1; },
            [this](const BinaryExpr&) { weight_ += 1; },
            [this](const QueryExpr&) { weight_ += 1000; } // XXX: this should never happen because of unnesting
        }, e, tag<ConstPreOrderExprVisitor>{});
    }
};

std::vector<cnf::CNF> Optimizer::optimize_filter(cnf::CNF filter)
{
    constexpr unsigned MAX_WEIGHT = 12; // equals to 4 comparisons of fixed-length values
    M_insist(not filter.empty());

    /* Compute clause weights. */
    std::vector<unsigned> clause_weights;
    clause_weights.reserve(filter.size());
    for (auto &clause : filter) {
        WeighExpr W;
        W(clause);
        clause_weights.emplace_back(W);
    }

    /* Sort clauses by weight using an index vector. */
    std::vector<std::size_t> order(filter.size(), 0);
    std::iota(order.begin(), order.end(), 0);
    std::sort(order.begin(), order.end(), [&clause_weights](std::size_t first, std::size_t second) -> bool {
        return clause_weights[first] < clause_weights[second];
    });

    /* Dissect filter into sequence of filters. */
    std::vector<cnf::CNF> optimized_filters;
    unsigned current_weight = 0;
    cnf::CNF current_filter;
    for (std::size_t i = 0, end = filter.size(); i != end; ++i) {
        const std::size_t idx = order[i];
        cnf::Clause clause(std::move(filter[idx]));
        M_insist(not clause.empty());
        const unsigned clause_weight = clause_weights[idx];

        if (not current_filter.empty() and current_weight + clause_weight > MAX_WEIGHT) {
            optimized_filters.emplace_back(std::move(current_filter)); // empties current_filter
            current_weight = 0;
        }

        current_filter.emplace_back(std::move(clause));
        current_weight += clause_weight;
    }
    if (not current_filter.empty())
        optimized_filters.emplace_back(std::move(current_filter));

    M_insist(not optimized_filters.empty());
    return optimized_filters;
}

std::vector<Optimizer::projection_type>
Optimizer::compute_projections_required_for_order_by(const std::vector<projection_type> &projections,
                                                     const std::vector<order_type> &order_by)
{
    std::vector<Optimizer::projection_type> required_projections;

    /* Collect all required `Designator`s which are not included in the projection. */
    auto get_required_designator = overloaded {
        [&](const ast::Designator &d) -> void {
            if (auto t = std::get_if<const Expr*>(&d.target())) { // refers to another expression?
                /*----- Find `t` in projections. -----*/
                for (auto &[expr, _] : projections) {
                    if (*t == &expr.get())
                        return; // found
                }
            }
            /*----- Find `d` in projections. -----*/
            for (auto &[expr, alias] : projections) {
                if (not alias.has_value() and d == expr.get())
                    return; // found
            }
            required_projections.emplace_back(d, ThreadSafePooledOptionalString{});
        },
        [&](const ast::FnApplicationExpr &fn) -> void {
            /*----- Find `fn` in projections. -----*/
            for (auto &[expr, alias] : projections) {
                if (not alias.has_value() and fn == expr.get())
                    throw visit_skip_subtree(); // found
            }
            required_projections.emplace_back(fn, ThreadSafePooledOptionalString{});
            throw visit_skip_subtree();
        },
        [](auto&&) -> void { /* nothing to be done */ },
    };
    /* Process the ORDER BY clause. */
    for (auto [expr, _] : order_by)
        visit(get_required_designator, expr.get(), m::tag<m::ast::ConstPreOrderExprVisitor>());

    return required_projections;
}


/*======================================================================================================================
 * Optimizer
 *====================================================================================================================*/

std::pair<std::unique_ptr<Producer>, PlanTableEntry> Optimizer::optimize(QueryGraph &G) const
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
std::pair<std::unique_ptr<Producer>, PlanTable> Optimizer::optimize_with_plantable(QueryGraph &G) const
{
    PlanTable PT(G);
    const auto num_sources = G.sources().size();
    auto &C = Catalog::Get();
    auto &CE = C.get_database_in_use().cardinality_estimator();

    if (num_sources == 0) {
        PT.get_final().cost = 0; // no sources → no cost
        PT.get_final().model = CE.empty_model(); // XXX: should rather be 1 (single tuple) than empty
        return { std::make_unique<ProjectionOperator>(G.projections()), std::move(PT) };
    }

    /*----- Initialize plan table and compute plans for data sources. -----*/
    auto source_plans = optimize_source_plans(G, PT);

    /*----- Compute join order and construct plan containing all joins. -----*/
    optimize_join_order(G, PT);
    std::unique_ptr<Producer> plan = construct_join_order(G, PT, source_plans);
    auto &entry = PT.get_final();

    /*----- Construct plan for remaining operations. -----*/
    plan = optimize_plan(G, std::move(plan), entry);

    // here we have the final logical plan!
    return { std::move(plan), std::move(PT) };
}

template<typename PlanTable>
std::unique_ptr<Producer*[]> Optimizer::optimize_source_plans(const QueryGraph &G, PlanTable &PT) const
{
    const auto num_sources = G.sources().size();
    auto &CE = Catalog::Get().get_database_in_use().cardinality_estimator();

    auto source_plans = std::make_unique<Producer*[]>(num_sources);
    for (auto &ds : G.sources()) {
        Subproblem s = Subproblem::Singleton(ds->id());
        if (auto bt = cast<const BaseTable>(ds.get())) {
            /* Produce a scan for base tables. */
            PT[s].cost = 0;
            PT[s].model = CE.estimate_scan(G, s);
            auto &store = bt->table().store();
            auto source = std::make_unique<ScanOperator>(store, bt->name().assert_not_none());

            /* Set operator information. */
            auto source_info = std::make_unique<OperatorInformation>();
            source_info->subproblem = s;
            source_info->estimated_cardinality = CE.predict_cardinality(*PT[s].model);
            source->info(std::move(source_info));

            source_plans[ds->id()] = source.release();
        } else {
            /* Recursively solve nested queries. */
            auto &Q = as<const Query>(*ds);
            const bool old = std::exchange(needs_projection_, Q.alias().has_value()); // aliased nested queries need projection
            auto [sub_plan, sub] = optimize(Q.query_graph());
            needs_projection_ = old;

            /* If an alias for the nested query is given, prefix every attribute with the alias. */
            if (Q.alias().has_value()) {
                M_insist(is<ProjectionOperator>(sub_plan), "only projection may rename attributes");
                Schema S;
                for (auto &e : sub_plan->schema())
                    S.add({ Q.alias(), e.id.name }, e.type, e.constraints);
                sub_plan->schema() = S;
            }

            /* Update the plan table with the `DataModel` and cost of the nested query and save the plan in the array of
             * source plans. */
            PT[s].cost = sub.cost;
            sub.model->assign_to(s); // adapt model s.t. it describes the result of the current subproblem
            PT[s].model = std::move(sub.model);
            source_plans[ds->id()] = sub_plan.release();
        }

        /* Apply filter, if any. */
        if (ds->filter().size()) {
            /* Optimize the filter by splitting into smaller filters and ordering them. */
            std::vector<cnf::CNF> filters = Optimizer::optimize_filter(ds->filter());
            Producer *filtered_ds = source_plans[ds->id()];

            /* Construct a plan as a sequence of filters. */
            for (auto &&filter : filters) {
                /* Update data model with filter. */
                auto new_model = CE.estimate_filter(G, *PT[s].model, filter);
                PT[s].model = std::move(new_model);

                if (filter.size() == 1 and filter[0].size() > 1) { // disjunctive filter
                    auto tmp = std::make_unique<DisjunctiveFilterOperator>(std::move(filter));
                    tmp->add_child(filtered_ds);
                    filtered_ds = tmp.release();
                } else {
                    auto tmp = std::make_unique<FilterOperator>(std::move(filter));
                    tmp->add_child(filtered_ds);
                    filtered_ds = tmp.release();
                }

                /* Set operator information. */
                auto source_info = std::make_unique<OperatorInformation>();
                source_info->subproblem = s;
                source_info->estimated_cardinality = CE.predict_cardinality(*PT[s].model); // includes filters, if any
                filtered_ds->info(std::move(source_info));
            }

            source_plans[ds->id()] = filtered_ds;
        }
    }
    return source_plans;
}

template<typename PlanTable>
void Optimizer::optimize_join_order(const QueryGraph &G, PlanTable &PT) const
{
    Catalog &C = Catalog::Get();
    auto &CE = C.get_database_in_use().cardinality_estimator();

#ifndef NDEBUG
    if (Options::Get().statistics) {
        std::size_t num_CSGs = 0, num_CCPs = 0;
        const Subproblem All = Subproblem::All(G.num_sources());
        auto inc_CSGs = [&num_CSGs](Subproblem) { ++num_CSGs; };
        auto inc_CCPs = [&num_CCPs](Subproblem, Subproblem) { ++num_CCPs; };
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
std::unique_ptr<Producer> Optimizer::construct_join_order(const QueryGraph &G, const PlanTable &PT,
                                                          const std::unique_ptr<Producer*[]> &source_plans) const
{
    auto &CE = Catalog::Get().get_database_in_use().cardinality_estimator();

    std::vector<std::reference_wrapper<Join>> joins;
    for (auto &J : G.joins()) joins.emplace_back(*J);

    /* Use nested lambdas to implement recursive lambda using CPS. */
    const auto construct_recursive = [&](Subproblem s) -> Producer* {
        auto construct_plan_impl = [&](Subproblem s, auto &construct_plan_rec) -> Producer* {
            auto subproblems = PT[s].get_subproblems();
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
                auto join = std::make_unique<JoinOperator>(join_condition);
                for (auto sub_plan : sub_plans)
                    join->add_child(sub_plan);
                auto join_info = std::make_unique<OperatorInformation>();
                join_info->subproblem = s;
                join_info->estimated_cardinality = CE.predict_cardinality(*PT[s].model);
                join->info(std::move(join_info));
                return join.release();
            }
        };
        return construct_plan_impl(s, construct_plan_impl);
    };

    return std::unique_ptr<Producer>(construct_recursive(Subproblem::All(G.sources().size())));
}

std::unique_ptr<Producer> Optimizer::optimize_plan(const QueryGraph &G, std::unique_ptr<Producer> plan,
                                                   PlanTableEntry &entry) const
{
    auto &CE = Catalog::Get().get_database_in_use().cardinality_estimator();

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
        info->subproblem = Subproblem::All(G.sources().size());
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
        info->subproblem = Subproblem::All(G.sources().size());
        info->estimated_cardinality = CE.predict_cardinality(*entry.model);

        agg->info(std::move(info));
        plan = std::move(agg);
    }

    auto additional_projections = Optimizer::compute_projections_required_for_order_by(G.projections(), G.order_by());
    const bool requires_post_projection = not additional_projections.empty();

    /* Perform projection. */
    if (not additional_projections.empty() or not G.projections().empty()) {
        /* Merge original projections with additional projections. */
        additional_projections.insert(additional_projections.end(), G.projections().begin(), G.projections().end());
        auto projection = std::make_unique<ProjectionOperator>(std::move(additional_projections));
        projection->add_child(plan.release());

        /* Set operator information. */
        auto info = std::make_unique<OperatorInformation>();
        info->subproblem = Subproblem::All(G.sources().size());
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
        info->subproblem = Subproblem::All(G.sources().size());
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
        info->subproblem = Subproblem::All(G.sources().size());
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
            if (alias.has_value()) {
                Token name(expr.get().tok.pos, alias.assert_not_none(), TK_IDENTIFIER);
                auto d = std::make_unique<const Designator>(Token::CreateArtificial(), Token::CreateArtificial(),
                                                            std::move(name), expr.get().type(), &expr.get());
                adapted_projections.emplace_back(*d, ThreadSafePooledOptionalString{});
                created_exprs_.emplace_back(std::move(d));
            } else {
                adapted_projections.emplace_back(expr, ThreadSafePooledOptionalString{});
            }
        }
        auto projection = std::make_unique<ProjectionOperator>(std::move(adapted_projections));
        projection->add_child(plan.release());

        /* Set operator information. */
        auto info = std::make_unique<OperatorInformation>();
        info->subproblem = Subproblem::All(G.sources().size());
        info->estimated_cardinality = projection->child(0)->info().estimated_cardinality;

        projection->info(std::move(info));
        plan = std::move(projection);
    }
    return plan;
}

#define DEFINE(PLANTABLE) \
template \
std::pair<std::unique_ptr<Producer>, PLANTABLE> \
Optimizer::optimize_with_plantable(QueryGraph&) const; \
template \
std::unique_ptr<Producer*[]> \
Optimizer::optimize_source_plans(const QueryGraph&, PLANTABLE&) const; \
template \
void \
Optimizer::optimize_join_order(const QueryGraph&, PLANTABLE&) const; \
template \
std::unique_ptr<Producer> \
Optimizer::construct_join_order(const QueryGraph&, const PLANTABLE&, const std::unique_ptr<Producer*[]>&) const
DEFINE(PlanTableSmallOrDense);
DEFINE(PlanTableLargeAndSparse);
#undef DEFINE

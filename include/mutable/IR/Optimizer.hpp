#pragma once

#include <mutable/mutable-config.hpp>
#include <mutable/catalog/CostFunction.hpp>
#include <mutable/IR/PlanEnumerator.hpp>
#include <mutable/IR/PlanTable.hpp>


namespace m {

/** The optimizer interface.
 *
 * The `Optimizer` applies a join ordering algorithm to a query graph to compute a join order that minimizes the costs
 * under a given logical cost function.
 * Additionally, the optimizer may apply several semantics preserving transformations to improve performance.  Such
 * transformations include query unnesting and predicate inference.
 */
struct M_EXPORT Optimizer
{
    using Subproblem = QueryGraph::Subproblem;
    using projection_type = QueryGraph::projection_type;
    using order_type = QueryGraph::order_type;

    private:
    const pe::PlanEnumerator &pe_;
    const CostFunction &cf_;
    mutable std::vector<std::unique_ptr<const ast::Expr>> created_exprs_; ///< additionally created expressions
    mutable bool needs_projection_ = false; ///< flag to determine whether current query needs a projection as root

    public:
    Optimizer(const pe::PlanEnumerator &pe, const CostFunction &cf) : pe_(pe), cf_(cf) { }

    auto & plan_enumerator() const { return pe_; }
    auto & cost_function() const { return cf_; }

    /** Applies this optimizer to the given query graph \p G to compute an optimal logical operator tree. */
    std::unique_ptr<Producer> operator()(QueryGraph &G) const { return optimize(G).first; }

    /** Computes and constructs an optimal logical plan for the given query graph \p G.  Selects a `PlanTableBase`
     * type to represent the internal state of planning progress, then delegates to `optimize_with_plantable<>()`. */
    std::pair<std::unique_ptr<Producer>, PlanTableEntry> optimize(QueryGraph &G) const;

    /** Recursively computes and constructs an optimal logical plan for the given query graph \p G, using the given
     * \tparam PlanTable type to represent the state of planning progress. */
    template<typename PlanTable>
    std::pair<std::unique_ptr<Producer>, PlanTable> optimize_with_plantable(QueryGraph &G) const;

    private:
    /** Initializes the plan table \p PT with the data source entries contained in \p G.  Returns the
     * (potentially recursively optimized) logical plan for each data source. */
    template<typename PlanTable>
    std::unique_ptr<Producer*[]> optimize_source_plans(const QueryGraph &G, PlanTable &PT) const;

    /** Optimizes the join order using the plan table \p PT which already contains entries for all data sources of
     * the query graph \p G. */
    template<typename PlanTable>
    void optimize_join_order(const QueryGraph &G, PlanTable &PT) const;

    /** Constructs a join operator tree given a solved plan table \p PT and the plans to compute the data sources
     * \p source_plans of the query graph \p G. */
    template<typename PlanTable>
    std::unique_ptr<Producer> construct_join_order(const QueryGraph &G, const PlanTable &PT,
                                                   const std::unique_ptr<Producer*[]> &source_plans) const;

    /** Optimizes and constructs an operator tree given a join operator tree \p plan and the final plan table entry
     * \p entry for the query graph \p G. */
    std::unique_ptr<Producer> optimize_plan(const QueryGraph &G, std::unique_ptr<Producer> plan,
                                            PlanTableEntry &entry) const;

    /** Optimizes the filter \p filter by splitting it into smaller filters and ordering them. */
    static std::vector<cnf::CNF> optimize_filter(cnf::CNF filter);

    /** Computes and returns a `std::vector` of additional projections required *before* evaluating the ORDER BY clause.
     * The returned `std::vector` may be empty, in which case *no* additional projection is required. */
    static std::vector<projection_type>
    compute_projections_required_for_order_by(const std::vector<projection_type> &projections,
                                              const std::vector<order_type> &order_by);
};

}

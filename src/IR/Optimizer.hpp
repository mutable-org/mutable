#pragma once

#include "mutable/IR/Optimizer.hpp"


namespace m {

/** The optimizer interface.
 *
 * The optimizer applies a join ordering algorithm to a query graph to compute a join order that minimizes the costs
 * under a given cost function.
 * Additionally, the optimizer may apply several semantics preserving transformations to improve performance.  Such
 * transformations include query unnesting and predicate inference.
 */
struct Optimizer
{
    using Subproblem = QueryGraph::Subproblem;

    private:
    const PlanEnumerator &pe_;
    const CostFunction &cf_;

    public:
    Optimizer(const PlanEnumerator &pe, const CostFunction &cf) : pe_(pe), cf_(cf) { }

    auto & plan_enumerator() const { return pe_; };
    auto & cost_function() const { return cf_; }

    /** Apply this optimizer to the given query graph to compute an operator tree. */
    std::unique_ptr<Producer> operator()(const QueryGraph &G) const;

    /** Recursively computes and constructs an optimial plan for the given query graph.  */
    std::pair<std::unique_ptr<Producer>, PlanTable> optimize(const QueryGraph &G) const;

    /** Optimizes a plan table after initialization of the data source entries. */
    void optimize_locally(const QueryGraph &G, PlanTable &plan_table) const;

    /** Constructs an operator tree given a solved plan table and the plans to compute the data sources of the query. */
    std::unique_ptr<Producer> construct_plan(const QueryGraph &G, PlanTable &plan_table, Producer **source_plans) const;
};

}

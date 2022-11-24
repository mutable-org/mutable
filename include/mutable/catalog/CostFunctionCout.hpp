#pragma once

#include <mutable/catalog/CardinalityEstimator.hpp>
#include <mutable/catalog/CostFunction.hpp>
#include <mutable/IR/PlanTable.hpp>


namespace m {

/** Implements the cassical, widely used cost function *C_out* from Sophie Cluet and Guido Moerkotte. "On the
 * complexity of generating optimal left-deep processing trees with cross products." 1995.
 * *C_out* provides the *adjacent sequence interchange* (ASI) property.
 */
struct CostFunctionCout : CostFunctionCRTP<CostFunctionCout>
{
    template<typename PlanTable>
    double operator()(calculate_filter_cost_tag, const PlanTable &PT, const QueryGraph &G,
                      const CardinalityEstimator &CE, Subproblem sub, const cnf::CNF &condition) const
    {
        return double(CE.predict_cardinality(*PT[sub].model)) + PT[sub].cost;
    }

    template<typename PlanTable>
    double operator()(calculate_join_cost_tag, const PlanTable &PT, const QueryGraph &G, const CardinalityEstimator &CE,
                      Subproblem left, Subproblem right, const cnf::CNF &condition) const
    {
        return /* |T| =        */ double(CE.predict_cardinality(*PT[left|right].model)) +
               /* C_out(T_1) = */ PT[left].cost +
               /* C_out(T_2) = */ PT[right].cost;
    }

    template<typename PlanTable>
    double operator()(calculate_grouping_cost_tag, const PlanTable &PT, const QueryGraph &G,
                      const CardinalityEstimator &CE, Subproblem sub,
                      const std::vector<const ast::Expr*> &group_by) const
    {
        return double(CE.predict_cardinality(*PT[sub].model)) + PT[sub].cost;
    }
};

}

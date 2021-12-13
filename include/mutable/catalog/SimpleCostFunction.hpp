#pragma once

#include <mutable/catalog/CardinalityEstimator.hpp>
#include <mutable/catalog/CostFunction.hpp>


namespace m {

struct SimpleCostFunction : CostFunctionCRTP<SimpleCostFunction>
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
        return double(CE.predict_cardinality(*PT[left].model)) +
               double(CE.predict_cardinality(*PT[right].model)) +
               PT[left].cost + PT[right].cost;
    }

    template<typename PlanTable>
    double operator()(calculate_grouping_cost_tag, const PlanTable &PT, const QueryGraph &G,
                      const CardinalityEstimator &CE, Subproblem sub,
                      const std::vector<const Expr*> &group_by) const
    {
        return double(CE.predict_cardinality(*PT[sub].model)) + PT[sub].cost;
    }
};

}

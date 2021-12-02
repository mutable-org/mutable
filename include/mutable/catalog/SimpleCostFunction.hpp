#pragma once

#include "mutable/catalog/CostFunction.hpp"


namespace m {

struct SimpleCostFunction : CostFunction
{
    double calculate_filter_cost(const QueryGraph &G, const PlanTable &PT, const CardinalityEstimator &CE,
                                 const Subproblem &sub, const cnf::CNF &condition) const override;

    double calculate_join_cost(const QueryGraph &G, const PlanTable &PT, const CardinalityEstimator &CE,
                               const Subproblem &left, const Subproblem &right,
                               const cnf::CNF &condition) const override;

    double calculate_grouping_cost(const QueryGraph &G, const PlanTable &PT, const CardinalityEstimator &CE,
                                   const Subproblem &sub, const std::vector<const Expr*> &group_by) const override;
};

}

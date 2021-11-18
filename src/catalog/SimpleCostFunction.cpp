#include "mutable/catalog/SimpleCostFunction.hpp"

#include "mutable/IR/PlanTable.hpp"


using namespace m;


double SimpleCostFunction::calculate_filter_cost(const PlanTable &PT, const CardinalityEstimator &CE,
                                                 const Subproblem &sub, const cnf::CNF&) const
{
    return double(CE.predict_cardinality(*PT[sub].model)) + PT[sub].cost;
}

double SimpleCostFunction::calculate_join_cost(const PlanTable &PT, const CardinalityEstimator &CE,
                                               const Subproblem &left, const Subproblem &right, const cnf::CNF&) const
{
    return double(CE.predict_cardinality(*PT[left].model)) + double(CE.predict_cardinality(*PT[right].model)) +
           PT[left].cost + PT[right].cost;
}

double SimpleCostFunction::calculate_grouping_cost(const PlanTable &PT, const CardinalityEstimator &CE,
                                                   const Subproblem &sub, std::vector<const Expr*> &group_by) const
{
    return double(CE.predict_cardinality(*PT[sub].model)) + PT[sub].cost;
}

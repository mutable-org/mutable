#include <mutable/catalog/SimpleCostFunction.hpp>

#include <mutable/IR/PlanTable.hpp>


using namespace m;


double SimpleCostFunction::calculate_filter_cost(const QueryGraph&, const PlanTable &PT,
                                                 const CardinalityEstimator &CE, const Subproblem &sub,
                                                 const cnf::CNF&) const
{
    return double(CE.predict_cardinality(*PT[sub].model)) + PT[sub].cost;
}

double SimpleCostFunction::calculate_join_cost(const QueryGraph&, const PlanTable &PT, const CardinalityEstimator &CE,
                                               const Subproblem &left, const Subproblem &right, const cnf::CNF&) const
{
    return double(CE.predict_cardinality(*PT[left].model)) + double(CE.predict_cardinality(*PT[right].model)) +
           PT[left].cost + PT[right].cost;
}

double SimpleCostFunction::calculate_grouping_cost(const QueryGraph&, const PlanTable &PT,
                                                   const CardinalityEstimator &CE, const Subproblem &sub,
                                                   const std::vector<const Expr*>&) const
{
    return double(CE.predict_cardinality(*PT[sub].model)) + PT[sub].cost;
}

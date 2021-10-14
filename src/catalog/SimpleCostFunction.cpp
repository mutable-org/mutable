#include "mutable/catalog/SimpleCostFunction.hpp"

#include "mutable/IR/PlanTable.hpp"


double m::SimpleCostFunction::calculate_filter_cost(const m::PlanTable &PT, const m::Subproblem &sub,
                                                    const m::cnf::CNF&) const
{
    auto &CE = Catalog::Get().get_database_in_use().cardinality_estimator();
    return double(CE.predict_cardinality(*PT[sub].model)) + double(PT[sub].cost);
}

double m::SimpleCostFunction::calculate_join_cost(const m::PlanTable &PT, const m::Subproblem &left,
                                                  const m::Subproblem &right, const m::cnf::CNF&) const
{
    auto &CE = Catalog::Get().get_database_in_use().cardinality_estimator();
    return double(CE.predict_cardinality(*PT[left].model)) + double(CE.predict_cardinality(*PT[right].model))
        + double(PT[left].cost) + double(PT[right].cost);
}

double m::SimpleCostFunction::calculate_grouping_cost(const m::PlanTable &PT, const m::Subproblem &sub,
                                                      std::vector<const Expr*> &group_by) const
{
    auto &CE = Catalog::Get().get_database_in_use().cardinality_estimator();
    return double(CE.predict_cardinality(*PT[sub].model)) + double(PT[sub].cost);
}

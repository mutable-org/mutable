#include <mutable/catalog/TrainedCostFunction.hpp>

#include <mutable/catalog/CardinalityEstimator.hpp>
#include <mutable/IR/PlanTable.hpp>


using namespace m;
using namespace m::ast;


template<typename PlanTable>
double TrainedCostFunction::operator()(calculate_filter_cost_tag, PlanTable &&PT, const QueryGraph &G,
                                       const CardinalityEstimator &CE, Subproblem sub, const cnf::CNF &condition) const
{
    auto cardinality = CE.predict_cardinality(*PT[sub].model);
    auto post_filter = CE.estimate_filter(G, *PT[sub].model, condition);
    auto result_size = CE.predict_cardinality(*post_filter);
    M_insist(cardinality > 0);
    auto selectivity = double(result_size) / double(cardinality);

    Eigen::RowVectorXd feature_matrix(3);
    feature_matrix << 1, // add 1 for y-intercept coefficient
            cardinality,
            selectivity;

    return filter_model_->predict_target(feature_matrix);
}

template<typename PlanTable>
double TrainedCostFunction::operator()(calculate_join_cost_tag, PlanTable &&PT, const QueryGraph &G,
                                       const CardinalityEstimator &CE, Subproblem left, Subproblem right,
                                       const cnf::CNF &condition) const
{
    auto cardinality_left = CE.predict_cardinality(*PT[left].model);
    auto cardinality_right = CE.predict_cardinality(*PT[right].model);
    auto num_distinct_values_left = CE.predict_number_distinct_values(*PT[left].model);
    auto num_distinct_values_right = CE.predict_number_distinct_values(*PT[right].model);
    M_insist(cardinality_left > 0 and cardinality_right > 0);
    auto redundancy_left = cardinality_left / num_distinct_values_left;
    auto redundancy_right = cardinality_right / num_distinct_values_right;
    // TODO before calculating the model for the join result, check whether we already have that model in the plan table
    // to avoid recalculating it
    auto post_join = CE.estimate_join(G, *PT[left].model, *PT[right].model, condition);
    auto result_size = CE.predict_cardinality(*post_join);

    Eigen::RowVectorXd feature_matrix(6);
    feature_matrix << 1,    // add 1 for y-intercept coefficient
            cardinality_left,
            cardinality_right,
            redundancy_left,
            redundancy_right,
            result_size;
    return join_model_->predict_target(feature_matrix);
}

template<typename PlanTable>
double TrainedCostFunction::operator()(calculate_grouping_cost_tag, PlanTable &&PT, const QueryGraph&,
                                       const CardinalityEstimator &CE, Subproblem sub,
                                       const std::vector<const Expr*>&) const
{
    Eigen::RowVectorXd feature_matrix(3);
    feature_matrix << 1, // add 1 for y-intercept coefficient
            CE.predict_cardinality(*PT[sub].model),
            CE.predict_number_distinct_values(*PT[sub].model);
    return grouping_model_->predict_target(feature_matrix);
}

template
double TrainedCostFunction::operator()<const PlanTableSmallOrDense&>(calculate_filter_cost_tag, const PlanTableSmallOrDense &PT, const QueryGraph &G,
                                       const CardinalityEstimator &CE, Subproblem sub, const cnf::CNF &condition) const;
template
double TrainedCostFunction::operator()<const PlanTableLargeAndSparse&>(calculate_filter_cost_tag, const PlanTableLargeAndSparse &PT, const QueryGraph &G,
                                       const CardinalityEstimator &CE, Subproblem sub, const cnf::CNF &condition) const;


template
double TrainedCostFunction::operator()<const PlanTableSmallOrDense&>(calculate_join_cost_tag, const PlanTableSmallOrDense &PT, const QueryGraph &G,
                                       const CardinalityEstimator &CE, Subproblem left, Subproblem right,
                                       const cnf::CNF &condition) const;
template
double TrainedCostFunction::operator()<const PlanTableLargeAndSparse&>(calculate_join_cost_tag, const PlanTableLargeAndSparse &PT, const QueryGraph &G,
                                       const CardinalityEstimator &CE, Subproblem left, Subproblem right,
                                       const cnf::CNF &condition) const;

template
double TrainedCostFunction::operator()<const PlanTableSmallOrDense&>(calculate_grouping_cost_tag, const PlanTableSmallOrDense &PT, const QueryGraph&,
                                       const CardinalityEstimator &CE, Subproblem sub,
                                       const std::vector<const Expr*>&) const;
template
double TrainedCostFunction::operator()<const PlanTableLargeAndSparse&>(calculate_grouping_cost_tag, const PlanTableLargeAndSparse &PT, const QueryGraph&,
                                       const CardinalityEstimator &CE, Subproblem sub,
                                       const std::vector<const Expr*>&) const;

#pragma once

#include <mutable/catalog/CostFunction.hpp>
#include <mutable/catalog/CostModel.hpp>


namespace m {

struct TrainedCostFunction : CostFunction
{
    private:
    std::unique_ptr<CostModel> filter_model_;
    std::unique_ptr<CostModel> join_model_;
    std::unique_ptr<CostModel> grouping_model_;

    public:
    TrainedCostFunction(std::unique_ptr<CostModel> filter_model, std::unique_ptr<CostModel> join_model,
                        std::unique_ptr<CostModel> grouping_model)
        : CostFunction(), filter_model_(std::move(filter_model)), join_model_(std::move(join_model))
        , grouping_model_(std::move(grouping_model)) {}

    double calculate_filter_cost(const QueryGraph &G, const PlanTable &PT, const CardinalityEstimator &CE,
                                 const Subproblem &sub, const cnf::CNF &condition) const override;

    double calculate_join_cost(const QueryGraph &G, const PlanTable &PT, const CardinalityEstimator &CE,
                               const Subproblem &left, const Subproblem &right,
                               const cnf::CNF &condition) const override;

    double calculate_grouping_cost(const QueryGraph &G, const PlanTable &PT, const CardinalityEstimator &CE,
                                   const Subproblem &sub, const std::vector<const Expr*> &group_by) const override;
};

}

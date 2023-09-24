#pragma once

#include <mutable/catalog/CostFunction.hpp>
#include <mutable/catalog/CostModel.hpp>


namespace m {

struct TrainedCostFunction : CostFunctionCRTP<TrainedCostFunction>
{
    private:
    std::unique_ptr<CostModel> filter_model_;
    std::unique_ptr<CostModel> join_model_;
    std::unique_ptr<CostModel> grouping_model_;

    public:
    TrainedCostFunction(std::unique_ptr<CostModel> filter_model, std::unique_ptr<CostModel> join_model,
                        std::unique_ptr<CostModel> grouping_model)
        : filter_model_(std::move(filter_model))
        , join_model_(std::move(join_model))
        , grouping_model_(std::move(grouping_model)) {}

    template<typename PlanTable>
    double operator()(calculate_filter_cost_tag, PlanTable &&PT, const QueryGraph &G,
                      const CardinalityEstimator &CE, Subproblem sub, const cnf::CNF &condition) const;

    template<typename PlanTable>
    double operator()(calculate_join_cost_tag, PlanTable &&PT, const QueryGraph &G, const CardinalityEstimator &CE,
                      Subproblem left, Subproblem right, const cnf::CNF &condition) const;

    template<typename PlanTable>
    double operator()(calculate_grouping_cost_tag, PlanTable &&PT, const QueryGraph &G,
                      const CardinalityEstimator &CE, Subproblem sub,
                      const std::vector<const ast::Expr*> &group_by) const;
};

}

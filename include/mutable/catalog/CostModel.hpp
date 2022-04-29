#pragma once

#include <Eigen/Core>
#include <Eigen/LU>
#include <mutable/mutable-config.hpp>
#include <mutable/catalog/CostFunction.hpp>
#include <mutable/IR/Operator.hpp>
#include <mutable/IR/PlanTable.hpp>
#include <mutable/util/LinearModel.hpp>
#include <utility>


namespace m {

/* We use LinearModel to express our learned cost models. */
using CostModel = LinearModel;

struct M_EXPORT CostModelFactory
{
    /** Generates a cost model for the filter operator.
     * @tparam T the type the filter has to process
     */
    template<typename T>
    static CostModel generate_filter_cost_model(unsigned degree, const char *csv_folder_path = nullptr);


    /** Generates a cost model for the group by operator.
     * @tparam T the type the group by has to process
     */
    template<typename T>
    static CostModel generate_group_by_cost_model(const char *csv_folder_path = nullptr);


    /** Generates a cost model for the join operator.
     * @tparam T the type the join has to process
     */
    template<typename T>
    static CostModel generate_join_cost_model(const char *csv_folder_path = nullptr);

public:
    template<typename T>
    static CostModel get_cost_model(OperatorKind op, const char *csv_folder_path = nullptr, unsigned degree = 9) {
        /* generate new cost model. */
        switch(op) {
            case OperatorKind::FilterOperator:
                return generate_filter_cost_model<T>(degree, csv_folder_path);
            case OperatorKind::GroupingOperator:
                return generate_group_by_cost_model<T>(csv_folder_path);
            case OperatorKind::JoinOperator:
                return generate_join_cost_model<T>(csv_folder_path);
            default:
                M_unreachable("operator not supported.");
        }
    }

    /** Generates a `CostFunction` containing `CostModels` for cost estimation.
     * Not every Operator is currently supported.
     * For unsupported Operator the `CostFunction` returns the cost of the first subproblem,
     * therefore the subproblems vector must not be empty
     */
    static std::unique_ptr<CostFunction> get_cost_function();
};

}

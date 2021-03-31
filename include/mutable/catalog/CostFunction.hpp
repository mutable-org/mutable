#pragma once

#include <mutable/IR/Operator.hpp>
#include <mutable/IR/QueryGraph.hpp>


namespace m {

struct PlanTable;

struct CostFunction
{
    using Subproblem = QueryGraph::Subproblem;
    using function_type = std::function<uint64_t(Subproblem, Subproblem, OperatorKind, const PlanTable&)>;

    private:
    function_type fn_;

    public:
    CostFunction(function_type fn) : fn_(fn) { }

    uint64_t operator()(Subproblem left, Subproblem right, OperatorKind op, const PlanTable &plan) const {
        return fn_(left, right, op, plan);
    }
};

}

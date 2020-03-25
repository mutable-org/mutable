#pragma once

#include "IR/Operator.hpp"


namespace db {

template<bool C>
struct TheOperatorVisitor
{
    static constexpr bool is_constant = C;

    template<typename T>
    using Const = std::conditional_t<is_constant, const T, T>;

    virtual ~TheOperatorVisitor() { }

    void operator()(Const<Operator> &op) { op.accept(*this); }
#define DECLARE(CLASS) virtual void operator()(Const<CLASS> &op) = 0
    DECLARE(ScanOperator);
    DECLARE(CallbackOperator);
    DECLARE(PrintOperator);
    DECLARE(NoOpOperator);
    DECLARE(FilterOperator);
    DECLARE(JoinOperator);
    DECLARE(ProjectionOperator);
    DECLARE(LimitOperator);
    DECLARE(GroupingOperator);
    DECLARE(SortingOperator);
#undef DECLARE
};

using OperatorVisitor = TheOperatorVisitor<false>;
using ConstOperatorVisitor = TheOperatorVisitor<true>;

}

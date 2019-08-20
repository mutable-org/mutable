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
    void operator()(Const<Operator> &op, tuple_type &T) { op.accept(*this, T); }

#define DECLARE(CLASS) \
    virtual void operator()(Const<CLASS> &op) = 0
#define DECLARE_CONSUMER(CLASS) \
    DECLARE(CLASS); \
    virtual void operator()(Const<CLASS> &op, tuple_type &t) = 0

    DECLARE(ScanOperator);

    DECLARE_CONSUMER(CallbackOperator);
    DECLARE_CONSUMER(FilterOperator);
    DECLARE_CONSUMER(JoinOperator);
    DECLARE_CONSUMER(ProjectionOperator);
    DECLARE_CONSUMER(LimitOperator);
    DECLARE_CONSUMER(GroupingOperator);

#undef DECLARE_CONSUMER
#undef DECLARE
};

using OperatorVisitor = TheOperatorVisitor<false>;
using ConstOperatorVisitor = TheOperatorVisitor<true>;

}

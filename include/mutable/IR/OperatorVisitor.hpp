#pragma once

#include "mutable/IR/Operator.hpp"


namespace m {

template<bool C>
struct TheOperatorVisitor
{
    static constexpr bool is_constant = C;

    template<typename T>
    using Const = std::conditional_t<is_constant, const T, T>;

    virtual ~TheOperatorVisitor() { }

    void operator()(Const<Operator> &op) { op.accept(*this); }

#define DECLARE(CLASS) virtual void operator()(Const<CLASS> &op) = 0;
    DB_OPERATOR_LIST(DECLARE)
#undef DECLARE
};

using OperatorVisitor = TheOperatorVisitor<false>;
using ConstOperatorVisitor = TheOperatorVisitor<true>;

}

#pragma once

#include <mutable/IR/Operator.hpp>


namespace m {

struct OperatorDot : ConstOperatorVisitor
{
    static constexpr const char * const GRAPH_TYPE = "digraph";
    static constexpr const char * const EDGE = " -> ";

    public:
    std::ostream &out;

    OperatorDot(std::ostream &out);
    ~OperatorDot();

    using ConstOperatorVisitor::operator();
#define DECLARE(CLASS) void operator()(Const<CLASS> &op) override;
    M_OPERATOR_LIST(DECLARE)
#undef DECLARE
};

}

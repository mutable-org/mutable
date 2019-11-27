#pragma once

#include "IR/OperatorVisitor.hpp"


namespace db {

struct OperatorDot : ConstOperatorVisitor
{
    static constexpr const char * const GRAPH_TYPE = "digraph";
    static constexpr const char * const EDGE = " -> ";

    public:
    std::ostream &out;

    OperatorDot(std::ostream &out);
    ~OperatorDot();

    using ConstOperatorVisitor::operator();
#define DECLARE(CLASS) void operator()(Const<CLASS> &op) override
    DECLARE(ScanOperator);
    DECLARE(CallbackOperator);
    DECLARE(FilterOperator);
    DECLARE(JoinOperator);
    DECLARE(ProjectionOperator);
    DECLARE(LimitOperator);
    DECLARE(GroupingOperator);
    DECLARE(SortingOperator);
#undef DECLARE
};

}

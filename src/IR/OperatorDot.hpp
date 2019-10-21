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

#define DECLARE(CLASS) \
    void operator()(Const<CLASS> &op) override
#define DECLARE_CONSUMER(CLASS) \
    void operator()(Const<CLASS>&, tuple_type&) override { } \
    DECLARE(CLASS)

    DECLARE(ScanOperator);
    DECLARE_CONSUMER(CallbackOperator);
    DECLARE_CONSUMER(FilterOperator);
    DECLARE_CONSUMER(JoinOperator);
    DECLARE_CONSUMER(ProjectionOperator);
    DECLARE_CONSUMER(LimitOperator);
    DECLARE_CONSUMER(GroupingOperator);
    DECLARE_CONSUMER(SortingOperator);

#undef DECLARE_CONSUMER
#undef DECLARE
};

}

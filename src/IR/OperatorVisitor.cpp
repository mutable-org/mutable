#include "IR/OperatorVisitor.hpp"


using namespace db;


/*======================================================================================================================
 * Operator methods to accept the visitor
 *====================================================================================================================*/

#define ACCEPT(CLASS) \
    void CLASS::accept(OperatorVisitor &v)            { v(*this); } \
    void CLASS::accept(ConstOperatorVisitor &v) const { v(*this); }

#define ACCEPT_CONSUMER(CLASS) \
    ACCEPT(CLASS) \
    void CLASS::accept(OperatorVisitor &v, tuple_type &t)            { v(*this, t); } \
    void CLASS::accept(ConstOperatorVisitor &v, tuple_type &t) const { v(*this, t); }

ACCEPT(ScanOperator);

ACCEPT_CONSUMER(CallbackOperator);
ACCEPT_CONSUMER(FilterOperator);
ACCEPT_CONSUMER(JoinOperator);
ACCEPT_CONSUMER(ProjectionOperator);
ACCEPT_CONSUMER(LimitOperator);
ACCEPT_CONSUMER(GroupingOperator);

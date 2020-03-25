#include "IR/OperatorVisitor.hpp"


using namespace db;


/*======================================================================================================================
 * Operator methods to accept the visitor
 *====================================================================================================================*/

#define ACCEPT(CLASS) \
    void CLASS::accept(OperatorVisitor &v)            { v(*this); } \
    void CLASS::accept(ConstOperatorVisitor &v) const { v(*this); }

ACCEPT(ScanOperator);
ACCEPT(CallbackOperator);
ACCEPT(PrintOperator);
ACCEPT(NoOpOperator);
ACCEPT(FilterOperator);
ACCEPT(JoinOperator);
ACCEPT(ProjectionOperator);
ACCEPT(LimitOperator);
ACCEPT(GroupingOperator);
ACCEPT(SortingOperator);

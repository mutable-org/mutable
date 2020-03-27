#include "IR/OperatorVisitor.hpp"


using namespace db;


/*======================================================================================================================
 * Operator methods to accept the visitor
 *====================================================================================================================*/

#define ACCEPT(CLASS) \
    void CLASS::accept(OperatorVisitor &v)            { v(*this); } \
    void CLASS::accept(ConstOperatorVisitor &v) const { v(*this); }

DB_OPERATOR_LIST(ACCEPT)

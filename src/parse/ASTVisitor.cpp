#include "mutable/parse/ASTVisitor.hpp"


using namespace m;


/*======================================================================================================================
 * AST methods to accept the visitor
 *====================================================================================================================*/

/*===== Expressions ==================================================================================================*/

#define ACCEPT_EXPR(CLASS) \
    void CLASS::accept(ASTExprVisitor &v)            { v(*this); } \
    void CLASS::accept(ConstASTExprVisitor &v) const { v(*this); }

DB_AST_EXPR_LIST(ACCEPT_EXPR)

/*===== Clauses ======================================================================================================*/

#define ACCEPT_CLAUSE(CLASS) \
    void CLASS::accept(ASTClauseVisitor &v)            { v(*this); } \
    void CLASS::accept(ConstASTClauseVisitor &v) const { v(*this); }

DB_AST_CLAUSE_LIST(ACCEPT_CLAUSE)

/*===== Constraints ==================================================================================================*/

#define ACCEPT_CONSTRAINT(CLASS) \
    void CLASS::accept(ASTConstraintVisitor &v)            { v(*this); } \
    void CLASS::accept(ConstASTConstraintVisitor &v) const { v(*this); }

DB_AST_CONSTRAINT_LIST(ACCEPT_CONSTRAINT)

/*===== Statements ===================================================================================================*/

#define ACCEPT_STMT(CLASS) \
    void CLASS::accept(ASTStmtVisitor &v)            { v(*this); } \
    void CLASS::accept(ConstASTStmtVisitor &v) const { v(*this); }

DB_AST_STMT_LIST(ACCEPT_STMT)

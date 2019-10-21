#include "parse/ASTVisitor.hpp"


using namespace db;


/*======================================================================================================================
 * AST methods to accept the visitor
 *====================================================================================================================*/

#define ACCEPT(CLASS) \
    void CLASS::accept(ASTVisitor &v)            { v(*this); } \
    void CLASS::accept(ConstASTVisitor &v) const { v(*this); }

/*===== Expressions ==================================================================================================*/

ACCEPT(ErrorExpr);
ACCEPT(Designator);
ACCEPT(Constant);
ACCEPT(FnApplicationExpr);
ACCEPT(UnaryExpr);
ACCEPT(BinaryExpr);

/*===== Clauses ======================================================================================================*/

ACCEPT(ErrorClause);
ACCEPT(SelectClause);
ACCEPT(FromClause);
ACCEPT(WhereClause);
ACCEPT(GroupByClause);
ACCEPT(HavingClause);
ACCEPT(OrderByClause);
ACCEPT(LimitClause);

/*===== Statements ===================================================================================================*/

ACCEPT(ErrorStmt);
ACCEPT(EmptyStmt);
ACCEPT(CreateDatabaseStmt);
ACCEPT(UseDatabaseStmt);
ACCEPT(CreateTableStmt);
ACCEPT(SelectStmt);
ACCEPT(InsertStmt);
ACCEPT(UpdateStmt);
ACCEPT(DeleteStmt);
ACCEPT(DSVImportStmt);

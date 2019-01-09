#include "parse/Sema.hpp"


using namespace db;


/*===== Expressions ==================================================================================================*/
void Sema::operator()(Const<ErrorExpr>&)
{
    /* nothing to be done */
}

void Sema::operator()(Const<Designator> &e)
{
    /* TODO A designator has the type of the referred attribute. */
}

void Sema::operator()(Const<Constant> &e)
{
    /* TODO Detect the type of the constant. */
}

void Sema::operator()(Const<FnApplicationExpr> &e)
{
}

void Sema::operator()(Const<UnaryExpr> &e)
{
    /* Analyze sub-expression. */
    (*this)(*e.expr);
    /* TODO Check if unary expression is compatible with sub-expression type. */
}

void Sema::operator()(Const<BinaryExpr> &e)
{
    /* Analyze sub-expressions. */
    (*this)(*e.lhs);
    (*this)(*e.rhs);
    /* TODO Check if both binary operator is compatible with sub-expression types. */
}

/*===== Statements ===================================================================================================*/
void Sema::operator()(Const<ErrorStmt>&)
{
    /* nothing to be done */
}

void Sema::operator()(Const<CreateDatabaseStmt> &s)
{
}

void Sema::operator()(Const<UseDatabaseStmt> &s)
{
}

void Sema::operator()(Const<CreateTableStmt> &s)
{
}

void Sema::operator()(Const<SelectStmt> &s)
{
}

void Sema::operator()(Const<InsertStmt> &s)
{
}

void Sema::operator()(Const<UpdateStmt> &s)
{
}

void Sema::operator()(Const<DeleteStmt> &s)
{
}

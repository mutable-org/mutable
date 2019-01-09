#include "parse/Sema.hpp"


using namespace db;


/*===== Expressions ==================================================================================================*/
void Sema::operator()(Const<ErrorExpr> &e)
{
}

void Sema::operator()(Const<Designator> &e)
{
}

void Sema::operator()(Const<Constant> &e)
{
}

void Sema::operator()(Const<FnApplicationExpr> &e)
{
}

void Sema::operator()(Const<UnaryExpr> &e)
{
}

void Sema::operator()(Const<BinaryExpr> &e)
{
}

/*===== Statements ===================================================================================================*/
void Sema::operator()(Const<ErrorStmt> &s)

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

#include "parse/AST.hpp"

#include "catalog/Schema.hpp"
#include "parse/ASTDumper.hpp"
#include "parse/ASTPrinter.hpp"


using namespace db;


/*======================================================================================================================
 * Destructors
 *====================================================================================================================*/

/*===== Expr =========================================================================================================*/

FnApplicationExpr::~FnApplicationExpr()
{
    delete fn;
    for (auto arg : args)
        delete arg;
}

/*===== Clause =======================================================================================================*/

SelectClause::~SelectClause()
{
    for (auto s : select)
        delete s.first;
}

WhereClause::~WhereClause()
{
    delete where;
}

GroupByClause::~GroupByClause()
{
    for (auto e : group_by)
        delete e;
}

HavingClause::~HavingClause()
{
    delete having;
}

OrderByClause::~OrderByClause()
{
    for (auto o : order_by)
        delete o.first;
}

/*===== Stmt =========================================================================================================*/

SelectStmt::~SelectStmt()
{
    delete select;
    delete from;
    delete where;
    delete group_by;
    delete having;
    delete order_by;
    delete limit;
}

InsertStmt::~InsertStmt()
{
    for (value_type &v : values) {
        for (element_type &e : v)
            delete e.second;
    }
}

UpdateStmt::~UpdateStmt()
{
    for (auto &s : set)
        delete s.second;
    delete where;
}

DeleteStmt::~DeleteStmt()
{
    delete where;
}

/*======================================================================================================================
 * Stream printing
 *====================================================================================================================*/

std::ostream & db::operator<<(std::ostream &out, const Expr &e) {
    ASTPrinter p(out);
    p(e);
    return out;
}

std::ostream & db::operator<<(std::ostream &out, const Clause &c) {
    ASTPrinter p(out);
    p(c);
    return out;
}

std::ostream & db::operator<<(std::ostream &out, const Stmt &s) {
    ASTPrinter p(out);
    p(s);
    return out;
}

/*======================================================================================================================
 * AST Dump
 *====================================================================================================================*/

void Expr::dump(std::ostream &out) const
{
    ASTDumper dumper(out);
    dumper(*this);
    out << std::endl;
}
void Expr::dump() const { dump(std::cerr); }

void Clause::dump(std::ostream &out) const
{
    ASTDumper dumper(out);
    dumper(*this);
    out << std::endl;
}
void Clause::dump() const { dump(std::cerr); }

void Stmt::dump(std::ostream &out) const
{
    ASTDumper dumper(out);
    dumper(*this);
    out << std::endl;
}
void Stmt::dump() const { dump(std::cerr); }

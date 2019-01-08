#include "parse/AST.hpp"

#include "catalog/Schema.hpp"
#include "parse/ASTDumper.hpp"
#include "parse/ASTPrinter.hpp"


using namespace db;


FnApplicationExpr::~FnApplicationExpr()
{
    delete fn;
    for (auto arg : args)
        delete arg;
}

SelectStmt::~SelectStmt()
{
    for (auto &s : select)
        delete s.first;

    delete where;

    for (auto g : group_by)
        delete g;

    for (auto &o : order_by)
        delete o.first;

    delete limit.first;
    delete limit.second;
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
}

void Expr::dump() const { dump(std::cerr); }

void Stmt::dump(std::ostream &out) const
{
    ASTDumper dumper(out);
    dumper(*this);
}

void Stmt::dump() const { dump(std::cerr); }

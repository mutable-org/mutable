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
 * operator==
 *====================================================================================================================*/

bool ErrorExpr::operator==(const Expr &o) const { return cast<const ErrorExpr>(&o) != nullptr; }

bool Designator::operator==(const Expr &o) const
{
    auto other = cast<const Designator>(&o);
    if (not other) return false;
    return this->table_name.text == other->table_name.text and
           this->attr_name.text == other->attr_name.text;
}

bool Constant::operator==(const Expr &o) const
{
    auto other = cast<const Constant>(&o);
    if (not other) return false;
    return this->tok.text == other->tok.text;
}

bool FnApplicationExpr::operator==(const Expr &o) const
{
    auto other = cast<const FnApplicationExpr>(&o);
    if (not other) return false;
    if (*this->fn != *other->fn) return false;
    if (this->args.size() != other->args.size()) return false;
    for (std::size_t i = 0, end = this->args.size(); i != end; ++i) {
        if (*this->args[i] != *other->args[i])
            return false;
    }
    return true;
}

bool UnaryExpr::operator==(const Expr &o) const
{
    auto other = cast<const UnaryExpr>(&o);
    if (not other) return false;
    if (this->op.text != other->op.text) return false;
    return *this->expr == *other->expr;
}

bool BinaryExpr::operator==(const Expr &o) const
{
    auto other = cast<const BinaryExpr>(&o);
    if (not other) return false;
    if (this->op.text != other->op.text) return false;
    return *this->lhs == *other->lhs and *this->rhs == *other->rhs;
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

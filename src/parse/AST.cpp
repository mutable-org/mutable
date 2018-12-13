#include "parse/AST.hpp"


using namespace db;


FnApplicationExpr::FnApplicationExpr(Expr *fn, std::vector<Expr*> args)
    : fn(notnull(fn))
    , args(args)
{
    for (Expr *e : args)
        notnull(e);
}


void ErrorExpr::print(std::ostream &out) const
{
    out << "[error]";
}

void Designator::print(std::ostream &out) const
{
    if (table_name)
        out << table_name.text << '.';
    out << attr_name.text;
}

void Constant::print(std::ostream &out) const
{
    out << tok.text;
}

void FnApplicationExpr::print(std::ostream &out) const
{
    fn->print(out);
    out << '(';
    for (auto it = args.cbegin(), end = args.cend(); it != end; ++it) {
        if (it != args.cbegin()) out << ", ";
        (*it)->print(out);
    }
    out << ')';
}

void UnaryExpr::print(std::ostream &out) const
{
    out << '(' << op.text;
    if (op == TK_Not) out << ' ';
    expr->print(out);
    out << ')';
}

void BinaryExpr::print(std::ostream &out) const
{
    out << '(';
    lhs->print(out);
    out << ' ' << op.text << ' ';
    rhs->print(out);
    out << ')';
}

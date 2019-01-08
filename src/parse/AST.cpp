#include "parse/AST.hpp"

#include "catalog/Schema.hpp"
#include "parse/ASTPrinter.hpp"


using namespace db;


namespace {

std::ostream & indent(std::ostream &out, int i)
{
    if (i)
        out << std::string(2 * i - 2, ' ') << "` ";
    return out;
}

}

FnApplicationExpr::FnApplicationExpr(Expr *fn, std::vector<Expr*> args)
    : fn(notnull(fn))
    , args(args)
{
    for (Expr *e : args)
        notnull(e);
}

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

/*===== Expr =========================================================================================================*/

void Expr::dump() const { dump(std::cerr); }

void ErrorExpr::dump(std::ostream &out, int i) const
{
    indent(out, i) << "ErrorExpr '" << tok.text << "' (" << tok.pos << ')' << std::endl;
}

void Designator::dump(std::ostream &out, int i) const
{
    if (has_table_name()) {
        indent(out, i) << "Designator\n";
        indent(out, i + 1) << "table name: '" << table_name.text << "' (" << table_name.pos << ')' << std::endl;
        indent(out, i + 1) << "attribute name: '" << attr_name.text << "' (" << attr_name.pos << ')' << std::endl;
    } else {
        indent(out, i) << "identifier: '" << attr_name.text << "' (" << attr_name.pos << ')' << std::endl;
    }
}

void Constant::dump(std::ostream &out, int i) const
{
    indent(out, i) << "Constant: " << tok.text << " (" << tok.pos << ')' << std::endl;
}

void FnApplicationExpr::dump(std::ostream &out, int i) const
{
    indent(out, i) << "FnApplicationExpr" << std::endl;
    fn->dump(out, i + 1);
    indent(out, i + 1) << "args" << std::endl;
    for (auto expr : args)
        expr->dump(out, i + 2);
}

void UnaryExpr::dump(std::ostream &out, int i) const
{
    indent(out, i) << "UnaryExpr: '" << op.text << "' (" << op.pos << ')' << std::endl;
    expr->dump(out, i + 1);
}

void BinaryExpr::dump(std::ostream &out, int i) const
{
    indent(out, i) << "BinaryExpr: '" << op.text << "' (" << op.pos << ')' << std::endl;
    lhs->dump(out, i + 1);
    rhs->dump(out, i + 1);
}

/*===== Stmt =========================================================================================================*/

void Stmt::dump() const { dump(std::cerr); }

void ErrorStmt::dump(std::ostream &out, int i) const
{
    indent(out, i) << "ErrorStmt: '" << tok.text << "' (" << tok.pos << ')' << std::endl;
}

void CreateTableStmt::dump(std::ostream &out, int i) const
{
    indent(out, i) << "CreateTableStmt: table " << table_name.text << " (" << table_name.pos << ')' << std::endl;
    indent(out, i + 1) << "attributes" << std::endl;
    for (auto &attr : attributes)
        indent(out, i + 2) << attr.first.text << " : " << *attr.second << " (" << attr.first.pos << ")" << std::endl;
}

void SelectStmt::dump(std::ostream &out, int i) const
{
    indent(out, i) << "SelectStmt" << std::endl;
    indent(out, i + 1) << "SELECT: select_all=" << (select_all ? "true" : "false") << std::endl;
    for (auto s : select) {
        if (s.second) {
            indent(out, i + 2) << "AS '" << s.second.text << "' (" << s.second.pos << ')' << std::endl;
            s.first->dump(out, i + 3);
        } else
            s.first->dump(out, i + 2);
    }
    indent(out, i + 1) << "FROM" << std::endl;
    for (auto f : from) {
        indent(out, i + 2) << '\'' << f.first.text << '\'';
        if (f.second)
            out << " AS '" << f.second.text << '\'';
        out << " (" << f.first.pos << ')' << std::endl;
    }
    if (where) {
        indent(out, i + 1) << "WHERE" << std::endl;
        where->dump(out, i + 2);
    }
    if (not group_by.empty()) {
        indent(out, i + 1) << "GROUP BY" << std::endl;
        for (auto g : group_by)
            g->dump(out, i + 2);
    }
    if (having) {
        indent(out, i + 1) << "HAVING" << std::endl;
        having->dump(out, i + 2);
    }
    if (not order_by.empty()) {
        indent(out, i + 1) << "ORDER BY" << std::endl;
        for (auto o : order_by) {
            indent(out, i + 2) << (o.second ? "ASC" : "DESC") << std::endl;
            o.first->dump(out, i + 3);
        }
    }
    if (limit.first) {
        indent(out, i + 1) << "LIMIT" << std::endl;
        limit.first->dump(out, i + 2);
        if (limit.second) {
            indent(out, i + 2) << "OFFSET" << std::endl;
            limit.second->dump(out, i + 3);
        }
    }
}

void InsertStmt::dump(std::ostream &out, int i) const
{
    indent(out, i) << "InsertStmt: table " << table_name.text << " (" << table_name.pos << ')' << std::endl;
    indent(out, i + 1) << "values" << std::endl;
    for (std::size_t idx = 0, end = values.size(); idx != end; ++idx) {
        indent(out, i + 2) << '[' << idx << ']' << std::endl;
        const value_type &v = values[idx];
        for (auto &e : v) {
            switch (e.first) {
                case I_Default:
                    indent(out, i + 3) << "DEFAULT" << std::endl;
                    break;

                case I_Null:
                    indent(out, i + 3) << "NULL" << std::endl;
                    break;

                case I_Expr:
                    e.second->dump(out, i + 3);
                    break;
            }
        }
    }
}

void UpdateStmt::dump(std::ostream &out, int i) const
{
    indent(out, i) << "UpdateStmt: table " << table_name.text << " (" << table_name.pos << ')' << std::endl;
    indent(out, i + 1) << "set" << std::endl;
    for (auto s : set) {
        indent(out, i + 2) << s.first.text << " (" << s.first.pos << ')' << std::endl;
        s.second->dump(out, i + 3);
    }

    if (where) {
        indent(out, i + 1) << "where" << std::endl;
        where->dump(out, i + 2);
    }
}

void DeleteStmt::dump(std::ostream &out, int i) const
{
    indent(out, i) << "DeleteStmt: table " << table_name.text << " (" << table_name.pos << ')' << std::endl;

    if (where) {
        indent(out, i + 1) << "where" << std::endl;
        where->dump(out, i + 2);
    }
}

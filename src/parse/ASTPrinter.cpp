#include "parse/ASTPrinter.hpp"

#include "catalog/Schema.hpp"


using namespace db;


/*===== Expr =========================================================================================================*/

void ASTPrinter::operator()(Const<ErrorExpr>&)
{
    out << "[error-expression]";
}

void ASTPrinter::operator()(Const<Designator> &e)
{
    if (e.table_name)
        out << e.table_name.text << '.';
    out << e.attr_name.text;
}

void ASTPrinter::operator()(Const<Constant> &e)
{
    out << e.tok.text;
}

void ASTPrinter::operator()(Const<FnApplicationExpr> &e)
{
    out << *e.fn << '(';
    for (auto it = e.args.cbegin(), end = e.args.cend(); it != end; ++it) {
        if (it != e.args.cbegin()) out << ", ";
        out << **it;
    }
    out << ')';
}

void ASTPrinter::operator()(Const<UnaryExpr> &e)
{
    out << '(' << e.op.text;
    if (e.op == TK_Not) out << ' ';
    out << *e.expr << ')';
}

void ASTPrinter::operator()(Const<BinaryExpr> &e)
{
    out << '(' << *e.lhs << ' ' << e.op.text << ' ' << *e.rhs << ')';
}

/*===== Clause =======================================================================================================*/

void ASTPrinter::operator()(Const<ErrorClause>&)
{
    out << "[error-clause]";
}

void ASTPrinter::operator()(Const<SelectClause> &c)
{
    out << "SELECT ";
    if (c.select_all) out << '*';
    for (auto it = c.select.cbegin(), end = c.select.cend(); it != end; ++it) {
        if (c.select_all or it != c.select.cbegin()) out << ", ";
        (*this)(*it->first);
        if (it->second) out << " AS " << it->second.text;
    }
}

void ASTPrinter::operator()(Const<FromClause> &c)
{
    out << "FROM ";
    for (auto it = c.from.cbegin(), end = c.from.cend(); it != end; ++it) {
        if (it != c.from.cbegin()) out << ", ";
        out << it->name.text;
        if (it->alias) out << " AS " << it->alias.text;
    }
}

void ASTPrinter::operator()(Const<WhereClause> &c)
{
    out << "WHERE " << *c.where;
}

void ASTPrinter::operator()(Const<GroupByClause> &c)
{
    out << "GROUP BY ";
    for (auto it = c.group_by.cbegin(), end = c.group_by.cend(); it != end; ++it) {
        if (it != c.group_by.cbegin()) out << ", ";
        (*this)(**it);
    }
}

void ASTPrinter::operator()(Const<HavingClause> &c)
{
    out << "HAVING " << *c.having;
}

void ASTPrinter::operator()(Const<OrderByClause> &c)
{
    out << "ORDER BY ";
    for (auto it = c.order_by.cbegin(), end = c.order_by.cend(); it != end; ++it) {
        if (it != c.order_by.cbegin()) out << ", ";
        (*this)(*it->first);
        if (it->second) out << " ASC";
        else            out << " DESC";
    }
}

void ASTPrinter::operator()(Const<LimitClause> &c)
{
    out << "LIMIT " << c.limit.text;
    if (c.offset)
        out << " OFFSET " << c.offset.text;
}

/*===== Stmt =========================================================================================================*/

void ASTPrinter::operator()(Const<ErrorStmt>&)
{
    out << "[error-statement];";
}

void ASTPrinter::operator()(Const<EmptyStmt>&)
{
    out << ';';
}

void ASTPrinter::operator()(Const<CreateDatabaseStmt> &s)
{
    out << "CREATE DATABASE " << s.database_name.text << ';';
}

void ASTPrinter::operator()(Const<UseDatabaseStmt> &s)
{
    out << "USE " << s.database_name.text << ';';
}

void ASTPrinter::operator()(Const<CreateTableStmt> &s)
{
    out << "CREATE TABLE " << s.table_name.text << "\n(";
    for (auto it = s.attributes.cbegin(), end = s.attributes.cend(); it != end; ++it) {
        if (it != s.attributes.cbegin()) out << ',';
        out << "\n    " << it->first.text << ' ' << *it->second;
    }
    out << "\n);";
}

void ASTPrinter::operator()(Const<SelectStmt> &s)
{
    (*this)(*s.select);
    out << '\n';
    (*this)(*s.from);

    if (s.where) {
        out << '\n';
        (*this)(*s.where);
    }
    if (s.group_by) {
        out << '\n';
        (*this)(*s.group_by);
    }
    if (s.having) {
        out << '\n';
        (*this)(*s.having);
    }
    if (s.order_by) {
        out << '\n';
        (*this)(*s.order_by);
    }
    if (s.limit) {
        out << '\n';
        (*this)(*s.limit);
    }

    out << ';';
}

void ASTPrinter::operator()(Const<InsertStmt> &s)
{
    out << "INSERT INTO " << s.table_name.text << "\nVALUES\n    ";
    for (auto value_it = s.values.cbegin(), end = s.values.cend(); value_it != end; ++value_it) {
        if (value_it != s.values.cbegin()) out << ",\n    ";
        out << '(';
        for (auto elem_it = value_it->cbegin(), elem_end = value_it->cend(); elem_it != elem_end; ++elem_it) {
            if (elem_it != value_it->cbegin()) out << ", ";
            switch (elem_it->first) {
                case InsertStmt::I_Default: out << "DEFAULT";   break;
                case InsertStmt::I_Null:    out << "NULL";      break;
                case InsertStmt::I_Expr:    out << *elem_it->second;     break;
            }
        }
        out << ')';
    }
    out << ';';
}

void ASTPrinter::operator()(Const<UpdateStmt> &s)
{
    out << "UPDATE " << s.table_name.text << "\nSET\n";
    for (auto it = s.set.cbegin(), end = s.set.cend(); it != end; ++it) {
        if (it != s.set.cbegin()) out << ",\n";
        out << "    " << it->first.text << " = " << *it->second;
    }
    if (s.where) out << '\n' << *s.where;
    out << ';';
}

void ASTPrinter::operator()(Const<DeleteStmt> &s)
{
    out << "DELETE FROM " << s.table_name.text;
    if (s.where) out << '\n' << *s.where;
    out << ';';
}

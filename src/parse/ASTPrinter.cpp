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

/*===== Stmt =========================================================================================================*/

void ASTPrinter::operator()(Const<ErrorStmt>&)
{
    out << "[error-statement];";
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
    out << "SELECT ";
    if (s.select_all) out << '*';
    for (auto it = s.select.cbegin(), end = s.select.cend(); it != end; ++it) {
        if (s.select_all or it != s.select.cbegin()) out << ", ";
        (*this)(*it->first);
        if (it->second) out << " AS " << it->second.text;
    }
    out << "\nFROM ";
    for (auto it = s.from.cbegin(), end = s.from.cend(); it != end; ++it) {
        if (it != s.from.cbegin()) out << ", ";
        out << it->first.text;
        if (it->second) out << " AS " << it->second.text;
    }
    if (s.where) out << "\nWHERE " << *s.where;
    if (not s.group_by.empty()) {
        out << "\nGROUP BY ";
        for (auto it = s.group_by.cbegin(), end = s.group_by.cend(); it != end; ++it) {
            if (it != s.group_by.cbegin()) out << ", ";
            (*this)(**it);
        }
    }
    if (s.having) out << "\nHAVING " << *s.having;
    if (not s.order_by.empty()) {
        out << "\nORDER BY ";
        for (auto it = s.order_by.cbegin(), end = s.order_by.cend(); it != end; ++it) {
            if (it != s.order_by.cbegin()) out << ", ";
            (*this)(*it->first);
            if (it->second) out << " ASC";
            else            out << " DESC";
        }
    }
    if (s.limit.first) {
        out << "\nLIMIT " << *s.limit.first;
        if (s.limit.second) out << " OFFSET " << *s.limit.second;
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
    if (s.where) out << "\nWHERE " << *s.where;
    out << ';';
}

void ASTPrinter::operator()(Const<DeleteStmt> &s)
{
    out << "DELETE FROM " << s.table_name.text;
    if (s.where) out << " WHERE " << *s.where;
    out << ';';
}

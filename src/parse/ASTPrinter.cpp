#include "parse/ASTPrinter.hpp"

#include <mutable/catalog/Schema.hpp>


using namespace m;
using namespace m::ast;


/*===== Expr =========================================================================================================*/

void ASTPrinter::operator()(Const<ErrorExpr>&)
{
    out << "[error-expression]";
}

void ASTPrinter::operator()(Const<Designator> &e)
{
    if (e.has_explicit_table_name())
        out << e.get_table_name() << '.';
    out << e.attr_name.text;
}

void ASTPrinter::operator()(Const<Constant> &e)
{
    out << e.tok.text;
}

void ASTPrinter::operator()(Const<FnApplicationExpr> &e)
{
    (*this)(*e.fn);
    out << '(';
    for (auto it = e.args.cbegin(), end = e.args.cend(); it != end; ++it) {
        if (it != e.args.cbegin()) out << ", ";
        (*this)(**it);
    }
    out << ')';
}

void ASTPrinter::operator()(Const<UnaryExpr> &e)
{
    out << '(' << e.op().text;
    if (e.op() == TK_Not) out << ' ';
    (*this)(*e.expr);
    out << ')';
}

void ASTPrinter::operator()(Const<BinaryExpr> &e)
{
    out << '(';
    (*this)(*e.lhs);
    out << ' ' << e.op().text << ' ';
    (*this)(*e.rhs);
    out << ')';
}

void ASTPrinter::operator()(Const<QueryExpr> &e)
{
    if (expand_nested_queries_) {
        out << '(';
        (*this)(*e.query);
        out << ')';
    } else {
        out << e.alias() << '.' << e.alias(); // name of artificial source followed by name of single result
    }
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
        if (auto tok = std::get_if<Token>(&it->source)) {
            out << tok->text;
        } else if (auto stmt = std::get_if<Stmt*>(&it->source)) {
            out << '(';
            (*this)(**stmt);
            out << ')';
        } else {
            M_unreachable("illegal variant");
        }
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
        auto &[grp, alias] = *it;
        (*this)(*grp);
        if (it->second)
            out << " AS " << alias.text;
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

/*===== Constraint ===================================================================================================*/

void ASTPrinter::operator()(Const<PrimaryKeyConstraint>&)
{
    out << "PRIMARY KEY";
}

void ASTPrinter::operator()(Const<UniqueConstraint>&)
{
    out << "UNIQUE";
}

void ASTPrinter::operator()(Const<NotNullConstraint>&)
{
    out << "NOT NULL";
}

void ASTPrinter::operator()(Const<CheckConditionConstraint> &c)
{
    out << "CHECK (" << *c.cond << ')';
}

void ASTPrinter::operator()(Const<ReferenceConstraint> &c)
{
    out << "REFERENCES " << c.table_name.text << '(' << c.attr_name.text << ')';
}


/*===== Instruction ==================================================================================================*/

void ASTPrinter::operator()(Const<Instruction> &inst)
{
    out << inst.tok.text;
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
        auto &attr = *it;
        if (it != s.attributes.cbegin()) out << ',';
        out << "\n    " << attr->name.text << ' ' << *attr->type;
        for (auto &c : attr->constraints) {
            out << ' ';
            (*this)(*c);
        }
    }
    out << "\n);";
}

void ASTPrinter::operator()(Const<SelectStmt> &s)
{
    bool was_nested = is_nested_;
    is_nested_ = true;

    (*this)(*s.select);

    if (s.from) {
        out << '\n';
        (*this)(*s.from);
    }

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

    is_nested_ = was_nested;
    if (not is_nested_)
        out << ';';
}

void ASTPrinter::operator()(Const<InsertStmt> &s)
{
    out << "INSERT INTO " << s.table_name.text << "\nVALUES\n    ";
    for (auto value_it = s.tuples.cbegin(), end = s.tuples.cend(); value_it != end; ++value_it) {
        if (value_it != s.tuples.cbegin()) out << ",\n    ";
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

void ASTPrinter::operator()(Const<DSVImportStmt> &s)
{
    out << "IMPORT INTO " << s.table_name.text << " DSV " << s.path.text;
    if (s.rows)
        out << " ROWS " << s.rows.text;
    if (s.delimiter)
        out << " DELIMITER " << s.delimiter.text;
    if (s.has_header)
        out << " HAS HEADER";
    if (s.skip_header)
        out << " SKIP HEADER";
    out << ';';
}

#include "parse/ASTDumper.hpp"

#include "catalog/Schema.hpp"


using namespace db;


void ASTDumper::operator()(Const<ErrorExpr> &e)
{
    indent() << "ErrorExpr '" << e.tok.text << "' (" << e.tok.pos << ')' << std::endl;
}

void ASTDumper::operator()(Const<Designator> &e)
{
    if (e.has_table_name()) {
        indent() << "Designator\n";
        ++indent_;
        indent() << "table name: '" << e.table_name.text << "' (" << e.table_name.pos << ')' << std::endl;
        indent() << "attribute name: '" << e.attr_name.text << "' (" << e.attr_name.pos << ')' << std::endl;
        --indent_;
    } else {
        indent() << "identifier: '" << e.attr_name.text << "' (" << e.attr_name.pos << ')' << std::endl;
    }
}

void ASTDumper::operator()(Const<Constant> &e)
{
    indent() << "Constant: " << e.tok.text << " (" << e.tok.pos << ')' << std::endl;
}

void ASTDumper::operator()(Const<FnApplicationExpr> &e)
{
    indent() << "FnApplicationExpr" << std::endl;
    ++indent_;
    (*this)(*e.fn);
    indent() << "args" << std::endl;
    ++indent_;
    for (auto expr : e.args)
        (*this)(*expr);
    --indent_;
    --indent_;
}

void ASTDumper::operator()(Const<UnaryExpr> &e)
{
    indent() << "UnaryExpr: '" << e.op.text << "' (" << e.op.pos << ')' << std::endl;
    ++indent_;
    (*this)(*e.expr);
    --indent_;
}

void ASTDumper::operator()(Const<BinaryExpr> &e)
{
    indent() << "BinaryExpr: '" << e.op.text << "' (" << e.op.pos << ')' << std::endl;
    ++indent_;
    (*this)(*e.lhs);
    (*this)(*e.rhs);
    --indent_;
}

void ASTDumper::operator()(Const<ErrorStmt> &s)
{
    indent() << "ErrorStmt: '" << s.tok.text << "' (" << s.tok.pos << ')' << std::endl;
}

void ASTDumper::operator()(Const<UseDatabaseStmt> &s)
{
    indent() << "UseDatabaseStmt: '" << s.database_name.text << "' (" << s.database_name.pos << ')' << std::endl;
}

void ASTDumper::operator()(Const<CreateTableStmt> &s)
{
    indent() << "CreateTableStmt: table " << s.table_name.text << " (" << s.table_name.pos << ')' << std::endl;
    ++indent_;
    indent() << "attributes" << std::endl;
    ++indent_;
    for (auto &attr : s.attributes)
        indent() << attr.first.text << " : " << *attr.second << " (" << attr.first.pos << ")" << std::endl;
    --indent_;
    --indent_;
}

void ASTDumper::operator()(Const<SelectStmt> &s)
{
    indent() << "SelectStmt" << std::endl;
    ++indent_;
    indent() << "SELECT: select_all=" << (s.select_all ? "true" : "false") << std::endl;
    ++indent_;
    for (auto select : s.select) {
        if (select.second) {
            indent() << "AS '" << select.second.text << "' (" << select.second.pos << ')' << std::endl;
            ++indent_;
            (*this)(*select.first);
            --indent_;
        } else
            (*this)(*select.first);
    }
    --indent_;
    indent() << "FROM" << std::endl;
    ++indent_;
    for (auto f : s.from) {
        indent() << '\'' << f.first.text << '\'';
        if (f.second)
            out << " AS '" << f.second.text << '\'';
        out << " (" << f.first.pos << ')' << std::endl;
    }
    --indent_;
    if (s.where) {
        indent() << "WHERE" << std::endl;
        ++indent_;
        (*this)(*s.where);
        --indent_;
    }
    if (not s.group_by.empty()) {
        indent() << "GROUP BY" << std::endl;
        ++indent_;
        for (auto g : s.group_by)
            (*this)(*g);
        --indent_;
    }
    if (s.having) {
        indent() << "HAVING" << std::endl;
        ++indent_;
        (*this)(*s.having);
        --indent_;
    }
    if (not s.order_by.empty()) {
        indent() << "ORDER BY" << std::endl;
        ++indent_;
        for (auto o : s.order_by) {
            indent() << (o.second ? "ASC" : "DESC") << std::endl;
            ++indent_;
            (*this)(*o.first);
            --indent_;
        }
        --indent_;
    }
    if (s.limit.first) {
        indent() << "LIMIT" << std::endl;
        ++indent_;
        (*this)(*s.limit.first);
        if (s.limit.second) {
            indent() << "OFFSET" << std::endl;
            ++indent_;
            (*this)(*s.limit.second);
            --indent_;
        }
        --indent_;
    }
}

void ASTDumper::operator()(Const<InsertStmt> &s)
{
    indent() << "InsertStmt: table " << s.table_name.text << " (" << s.table_name.pos << ')' << std::endl;
    ++indent_;
    indent() << "values" << std::endl;
    ++indent_;
    for (std::size_t idx = 0, end = s.values.size(); idx != end; ++idx) {
        indent() << '[' << idx << ']' << std::endl;
        const InsertStmt::value_type &v = s.values[idx];
        ++indent_;
        for (auto &e : v) {
            switch (e.first) {
                case InsertStmt::I_Default:
                    indent() << "DEFAULT" << std::endl;
                    break;

                case InsertStmt::I_Null:
                    indent() << "NULL" << std::endl;
                    break;

                case InsertStmt::I_Expr:
                    (*this)(*e.second);
                    break;
            }
        }
        --indent_;
    }
    --indent_;
    --indent_;
}

void ASTDumper::operator()(Const<UpdateStmt> &s)
{
    indent() << "UpdateStmt: table " << s.table_name.text << " (" << s.table_name.pos << ')' << std::endl;
    ++indent_;
    indent() << "set" << std::endl;
    ++indent_;
    for (auto s : s.set) {
        indent() << s.first.text << " (" << s.first.pos << ')' << std::endl;
        ++indent_;
        (*this)(*s.second);
        --indent_;
    }
    --indent_;
    --indent_;

    if (s.where) {
        ++indent_;
        indent() << "where" << std::endl;
        ++indent_;
        (*this)(*s.where);
        --indent_;
        --indent_;
    }
    --indent_;
}

void ASTDumper::operator()(Const<DeleteStmt> &s)
{
    indent() << "DeleteStmt: table " << s.table_name.text << " (" << s.table_name.pos << ')' << std::endl;

    if (s.where) {
        ++indent_;
        indent() << "where" << std::endl;
        ++indent_;
        (*this)(*s.where);
        --indent_;
        --indent_;
    }
}

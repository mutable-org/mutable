#include "parse/AST.hpp"


using namespace db;


std::ostream & indent(std::ostream &out, int i)
{
    if (i)
        out << std::string(2 * i - 2, ' ') << "` ";
    return out;
}


FnApplicationExpr::FnApplicationExpr(Expr *fn, std::vector<Expr*> args)
    : fn(notnull(fn))
    , args(args)
{
    for (Expr *e : args)
        notnull(e);
}

/*======================================================================================================================
 * AST Dump
 *====================================================================================================================*/

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

void ErrorStmt::dump(std::ostream &out, int i) const
{
    indent(out, i) << "ErrorStmt: '" << tok.text << "' (" << tok.pos << ')' << std::endl;
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
    for (auto v : values) {
        switch (v.kind) {
            case I_Default:
                indent(out, i + 2) << "DEFAULT";
                break;

            case I_Null:
                indent(out, i + 2) << "DEFAULT";
                break;

            case I_Expr:
                v.expr->dump(out, i + 2);
                break;
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

/*======================================================================================================================
 * AST Pretty Printing
 *====================================================================================================================*/

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

void ErrorStmt::print(std::ostream &out) const
{
    out << "[Error];";
}

void SelectStmt::print(std::ostream &out) const
{
    out << "SELECT ";
    if (select_all) out << '*';
    for (auto it = select.cbegin(), end = select.cend(); it != end; ++it) {
        if (select_all or it != select.cbegin()) out << ", ";
        it->first->print(out);
        if (it->second) out << " AS " << it->second.text;
    }
    out << "\nFROM ";
    for (auto it = from.cbegin(), end = from.cend(); it != end; ++it) {
        if (it != from.cbegin()) out << ", ";
        out << it->first.text;
        if (it->second) out << " AS " << it->second.text;
    }
    if (where) {
        out << "\nWHERE ";
        where->print(out);
    }
    if (not group_by.empty()) {
        out << "\nGROUP BY ";
        for (auto it = group_by.cbegin(), end = group_by.cend(); it != end; ++it) {
            if (it != group_by.cbegin()) out << ", ";
            (*it)->print(out);
        }
    }
    if (having) {
        out << "\nHAVING ";
        having->print(out);
    }
    if (not order_by.empty()) {
        out << "\nORDER BY ";
        for (auto it = order_by.cbegin(), end = order_by.cend(); it != end; ++it) {
            if (it != order_by.cbegin()) out << ", ";
            it->first->print(out);
            if (it->second) out << " ASC";
            else            out << " DESC";
        }
    }
    if (limit.first) {
        out << "\nLIMIT ";
        limit.first->print(out);
        if (limit.second) {
            out << " OFFSET ";
            limit.second->print(out);
        }
    }
    out << ';';
}

void InsertStmt::print(std::ostream &out) const
{
    out << "INSERT INTO " << table_name.text << "\nVALUES";
    for (auto v : values) {
        out << "\n    ";
        switch (v.kind) {
            case I_Default: out << "DEFAULT";   break;
            case I_Null:    out << "NULL";      break;
            case I_Expr:    v.expr->print(out); break;
        }
    }
    out << ';';
}

void UpdateStmt::print(std::ostream &out) const
{
    out << "UPDATE " << table_name.text << " SET\n";
    for (auto it = set.cbegin(), end = set.cend(); it != end; ++it) {
        if (it != set.cbegin()) out << ",\n";
        out << "    " << it->first.text << " = ";
        it->second->print(out);
    }
    if (where) {
        out << "WHERE ";
        where->print(out);
    }
    out << ';';
}

void DeleteStmt::print(std::ostream &out) const
{
    out << "DELETE FROM " << table_name.text;
    if (where) {
        out << " WHERE ";
        where->print(out);
    }
    out << ';';
}

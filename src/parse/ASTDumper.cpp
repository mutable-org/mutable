#include "parse/ASTDumper.hpp"

#include <mutable/catalog/Schema.hpp>


using namespace m;
using namespace m::ast;


void ASTDumper::print_type(const Expr &e) const
{
    if (e.has_type()) {
        out << " of type " << *e.type();
        if (auto pt = cast<const PrimitiveType>(e.type()))
            out << (pt->is_scalar() ? " scalar" : " vectorial");
    }
}

/*===== Expr =========================================================================================================*/

void ASTDumper::operator()(Const<ErrorExpr> &e)
{
    indent() << "ErrorExpr '" << e.tok.text << "' (" << e.tok.pos << ')';
}

void ASTDumper::operator()(Const<Designator> &e)
{
    if (e.has_explicit_table_name()) {
        indent() << "Designator";
        print_type(e);
        ++indent_;
        indent() << "table name '" << e.table_name.text << "' (" << e.table_name.pos << ')';
        indent() << "attribute name '" << e.attr_name.text << "' (" << e.attr_name.pos << ')';
        --indent_;
    } else {
        indent() << "Identifier '" << e.attr_name.text << '\'';
        if (e.has_table_name()) out << " deduced to table '" << e.get_table_name() << '\'';
        print_type(e);
        out << " (" << e.attr_name.pos << ')';
    }
}

void ASTDumper::operator()(Const<Constant> &e)
{
    indent() << "Constant " << e.tok.text;
    if (e.has_type()) out << " of type " << *e.type();
    out << " (" << e.tok.pos << ')';
}

void ASTDumper::operator()(Const<FnApplicationExpr> &e)
{
    indent() << "FnApplicationExpr";
    print_type(e);
    ++indent_;
    (*this)(*e.fn);
    if (not e.args.empty()) {
        indent() << "args";
        ++indent_;
        for (auto &expr : e.args)
            (*this)(*expr);
        --indent_;
    }
    --indent_;
}

void ASTDumper::operator()(Const<UnaryExpr> &e)
{
    indent() << "UnaryExpr '" << e.op().text << "'";
    print_type(e);
    out << " (" << e.op().pos << ')';
    ++indent_;
    (*this)(*e.expr);
    --indent_;
}

void ASTDumper::operator()(Const<BinaryExpr> &e)
{
    indent() << "BinaryExpr '" << e.op().text << "'";
    print_type(e);
    out << " (" << e.op().pos << ')';
    ++indent_;
    (*this)(*e.lhs);
    (*this)(*e.rhs);
    --indent_;
}

void ASTDumper::operator()(Const<QueryExpr> &e)
{
    indent() << "QueryExpr";
    print_type(e);
    ++indent_;
    (*this)(*e.query);
    --indent_;
}


/*===== Clause =======================================================================================================*/

void ASTDumper::operator()(Const<ErrorClause> &c)
{
    indent() << "ErrorClause '" << c.tok.text << "' (" << c.tok.pos << ')';
}

void ASTDumper::operator()(Const<SelectClause> &c)
{
    indent() << "SelectClause (" << c.tok.pos << ')';
    ++indent_;
    if (c.select_all)
        indent() << "* (" << c.select_all.pos << ')';
    for (auto &s : c.select) {
        if (s.second) {
            indent() << "AS '" << s.second.text << "' (" << s.second.pos << ')';
            ++indent_;
            (*this)(*s.first);
            --indent_;
        } else {
            (*this)(*s.first);
        }
    }
    --indent_;
}

void ASTDumper::operator()(Const<FromClause> &c)
{
    indent() << "FromClause (" << c.tok.pos << ')';
    ++indent_;
    for (auto f : c.from) {
        if (f.alias) {
            indent() << "AS '" << f.alias.text << "' (" << f.alias.pos << ')';
            ++indent_;
            if (auto tok = std::get_if<Token>(&f.source)) {
                indent() << tok->text << " (" << tok->pos << ')';
            } else if (auto stmt = std::get_if<Stmt*>(&f.source)) {
                (*this)(**stmt);
            } else {
                M_unreachable("illegal variant");
            }
            --indent_;
        } else {
            M_insist(std::holds_alternative<Token>(f.source), "nested statements require an alias");
            Token &tok = std::get<Token>(f.source);
            indent() << tok.text << " (" << tok.pos << ')';
        }
    }
    --indent_;
}

void ASTDumper::operator()(Const<WhereClause> &c)
{
    indent() << "WhereClause (" << c.tok.pos << ')';
    ++indent_;
    (*this)(*c.where);
    --indent_;
}


void ASTDumper::operator()(Const<GroupByClause> &c)
{
    indent() << "GroupByClause (" << c.tok.pos << ')';
    ++indent_;
    for (auto &[expr, alias] : c.group_by) {
        if (alias)
            indent() << "AS '" << alias.text << "' (" << alias.pos << ')';
        ++indent_;
        (*this)(*expr);
        --indent_;
    }
    --indent_;
}

void ASTDumper::operator()(Const<HavingClause> &c)
{
    indent() << "HavingClause (" << c.tok.pos << ')';
    ++indent_;
    (*this)(*c.having);
    --indent_;
}

void ASTDumper::operator()(Const<OrderByClause> &c)
{
    indent() << "OrderByClause (" << c.tok.pos << ')';
    ++indent_;
    for (auto &o : c.order_by) {
        indent() << (o.second ? "ASC" : "DESC");
        ++indent_;
        (*this)(*o.first);
        --indent_;
    }
    --indent_;
}

void ASTDumper::operator()(Const<LimitClause> &c)
{
    indent() << "LimitClause (" << c.tok.pos << ')';
    ++indent_;

    indent() << "LIMIT " << c.limit.text << " (" << c.limit.pos << ')';

    if (c.offset)
        indent() << "OFFSET " << c.offset.text << " (" << c.offset.pos << ')';

    --indent_;
}


/*===== Constraint ===================================================================================================*/

void ASTDumper::operator()(Const<PrimaryKeyConstraint> &c)
{
    indent() << "PrimaryKeyConstraint (" << c.tok.pos << ')';
}

void ASTDumper::operator()(Const<UniqueConstraint> &c)
{
    indent() << "UniqueConstraint (" << c.tok.pos << ')';
}

void ASTDumper::operator()(Const<NotNullConstraint> &c)
{
    indent() << "NotNullConstraint (" << c.tok.pos << ')';
}

void ASTDumper::operator()(Const<CheckConditionConstraint> &c)
{
    indent() << "CheckConditionConstraint (" << c.tok.pos << ')';
    ++indent_;
    (*this)(*c.cond);
    --indent_;
}

void ASTDumper::operator()(Const<ReferenceConstraint> &c)
{
    indent() << "ReferenceConstraint (" << c.tok.pos << ')';
    ++indent_;
    indent() << c.table_name.text << '(' << c.attr_name.text << ')';
    --indent_;
}


/*===== Instruction ==================================================================================================*/

void ASTDumper::operator()(Const<Instruction> &inst)
{
    out << "Instruction(" << inst.name;
    for (auto &arg : inst.args)
        out << ", " << arg;
    out << ')';
}


/*===== Stmt =========================================================================================================*/

void ASTDumper::operator()(Const<ErrorStmt> &s)
{
    indent() << "ErrorStmt: '" << s.tok.text << "' (" << s.tok.pos << ')';
}

void ASTDumper::operator()(Const<EmptyStmt> &s)
{
    indent() << "EmptyStmt: '" << s.tok.text << "' (" << s.tok.pos << ')';
}

void ASTDumper::operator()(Const<CreateDatabaseStmt> &s)
{
    indent() << "CreateDatabaseStmt: '" << s.database_name.text << "' (" << s.database_name.pos << ')';
}

void ASTDumper::operator()(Const<UseDatabaseStmt> &s)
{
    indent() << "UseDatabaseStmt: '" << s.database_name.text << "' (" << s.database_name.pos << ')';
}

void ASTDumper::operator()(Const<CreateTableStmt> &s)
{
    indent() << "CreateTableStmt: table " << s.table_name.text << " (" << s.table_name.pos << ')';
    ++indent_;
    indent() << "attributes";
    ++indent_;
    for (auto &attr : s.attributes) {
        indent() << attr->name.text << " : " << *attr->type << " (" << attr->name.pos << ')';
        ++indent_;
        for (auto &c : attr->constraints) {
            if (is<PrimaryKeyConstraint>(c)) {
                indent() << "PRIMARY KEY (" << c->tok.pos << ')';
            } else if (is<UniqueConstraint>(c)) {
                indent() << "UNIQUE (" << c->tok.pos << ')';
            } else if (is<NotNullConstraint>(c)) {
                indent() << "NOT NULL (" << c->tok.pos << ')';
            } else if (auto check = cast<CheckConditionConstraint>(c.get())) {
                indent() << "CHECK (" << c->tok.pos << ')';
                ++indent_;
                (*this)(*check->cond);
                --indent_;
            } else if (auto ref = cast<ReferenceConstraint>(c.get())) {
                indent() << "REFERENCES " << ref->table_name.text << '(' << ref->attr_name.text << ") (" << c->tok.pos
                         << ')';
            } else {
                M_unreachable("invalid constraint");
            }
        }
        --indent_;
    }
    --indent_;
    --indent_;
}

void ASTDumper::operator()(Const<SelectStmt> &s)
{
    indent() << "SelectStmt";
    ++indent_;

    (*this)(*s.select);

    if (s.from) (*this)(*s.from);
    if (s.where) (*this)(*s.where);
    if (s.group_by) (*this)(*s.group_by);
    if (s.having) (*this)(*s.having);
    if (s.order_by) (*this)(*s.order_by);
    if (s.limit) (*this)(*s.limit);

    --indent_;
}

void ASTDumper::operator()(Const<InsertStmt> &s)
{
    indent() << "InsertStmt: table " << s.table_name.text << " (" << s.table_name.pos << ')';
    ++indent_;
    indent() << "values";
    ++indent_;
    for (std::size_t idx = 0, end = s.tuples.size(); idx != end; ++idx) {
        indent() << '[' << idx << ']';
        const InsertStmt::tuple_t &v = s.tuples[idx];
        ++indent_;
        for (auto &e : v) {
            switch (e.first) {
                case InsertStmt::I_Default:
                    indent() << "DEFAULT";
                    break;

                case InsertStmt::I_Null:
                    indent() << "NULL";
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
    indent() << "UpdateStmt: table " << s.table_name.text << " (" << s.table_name.pos << ')';
    ++indent_;
    indent() << "set";
    ++indent_;
    for (auto &s : s.set) {
        indent() << s.first.text << " (" << s.first.pos << ')';
        ++indent_;
        (*this)(*s.second);
        --indent_;
    }
    --indent_;

    if (s.where) (*this)(*s.where);

    --indent_;
}

void ASTDumper::operator()(Const<DeleteStmt> &s)
{
    indent() << "DeleteStmt: table " << s.table_name.text << " (" << s.table_name.pos << ')';

    if (s.where) {
        ++indent_;
        (*this)(*s.where);
        --indent_;
    }
}

void ASTDumper::operator()(Const<DSVImportStmt> &s)
{
    indent() << "ImportStmt (DSV): table " << s.table_name.text << " (" << s.table_name.pos << ')';

    ++indent_;
    indent() << s.path.text << " (" << s.path.pos << ')';
    if (s.rows)
        indent() << "rows " << s.rows.text << " (" << s.rows.pos << ')';
    if (s.delimiter)
        indent() << "delimiter " << s.delimiter.text << " (" << s.delimiter.pos << ')';
    if (s.has_header)
        indent() << "HAS HEADER";
    if (s.skip_header)
        indent() << "SKIP HEADER";
    --indent_;
}

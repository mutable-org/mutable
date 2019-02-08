#include "parse/ASTDot.hpp"

#include <iomanip>

#define q(X) '"' << X << '"' // quote
#define id(X) q(std::hex << &X) // convert virtual address to identifier


using namespace db;


ASTDot::ASTDot(std::ostream &out)
    : out(out)
{
    out << GRAPH_TYPE << " ast\n{\n";
}

ASTDot::~ASTDot()
{
    out << '}' << std::endl;
}

/*--- Expressions ----------------------------------------------------------------------------------------------------*/

void ASTDot::operator()(Const<ErrorExpr> &e)
{
    out << std::hex
        << id(e) << " [label=\"ErrorExpr\"];\n";
}

void ASTDot::operator()(Const<Designator> &e)
{
    out << std::hex
        << id(e) << " [label=\"";
    if (e.has_table_name()) out << e.table_name.text << ".";
    out << e.attr_name.text << "\"];\n";
}

void ASTDot::operator()(Const<Constant> &e)
{
    out << id(e);

    if (e.is_string()) {
        std::string s(e.tok.text);
        auto sub = s.substr(1, s.length() - 2);
        out << " [label=\"\\\"" << sub.c_str() << "\\\"\"];\n";
    } else {
        out << " [label=\"" << e.tok.text << "\"];\n";
    }
}

void ASTDot::operator()(Const<FnApplicationExpr> &e)
{
    (*this)(*e.fn);
    out << id(e) << " [label=\"()\"];\n"
        << id(e) << EDGE << id(*e.fn) << ";\n";

    for (auto arg : e.args) {
        (*this)(*arg);
        out << id(e) << EDGE << id(*arg) << ";\n";
    }
}

void ASTDot::operator()(Const<UnaryExpr> &e)
{
    (*this)(*e.expr);
    out << id(e) << " [label=\"" << e.op.text << "\"];\n"
        << id(e) << EDGE << id(*e.expr) << ";\n";
}

void ASTDot::operator()(Const<BinaryExpr> &e)
{
    (*this)(*e.lhs);
    (*this)(*e.rhs);
    out << id(e) << " [label=\"" << e.op.text << "\"];\n"
        << id(e) << EDGE << id(*e.lhs) << ";\n"
        << id(e) << EDGE << id(*e.rhs) << ";\n";
}

/*--- Clauses --------------------------------------------------------------------------------------------------------*/

void ASTDot::operator()(Const<ErrorClause> &c)
{
    out << id(c) << " [label=\"ErrorClause\"];\n";
}

void ASTDot::operator()(Const<SelectClause> &c)
{
    out << id(c) << " [label=\"SELECT\"];\n";
    if (c.select_all) {
        out << q(std::hex << c << '*') << "[label=\"*\"];\n"
            << id(c) << EDGE << q(std::hex << c << '*') << ";\n";
    }
    for (auto s : c.select) {
        (*this)(*s.first);
        if (s.second) {
            out << id(s.second) << " [label=\"AS " << s.second.text << "\"];\n"
                << id(c) << EDGE << id(s.second) << ";\n"
                << id(s.second) << EDGE << id(*s.first) << ";\n";
        } else {
            out << id(c) << EDGE << id(*s.first) << ";\n";
        }
    }
}

void ASTDot::operator()(Const<FromClause> &c)
{
    out << id(c) << " [label=\"FROM\"];\n";
}

void ASTDot::operator()(Const<WhereClause> &c)
{
    out << id(c) << " [label=\"WHERE\"];\n";
}

void ASTDot::operator()(Const<GroupByClause> &c)
{
    out << id(c) << " [label=\"GROUP BY\"];\n";
}

void ASTDot::operator()(Const<HavingClause> &c)
{
    out << id(c) << " [label=\"HAVING\"];\n";
}

void ASTDot::operator()(Const<OrderByClause> &c)
{
    out << id(c) << " [label=\"ORDER BY\"];\n";
}

void ASTDot::operator()(Const<LimitClause> &c)
{
    out << id(c) << " [label=\"LIMIT\"];\n";
}

/*--- Statements -----------------------------------------------------------------------------------------------------*/

void ASTDot::operator()(Const<ErrorStmt> &s)
{
}

void ASTDot::operator()(Const<EmptyStmt> &s)
{
}

void ASTDot::operator()(Const<CreateDatabaseStmt> &s)
{
}

void ASTDot::operator()(Const<UseDatabaseStmt> &s)
{
}

void ASTDot::operator()(Const<CreateTableStmt> &s)
{
}

void ASTDot::operator()(Const<SelectStmt> &s)
{
    (*this)(*s.select);

    out << id(s) << " [label=\"SelectStmt\"];\n";
}

void ASTDot::operator()(Const<InsertStmt> &s)
{
}

void ASTDot::operator()(Const<UpdateStmt> &s)
{
}

void ASTDot::operator()(Const<DeleteStmt> &s)
{
}


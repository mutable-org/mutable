#include "parse/ASTDot.hpp"

#include <iomanip>
#include <mutable/catalog/Schema.hpp>
#include <sstream>

#define q(X) '"' << X << '"' // quote
#define id(X) q(std::hex << &X << std::dec) // convert virtual address to identifier


using namespace m;
using namespace m::ast;


ASTDot::ASTDot(std::ostream &out, int i)
    : out(out)
    , indent_(i)
{
    out << GRAPH_TYPE << " ast\n{";
    ++indent_;
    indent() << "forcelabels=true;";
    indent() << "graph [fontname = \"DejaVu Sans\"];";
    indent() << "node [fontname = \"DejaVu Sans\"];";
    indent() << "edge [fontname = \"DejaVu Sans\"];";
}

ASTDot::~ASTDot()
{
    --indent_;
    M_insist(indent_ == 0);
    out << "\n}" << std::endl;
}

void ASTDot::cluster(Const<Clause> &c, const char *name, const char *label, const char *color)
{
    indent() << "subgraph cluster_" << name << '_' << &c;
    indent() << '{';
    ++indent_;
    indent() << "style=\"rounded,filled\";";
    indent() << "color=\"" << color << "\";";
    indent() << "penwidth=\"4\";";
    indent() << id(c) << " [label=\"" << label << "\"];";

    (*this)(c);

    --indent_;
    indent() << '}';
}


/*--- Expressions ----------------------------------------------------------------------------------------------------*/

void ASTDot::operator()(Const<ErrorExpr> &e)
{
    indent() << id(e) << " [label=<<FONT COLOR=\"red\"><B>ErrorExpr</B></FONT>>];";
}

void ASTDot::operator()(Const<Designator> &e)
{
    indent() << id(e) << " [label=<<B>";

    if (e.has_explicit_table_name()) out << e.table_name.text << '.';
    out << e.attr_name.text << "</B>";
    if (e.has_type()) {
        std::ostringstream oss;
        oss << *e.type();
        out << "<FONT POINT-SIZE=\"11\"><I> : " << html_escape(oss.str());
        if (auto pt = cast<const PrimitiveType>(e.type()))
            out << "<SUB>" << (pt->is_scalar() ? "s" : "v") << "</SUB>";
        out << "</I></FONT>";
    }
    out << ">];";

    /* Dot edge to table. */
    const auto &t = e.target();
    if (auto val = std::get_if<const Attribute*>(&t)) {
        const Attribute &A = **val;
        indent() << id(e) << EDGE << A.table.name << ':' << A.name
            << " [style=\"dashed\",dir=\"forward\",color=\"#404040\"];";
    } else if (auto val = std::get_if<const Expr*>(&t)) {
        const Expr *expr = *val;
        indent() << id(e) << EDGE << id(*expr) << " [style=\"dashed\",dir=\"forward\",color=\"crimson\"];";
    }
}

void ASTDot::operator()(Const<Constant> &e)
{
    indent() << id(e) << " [label=<<B>";

    if (e.is_string()) {
        out << html_escape(e.tok.text);
    } else {
        out << e.tok.text;
    }
    out << "</B>";

    if (e.has_type()) {
        std::ostringstream oss;
        oss << *e.type();
        out << "<FONT POINT-SIZE=\"11\"><I> : " << html_escape(oss.str()) << "</I></FONT>";
    }

    out << ">];";
}

void ASTDot::operator()(Const<FnApplicationExpr> &e)
{
    (*this)(*e.fn);
    indent() << id(e) << " [label=<()";

    if (e.has_type()) {
        std::ostringstream oss;
        oss << *e.type();
        out << "<FONT POINT-SIZE=\"11\"><I> : " << html_escape(oss.str()) << "</I></FONT>";
    }

    out << ">];";
    indent() << id(e) << EDGE << id(*e.fn) << ';';

    for (auto &arg : e.args) {
        (*this)(*arg);
        indent() << id(e) << EDGE << id(*arg) << ';';
    }
}

void ASTDot::operator()(Const<UnaryExpr> &e)
{
    (*this)(*e.expr);
    indent() << id(e) << " [label=<" << html_escape(e.op().text);

    if (e.has_type()) {
        std::ostringstream oss;
        oss << *e.type();
        out << "<FONT POINT-SIZE=\"11\"><I> : " << html_escape(oss.str()) << "</I></FONT>";
    }

    out << ">];";
    indent() << id(e) << EDGE << id(*e.expr) << ';';
}

void ASTDot::operator()(Const<BinaryExpr> &e)
{
    (*this)(*e.lhs);
    (*this)(*e.rhs);
    indent() << id(e) << " [label=<" << html_escape(e.op().text);

    if (e.has_type()) {
        std::ostringstream oss;
        oss << *e.type();
        out << "<FONT POINT-SIZE=\"11\"><I> : " << html_escape(oss.str()) << "</I></FONT>";
    }

    out << ">];";
    indent() << id(e) << EDGE << id(*e.lhs) << ';';
    indent() << id(e) << EDGE << id(*e.rhs) << ';';
}

void ASTDot::operator()(Const<QueryExpr> &e)
{
    (*this)(*e.query);
    indent() << id(e) << " [label=<QueryExpr";

    if (e.has_type()) {
        std::ostringstream oss;
        oss << *e.type();
        out << "<FONT POINT-SIZE=\"11\"><I> : " << html_escape(oss.str()) << "</I></FONT>";
    }

    out << ">];";
    indent() << id(e) << EDGE << id(*e.query) << ';';
}


/*--- Clauses --------------------------------------------------------------------------------------------------------*/

void ASTDot::operator()(Const<ErrorClause> &c)
{
    indent() << id(c) << " [label=\"ErrorClause\"];";
}

void ASTDot::operator()(Const<SelectClause> &c)
{
    if (c.select_all) {
        out << '\n';
        indent() << q(std::hex << c << '*') << "[label=\"*\"];";
        indent() << id(c) << EDGE << q(std::hex << c << '*') << ';';
    }
    for (auto &s : c.select) {
        out << '\n';
        (*this)(*s.first);
        if (s.second) {
            indent() << id(s.second) << " [label=\"AS " << s.second.text << "\"];";
            indent() << id(c) << EDGE << id(s.second) << ';';
            indent() << id(s.second) << EDGE << id(*s.first) << ';';
        } else {
            indent() << id(c) << EDGE << id(*s.first) << ';';
        }
    }
}

void ASTDot::operator()(Const<FromClause> &c)
{
    for (auto &t : c.from) {
        out << '\n';
        if (auto name = std::get_if<Token>(&t.source)) {
            if (t.alias) {
                indent() << id(t.alias) << " [label=\"AS " << t.alias.text << "\"];";
                indent() << id (*name) << " [label=\"" << name->text << "\"];";
                indent() << id(c) << EDGE << id(t.alias) << EDGE << id(*name) << ';';
            } else {
                indent() << id(*name) << " [label=\"" << name->text << "\"];";
                indent() << id(c) << EDGE << id(*name) << ';';
            }
        } else if (auto stmt = std::get_if<Stmt*>(&t.source)) {
            M_insist(t.alias, "nested statements must have an alias");
            indent() << id(t.alias) << " [label=\"AS " << t.alias.text << "\"];";
            (*this)(**stmt);
            indent() << id(c) << EDGE << id(t.alias) << EDGE << id(**stmt) << ';';
        } else {
            M_unreachable("invalid variant");
        }
        if (t.has_table()) {
            M_insist(std::holds_alternative<Token>(t.source));
            auto &name = std::get<Token>(t.source);
            auto &R = t.table();
            indent() << id(name) << EDGE << R.name << ":n [dir=\"forward\",color=\"#404040\"];";
        }
    }
}

void ASTDot::operator()(Const<WhereClause> &c)
{
    out << '\n';
    (*this)(*c.where);
    indent() << id(c) << EDGE << id(*c.where) << ';';
}

void ASTDot::operator()(Const<GroupByClause> &c)
{
    for (auto &[grp, alias] : c.group_by) {
        out << '\n';
        (*this)(*grp);
        indent() << id(c) << EDGE << id(*grp) << ';';
    }
}

void ASTDot::operator()(Const<HavingClause> &c)
{
    out << '\n';
    (*this)(*c.having);
    indent() << id(c) << EDGE << id(*c.having) << ';';
}

void ASTDot::operator()(Const<OrderByClause> &c)
{
    for (auto &o : c.order_by) {
        out << '\n';
        if (o.second)
            indent() << id(o.second) << " [label=\"ASC\"];";
        else
            indent() << id(o.second) << " [label=\"DESC\"];";
        indent() << id(c) << EDGE << id(o.second) << ';';
        (*this)(*o.first);
        indent() << id(o.second) << EDGE << id(*o.first) << ';';
    }
}

void ASTDot::operator()(Const<LimitClause> &c)
{
    out << '\n';
    indent() << id(c.limit) << " [label=<<B>" << c.limit.text << "</B>>];";
    indent() << id(c) << EDGE << id(c.limit) << ';';

    if (c.offset) {
        out << '\n';
        indent() << id(c.offset) << " [label=<OFFSET <B>" << c.offset.text << "</B>>];";
        indent() << id(c) << EDGE << id(c.offset) << ';';
    }
}


/*--- Constraints ----------------------------------------------------------------------------------------------------*/

void ASTDot::operator()(Const<PrimaryKeyConstraint> &c)
{
    indent() << id(c) << " [label=<<B>PRIMARY KEY</B>>];";
}

void ASTDot::operator()(Const<UniqueConstraint> &c)
{
    indent() << id(c) << " [label=<<B>UNIQUE</B>>];";
}

void ASTDot::operator()(Const<NotNullConstraint> &c)
{
    indent() << id(c) << " [label=<<B>NOT NULL</B>>];";
}

void ASTDot::operator()(Const<CheckConditionConstraint> &c)
{
    (*this)(*c.cond);
    indent() << id(c) << " [label=<<B>CHECK()</B>>];";
    indent() << id(c) << EDGE << id(*c.cond) << ';';
}

void ASTDot::operator()(Const<ReferenceConstraint> &c)
{
    indent() << id(c) << " [label=<<B>REFERENCES " << c.table_name.text << '(' << c.attr_name.text << ")</B>>];";
}


/*----- Instruction --------------------------------------------------------------------------------------------------*/

void ASTDot::operator()(const Instruction &inst)
{
    out << '\n';
    std::ostringstream oss;
    indent() << id(inst) << " [label=\"" << inst.tok.text << "\"];";
}


/*--- Stmt -----------------------------------------------------------------------------------------------------------*/

void ASTDot::operator()(Const<ErrorStmt>&)
{
    // TODO implement
}

void ASTDot::operator()(Const<EmptyStmt>&)
{
    // TODO implement
}

void ASTDot::operator()(Const<CreateDatabaseStmt>&)
{
    // TODO implement
}

void ASTDot::operator()(Const<UseDatabaseStmt>&)
{
    // TODO implement
}

void ASTDot::operator()(Const<CreateTableStmt>&)
{
    // TODO implement
}

void ASTDot::operator()(Const<SelectStmt> &s)
{
    out << '\n';
    std::ostringstream oss;
    indent() << id(s) << " [label=\"SelectStmt\"];";

    if (s.from) {
        /* Dot the accessed tables first. */
        indent() << "subgraph sources";
        indent() << '{';
        ++indent_;
        if (auto f = cast<FromClause>(s.from.get())) {
            for (auto &t : f->from) {
                if (t.has_table()) {
                    auto &R = t.table();

                    indent() << R.name << "[shape=none,style=filled,fillcolor=white,label=<";
                    ++indent_;
                    indent() << "<TABLE>";
                    ++indent_;
                    indent() << "<TR><TD BORDER=\"0\"><B>" << R.name << "</B></TD></TR>";

                    for (auto &A : R) {
                        oss.str("");
                        oss << *A.type;
                        indent() << "<TR><TD PORT=\"" << A.name << "\">" << A.name
                                 << "<FONT POINT-SIZE=\"11\"><I> : " << html_escape(oss.str()) << "</I></FONT>"
                                 << "</TD></TR>";
                    }

                    --indent_;
                    indent() << "</TABLE>";
                    --indent_;
                    indent() << ">];";
                }
            }
        }
        --indent_;
        indent() << "}\n";
    }

    cluster(*s.select, "select", "SELECT", "#e6194B20");
    indent() << id(s) << EDGE << id(*s.select) << ';';

#define DOT(NAME, LABEL, COLOR) \
    if (s.NAME) { \
        out << '\n'; \
        cluster(*s.NAME, #NAME, LABEL, COLOR); \
        indent() << id(s) << EDGE << id(*s.NAME) << ';'; \
    }
    DOT(from,     "FROM",     "#bfef4550");
    DOT(where,    "WHERE",    "#42d4f430");
    DOT(group_by, "GROUP BY", "#3cb44b30");
    DOT(having,   "HAVING",   "#aaffc350");
    DOT(order_by, "ORDER BY", "#ffe11950");
    DOT(limit,    "LIMIT",    "#80800040");
#undef DOT
}

void ASTDot::operator()(Const<InsertStmt>&)
{
    // TODO implement
}

void ASTDot::operator()(Const<UpdateStmt>&)
{
    // TODO implement
}

void ASTDot::operator()(Const<DeleteStmt>&)
{
    // TODO implement
}

void ASTDot::operator()(Const<DSVImportStmt>&)
{
    // TODO implement
}

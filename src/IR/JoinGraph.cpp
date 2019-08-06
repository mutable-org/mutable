#include "IR/JoinGraph.hpp"

#include "catalog/Schema.hpp"
#include "parse/AST.hpp"
#include "parse/ASTVisitor.hpp"
#include "util/macro.hpp"
#include <set>
#include <unordered_map>


using namespace db;


/** Compute the condition of the WHERE clause of a statement in conjunctive normal form. */
cnf::CNF get_cnf(const Stmt &stmt)
{
    cnf::CNFGenerator gen;
    gen(stmt);
    return gen.get();
}

/** Helper structure to extract the tables required by an expression. */
struct GetTables : ConstASTVisitor
{
    private:
    std::set<const char*> tables_;

    public:
    GetTables() { }

    std::set<const char*> get() { return std::move(tables_); }

    /* Expr */
    void operator()(Const<Expr> &e) { e.accept(*this); }
    void operator()(Const<ErrorExpr>&) { unreachable("graph must not contain errors"); }

    void operator()(Const<Designator> &e) { tables_.emplace(e.get_table_name()); }

    void operator()(Const<Constant>&) { /* nothing to be done */ }

    void operator()(Const<FnApplicationExpr> &e) {
        (*this)(*e.fn);
        for (auto arg : e.args)
            (*this)(*arg);
    }

    void operator()(Const<UnaryExpr> &e) { (*this)(*e.expr); }

    void operator()(Const<BinaryExpr> &e) { (*this)(*e.lhs); (*this)(*e.rhs); }

    /* Clauses */
    void operator()(Const<Clause>&) { unreachable("not implemented"); }
    void operator()(Const<ErrorClause>&) { unreachable("not implemented"); }
    void operator()(Const<SelectClause>&) { unreachable("not implemented"); }
    void operator()(Const<FromClause>&) { unreachable("not implemented"); }
    void operator()(Const<WhereClause>&) { unreachable("not implemented"); }
    void operator()(Const<GroupByClause>&) { unreachable("not implemented"); }
    void operator()(Const<HavingClause>&) { unreachable("not implemented"); }
    void operator()(Const<OrderByClause>&) { unreachable("not implemented"); }
    void operator()(Const<LimitClause>&) { unreachable("not implemented"); }

    /* Statements */
    void operator()(Const<Stmt>&) { unreachable("no implemented"); }
    void operator()(Const<ErrorStmt>&) { unreachable("not implemented"); }
    void operator()(Const<EmptyStmt>&) { unreachable("not implemented"); }
    void operator()(Const<CreateDatabaseStmt>&) { unreachable("not implemented"); }
    void operator()(Const<UseDatabaseStmt>&) { unreachable("not implemented"); }
    void operator()(Const<CreateTableStmt>&) { unreachable("not implemented"); }
    void operator()(Const<SelectStmt>&) { unreachable("not implemented"); }
    void operator()(Const<InsertStmt>&) { unreachable("not implemented"); }
    void operator()(Const<UpdateStmt>&) { unreachable("not implemented"); }
    void operator()(Const<DeleteStmt>&) { unreachable("not implemented"); }
};

/** Given a clause of a CNF formula, compute the tables that are required by this clause. */
auto get_tables(const cnf::Clause &clause)
{
    using std::begin, std::end;
    GetTables GT;
    for (auto p : clause)
        GT(*p.expr());
    return GT.get();
}

/*======================================================================================================================
 * GraphBuilder
 *
 * An AST Visitor that constructs the join graph.
 *====================================================================================================================*/

struct db::GraphBuilder : ConstASTVisitor
{
    private:
    std::unique_ptr<JoinGraph> graph_; ///< the constructed join graph
    std::unordered_map<const char*, DataSource*> aliases; ///< maps aliases to data sources

    public:
    GraphBuilder() : graph_(std::make_unique<JoinGraph>()) { }

    std::unique_ptr<JoinGraph> get() { return std::move(graph_); }

    /* Expr */
    void operator()(Const<Expr> &e) { e.accept(*this); }
    void operator()(Const<ErrorExpr>&) { unreachable("graph must not contain errors"); }

    void operator()(Const<Designator> &e) {
        unreachable("not implemented");
        (void) e;
    }

    void operator()(Const<Constant> &e) {
        unreachable("not implemented");
        (void) e;
    }

    void operator()(Const<FnApplicationExpr> &e) {
        unreachable("not implemented");
        (void) e;
    }

    void operator()(Const<UnaryExpr> &e) {
        unreachable("not implemented");
        (void) e;
    }

    void operator()(Const<BinaryExpr> &e) {
        unreachable("not implemented");
        (void) e;
    }

    /* Clauses */
    void operator()(Const<Clause> &c) { c.accept(*this); }
    void operator()(Const<ErrorClause>&) { unreachable("graph must not contain errors"); }
    void operator()(Const<SelectClause>&) { unreachable("not implemented"); }
    void operator()(Const<WhereClause>&) { unreachable("not implemented"); }
    void operator()(Const<GroupByClause>&) { unreachable("not implemented"); }
    void operator()(Const<HavingClause>&) { unreachable("not implemented"); }
    void operator()(Const<OrderByClause>&) { unreachable("not implemented"); }
    void operator()(Const<LimitClause>&) { unreachable("not implemented"); }

    void operator()(Const<FromClause> &c) {
        for (auto &tbl : c.from) {
            if (auto tok = std::get_if<Token>(&tbl.source)) {
                /* Create a new base table. */
                insist(tbl.has_relation());
                Token alias = tbl.alias ? tbl.alias : *tok;
                auto base = new BaseTable(tbl.relation(), alias.text);
                aliases.emplace(alias.text, base);
                graph_->sources.emplace_back(base);
            } else if (auto stmt = std::get_if<Stmt*>(&tbl.source)) {
                /* TODO Create a join graph for the nested statement. */
                insist(tbl.alias.text, "every nested statement requires an alias");
                auto q = new Query(tbl.alias.text);
                insist(tbl.alias);
                aliases.emplace(tbl.alias.text, q);
                graph_->sources.emplace_back(q);
            } else {
                unreachable("invalid variant");
            }
        }
    }

    /* Statements */
    void operator()(Const<Stmt> &s) { s.accept(*this); }
    void operator()(Const<ErrorStmt>&) { unreachable("graph must not contain errors"); }

    void operator()(Const<EmptyStmt>&) { /* nothing to be done */ }

    void operator()(Const<CreateDatabaseStmt> &s) {
        /* TODO: implement */
    }

    void operator()(Const<UseDatabaseStmt> &s) {
        /* TODO: implement */
    }

    void operator()(Const<CreateTableStmt> &s) {
        /* TODO: implement */
    }

    void operator()(Const<SelectStmt> &s) {
        /* Compute CNF of WHERE clause. */
        auto cnf = get_cnf(s);

        /* Create data sources. */
        (*this)(*s.from);

        /* Dissect CNF into joins and filters. */
        for (auto &clause : cnf) {
            auto tables = get_tables(clause);
            if (tables.size() == 0) {
                // TODO
            } else if (tables.size() == 1) {
                /* This clause is a filter condition. */
                auto t = *begin(tables);
                auto ds = aliases.at(t);
                ds->update_filter(cnf::CNF({clause}));
            } else {
                /* This clause is a join condition. */
                Join::sources_t sources;
                for (auto t : tables)
                    sources.emplace_back(aliases.at(t));
                auto J = graph_->joins.emplace_back(new Join(cnf::CNF({clause}), sources));
                for (auto ds : J->sources())
                    ds->add_join(J);
            }
        }

        /* TODO Create joins in the join graph. */
        /* TODO Add filters to data sources. */
    }

    void operator()(Const<InsertStmt> &s) {
        /* TODO: implement */
    }

    void operator()(Const<UpdateStmt> &s) {
        /* TODO: implement */
    }

    void operator()(Const<DeleteStmt> &s) {
        /* TODO: implement */
    }
};

/*======================================================================================================================
 * JoinGraph
 *====================================================================================================================*/

JoinGraph::~JoinGraph()
{
    for (auto src : sources)
        delete src;
    for (auto j : joins)
        delete j;
}

std::unique_ptr<JoinGraph> JoinGraph::Build(const Stmt *stmt)
{
    GraphBuilder builder;
    builder(*stmt);
    return builder.get();
}

void JoinGraph::dot(std::ostream &out) const
{
#define q(X) '"' << X << '"' // quote
#define id(X) q(std::hex << &X) // convert virtual address to identifier
    out << "graph join_graph\n{\n"
        << "    forcelabels=true;\n"
        << "    overlap=false;\n";

    for (auto ds : sources) {
        out << "    " << id(*ds) << " [label=<<B>" << ds->alias() << "</B>";
        if (ds->filter().size())
            out << "<BR/><FONT COLOR=\"0.0 0.0 0.25\" POINT-SIZE=\"10\">" << ds->filter() << "</FONT>";
        out << ">,style=filled,fillcolor=\"0.0 0.0 0.8\"];\n";
    }

    for (auto j : joins) {
        out << "    " << id(*j) << " [label=<" << html_escape(to_string(j->condition())) << ">,style=filled,fillcolor=\"0.0 0.0 0.95\"];\n";
        for (auto ds : j->sources())
            out << "    " << id(*j) << " -- " << id(*ds) << ";\n";
    }

    out << '}' << std::endl;
#undef id
#undef q
}

void JoinGraph::dump(std::ostream &out) const
{
    out << "JoinGraph {\n  sources:";
    for (auto src : sources) {
        out << "\n    ";
        if (auto q = cast<Query>(src))
            out << "(Q)";
        else {
            auto bt = as<BaseTable>(src);
            out << bt->relation().name;
        }
        out << ' ' << src->alias() << "  " << src->filter();
    }
    out << "\n  joins:";
    for (auto j : joins) {
        out << "\n    {";
        auto &srcs = j->sources();
        for (auto it = srcs.begin(), end = srcs.end(); it != end; ++it) {
            if (it != srcs.begin()) out << ' ';
            out << (*it)->alias();
        }
        out << "}  " << j->condition();
    }
    out << "\n}" << std::endl;
}
void JoinGraph::dump() const { dump(std::cerr); }

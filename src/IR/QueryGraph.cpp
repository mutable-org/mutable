#include "IR/QueryGraph.hpp"

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
    using ConstASTVisitor::operator();

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
    void operator()(Const<ErrorClause>&) { unreachable("not implemented"); }
    void operator()(Const<SelectClause>&) { unreachable("not implemented"); }
    void operator()(Const<FromClause>&) { unreachable("not implemented"); }
    void operator()(Const<WhereClause>&) { unreachable("not implemented"); }
    void operator()(Const<GroupByClause>&) { unreachable("not implemented"); }
    void operator()(Const<HavingClause>&) { unreachable("not implemented"); }
    void operator()(Const<OrderByClause>&) { unreachable("not implemented"); }
    void operator()(Const<LimitClause>&) { unreachable("not implemented"); }

    /* Statements */
    void operator()(Const<ErrorStmt>&) { unreachable("not implemented"); }
    void operator()(Const<EmptyStmt>&) { unreachable("not implemented"); }
    void operator()(Const<CreateDatabaseStmt>&) { unreachable("not implemented"); }
    void operator()(Const<UseDatabaseStmt>&) { unreachable("not implemented"); }
    void operator()(Const<CreateTableStmt>&) { unreachable("not implemented"); }
    void operator()(Const<SelectStmt>&) { unreachable("not implemented"); }
    void operator()(Const<InsertStmt>&) { unreachable("not implemented"); }
    void operator()(Const<UpdateStmt>&) { unreachable("not implemented"); }
    void operator()(Const<DeleteStmt>&) { unreachable("not implemented"); }
    void operator()(Const<DSVImportStmt>&) { }
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

/** Helper structure to extract the aggregate functions. */
struct GetAggregates : ConstASTVisitor
{
    using ConstASTVisitor::operator();

    private:
    std::vector<const Expr*> aggregates_;

    public:
    GetAggregates() { }

    auto get() { return std::move(aggregates_); }

    /* Expr */
    void operator()(Const<Expr> &e) { e.accept(*this); }
    void operator()(Const<ErrorExpr>&) { unreachable("graph must not contain errors"); }

    void operator()(Const<Designator>&) { /* nothing to be done */ }
    void operator()(Const<Constant>&) { /* nothing to be done */ }
    void operator()(Const<UnaryExpr> &e) { (*this)(*e.expr); }
    void operator()(Const<BinaryExpr> &e) { (*this)(*e.lhs); (*this)(*e.rhs); }

    void operator()(Const<FnApplicationExpr> &e) {
        insist(e.has_function());
        if (e.get_function().is_aggregate()) { // test that this is an aggregation
            using std::find_if, std::to_string;
            std::string str = to_string(e);
            auto exists = [&](const Expr *agg) { return to_string(*agg) == str; };
            if (find_if(aggregates_.begin(), aggregates_.end(), exists) == aggregates_.end()) // test if already present
                aggregates_.push_back(&e);
        }
    }

    /* Clauses */
    void operator()(Const<ErrorClause>&) { unreachable("not implemented"); }
    void operator()(Const<FromClause>&) { unreachable("not implemented"); }
    void operator()(Const<WhereClause>&) { unreachable("not implemented"); }
    void operator()(Const<GroupByClause>&) { unreachable("not implemented"); }
    void operator()(Const<LimitClause>&) { unreachable("not implemented"); }


    void operator()(Const<SelectClause> &c) {
        for (auto s : c.select)
            (*this)(*s.first);
    }

    void operator()(Const<HavingClause> &c) { (*this)(*c.having); }

    void operator()(Const<OrderByClause> &c) {
        for (auto o : c.order_by)
            (*this)(*o.first);
    }

    /* Statements */
    void operator()(Const<ErrorStmt>&) { unreachable("not implemented"); }
    void operator()(Const<EmptyStmt>&) { unreachable("not implemented"); }
    void operator()(Const<CreateDatabaseStmt>&) { unreachable("not implemented"); }
    void operator()(Const<UseDatabaseStmt>&) { unreachable("not implemented"); }
    void operator()(Const<CreateTableStmt>&) { unreachable("not implemented"); }
    void operator()(Const<InsertStmt>&) { unreachable("not implemented"); }
    void operator()(Const<UpdateStmt>&) { unreachable("not implemented"); }
    void operator()(Const<DeleteStmt>&) { unreachable("not implemented"); }
    void operator()(Const<DSVImportStmt>&) { }

    void operator()(Const<SelectStmt> &s) {
        if (s.having) (*this)(*s.having);
        (*this)(*s.select);
        if (s.order_by) (*this)(*s.order_by);
    }
};

/** Given a select statement, extract the aggregates to compute while grouping. */
auto get_aggregates(const Stmt &stmt)
{
    GetAggregates GA;
    GA(stmt);
    return GA.get();
}

/*======================================================================================================================
 * GraphBuilder
 *
 * An AST Visitor that constructs the query graph.
 *====================================================================================================================*/

struct db::GraphBuilder : ConstASTVisitor
{
    private:
    std::unique_ptr<QueryGraph> graph_; ///< the constructed query graph
    std::unordered_map<const char*, DataSource*> aliases; ///< maps aliases to data sources

    public:
    GraphBuilder() : graph_(std::make_unique<QueryGraph>()) { }

    std::unique_ptr<QueryGraph> get() { return std::move(graph_); }

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
                insist(tbl.has_table());
                Token alias = tbl.alias ? tbl.alias : *tok;
                auto base = new BaseTable(tbl.table(), alias.text);
                aliases.emplace(alias.text, base);
                graph_->sources_.emplace_back(base);
            } else if (auto stmt = std::get_if<Stmt*>(&tbl.source)) {
                insist(tbl.alias.text, "every nested statement requires an alias");
                if (auto select = cast<SelectStmt>(*stmt)) {
                    /* Create a graph for subquery. */
                    GraphBuilder builder;
                    builder(*select);
                    auto graph = builder.get();
                    auto q = new Query(tbl.alias.text, graph.release());
                    insist(tbl.alias);
                    aliases.emplace(tbl.alias.text, q);
                    graph_->sources_.emplace_back(q);
                } else
                    unreachable("invalid variant");
            } else {
                unreachable("invalid variant");
            }
        }
    }

    /* Statements */
    void operator()(Const<Stmt> &s) { s.accept(*this); }
    void operator()(Const<ErrorStmt>&) { unreachable("graph must not contain errors"); }

    void operator()(Const<EmptyStmt>&) { /* nothing to be done */ }

    void operator()(Const<CreateDatabaseStmt>&) {
        /* TODO: implement */
    }

    void operator()(Const<UseDatabaseStmt>&) {
        /* TODO: implement */
    }

    void operator()(Const<CreateTableStmt>&) {
        /* TODO: implement */
    }

    void operator()(Const<SelectStmt> &s) {
        /* Compute CNF of WHERE clause. */
        auto cnf = get_cnf(s);

        /* Create data sources. */
        if (s.from)
            (*this)(*s.from);

        /* Dissect CNF into joins and filters. */
        for (auto &clause : cnf) {
            auto tables = get_tables(clause);
            if (tables.size() == 0) {
                /* This clause is a filter and constant.  It applies to all data sources. */
                for (auto &alias : aliases)
                    alias.second->update_filter(cnf::CNF{clause});
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
                auto J = graph_->joins_.emplace_back(new Join(cnf::CNF({clause}), sources));
                for (auto ds : J->sources())
                    ds->add_join(J);
            }
        }

        /* Add groups. */
        if (s.group_by) {
            auto G = as<GroupByClause>(s.group_by);
            graph_->group_by_.insert(graph_->group_by_.begin(), G->group_by.begin(), G->group_by.end());
        }

        /* Add aggregates. */
        graph_->aggregates_ = get_aggregates(s);

        /* Add projections */
        {
            auto S = as<SelectClause>(s.select);
            for (auto s : S->select)
                graph_->projections_.emplace_back(s.first, s.second.text);
        }

        /* Add order by. */
        if (s.order_by) {
            auto O = as<OrderByClause>(s.order_by);
            for (auto o : O->order_by)
                graph_->order_by_.emplace_back(o.first, o.second);
        }

        /* Add limit. */
        if (s.limit) {
            auto L = as<LimitClause>(s.limit);
            errno = 0;
            graph_->limit_.limit = strtoull(L->limit.text, nullptr, 10);
            insist(errno == 0);
            if (L->offset) {
                graph_->limit_.offset = strtoull(L->offset.text, nullptr, 10);
                insist(errno == 0);
            }
        }
    }

    void operator()(Const<InsertStmt>&) {
        /* TODO: implement */
    }

    void operator()(Const<UpdateStmt>&) {
        /* TODO: implement */
    }

    void operator()(Const<DeleteStmt>&) {
        /* TODO: implement */
    }

    void operator()(Const<DSVImportStmt>&) { }
};

/*======================================================================================================================
 * QueryGraph
 *====================================================================================================================*/

QueryGraph::~QueryGraph()
{
    for (auto src : sources_)
        delete src;
    for (auto j : joins_)
        delete j;
}

std::unique_ptr<QueryGraph> QueryGraph::Build(const Stmt &stmt)
{
    GraphBuilder builder;
    builder(stmt);
    return builder.get();
}

void QueryGraph::dot(std::ostream &out) const
{
#define q(X) '"' << X << '"' // quote
#define id(X) q(std::hex << &X << std::dec) // convert virtual address to identifier
    out << "graph query_graph\n{\n"
        << "    forcelabels=true;\n"
        << "    overlap=false;\n"
        << "    labeljust=\"l\";\n"
        << "    graph [compound=true];\n"
        << "    graph [fontname = \"DejaVu Sans\"];\n"
        << "    node [fontname = \"DejaVu Sans\"];\n"
        << "    edge [fontname = \"DejaVu Sans\"];\n";

    out << "  subgraph cluster_" << this << " {\n"
        << "    hideous [label=\"\",style=\"invis\"];\n";

    for (auto ds : sources_) {
        out << "    " << id(*ds) << " [label=<<B>" << ds->alias() << "</B>";
        if (ds->filter().size())
            out << "<BR/><FONT COLOR=\"0.0 0.0 0.25\" POINT-SIZE=\"10\">"
                << html_escape(to_string(ds->filter()))
                << "</FONT>";
        out << ">,style=filled,fillcolor=\"0.0 0.0 0.8\"];\n";
    }

    for (auto j : joins_) {
        out << "    " << id(*j) << " [label=<" << html_escape(to_string(j->condition())) << ">,style=filled,fillcolor=\"0.0 0.0 0.95\"];\n";
        for (auto ds : j->sources())
            out << "    " << id(*j) << " -- " << id(*ds) << ";\n";
    }

    out << "    label=<"
        << "<TABLE BORDER=\"0\" CELLPADDING=\"0\" CELLSPACING=\"0\">\n";

    /* Limit */
    if (limit_.limit != 0 or limit_.offset != 0) {
        out << "             <TR><TD ALIGN=\"LEFT\">\n"
            << "               <B>λ</B><FONT POINT-SIZE=\"9\">"
            << limit_.limit << ", " << limit_.offset
            << "</FONT>\n"
            << "             </TD></TR>\n";
    }

    /* Order by */
    if (order_by_.size()) {
        out << "             <TR><TD ALIGN=\"LEFT\">\n"
            << "               <B>ω</B><FONT POINT-SIZE=\"9\">";
        for (auto it = order_by_.begin(), end = order_by_.end(); it != end; ++it) {
            if (it != order_by_.begin()) out << ", ";
            out << html_escape(to_string(*it->first));
            out << ' ' << (it->second ? "ASC" : "DESC");
        }
        out << "</FONT>\n"
            << "             </TD></TR>\n";
    }

    /* Projections */
    out << "             <TR><TD ALIGN=\"LEFT\">\n"
        << "               <B>π</B><FONT POINT-SIZE=\"9\">";
    for (auto it = projections_.begin(), end = projections_.end(); it != end; ++it) {
        if (it != projections_.begin()) out << ", ";
        out << html_escape(to_string(*it->first));
        if (it->second)
            out << " AS " << html_escape(it->second);
    }
    out << "</FONT>\n"
        << "             </TD></TR>\n";

    /* Group by and aggregates */
    if (not group_by_.empty() or not aggregates_.empty()) {
        out << "             <TR><TD ALIGN=\"LEFT\">\n"
            << "               <B>γ</B><FONT POINT-SIZE=\"9\">";
        for (auto it = group_by_.begin(), end = group_by_.end(); it != end; ++it) {
            if (it != group_by_.begin()) out << ", ";
            out << html_escape(to_string(**it));
        }
        if (not group_by_.empty() and not aggregates_.empty())
            out << ", ";
        for (auto it = aggregates_.begin(), end = aggregates_.end(); it != end; ++it) {
            if (it != aggregates_.begin()) out << ", ";
            out << html_escape(to_string(**it));
        }
        out << "</FONT>\n"
            << "             </TD></TR>\n";
    }

    out << "           </TABLE>\n"
        << "          >;\n"
        << "  }\n"
        << '}' << std::endl;
#undef id
#undef q
}

void QueryGraph::dump(std::ostream &out) const
{
    out << "QueryGraph {\n  sources:";
    for (auto src : sources_) {
        out << "\n    ";
        if (auto q = cast<Query>(src))
            out << "(Q)";
        else {
            auto bt = as<BaseTable>(src);
            out << bt->table().name;
        }
        out << ' ' << src->alias() << "  " << src->filter();
    }
    out << "\n  joins:";
    for (auto j : joins_) {
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
void QueryGraph::dump() const { dump(std::cerr); }

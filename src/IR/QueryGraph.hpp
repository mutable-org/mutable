#pragma once

#include "util/hash.hpp"
#include <mutable/IR/QueryGraph.hpp>
#include <mutable/parse/AST.hpp>


namespace m {

struct GraphBuilder;

/** Collects info of a subquery, i.e. the `GraphBuilder` holding the constructed subquery and all its associated
 * information, as well as the alias assigned to the subquery. */
struct SubqueryInfo
{
    const ast::QueryExpr &expr;
    std::unique_ptr<GraphBuilder> builder;
    const char *alias;

    SubqueryInfo(const ast::QueryExpr &expr, std::unique_ptr<GraphBuilder> builder, const char *alias)
        : expr(expr)
        , builder(std::move(builder))
        , alias(alias)
    { }
};

/** Collects information of a `cnf::Clause`. */
struct ClauseInfo
{
    unsigned binding_depth = 0;

    std::unordered_set<const char*> data_sources;
    std::vector<SubqueryInfo> nested_queries;

    ClauseInfo(const cnf::Clause &clause);

    bool is_constant() const { return data_sources.empty(); }
    bool is_selection() const { return data_sources.size() == 1; }
};

struct GraphBuilder : ast::ConstASTCommandVisitor
{
    ///> maps a `cnf::Clause` to its `ClauseInfo`
    using clause_map = std::unordered_map<cnf::Clause, ClauseInfo>;

    private:
    /*----- Fields tracking correlation information -----*/
    ///> the grouping keys of the statement
    std::vector<std::reference_wrapper<const ast::Expr>> existing_grouping_keys_;
    ///> additionally required grouping keys to perform decorrelation
    std::unordered_set<std::reference_wrapper<const ast::Expr>> additional_grouping_keys_;

    ///> to be handled by the current query
    clause_map bound_clauses_;
    ///> to be handled by an outer query
    clause_map deferred_clauses_;

    ///> the query graph that is being constructed
    std::unique_ptr<QueryGraph> graph_;
    ///> maps `DataSource` names/aliases to the `DataSource` instance
    std::unordered_map<const char*, std::reference_wrapper<DataSource>> named_sources_;
    ///> whether this graph needs grouping; either by explicily grouping or implicitly by using aggregations
    bool needs_grouping_ = false;

    public:
    GraphBuilder();

    ///> returns the constructed `QueryGraph`
    std::unique_ptr<QueryGraph> get() { return std::move(graph_); }

    /*----- Visitor methods -----*/
    using ConstASTCommandVisitor::operator();
    void operator()(Const<ast::Stmt> &s) { s.accept(*this); }
    void operator()(Const<ast::ErrorStmt>&) { M_unreachable("graph must not contain errors"); }
    void operator()(Const<ast::EmptyStmt>&) { /* nothing to be done */ }
    void operator()(Const<ast::CreateDatabaseStmt>&) { M_unreachable("not implemented"); }
    void operator()(Const<ast::UseDatabaseStmt>&) { M_unreachable("not implemented"); }
    void operator()(Const<ast::CreateTableStmt>&) { M_unreachable("not implemented"); }
    void operator()(Const<ast::SelectStmt> &s);
    void operator()(Const<ast::InsertStmt>&) { M_unreachable("not implemented"); }
    void operator()(Const<ast::UpdateStmt>&) { M_unreachable("not implemented"); }
    void operator()(Const<ast::DeleteStmt>&) { M_unreachable("not implemented"); }
    void operator()(Const<ast::DSVImportStmt>&) { M_unreachable("not implemented"); }

    /** Computes correlation information of \p clause.  Analyzes the entire clause for how it can be decorrelated.
     *
     *  1. If the query does not have any grouping, clauses can be trivially decorrelated by lifting the clause into the
     *     join condition of the dependent join.
     *
     * If the query contains grouping, more in depth analysis is performed:
     *
     *  2. If the clause contains only bound variables, it is an uncorrelated selection of the query.
     *  3. If the clause contains only free variables, it is an uncorrelated selection of an *outer* statement.
     *  4. If it contains both bound and free variables, it is correlated and is analyzed more closely. Correlated
     *     clauses can be decorrelated if all bound *expressions* are composable of the grouping keys specified in the
     *     query.  If it is not possible to compose all bound expressions from grouping keys, the clause cannot be
     *     decorrelated.
     *  5. A special exception are clauses of a single equi-predicate, where one side of the equi-predicate contains
     *     only bound variables and the other contains only free variables.  In that particular case, we can decorrelate
     *     the clause by introducing the bound expression as an additional grouping key to the query.
     */
    void process_selection(cnf::Clause &clause);
};

}

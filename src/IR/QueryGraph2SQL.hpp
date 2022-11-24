#pragma once

#include <functional>
#include <mutable/IR/CNF.hpp>
#include <mutable/parse/AST.hpp>


namespace m {

struct QueryGraph;

/** Translates a query graph in SQL. */
struct QueryGraph2SQL : private ast::ConstASTExprVisitor
{
    private:
    std::ostream &out_; ///< the output stream to write to
    const QueryGraph *graph_; /// the graph to translate
    bool after_grouping_ = false; ///< indicates whether a grouping is already performed

    public:
    QueryGraph2SQL(std::ostream &out) : out_(out) { }
    private:
    QueryGraph2SQL(std::ostream &out, const QueryGraph *graph) : out_(out), graph_(graph) { }
    QueryGraph2SQL(std::ostream &out, const QueryGraph *graph, bool after_grouping)
        : out_(out), graph_(graph), after_grouping_(after_grouping) { }

    public:
    /** Translates the given `QueryGraph` into SQL. Note that no semicolon is appended. */
    void translate(const QueryGraph&);
    void operator()(const QueryGraph &graph) { translate(graph); }

    private:
    /** Inserts a projection for the given `Expr` which is computed by a grouping operator. Adds an alias iff the
     * expression has to be renamed, e.g. due to a multiple use of `.` in mu*t*able which is not valid in SQL. */
    void insert_projection(const ast::Expr*);
    /** Translates a projection for the given pair of `Expr` and alias. Adds an alias iff none is specified and the
     * expression has to be renamed, e.g. due to a multiple use of `.` in mu*t*able which is not valid in SQL. */
    void translate_projection(std::pair<std::reference_wrapper<const ast::Expr>, const char*>);

    /** Checks whether the given target references an expression contained in the group_by clause. */
    bool references_group_by(ast::Designator::target_type);

    static const char * make_unique_alias();

    using ConstASTExprVisitor::operator();
#define DECLARE(CLASS) void operator()(Const<CLASS>&) override;
    M_AST_EXPR_LIST(DECLARE)
#undef DECLARE

    void operator()(const cnf::Predicate&);
    void operator()(const cnf::Clause&);
    void operator()(const cnf::CNF&);
};

}

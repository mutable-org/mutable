#include "IR/QueryGraph.hpp"

#include "IR/QueryGraph2SQL.hpp"
#include "parse/ASTDumper.hpp"
#include <mutable/catalog/Catalog.hpp>
#include <mutable/IR/CNF.hpp>
#include <mutable/parse/AST.hpp>
#include <mutable/util/macro.hpp>
#include <queue>
#include <set>
#include <unordered_map>
#include <utility>


using namespace m;


struct Decorrelation;


/*======================================================================================================================
 * Query
 *====================================================================================================================*/

bool Query::is_correlated() const { return query_graph_->is_correlated(); }


/*======================================================================================================================
 * Decorrelation of nested queries
 * -------------------------------
 *
 * DEFINITION: A predicate is *correlated* if it contains at least one *free* variable *and* at least one *bound*
 * variable.
 *
 * NOTE: A free variable *must* be bound by an outer statement, otherwise the query is ill-formed (and should not have
 * passed semantic analysis).
 *
 * EXAMPLES:
 * (1)
 *      ```
 *      SELECT T.id
 *      FROM T
 *      WHERE T.n = (
 *          SELECT COUNT(*)
 *          FROM S
 *          WHERE S.x = T.y -- T.y is a free variable, bound by the outer statement.
 *      )
 *      ```
 *
 *      ```
 *        ⧑ (T.n = S.$agg0)  ← dependent join
 *       / \
 *      T   Γ (; COUNT(*) AS $agg0)
 *          |
 *          σ (S.x = T.y)  ← correlated predicate
 *          |
 *          S
 *      ```
 *
 *      To decorrelate T.y, group S by S.x, then lift join condition above grouping:
 *
 *      ```
 *      SELECT T.id
 *      FROM T, (
 *          SELECT S.x, COUNT(*) AS $agg0
 *          FROM S
 *          GROUP BY S.x -- group by the correlated equi-predicate
 *      ) AS S
 *      WHERE S.x = T.y -- unnest the inner query by joining with outer on the equi-predicate
 *        AND T.n = S.$agg0 -- and join on the transformed original predicate
 *      ```
 *
 *      ```
 *        ⨝ (S.x = T.y AND T.n = S.$agg0)  ← regular join
 *       / \
 *      T   Γ (S.x; COUNT(*) AS $agg0)  ← group by attribute of correlated predicate
 *          |
 *          S
 *      ```
 *
 * (2)
 *      ```
 *      SELECT T.id
 *      FROM T
 *      WHERE T.n = (
 *          SELECT COUNT(*)
 *          FROM S
 *          WHERE S.x = ISNULL(T.y) -- T.y is a free variable, bound by the outer statement.
 *      )
 *      ```
 *
 *      ```
 *        ⧑ (T.n = S.$agg0)  ← dependent join
 *       / \
 *      T   Γ (; COUNT(*) AS $agg0)
 *          |
 *          σ (S.x = ISNULL(T.y))  ← correlated predicate
 *          |
 *          S
 *      ```
 *
 *      To decorrelate T.y, group S by S.x, then lift join condition above grouping:
 *
 *      ```
 *      SELECT T.id
 *      FROM T, (
 *          SELECT S.x, COUNT(*) as $agg0
 *          FROM S
 *          GROUP BY S.x -- group by the correlated equi-predicate
 *      ) AS S
 *      WHERE S.x = ISNULL(T.y) -- unnest the inner query by joining with outer on the equi-predicate
 *        AND T.n = S.$agg0 -- and join on the transformed original predicate
 *      ```
 *
 *      ```
 *        ⨝ (S.x = ISNULL(T.y) AND T.n = S.$agg0)  ← dependent join
 *       / \
 *      T   Γ (S.x; COUNT(*) AS $agg0)  ← group by attribute of correlated predicate
 *          |
 *          S
 *      ```
 *
 * (3)
 *      ```
 *      SELECT T.id
 *      FROM T
 *      WHERE T.n = (
 *          SELECT COUNT(*)
 *          FROM S
 *          WHERE ISNULL(S.x) = T.y -- T.y is a free variable, bound by the outer statement.
 *      )
 *      ```
 *
 *      is converted to
 *
 *      ```
 *      SELECT T.id
 *      FROM T, (
 *          SELECT $expr0, COUNT(*) AS $agg0
 *          FROM S
 *          GROUP BY ISNULL(S.x) AS $expr0
 *      ) AS S
 *      WHERE S.$expr0 = T.y
 *        AND T.n = S.$agg0
 *      ```
 *
 * (4)
 *      ```
 *      SELECT T.id
 *      FROM T
 *      WHERE EXISTS (
 *          SELECT 1
 *          FROM S
 *          WHERE S.a + S.b = T.c -- T.c is a free variable, bound by the outer statement.
 *      )
 *      ```
 *
 *      ```
 *        ⧑ (TRUE) ← dependent join without condition (not a Cartesian product!)
 *       / \
 *      T   EXISTS
 *          |
 *          σ (S.a + S.b = T.c) ← correlated predicate
 *          |
 *          S
 *      ```
 *
 *      is converted to
 *
 *      ```
 *      SELECT T.id
 *      FROM T, (
 *          SELECT $expr0
 *          FROM S
 *          GROUP BY S.a + S.b AS $expr0
 *      ) AS S
 *      WHERE T.c = S.$expr0
 *      ```
 *
 *      ```
 *        ⨝ (T.c = S.$expr0)
 *       / \
 *      T   Γ (S.a + S.b AS $expr0; )
 *          |
 *          S
 *      ```
 *
 * (5)
 *      ```
 *      SELECT T.id
 *      FROM T
 *      WHERE EXISTS (
 *          SELECT 1
 *          FROM S
 *          WHERE S.a + T.b = S.c -- T.b is a free variable, bound by the outer statement.
 *      )
 *      ```
 *
 *      ```
 *        ⧑ (TRUE) ← dependent join without condition (not a Cartesian product!)
 *       / \
 *      T   EXISTS
 *          |
 *          σ (S.a + T.b = S.c) ← correlated predicate
 *          |
 *          S
 *      ```
 *
 *      **cannot** be converted to
 *
 *      ```
 *      SELECT T.id
 *      FROM T, (
 *          SELECT S.a, S.c
 *          FROM S
 *          GROUP BY S.a, S.c -- FIXME This is incorrect.  We should transform the predicate and group by S.c - S.a
 *      ) AS S
 *      WHERE S.a + T.b = S.c -- TODO Hypothetically, we must rewrite to T.b = S.c - S.a to have single group
 *      ```
 *
 *      ```
 *        ⨝ (T.b = S.a + S.c)
 *       / \
 *      T   Γ (S.a, S.b; )
 *          |
 *          S
 *      ```
 *
 * (6)
 *      ```
 *      SELECT T.id
 *      FROM T
 *      WHERE T.max = (
 *          SELECT MAX(S.a * T.b)
 *          FROM S
 *          WHERE S.c = T.c
 *      )
 *      ```
 *
 *      is converted to
 *
 *      ```
 *      SELECT T.id
 *      FROM T, (
 *          SELECT S.a, S.c
 *          FROM S
 *          GROUP BY S.a, S.c
 *      ) AS S
 *      WHERE T.max = S.a * T.b
 *        AND T.c = S.c
 *      ```
 *
 * (7)
 *      ```
 *      SELECT T.id
 *      FROM T
 *      WHERE T.max != ( -- non-equi predicate here is valid, its not a correlated predicate
 *          SELECT COUNT(*)
 *          FROM S
 *          WHERE S.x = T.y
 *      )
 *      ```
 *
 *      is converted to
 *
 *      ```
 *      SELECT T.id
 *      FROM T, (
 *          SELECT S.x, COUNT(*)
 *          FROM S
 *          GROUP BY S.x
 *      ) AS S
 *      WHERE T.max != S.x
 *        AND S.x = T.y
 *      ```
 *
 * (8)
 *      ```
 *      SELECT T.id
 *      FROM T
 *      WHERE T.max == (
 *          SELECT COUNT(*)
 *          FROM S
 *          WHERE S.x != T.y -- non-equi predicate here is correlated
 *      )
 *      ```
 *
 *      ```
 *        ⧑ (T.max = $agg0) ← dependent join
 *       / \
 *      T   Γ (; COUNT(*) AS $agg0)
 *          |
 *          σ (S.x != T.y) ← correlated predicate
 *          |
 *          S
 *      ```
 *
 *      We currently can't do better than a dependent join.  We cannout lift the correlated predicate above the grouping
 *      of the correlated query.
 *      We could lift the correlated predicate and the grouping above the dependent join. This would transform the
 *      dependent join to a regular join with the correlated predicate as join predicate and the original join predicate
 *      hoisted above grouping.  See below:
 *
 *      ```
 *        σ (T.max = $agg0) ← dependent join
 *        |
 *        Γ (T.id; COUNT(*) AS $agg0) ← grouping by PK of T implies grouping by all of T
 *        |
 *        ⨝ (S.x != T.y) ← formerly correlated predicate becomes decorrelated join predicate
 *       / \
 *      T   S
 *      ```
 *
 * TASKS:
 *
 * (1) Analyze the WHERE and HAVING clauses of a query for correlations.
 *
 * (2) To analyze a clause, we must analyze each predicate individually.  So far, we only support a single predicate
 * inside a correlated clause.  Abort, if a correlated clause has more than one predicate.
 *
 * (3) For each predicate, check whether it has free variables.  If this is not the case, we treat it as a regular
 * predicate.
 *
 * (4) If all predicates of a clause contain solely *free* variables, the entire clause can be lifted out of the nested
 * query and into the statement that *binds* the free variables.
 *
 * (5) If the predicate is correlated, we recursively analyze the predicate's expression (pre order).  If we find an
 * *n*-ary expression (e.g. binary or function application) that is correlated, we abort.  The reasoning is that *n*-ary
 * expressions cannot be broken apart into uncorrelated subexpression *while preserving query semantics*.  An
 * **important exception** to this rule are equi-predicates (i.e. `=`).
 *
 * (6) If the correlated expression is an equi-predicate of the form E1 = E2, recursively analyze E1 and E2. If E1 or E2
 * are correlated, we abort (see (5)). If both E1 and E2 are uncorrelated, then one expression contains only bound
 * variables while the other contains only free variables.  We can then transform the entire statement to decorrelate
 * the predicate.
 *
 * (7) To decorrelate the predicate, group the correlated subquery by the correlated subexpression of the equi-predicate
 * (E1 or E2).  Then, add the original predicate as a clause to the join condition of the dependent join between the
 * statement that binds the free variables of the subexpression and the nested query that contains the predicate
 * (potentially by multiple levels of nesting).
 *
 *
 * DEPENDENT JOIN:
 *
 * Transforming non-equi predicates gives no benefit over a dependent join, unless we can perform *re*-aggregation (e.g.
 * SUM of COUNTs).
 *
 *====================================================================================================================*/

/** Given a `SelectStmt` \p stmt, extract the aggregates to compute while grouping. */
std::vector<std::reference_wrapper<const ast::FnApplicationExpr>>
get_aggregates(const ast::SelectStmt &stmt)
{
    std::vector<std::reference_wrapper<const ast::FnApplicationExpr>> aggregates;
    auto handle_fn = overloaded {
        [](auto&) { },
        [&aggregates](const ast::FnApplicationExpr &e) {
            M_insist(e.has_function());
            if (e.get_function().is_aggregate()) { // test that this is an aggregation
                using std::find_if, std::to_string;
                const std::string str = to_string(e);
                auto exists = [&str](std::reference_wrapper<const ast::Expr> agg) { return to_string(agg.get()) == str; };
                if (find_if(aggregates.begin(), aggregates.end(), exists) == aggregates.end()) // if not already present
                    aggregates.push_back(e);
            }
        },
    };

    if (stmt.having)
        visit(handle_fn, *as<ast::HavingClause>(*stmt.having).having, m::tag<ast::ConstPreOrderExprVisitor>());

    for (auto &s : as<ast::SelectClause>(*stmt.select).select)
        visit(handle_fn, *s.first, m::tag<ast::ConstPreOrderExprVisitor>());

    if (stmt.order_by) {
        for (auto &o : as<ast::OrderByClause>(*stmt.order_by).order_by)
            visit(handle_fn, *o.first, m::tag<ast::ConstPreOrderExprVisitor>());
    }

    return aggregates;
}

/** Computes whether the bound parts of \p expr are composable of elements in \p components. */
bool is_composable_of(const ast::Expr &expr,
                      const std::vector<std::reference_wrapper<const ast::Expr>> components)
{
    auto recurse = overloaded {
        [&](const ast::Designator &d) -> bool {
            return d.contains_free_variables() or d.is_identifier(); // identifiers are implicitly composable, as they never refer into a table XXX do we have to check the target?
        },
        [&](const ast::FnApplicationExpr &e) -> bool {
            if (not is_composable_of(*e.fn, components)) return false;
            for (auto &arg : e.args) {
                if (not is_composable_of(*arg, components))
                    return false;
            }
            return true;
        },
        [&](const ast::UnaryExpr &e) -> bool { return is_composable_of(*e.expr, components); },
        [&](const ast::BinaryExpr &e) -> bool {
            return is_composable_of(*e.lhs, components) and is_composable_of(*e.rhs, components);
        },
        [](auto&) -> bool { return true; },
    };

    for (auto c : components)
        if (expr == c.get()) return true; // syntactically equivalent to a component
    return visit(recurse, expr, m::tag<m::ast::ConstASTExprVisitor>()); // attempt to recursively compose expr
}

GraphBuilder::GraphBuilder() : graph_(std::make_unique<QueryGraph>()) { }

/** Collects correlation information of all `cnf::Clause`s occurring in the `QueryGraph`.
 *
 * `QueryCorrelationInfo` collects all tables referenced by free variables (i.e. correlated `Designator`s).  It further
 * collects all correlated subexpressions occuring in the `QueryGraph` and dissects them based on whether they occur in
 * equi- or non-equi-predicates. */

ClauseInfo::ClauseInfo(const cnf::Clause &clause)
{
    /*----- Compute whether the clause has bound or free variables, what tables are referenced, and which nested queries
     * occur inside the clause. -----*/
    auto visitor = overloaded {
        [](auto&) -> void { },
        [this](const ast::Designator &D) {
            binding_depth = std::max(binding_depth, D.binding_depth());
            if (D.has_table_name())
                data_sources.emplace(D.get_table_name());
        },
        [this](const ast::QueryExpr &Q) {
            auto B = std::make_unique<GraphBuilder>();
            (*B)(*Q.query);
            nested_queries.emplace_back(Q, std::move(B), Q.alias());
            data_sources.emplace(Q.alias()); // by unnesting, this QueryExpr becomes a DataSource
        },
    };
    for (const auto &pred : clause)
        visit(visitor, *pred, m::tag<ast::ConstPreOrderExprVisitor>());
}

void GraphBuilder::process_selection(cnf::Clause &clause)
{
    ClauseInfo CI(clause);

    /*----- Introduce subqueries as sources. -----*/
    for (auto &subquery : CI.nested_queries) {
        auto &ref = graph_->add_source(subquery.alias, subquery.builder->get());
        named_sources_.emplace(subquery.alias, ref);
        for (auto &[deferred_clause, deferred_CI] : subquery.builder->deferred_clauses_) {
            M_insist(deferred_CI.binding_depth > 0, "was this clause bound in its subquery?");
            --deferred_CI.binding_depth;
            // TODO process all designators, redirecting free designators when they are bound; this would enable us to
            // lift correlated claues w/o having to introduce new designators that replace parts of the AST
            if (deferred_CI.binding_depth == 0)
                bound_clauses_.try_emplace(std::move(deferred_clause), std::move(deferred_CI)); // it's now bound
            else
                deferred_clauses_.try_emplace(std::move(deferred_clause), std::move(deferred_CI)); // still not bound, deferr further
        }
    }

    if (not needs_grouping_) { // no grouping ⇒ all clauses can trivially be decorrelated
        if (CI.binding_depth == 0)
            bound_clauses_.try_emplace(clause, std::move(CI));
        else
            deferred_clauses_.try_emplace(clause, std::move(CI));
        return;
    }

    /* A clause with a single predicate can be an equi-clause.  If it is an equi-clause, it can be decorrelated more
     * cleverly by introducing additional grouping keys. */
    if (CI.binding_depth != 0 and clause.size() == 1) {
        auto pred = clause[0];

        /*----- `pred` is correlated: `pred` contains both bound and free variables -----*/
        auto binary = cast<const ast::BinaryExpr>(&pred.expr());
        if (not binary) // not binary expression ⇒ no equi-clause
            goto fallback;

        if (binary->op().type != m::TK_EQUAL) // no equi-predicate ⇒ no equi-clause
            goto fallback;

        /*----- The clause contains a single equi-predicate.  Check whether it can be decorrelated. -----*/
        bool lhs_has_free_variables = binary->lhs->contains_free_variables();
        bool rhs_has_free_variables = binary->rhs->contains_free_variables();
        if (lhs_has_free_variables and binary->lhs->contains_bound_variables()) // mixed LHS
            goto fallback;
        if (rhs_has_free_variables and binary->rhs->contains_bound_variables()) // mixed RHS
            goto fallback;
        M_insist(lhs_has_free_variables != rhs_has_free_variables, "only either side may contain free variables");

        /*----- The clause contains a single equi-predicate, that can be decorrelated. -----*/
        deferred_clauses_.try_emplace(clause, std::move(CI));
        // auto &expr_with_free_vars  = lhs_has_free_variables ? *binary->lhs : *binary->rhs;
        auto &expr_with_bound_vars = lhs_has_free_variables ? *binary->rhs : *binary->lhs;

        /*----- Ensure that the *bound* part of the equi-predicate is comosable of the grouping keys. -----*/
        if (not is_composable_of(expr_with_bound_vars, existing_grouping_keys_)) // is bound part composable?
            additional_grouping_keys_.emplace(expr_with_bound_vars); // no ⇒ introduce new grouping key
        return;
    }

fallback:
    /*----- Clauses with more than one predicate are *always* non-equi clauses and must be analyzed accordingly. -----*/
    /* This is *not* an equi-clause, since it has more than one predicate joined by disjunction.
     * Check whether the clause is correlated, i.e. whether it contains both free and bound variables.  Note, that
     * it is not enough to check whether the predicates of this clause are correlated, as a predicate of only bound
     * variables together with a predicate of only free variables renders the clause correlated.
     *
     * If the clause is correlated, check whether *all* bound variables are embedded in the grouping keys. If this
     * is the case, we can *trivially* decorrelate the clause.  Otherwise, decorrelation is not possible and we
     * resort to dependent join. */

    /*----- Check whether the clause is correlated. -----*/
    if (CI.binding_depth == 0) {
        bound_clauses_.try_emplace(clause, std::move(CI));
    } else {
        /* The clause contains free variables.  Check whether the predicates are composable of the grouping keys. */
        for (auto pred : clause) {
            if (not is_composable_of(pred.expr(), existing_grouping_keys_)) {
                // TODO dependent join
                throw m::invalid_argument("the bound parts of a correlated clause cannot be composed of the "
                                          "grouping keys");
            }
        }
        /* All predicates of this clause are composable of the grouping keys.  Therefore, the entire clause can
         * trivially be decorrelated. */
        deferred_clauses_.try_emplace(clause, std::move(CI));
    }
}
























/** Returns `true` iff both designators has the same textual representation. */
bool equal(const ast::Designator &one, const ast::Designator &two) {
    return streq(to_string(one).c_str(), to_string(two).c_str());
}

/** Like `std::vector::emplace_back()` but adds only iff `pair` is not already contained in `pairs`. */
void emplace_back_unique(std::vector<std::pair<const ast::Expr*, const char*>> &pairs,
                         const std::pair<const ast::Designator*, const char*> &pair)
{
    for (auto p : pairs) {
        if (auto d = cast<const ast::Designator>(p.first); d and equal(*d, *pair.first))
            return;
    }
    pairs.emplace_back(pair);
}

/** Like `std::vector::emplace_back()` but adds only iff `pair` is not already contained in `pairs`. */
void emplace_back_unique(std::vector<std::pair<const ast::Expr*, const char*>> &pairs,
                         const std::pair<const ast::Expr*, const char*> &pair)
{
    if (auto d = cast<const ast::Designator>(pair.first))
        emplace_back_unique(pairs, std::pair<const ast::Designator*, const char*>(d, pair.second));
    else if (not contains(pairs, pair))
        pairs.emplace_back(pair);
}

/** Like `std::vector::emplace_back()` but adds only iff `des` is not already contained in `exprs`. */
void emplace_back(std::vector<const ast::Expr*> &exprs, const ast::Designator *des)
{
    for (auto e : exprs) {
        if (auto d = cast<const ast::Designator>(e); d and equal(*d, *des))
            return;
    }
    exprs.emplace_back(des);
}

/** Like `std::vector::emplace_back()` but adds only iff `expr` is not already contained in `exprs`. */
void emplace_back_unique(std::vector<const ast::Expr*> &exprs, const ast::Expr *expr)
{
    if (auto d = cast<const ast::Designator>(expr))
        emplace_back(exprs, d);
    else if (not contains(exprs, expr))
        exprs.emplace_back(expr);
}

/** Like `std::vector::emplace_back()` but adds only iff `src` is not already contained in `sources`. */
void emplace_back_unique(std::vector<DataSource*> &sources, DataSource *src)
{
    for (auto s : sources) {
        if (s->name() == src->name())
            return;
    }
    sources.emplace_back(src);
}

/** Like `std::vector::insert()` but adds only those elements of `insertions` which are not already contained in
 * `exprs`. */
void insert(std::vector<const ast::Expr*> &exprs, const std::vector<const ast::Designator*> &insertions) {
    for (auto d : insertions)
        emplace_back(exprs, d);
}

/** Like `std::vector::insert()` but adds only those elements of `insertions` which are not already contained in
 * `exprs`. */
void insert(std::vector<const ast::Expr*> &exprs, const std::vector<const ast::Expr*> &insertions) {
    for (auto e : insertions) {
        if (auto d = cast<const ast::Designator>(e))
            emplace_back(exprs, d);
        else if (not contains(exprs, e))
            exprs.emplace_back(e);
    }
}

/** Helper structure to extract the `QueryExpr`s in an expression. */
struct GetNestedQueries : ast::ConstASTExprVisitor
{
    private:
    std::vector<const ast::QueryExpr*> queries_;

    public:
    GetNestedQueries() { }

    std::vector<const ast::QueryExpr*> get() { return std::move(queries_); }

    using ConstASTExprVisitor::operator();
    void operator()(Const<ast::ErrorExpr>&) override { M_unreachable("graph must not contain errors"); }

    void operator()(Const<ast::Designator>&) override { /* nothing to be done */ }

    void operator()(Const<ast::Constant>&) override { /* nothing to be done */ }

    void operator()(Const<ast::FnApplicationExpr>&) override { /* nothing to be done */ }

    void operator()(Const<ast::UnaryExpr> &e) override { (*this)(*e.expr); }

    void operator()(Const<ast::BinaryExpr> &e) override { (*this)(*e.lhs); (*this)(*e.rhs); }

    void operator()(Const<ast::QueryExpr> &e) override { queries_.push_back(&e); }
};

/** Helper structure to compute and provide primary keys. */
#if 0
struct m::GetPrimaryKey
{
    private:
    std::vector<const ast::Expr*> primary_keys_; ///< a list of all primary keys

    public:
    GetPrimaryKey() { }

    auto get() { return std::move(primary_keys_); }

    void compute(DataSource *source) {
        if (auto base = cast<BaseTable>(source)) {
            Catalog &C = Catalog::Get();
            Position pos(C.pool("Decorrelation"));
            ast::Token dot(pos, C.pool("."), TK_DOT);
            ast::Token tbl(pos, base->name(), TK_IDENTIFIER);
            for (auto pkey : base->table().primary_key()) {
                ast::Token attr(pos, pkey->name, TK_IDENTIFIER);
                auto D = new ast::Designator(dot, tbl, attr, pkey->type, pkey);
                primary_keys_.push_back(D);
                // base->expansion_.push_back(D);
            }
        } else if (auto query = cast<Query>(source)) {
            auto &graph = query->query_graph();
            auto former_size = primary_keys_.size();
            if (graph.grouping()) {
                /* Grouping keys form a new primary key. */
                for (auto e : graph.group_by()) {
                    Catalog &C = Catalog::Get();
                    Position pos(C.pool("Decorrelation"));
                    ast::Token dot(pos, C.pool("."), TK_DOT);
                    ast::Token tbl(pos, query->name(), TK_IDENTIFIER);
                    std::ostringstream oss;
                    oss << e;
                    ast::Token attr(pos, C.pool(oss.str().c_str()), TK_IDENTIFIER);
                    auto D = new ast::Designator(dot, tbl, attr, as<const PrimitiveType>(e.get().type())->as_scalar(), e);
                    primary_keys_.push_back(D);
                }
            } else {
                /* Primary keys of all sources for the primary key. */
                for (auto ds : graph.sources())
                    compute(ds);
            }
            /* Provide all newly inserted primary keys. */
            if (not graph.projections().empty()) {
                while (former_size != primary_keys_.size()) {
                    emplace_back_unique(graph.projections_,
                                 std::pair<const ast::Expr *, const char *>(primary_keys_.at(former_size), nullptr));
                    ++former_size;
                }
            }
        } else {
            M_unreachable("invalid variant");
        }
    }
};
#endif

/** Compute and provide all primary keys of `source`. */
auto get_primary_key(DataSource *source) {
    // GetPrimaryKey GPK; // TODO we could write this much denser if we had a recursive Expr visitor
    // GPK.compute(source);
    // return GPK.get();
}

/** Helper structure to extract the `Designator`s (and `FnApplicationExpr`s) in an expression or a data source. */
#if 0
struct GetDesignators : ast::ConstASTExprVisitor
{
    private:
    std::vector<const ast::Expr*> designators_; ///< vector of all designators (and `FnApplicationExpr`s)
    bool aggregates_ = true; ///< indicates whether `FnApplicationExpr`s are considered or only their arguments

    public:
    GetDesignators() { }

    std::vector<const ast::Expr*> get() { return std::move(designators_); }

    void aggregates(bool aggregates) { aggregates_ = aggregates; }

    using ConstASTExprVisitor::operator();
    void operator()(Const<ast::ErrorExpr>&) override { M_unreachable("graph must not contain errors"); }

    void operator()(Const<ast::Designator> &e) override { designators_.push_back(&e); }

    void operator()(Const<ast::Constant>&) override { /* nothing to be done */ }

    void operator()(Const<ast::FnApplicationExpr> &e) override {
        if (aggregates_)
            designators_.push_back(&e);
        else {
            for (auto &arg : e.args)
                (*this)(*arg);
        }
    }

    void operator()(Const<ast::UnaryExpr> &e) override { (*this)(*e.expr); }

    void operator()(Const<ast::BinaryExpr> &e) override { (*this)(*e.lhs); (*this)(*e.rhs); }

    void operator()(Const<ast::QueryExpr>&) override { /* nothing to be done */ }
};
#endif

/** Given a query graph and a query within the graph, compute all needed attributes by `graph` and `query`, i.e. those
 * in the filter and joins of `query` plus those in the grouping (or the projection if no grouping exists). */
#if 0
auto get_needed_attrs(const QueryGraph *graph, const Query &query)
{
    M_insist(contains(graph->sources(), &query));

    GetDesignators GD; // TODO we could write this much denser if we had a recursive Expr visitor
    auto it = std::find(graph->sources().begin(), graph->sources().end(), &query);
    for (auto &c : (*it)->filter()) {
        for (auto &p : c)
            GD(*p.expr());
    }
    for (auto &j : (*it)->joins()) {
        for (auto &c : j->condition()) {
            for (auto &p : c)
                GD(*p.expr());
        }
    }
    if (graph->grouping()) {
        for (auto &e : graph->group_by())
            GD(*e);
        GD.aggregates(false); // to search for needed attributes in the arguments of aggregates
        for (auto &e : graph->aggregates())
            GD(*e);
    } else {
        for (auto &e : graph->projections())
            GD(*e.first);
    }
    return GD.get();
}
#endif

/** Helper structure to replace designators in an `Expr`. */
#if 0
struct ReplaceDesignators : ast::ConstASTExprVisitor
{
    private:
    const std::vector<const ast::Expr*> *origin_; ///< list of all reachable expressions
    ast::Designator *new_designator_ = nullptr; ///< the newly generated designator to use, `nullptr` if nothing to replace
    std::vector<const ast::Expr*> deletions_; ///< all removed expressions which have to be deleted manually

    public:
    ReplaceDesignators() { }

    /** Replaces all designators all predicates of `cnf` by new ones pointing to the respective designator in `origin`.
     *  Returns all replaced designators.
     *  IMPORTANT: Replaced designators are neither deleted by this structure nor by `~Expr()` anymore;
     *             caller must delete them!!! */
    std::vector<const ast::Expr*> replace(cnf::CNF &cnf, const std::vector<const ast::Expr*> *origin,
                                     bool target_deleted = false)
    {
        origin_ = origin;
        deletions_.clear();
        cnf::CNF cnf_new;
        for (auto &c : cnf) {
            cnf::Clause c_new;
            for (auto &p : c) {
                auto e = p.expr();
                (*this)(*e);
                if (cast<const ast::Designator>(e) and new_designator_) {
                    /* `e` has to be replaced by `new_designator_`. `e` has to be deleted manually iff the target is
                     * deleted (since it is removed from `target`) and `new_designator` has to be deleted manually iff the
                     * target is not deleted. */
                    c_new = c_new or cnf::Clause({cnf::Predicate::Create(new_designator_, p.negative())});
                    if (target_deleted)
                        deletions_.push_back(e);
                    else
                        deletions_.push_back(new_designator_);
                    new_designator_ = nullptr;
                } else {
                    c_new = c_new or cnf::Clause({p});
                }
            }
            cnf_new = cnf_new and cnf::CNF({c_new});
        }
        cnf = std::move(cnf_new);
        return std::move(deletions_);
    }

    /** Replaces all designators in `target` by new ones pointing to the respective designator in `origin`.
     *  Returns all replaced designators. `target_deleted` indicates whether `target` is deleted by another structure.
     *  IMPORTANT: Replaced designators are neither deleted by this structure nor by `~Expr()` anymore;
     *             caller must delete them!!! */
    std::vector<const ast::Expr*> replace(std::vector<const ast::Expr*> &target, const std::vector<const ast::Expr*> *origin,
                               bool target_deleted = false) {
        origin_ = origin;
        deletions_.clear();
        std::vector<const ast::Expr*> target_new;
        for (auto e : target) {
            (*this)(*e);
            if (cast<const ast::Designator>(e) and new_designator_) {
                /* `e` has to be replaced by `new_designator_`. `e` has to be deleted manually iff the target is
                 * deleted (since it is removed from `target`) and `new_designator` has to be deleted manually iff the
                 * target is not deleted. */
                target_new.emplace_back(new_designator_);
                if (target_deleted)
                    deletions_.push_back(e);
                else
                    deletions_.push_back(new_designator_);
                new_designator_ = nullptr;
            } else {
                target_new.emplace_back(e);
            }
        }
        target = std::move(target_new);
        return std::move(deletions_);
    }

    /** Replaces all designators in `target.first` by new ones pointing to the respective designator in `origin`.
     *  Returns all replaced designators.
     *  IMPORTANT: Replaced designators are neither deleted by this structure nor by `~Expr()` anymore;
     *             caller must delete them!!! */
    std::vector<const ast::Expr*> replace(std::vector<std::pair<const ast::Expr*, const char*>> &target,
                                          const std::vector<const ast::Expr*> *origin, bool target_deleted = false)
    {
        origin_ = origin;
        deletions_.clear();
        std::vector<std::pair<const ast::Expr*, const char*>> target_new;
        for (auto e : target) {
            (*this)(*e.first);
            if (cast<const ast::Designator>(e.first) and new_designator_) {
                /* `e.first` has to be replaced by `new_designator_`. `e.first` has to be deleted manually iff the
                 * target is deleted (since it is removed from `target`) and `new_designator` has to be deleted manually
                 * iff the target is not deleted. */
                target_new.emplace_back(new_designator_, e.second);
                if (target_deleted)
                    deletions_.push_back(e.first);
                else
                    deletions_.push_back(new_designator_);
                new_designator_ = nullptr;
            } else {
                target_new.emplace_back(e);
            }
        }
        target = std::move(target_new);
        return std::move(deletions_);
    }

    private:
    using ConstASTExprVisitor::operator();
    void operator()(Const<ast::ErrorExpr>&) override { M_unreachable("graph must not contain errors"); }

    void operator()(Const<ast::Designator> &e) override {
        if (auto d = findDesignator(e))
            new_designator_ = make_designator(d);
    }

    void operator()(Const<ast::Constant>&) override { /* nothing to be done */ }

    void operator()(Const<ast::FnApplicationExpr> &e) override {
        std::vector<std::unique_ptr<ast::Expr>> args_new;
        for (auto &arg : e.args) {
            (*this)(*arg);
            if (new_designator_) {
                /* Replace `arg` by the newly generated designator. */
                deletions_.push_back(arg.get());
                args_new.emplace_back(new_designator_);
                new_designator_ = nullptr;
            } else {
                args_new.emplace_back(arg.get());
            }
        }
        const_cast<ast::FnApplicationExpr*>(&e)->args = std::move(args_new);
    }

    void operator()(Const<ast::UnaryExpr> &e) override {
        (*this)(*e.expr);
        if (new_designator_) {
            /* Replace `expr` by the newly generated designator. */
            deletions_.push_back(e.expr.get());
            const_cast<ast::UnaryExpr*>(&e)->expr = std::unique_ptr<ast::Designator>(new_designator_);
            new_designator_ = nullptr;
        }
    }

    void operator()(Const<ast::BinaryExpr> &e) override {
        (*this)(*e.lhs);
        if (new_designator_) {
            /* Replace `expr` by the newly generated designator. */
            deletions_.push_back(e.lhs.get());
            const_cast<ast::BinaryExpr*>(&e)->lhs = std::unique_ptr<ast::Designator>(new_designator_);
            new_designator_ = nullptr;
        }
        (*this)(*e.rhs);
        if (new_designator_) {
            /* Replace `expr` by the newly generated designator. */
            deletions_.push_back(e.rhs.get());
            const_cast<ast::BinaryExpr*>(&e)->rhs = std::unique_ptr<ast::Designator>(new_designator_);
            new_designator_ = nullptr;
        }
    }

    void operator()(Const<ast::QueryExpr>&) override { /* nothing to be done */ }

    /** Returns a `Designator` equal to `des` iff it exists in `origin_`, and `nullptr` otherwise. */
    const ast::Designator * findDesignator(const ast::Designator &des) {
        for (auto e : *origin_) {
            if (auto d = cast<const ast::Designator>(e)) {
                if (equal(*d, des))
                    return d;
            }
        }
        return nullptr;
    }

    /** Returns a designator with an empty table name, the textual representation of `d` as attribute
     * name, the same type as `d`, and `d` as target. */
    ast::Designator * make_designator(const ast::Designator *d) {
        Catalog &C = Catalog::Get();
        Position pos(C.pool("Decorrelation"));
        ast::Token dot(pos, C.pool("."), TK_DOT);
        std::ostringstream oss;
        oss << *d;
        ast::Token attr(pos, C.pool(oss.str().c_str()), TK_IDENTIFIER);
        auto D = new ast::Designator(dot, ast::Token(), attr, as<const PrimitiveType>(d->type())->as_scalar(), d);
        return D;
    }
};
#endif

/** Helper structure to extract correlation information of a `cnf:CNF` formula. */
#if 0
struct m::GetCorrelationInfo : ast::ConstASTExprVisitor
{
    friend struct Decorrelation;

    /** Holds correlation information of a single `cnf::Clause`. */
    struct CorrelationInfo : ast::ASTExprVisitor
    {
        cnf::Clause clause; ///< the `cnf::Clause` the information is about
        std::set<const ast::Expr*> uncorrelated_exprs; ///< a set of all uncorrelated designators and aggregates in the `clause`
        std::set<const char*> sources; ///< a set of all sources of correlated designators occurring in the `clause`
        private:
        const char *table_name_; ///< the new table name used in `replace()`
        std::unordered_map<const ast::Designator*, std::unique_ptr<ast::Designator>> old_to_new_outer_; ///< maps old designators to new ones generated by the caller
        std::unordered_map<const ast::Designator*, std::unique_ptr<ast::Designator>> old_to_new_inner_; ///< maps old designators to new ones generated by `replace()`
        std::unordered_set<const ast::Designator*> replaced_designators_; ///< all replaced designators which have to be deleted
        bool is_decorrelation_finished_ = false; ///< indicates whether the decorrelation process is finished, used to remove the decorrelation flag of designators
                                                 // TODO move the post-decorrelation processing into a separate class

        public:
        CorrelationInfo(cnf::Clause clause) : clause(std::move(clause)) { }

        /** Updates `uncorrelated_exprs` according to `old_to_new` and replaces all table names of all designators in
         *  this list by `table_name`.  Apply all these changes to the `clause`, too.
         *  Since replaced designators are neither deleted by this structure nor by `~Expr()` anymore, add them to
         *  the given expansion list for later deletion. */
        void replace(std::unordered_map<const ast::Designator*, std::unique_ptr<ast::Designator>> &old_to_new,
                     const char *table_name, std::vector<const ast::Expr*> &expansion) {
            table_name_ = table_name;
            old_to_new_outer_ = std::move(old_to_new);
            /* Update `clause`. */
            for (auto pred : clause)
                (*this)(*pred);
            /* Update `uncorrelated_exprs`. */
            std::set<const ast::Expr*> uncorrelated_exprs_new;
            for (auto e : uncorrelated_exprs) {
                if (auto d = cast<const ast::Designator>(e)) {
                    auto d_new = get_new(d);
                    try {
                        uncorrelated_exprs_new.emplace(old_to_new_inner_.at(d_new));
                        /* Occurrence of `d` was possibly replaced by newly created designator and not by `d_new`.
                         * Therefore, add `d_new` to `replaced_designators` to ensure deletion by caller. */
                        replaced_designators_.emplace(d_new);
                    } catch (std::out_of_range&) {
                        /* `d_new` was not found in `old_to_new_inner` since no table name was specified and no new
                         * designator was created by this function. Add `d_new` as new uncorrelated expression. */
                        M_insist(not table_name_);
                        uncorrelated_exprs_new.emplace(d_new);
                    }
                } else {
                    uncorrelated_exprs_new.emplace(e);
                }
            }
            uncorrelated_exprs = std::move(uncorrelated_exprs_new);
            old_to_new_inner_.clear();
            /* Add replaced designators to expansion list. */
            for (auto d : replaced_designators_)
                expansion.push_back(d);
            replaced_designators_.clear();
        }

        /** Remove decorrelation flag of all occurring designators in `clause`. */
        void finish_decorrelation() {
            is_decorrelation_finished_ = true;
            for (auto &p : clause)
                (*this)(*p);
        }

        private:
        using ASTExprVisitor::operator();
        void operator()(Const<ast::ErrorExpr>&) override { M_unreachable("graph must not contain errors"); }

        void operator()(Const<ast::Designator> &d) override { if (is_decorrelation_finished_) d.set_bound(); }

        void operator()(Const<ast::Constant>&) override { /* nothing to be done */ }

        void operator()(Const<ast::FnApplicationExpr>&) override { /* nothing to be done */ }

        void operator()(Const<ast::UnaryExpr> &e) override {
            if (is_decorrelation_finished_) {
                (*this)(*e.expr);
            } else {
                if (auto d = cast<ast::Designator>(e.expr.get()); d and contains(uncorrelated_exprs, d)) {
                    /* Replace this designator by an equivalent new one. Set table name iff present. */
                    e.expr = std::unique_ptr<ast::Expr>(table_name_ ? set_table_name(get_new(d)) : get_new(d));
                    replaced_designators_.emplace(d);
                } else {
                    (*this)(*e.expr);
                }
            }
        }

        void operator()(Const<ast::BinaryExpr> &e) override {
            if (is_decorrelation_finished_) {
                (*this)(*e.lhs);
                (*this)(*e.rhs);
            } else {
                if (auto d = cast<ast::Designator>(e.lhs.get()); d and contains(uncorrelated_exprs, d)) {
                    /* Replace this designator by an equivalent new one. Set table name iff present. */
                    e.lhs = std::unique_ptr<ast::Expr>(table_name_ ? set_table_name(get_new(d)) : get_new(d));
                    replaced_designators_.emplace(d);
                } else {
                    (*this)(*e.lhs);
                }
                if (auto d = cast<ast::Designator>(e.rhs.get()); d and contains(uncorrelated_exprs, d)) {
                    /* Replace this designator by an equivalent new one. Set table name iff present. */
                    e.rhs = std::unique_ptr<ast::Expr>(table_name_ ? set_table_name(get_new(d)) : get_new(d));
                    replaced_designators_.emplace(d);
                } else {
                    (*this)(*e.rhs);
                }
            }
        }

        void operator()(Const<ast::QueryExpr>&) override { /* nothing to be done */ }

        ast::Designator * get_new(ast::Designator *d) {
            if (auto it = old_to_new_outer_.find(d); it != old_to_new_outer_.end())
                return it->second;
            else
                return d;
        }
        const ast::Designator * get_new(const ast::Designator *d) {
            if (auto it = old_to_new_outer_.find(d); it != old_to_new_outer_.end())
                return it->second;
            else
                return d;
        }

        ast::Designator * set_table_name(const ast::Designator *d) {
            Catalog &C = Catalog::Get();
            Position pos(C.pool("Decorrelation"));
            ast::Token dot(pos, C.pool("."), TK_DOT);
            ast::Token tbl(pos, C.pool(table_name_), TK_IDENTIFIER);
            auto D = new ast::Designator(dot, tbl, d->attr_name, d->type(), d);
            old_to_new_inner_.emplace(d, D);
            return D;
        }
    };

    private:
    std::vector<CorrelationInfo*> equi_; ///< a list of all correlation information of equi-predicates
    std::vector<CorrelationInfo*> non_equi_; ///< a list of all correlation information of non-equi-predicates

    CorrelationInfo *info_; ///< the current `CorrelationInfo` object

    std::vector<const ast::Expr*> expansion_; ///< list of designators expanded by decorrelation steps

    public:
    GetCorrelationInfo() { }
    ~GetCorrelationInfo() {
        for (auto d : expansion_)
            delete d;
    }

    /** Given a cnf::CNF formula, compute all correlation information of this formula.
     *  Return the remaining `cnf::CNF` which only consists of uncorrelated predicates. */
    cnf::CNF compute(const cnf::CNF &cnf) {
        cnf::CNF res;
        for (auto &c : cnf) { // iterate over a single clause (de facto)
            info_ = new CorrelationInfo(c); // store reference to the *currently built* CorrelationInfo
            bool is_equi = c.size() <= 1; // the clause can only be an equi-predicate if there is no disjunction
            for (auto &p : c) { // fill `info_` for each predicate of clause `c`
                (*this)(*p); // visitor GetCorrelationInfo(p), fills the current `info_` with data
                if (p.negative())
                    is_equi = is_equi and is_not_equal(&p.expr());
                else
                    is_equi = is_equi and is_equal(&p.expr());
            }
            if (info_->sources.empty()) {
                /* c is not correlated. */
                res = res and cnf::CNF({c});
                delete info_;
            } else {
                /* c is correlated. */
                if (is_equi)
                    equi_.push_back(info_);
                else
                    non_equi_.push_back(info_);
            }
        }
        return res;
    }

    /** Returns a list of all correlation information of equi-predicates. */
    std::vector<CorrelationInfo*> getEqui() { return std::move(equi_); }
    /** Returns a list of all correlation information of non-equi-predicates. */
    std::vector<CorrelationInfo*> getNonEqui() { return std::move(non_equi_); }

    bool empty() { return equi_.empty() and non_equi_.empty(); }
    bool non_equi() { return not non_equi_.empty(); }

    private:
    using ConstASTExprVisitor::operator();
    void operator()(Const<ast::ErrorExpr>&) override { M_unreachable("graph must not contain errors"); }

    void operator()(Const<ast::Designator> &e) override {
        if (e.contains_free_variables()) {
            auto names = find_table_name(e);
            info_->sources.insert(names.begin(), names.end());
        } else {
            info_->uncorrelated_exprs.emplace(&e);
        }
    }

    void operator()(Const<ast::Constant>&) override { /* nothing to be done */ }

    void operator()(Const<ast::FnApplicationExpr> &e) override { info_->uncorrelated_exprs.emplace(&e); }

    void operator()(Const<ast::UnaryExpr> &e) override { (*this)(*e.expr); }

    void operator()(Const<ast::BinaryExpr> &e) override { (*this)(*e.lhs); (*this)(*e.rhs); }

    void operator()(Const<ast::QueryExpr>&) override { /* nothing to be done */ }

    bool is_equal(const ast::Expr *e) {
        if (auto b = cast<const ast::BinaryExpr>(e))
            return b->op() == TK_EQUAL;
        else
            return true;
    }

    bool is_not_equal(const ast::Expr *e) {
        if (auto b = cast<const ast::BinaryExpr>(e))
            return b->op() == TK_BANG_EQUAL;
        else
            return true;
    }

    std::set<const char*> find_table_name(const ast::Designator &d) {
        if (d.has_table_name()) return {d.get_table_name()};
        if (std::holds_alternative<const Attribute*>(d.target())) {
            return {std::get<const Attribute *>(d.target())->table.name};
        } else if (std::holds_alternative<const ast::Expr*>(d.target())) {
            GetDesignators GD;
            GD(*std::get<const ast::Expr*>(d.target()));
            std::set<const char*> res;
            for (auto des : GD.get()) {
                /* Skip `FnApplicationExpr`s because they have no table name. */
                if (auto d = cast<const ast::Designator>(des)) {
                    auto names = find_table_name(*d);
                    res.insert(names.begin(), names.end());
                }
            }
            return res;
        } else {
            M_unreachable("target can not be `std::monostate`");
        }
    }
};
#endif

/** The method that decorrelates a `Query` in a `QueryGraph`. */
#if 0
struct m::Decorrelation
{
    private:
    ///> `first` is the query graph of query `second`
    using graphs_t = std::pair<QueryGraph*, Query*>;

    ///> the query to decorrelate, that is a `DataSource` of `graph_`
    Query &query_;
    ///> the query graph containing `query_` as `DataSource`
    QueryGraph *graph_;

    std::vector<graphs_t> correlated_nested_queries_; ///< a list of nested queries with correlated predicates
    std::vector<const ast::Expr*> needed_attrs_; ///< a list of all needed attributes of `graph_`

    public:
    Decorrelation(Query &query, QueryGraph *graph)
        : query_(query)
        , graph_(graph)
        , correlated_nested_queries_({{graph, nullptr}})
    { }

    /** Decorrelates `query_` by predicate push-up.
     *  Returns `true` if all correlated predicates are decorrelated. */
    bool decorrelate() {
        /* Find the innermost, correlated graph and save the path in `graphs_`. */
        find_correlated_nested_graphs(query_.query_graph(), &query_);

        /* Push-up all equi-predicates (starting with the innermost query) until the first non-equi-predicate below a
         * grouping is reached. */
        while (correlated_nested_queries_.size() > 1) {
            auto last = correlated_nested_queries_.back();
            if (last.first->correlation_info_->non_equi() and last.first->grouping()) break;
            correlated_nested_queries_.pop_back();
            push_predicates(correlated_nested_queries_.back().first, last.second);
        }

        M_insist(not correlated_nested_queries_.empty());

        if (correlated_nested_queries_.size() > 1) {
            /* Initialize `needed_attrs_` for operator push-up. */
            needed_attrs_ = get_needed_attrs(graph_, query_);
            /* Provide all table names explicitly because the attribute name does not have to be unique anymore. */
            for (auto des : needed_attrs_) {
                /* Skip `FnApplicationExpr`s because they have no table name. */
                if (auto d = cast<const ast::Designator>(des); d and not d->has_explicit_table_name())
                    const_cast<ast::Designator*>(d)->table_name.type = TK_IDENTIFIER;
            }

            /* Push-up all operators until the innermost non-equi-predicate is reached. */
            for (auto it = correlated_nested_queries_.begin(); it != std::prev(correlated_nested_queries_.end());) {
                auto &upper = *it;
                ++it;
                auto &lower = *it;
                pushOperators(lower.first, upper.first, lower.second);
            }

            /* Adapt all designators above the innermost non-equi-predicate. */
            auto *origin = &correlated_nested_queries_.back().first->group_by_;
            M_insist(not origin->empty());
            for (auto i = correlated_nested_queries_.size() - 1; i != 0; --i) {
                auto &upper = correlated_nested_queries_[i - 1];
                M_insist(not origin->empty());
                replace_designators(upper.first, origin);
                if (upper.first->grouping())
                    origin = &upper.first->group_by_;
            }
        }

        auto iterator = correlated_nested_queries_.end(); // iterator to the innermost graph which is not completely decorrelated
        for (auto it = correlated_nested_queries_.begin(); it != correlated_nested_queries_.end(); ++it) {
            if (not (*it).first->correlation_info_->empty()) {
                /* In this graph not all correlated predicates are decorrelated. */
                iterator = it;
            }
        }
        if (iterator == correlated_nested_queries_.end()) {
            /* All correlated predicates are decorrelated. */
            return true;
        } else {
            /* Not all correlated predicates are decorrelated. Unset `decorrelated_`-flag of all queries until the
             * innermost not decorrelated predicate. */
            for (auto it = correlated_nested_queries_.begin(); it != iterator;)
                (*++it).second->decorrelated_ = false;
            return false;
        }
    }

    private:
    /** Adds all not yet decorrelated query graphs to `graphs_` starting with `graph`.  Recurses into nested queries
     * that are still correlated. */
    void find_correlated_nested_graphs(QueryGraph *graph, Query *query) {
        correlated_nested_queries_.emplace_back(graph, query);

        bool found = false;
        for (auto ds : graph->sources()) {
            if (ds->decorrelated_) continue; // already decorrelated, nothing to be done
            M_insist(not found, "Multiple not yet decorrelated nested queries in one graph should not be possible.");
            found = true;
            auto q = as<Query>(ds);
            find_correlated_nested_graphs(q->query_graph(), q);
        }
    }

    /** Pushes all correlated predicates contained in `inner_graph` up into `outer_graph`. Therefore, the projection and
     * grouping of `inner_graph` are adapted and a join between `query` and all involved sources of each predicate is
     * added in `outer_graph` iff all sources are reachable. Otherwise, the correlated predicate has to be pushed
     * further up and, therefore, is added to `outer_graph.info_`. */
    void push_predicates(QueryGraph *outer_graph, Query *query) {
        QueryGraph *inner_graph = query->query_graph();
        M_insist(contains(outer_graph->sources(), query));

        const bool inner_has_projection = not inner_graph->projections().empty();
        auto grouping = inner_graph->grouping();
        auto equi = inner_graph->correlation_info_->getEqui();
        auto non_equi = inner_graph->correlation_info_->getNonEqui();
        inner_graph->correlation_info_->equi_.clear();
        inner_graph->correlation_info_->non_equi_.clear();
        bool is_equi = true;
        M_insist(non_equi.empty() or not grouping);
        for (auto it = equi.begin(); it != non_equi.end(); ++it) {
            if (it == equi.end()) {
                it = non_equi.begin(); // to iterate over equi and non_equi
                is_equi = false;
                if (it == non_equi.end()) break; // to skip empty non_equi
            }
            auto correlation_info = *it;
            /* Adapt projection and grouping. */
            std::unordered_map<const ast::Designator*, std::unique_ptr<ast::Designator>> old_to_new;
            for (auto e : correlation_info->uncorrelated_exprs) {
                const ast::Expr *e_new = e;
                if (grouping) { // ensure that `e` is maintained through grouping
                    emplace_back_unique(inner_graph->group_by_, e); // the `inner_graph` must group by `e`
                    /*----- Create name for projecting `e`. -----*/
                    if (auto d = cast<const ast::Designator>(e)) {
                        /* Create a new designator pointing to `d` (the grouping key) and add respective mapping. */
                        Catalog &C = Catalog::Get();
                        std::ostringstream oss;
                        oss << *d;
                        Position pos(C.pool("Decorrelation"));
                        ast::Token attr(pos, C.pool(oss.str().c_str()), TK_IDENTIFIER);
                        auto d_new = std::make_unique<ast::Designator>(attr, ast::Token(), attr, as<const PrimitiveType>(d->type())->as_scalar(), d);
                        auto res = old_to_new.emplace(d, std::move(d_new));
                        e_new = res.first->second.get();
                    }
                }
                if (inner_has_projection)
                    emplace_back_unique(
                        inner_graph->projections_,
                        std::pair<const ast::Expr*, const char*>(e_new, /* no alias */ nullptr)
                    );
            }
            /* Update `correlation_info->uncorrelated_expr` and `correlation_info->clause` by replacing the table name
             * of all uncorrelated designators by the alias of `query` (if given) because it will be pushed up and
             * therefore the former sources are only reachable via `query`. */
            correlation_info->replace(old_to_new, query->alias(), inner_graph->correlation_info_->expansion_);
            /* Search needed sources. */
            std::vector<DataSource*> sources{query};
            auto all_reachable = true;
            for (auto s : correlation_info->sources) {
                if (auto src = findSource(outer_graph->sources(), s)) {
                    emplace_back_unique(sources, src);
                } else {
                    /* The source s is not reachable. */
                    all_reachable = false;
                    break;
                }
            }
            /* Add join or rather filter. */
            if (all_reachable) {
                correlation_info->finish_decorrelation();
                if (auto j = findJoin(outer_graph->joins(), sources)) {
                    /* A join with the same data sources already exists so only update the predicate. */
                    j->update_condition(cnf::CNF({correlation_info->clause}));
                } else {
                    /* Create a new join for the data sources. */
                    auto J = outer_graph->joins_.emplace_back(new Join(cnf::CNF({correlation_info->clause}), sources));
                    for (auto ds : J->sources())
                        ds->add_join(J);
                }
                delete correlation_info;
            } else {
                /* Add `i` to the upper correlation information. */
                if (is_equi)
                    outer_graph->correlation_info_->equi_.push_back(correlation_info);
                else
                    outer_graph->correlation_info_->non_equi_.push_back(correlation_info);
            }
        }
        query->decorrelated_ = true;
    }

    /** Pushes all operators (projection and grouping) contained in `lower` up above the sources of `upper`. Therefore,
     *  the projection and grouping of `lower` are adapted and the sources of `upper` are added to the sources of
     *  `lower`. Furthermore, the former join of `query` is transformed into its filter and a join between all
     *  involved sources of each predicate is added in `lower` iff all sources are reachable.
     *  Otherwise, the operators of the graph below has to be pushed further up and, therefore, the correlated
     *  predicate is still contained in `lower.info_`. */
    void pushOperators(QueryGraph *lower, QueryGraph *upper, Query *query) {
        M_insist(query->query_graph() == lower);
        M_insist(contains(upper->sources(), query));
        auto lower_sources = lower->sources();

        /* Remove joins of `query` and add conditions as filter. */
        for (auto j : query->joins()) {
            query->update_filter(j->condition());
            upper->remove_join(j);
            for (auto ds : j->sources())
                ds->remove_join(j);
            delete j;
        }
        auto grouping = lower->grouping();
        /* Move all sources except `query` from `upper` to `lower` and adapt grouping of `lower`.
         * Change source id such that the id's of all sources of `lower` are sequential. */
        size_t num_sources = lower->sources().size();
        for (auto it = upper->sources_.begin(); it != upper->sources_.end(); ++it) {
            if (*it == query) continue;
            (*it)->id_ = num_sources++;
            lower->sources_.emplace_back(*it);
            if (grouping) {
                auto primary_key = get_primary_key(*it);
                M_insist(not primary_key.empty(), "can not push-up grouping above source without primary key");
                insert(lower->group_by_, primary_key);
            }
        }
        query->id_ = 0;
        upper->sources_ = {query};
        /* Move all joins of `upper` to `lower`. */
        lower->joins_.insert(lower->joins_.end(), upper->joins_.begin(), upper->joins_.end());
        upper->joins_.clear();
        /* Add all later needed attributes (of `graph_`) to grouping and projection of `lower`. */
        if (grouping)
            insert(lower->group_by_, needed_attrs_);
        if (not lower->projections().empty()) {
            for (auto attr : needed_attrs_)
                emplace_back_unique(lower->projections_, std::pair<const ast::Expr*, const char*>(attr, nullptr));
        }
        /* Add joins for correlated predicates iff possible. */
        auto equi = lower->correlation_info_->getEqui();
        auto non_equi = lower->correlation_info_->getNonEqui();
        lower->correlation_info_->equi_.clear();
        lower->correlation_info_->non_equi_.clear();
        bool is_equi = true;
        for (auto it = equi.begin(); it != non_equi.end(); ++it) {
            if (it == equi.end()) {
                it = non_equi.begin(); // to iterate over equi and non_equi
                is_equi = false;
                if (it == non_equi.end()) break; // to skip empty non_equi
            }
            auto i = *it;
            /* Search needed sources. Since `lower` could initially contain multiple sources which contribute to this
             * join we have to check the table names of uncorrelated designators, too. */
            std::vector<DataSource*> sources;
            auto all_reachable = true;
            for (auto e : i->uncorrelated_exprs) {
                if (auto d = cast<const ast::Designator>(e)) {
                    if (auto src = findSource(lower->sources(), d->get_table_name())) {
                        emplace_back_unique(sources, src);
                    } else {
                        /* The source d->get_table_name() is not reachable. */
                        all_reachable = false;
                        break;
                    }
                } else {
                    /* Aggregation can not be assigned to a specific source so add all initially available ones. */
                    sources = lower_sources;
                    break;
                }
            }
            for (auto s : i->sources) {
                if (auto src = findSource(lower->sources(), s)) {
                    emplace_back_unique(sources, src);
                } else {
                    /* The source s is not reachable. */
                    all_reachable = false;
                    break;
                }
            }
            /* Add join. */
            if (all_reachable) {
                i->finish_decorrelation();
                if (auto j = findJoin(lower->joins(), sources)) {
                    /* A join with the same data sources already exists so only update the predicate. */
                    j->update_condition(cnf::CNF({i->clause}));
                } else {
                    /* Create a new join for the data sources. */
                    auto J = lower->joins_.emplace_back(new Join(cnf::CNF({i->clause}), sources));
                    for (auto ds : J->sources())
                        ds->add_join(J);
                }
                delete i;
            } else {
                /* Add `i` again to the correlation information. */
                if (is_equi)
                    lower->correlation_info_->equi_.push_back(i);
                else
                    lower->correlation_info_->non_equi_.push_back(i);
            }
        }
    }

    /** Returns a `DataSource *` pointing to a data source with the same name as `name` iff it exists in `sources`,
     *  and `nullptr` otherwise. */
    DataSource * findSource(const std::vector<DataSource*> &sources, const char *name) {
        for (auto s : sources) {
            if (auto q = cast<Query>(s); name == s->name() or (q and findSource(q->query_graph()->sources(), name)))
                return s;
        }
        return nullptr;
    }

    /** Returns a `Join *` pointing to a join with the same sources as `sources` iff it exists in `joins`, and
     * `nullptr` otherwise. */
    Join * findJoin(const std::vector<Join*> &joins, const std::vector<DataSource*> &sources) {
        std::unordered_set<DataSource*> cmp(sources.begin(), sources.end());
        for (auto j : joins) {
            if (cmp == std::unordered_set<DataSource*>(j->sources().begin(), j->sources().end()))
                return j;
        }
        return nullptr;
    }

    /** Replaces all designators in `graph` by new ones pointing to the respective designator in `origin`. */
    void replace_designators(QueryGraph *graph, const std::vector<const ast::Expr*> *origin) {
        M_insist(graph->sources().size() == 1);
        ReplaceDesignators RP;
        auto ds = *graph->sources().begin();
        auto filter_new = ds->filter();
        auto expansions = RP.replace(filter_new, origin);
        ds->filter_ = filter_new;
        graph->correlation_info_->expansion_.insert(graph->correlation_info_->expansion_.end(), expansions.begin(), expansions.end());
        if (graph->grouping()) {
            expansions = RP.replace(graph->group_by_, origin);
            graph->correlation_info_->expansion_.insert(graph->correlation_info_->expansion_.end(), expansions.begin(), expansions.end());
            expansions = RP.replace(graph->aggregates_, origin);
            graph->correlation_info_->expansion_.insert(graph->correlation_info_->expansion_.end(), expansions.begin(), expansions.end());
            expansions = RP.replace(graph->projections_, &graph->group_by());
            graph->correlation_info_->expansion_.insert(graph->correlation_info_->expansion_.end(), expansions.begin(), expansions.end());
        } else {
            expansions = RP.replace(graph->projections_, origin);
            graph->correlation_info_->expansion_.insert(graph->correlation_info_->expansion_.end(), expansions.begin(), expansions.end());
        }
    }
};
#endif

/*======================================================================================================================
 * GraphBuilder
 *
 * An AST Visitor that constructs the query graph.
 *====================================================================================================================*/


void m::GraphBuilder::operator()(const ast::SelectStmt &stmt) {
    Catalog &C = Catalog::Get();

    /*----- Collect all aggregates in the statement and save them in the graph. -----*/
    graph_->aggregates_ = get_aggregates(stmt);

    /*----- Compute whether the query needs grouping. -----*/
    needs_grouping_ = bool(stmt.group_by) or graph_->aggregates().size() > 0;

    /*----- Collect grouping keys of `stmt`. -----*/
    if (stmt.group_by) {
        auto &group_by = as<const ast::GroupByClause>(*stmt.group_by);
        for (auto &[grp, alias] : group_by.group_by)
            existing_grouping_keys_.emplace_back(*grp); // TODO correct?
    }

    ///> holds all nested queries occurring in the `SelectClause` of `stmt`
    std::vector<std::pair<std::reference_wrapper<const ast::QueryExpr>, const char*>> nested_queries_in_select;

    /*----- Process FROM and create data sources. -----*/
    if (stmt.from) {
        auto &FROM = as<ast::FromClause>(*stmt.from);
        for (auto &tbl : FROM.from) {
            if (auto tok = std::get_if<ast::Token>(&tbl.source)) {
                /* Create a new base table. */
                M_insist(tbl.has_table());
                auto &base = graph_->add_source(tbl.alias ? tbl.alias.text : nullptr, tbl.table());
                named_sources_.emplace(base.name(), base);
            } else if (auto stmt = std::get_if<ast::Stmt*>(&tbl.source)) {
                M_insist(tbl.alias.text, "every nested statement requires an alias");
                auto &select = as<ast::SelectStmt>(**stmt);
                /* Create a graph for the sub query. */
                auto graph = QueryGraph::Build(select);
                auto &Q = graph_->add_source(tbl.alias.text, std::move(graph));
                M_insist(tbl.alias);
                named_sources_.emplace(tbl.alias.text, Q);
            } else {
                M_unreachable("invalid variant");
            }
        }
    }

    /* Do some computation before first nested query is decorrelated to ensure that `get_needed_attrs()` works as
     * intended: */

    /*----- Process GROUP BY and collect grouping keys. -----*/
    if (stmt.group_by) {
        auto &GROUP_BY = as<ast::GroupByClause>(*stmt.group_by);
        for (auto &[grp, alias] : GROUP_BY.group_by)
            graph_->group_by_.emplace_back(*grp, alias.text);
    }

    /*----- Process SELECT and collect projections. -----*/
    auto SELECT = as<ast::SelectClause>(stmt.select.get());
    for (auto &e : SELECT->expanded_select_all)
        graph_->projections_.emplace_back(*e, nullptr); // expansions never contain nested queries; directly add to
                                                        // projections
    /* Collect nested queries in SELECT clause. */
    for (auto &s : SELECT->select) {
        if (auto query = cast<const ast::QueryExpr>(s.first)) // found nested query
            nested_queries_in_select.emplace_back(*query, s.second.text);
        else
            graph_->projections_.emplace_back(*s.first, s.second.text); // add to projections
    }

    /*----- Process WHERE clause. -----*/
    if (stmt.where) {
        /*----- Get WHERE clause and convert to CNF. -----*/
        auto &WHERE = as<ast::WhereClause>(*stmt.where);
        auto cnf_where = cnf::to_CNF(*WHERE.where);

        /*----- Analyze all CNF clauses of the WHERE clause. -----*/
        for (auto &clause : cnf_where)
            process_selection(clause);

        /*----- Introduce additional grouping keys. -----*/
        for (auto e : additional_grouping_keys_) {
            graph_->group_by_.emplace_back(e, nullptr);
            graph_->projections_.emplace_back(e, nullptr);
        }

        for (auto &[clause, CI] : bound_clauses_) {
            if (CI.is_constant()) {
                for (auto &[_, src] : named_sources_)
                    src.get().update_filter(cnf::CNF{std::move(clause)}); // NOTE: kind of silly, but it works
            } else if (CI.is_selection()) {
                auto it = named_sources_.find(*CI.data_sources.begin());
                M_insist(it != named_sources_.end(), "data source with that name was not found");
                it->second.get().update_filter(cnf::CNF{std::move(clause)});
            } else {
                Join::sources_t sources;
                for (auto src_name : CI.data_sources) {
                    auto it = named_sources_.find(src_name);
                    M_insist(it != named_sources_.end(), "data source with that name was not found");
                    sources.emplace_back(it->second);
                }
                auto J = std::make_unique<Join>(cnf::CNF{std::move(clause)}, std::move(sources));
                auto &ref = *graph_->joins_.emplace_back(std::move(J)); // add join to query graph
                for (auto ds : ref.sources())
                    ds.get().add_join(ref); // add join to all data sources
            }
        }
    }

#if 0
    /* Dissect CNF into joins, filters and nested queries.  Performs unnesting immediately during graph
     * construction.  Decorrelation is performed afterwards where possible. */
    /* TODO iterate over clauses in the order joins → filters/nested queries (equi) → nested queries (non-equi) */
    for (auto &clause : cnf_where) {
        const auto nested_queries = get_nested_queries(clause); // collect all nested queries of `clause`
        const auto tables = get_tables(clause);
        if (nested_queries.empty()) {
            /* Compute correlation information of this clause analogously. */
            if (tables.empty()) {
                /* This clause is a filter and constant.  It applies to all data sources.  Therefore, add it as a
                 * filter condition to *each* source. */
                for (auto [_, source] : named_sources_)
                    ; // TODO XXX
                    source->update_filter(graph_->correlation_info_->compute(cnf::CNF{clause}));
            } else if (tables.size() == 1) {
                /* This clause is a filter condition on its single data source. */
                auto t = *begin(tables);
                auto ds = named_sources_.at(t);
                // TODO compute correlation information of `clause`
                // XXX
                // ds->update_filter(graph_->correlation_info_->compute(cnf::CNF({clause}))); //XXX
            } else {
                /* This clause is a join condition between two or more data sources.  Create a new join in the query
                 * graph. */
                Join::sources_t sources;
                for (auto t : tables)
                    sources.emplace_back(named_sources_.at(t));
                // XXX
                // auto J = graph_->joins_.emplace_back(new Join(graph_->correlation_info_->compute(cnf::CNF({clause})), sources));
                // for (auto ds : J->sources())
                    // ds->add_join(J);
            }
        } else {
            M_insist(nested_queries.size() == 1, "Multiple nested queries in one clause are not supported.");
            auto &nested_query = nested_queries.front().get();
            auto &nested_stmt = as<const ast::SelectStmt>(*nested_query.query); // get single, nested query
            auto &nested_select_clause = as<ast::SelectClause>(*nested_stmt.select);
            M_insist(not nested_select_clause.select_all, "* can not yet be used in nested queries.");
            M_insist(nested_select_clause.select.size() == 1, "only a single projection allowed");

            /* Replace the alias of the single projection by `$res`. */
            auto &single_projection = nested_select_clause.select.front();
            single_projection.second.text = C.pool("$res");

            /* Create a graph for the nested query.  Unnest the query by adding it as a source to the outer graph */
            auto nested_query_name = nested_query.alias();
            auto &Q = graph_->add_source(nested_query_name, QueryGraph::Build(nested_stmt));

            /* Create a dependent join between the nested query and all tables in the clause.  The dependent join
             * will later be resolved through decorrelation. */
            if (tables.empty()) {
                /* This clause is predicate with nested query `Q` and *no* tables of outer.  It behaves like a
                 * regular selection on outer.  Since it contains the nested query `Q`, we have to use a join to an
                 * arbitrary data source.  TODO be clever when selecting the relation to join to. */
                M_insist(not named_sources_.empty(), "cannot join nested query to any source");
                /* TODO if Q is correlated use the referenced table */
                auto &any_data_source = *named_sources_.begin(); // pick *any* data source
                auto &J = graph_->joins_.emplace_back(new Join(cnf::CNF({clause}), {Q, any_data_source.second}));
                Q.add_join(*J); // add join to Q
                any_data_source.second.get().add_join(*J); // add join to the chosen data source of outer
            } else {
                /* This clause is a join condition. */
                Join::sources_t sources{Q};
                /* Create a join between outer's tables in `clause` and the nested query (as data source) `Q`. */
                for (auto t : tables)
                    sources.emplace_back(named_sources_.at(t));
                auto &J = graph_->joins_.emplace_back(new Join(cnf::CNF({clause}), sources));
                for (auto ds : J->sources())
                    ds.get().add_join(*J);
            }

            named_sources_.emplace(nested_query_name, Q); // XXX why?

            /* Decorrelate the nested query iff it is correlated. */
            if (Q.is_correlated())
                /* move the correlated predicate (i.e. the one with a free variable) to outer */
                decorrelate(Q);
        }
    }
#endif

    /* Implement HAVING as a regular selection filter on a sub query. */
    cnf::CNF cnf_having;
    if (stmt.having) {
        auto having_clause = as<ast::HavingClause>(stmt.having.get());
        cnf_having = cnf::to_CNF(*having_clause->having);
        auto sub_graph = std::exchange(graph_, std::make_unique<QueryGraph>());
        auto &sub = graph_->add_source(nullptr, std::move(sub_graph));
        sub.update_filter(cnf_having);
        named_sources_.emplace("HAVING", sub);
        /* Reset former computation of projection and move it after the HAVING. */
        std::swap(graph_->projections_, sub.query_graph().projections_);
        /* Update `decorrelated_`-flag. */
        if (sub.is_correlated())
            sub.decorrelated_ = false;
    }

    /* Dissect CNF into filters and nested queries. Decorrelate if possible. */
    /* TODO iterate over clauses in the order filters/nested queries (equi) < nested queries (non-equi) */
    for (auto &clause : cnf_having) {
        ClauseInfo CI(clause);
        auto &nested_queries = CI.nested_queries;
        if (nested_queries.empty()) {
            // XXX
            // named_sources_.at("HAVING")->update_filter(graph_->correlation_info_->compute(cnf::CNF({clause})));
        } else {
            M_insist(nested_queries.size() == 1, "Multiple nested queries in one clause are not yet supported.");
            auto &nested_query = nested_queries.front().expr;
            auto &nested_stmt = as<const ast::SelectStmt>(*nested_query.query);
            auto &nested_select_clause = as<ast::SelectClause>(*nested_stmt.select);
            M_insist(not nested_select_clause.select_all, "* can not yet be used in nested queries.");
            M_insist(nested_select_clause.select.size() == 1);

            /* Replace the alias of the expression by `$res`. */
            nested_select_clause.select.front().second.text = C.pool("$res");

            /* Create a graph for the nested query.  Unnest the query by adding it as a source to the outer graph */
            auto q_name = nested_query.alias();
            auto &Q = graph_->add_source(q_name, QueryGraph::Build(nested_stmt));
            named_sources_.emplace(q_name, Q);

            /* Create a join between the nested query and the HAVING. */
            auto &J = graph_->joins_.emplace_back(new Join(cnf::CNF({clause}), {Q, named_sources_.at("HAVING")}));
            for (auto ds : J->sources())
                ds.get().add_join(*J);
        }
    }

    /* To implement nested queries in SELECT clause when the query also has grouping, create a subquery (similar to
     * HAVING) if not already done. */
    if (not nested_queries_in_select.empty() and not stmt.having and
        (not graph_->group_by_.empty() or not graph_->aggregates_.empty()))
    {
        auto sub_graph = std::exchange(graph_, std::make_unique<QueryGraph>());
        auto &sub = graph_->add_source(nullptr, std::move(sub_graph)); // anonymous source for nested query
        named_sources_.emplace("SELECT", sub);
        /* Reset former computation of projection and move it after the SELECT. */
        std::swap(graph_->projections_, sub.query_graph().projections_);
        /* Update `decorrelated_`-flag. */
        if (sub.is_correlated())
            sub.decorrelated_ = false;
    }

    /* Add nested queries in SELECT. */
    /* TODO iterate over expressions in the order nested queries (equi) < nested queries (non-equi) */
    for (auto [_query, name] : nested_queries_in_select) {
        auto &query = _query.get();

        auto &select = as<ast::SelectStmt>(*query.query);
        auto &select_clause = as<ast::SelectClause>(*select.select);
        M_insist(not select_clause.select_all, "* can not yet be used in nested queries.");
        M_insist(select_clause.select.size() == 1);
        /* Replace the alias of the expression by `$res`. */
        select_clause.select.front().second.text = C.pool("$res");
        /* Create a sub graph for the nested query. */
        auto sub = QueryGraph::Build(select);
        auto q_name = query.alias();
        auto &Q = graph_->add_source(q_name, std::move(sub));
        /* Create a cartesian product between the nested query and an arbitrary source iff one exists. */
        if (not named_sources_.empty()) {
            /* TODO if Q is correlated use the referenced table */
            DataSource *source;
            try {
                source = &named_sources_.at("SELECT").get();
            } catch (std::out_of_range&) { try {
                source = &named_sources_.at("HAVING").get();
            } catch (std::out_of_range&) {
                source = &named_sources_.begin()->second.get(); // any source
            } }
            auto &J = graph_->joins_.emplace_back(new Join(cnf::CNF(), {Q, *source}));
            Q.add_join(*J);
            source->add_join(*J);
        }
        named_sources_.emplace(q_name, Q);
        graph_->projections_.emplace_back(query, name);
    }

    /* Add order by. */
    if (stmt.order_by) {
        auto ORDER_BY = as<ast::OrderByClause>(stmt.order_by.get());
        for (auto &o : ORDER_BY->order_by)
            graph_->order_by_.emplace_back(*o.first, o.second);
    }

    /* Add limit. */
    if (stmt.limit) {
        auto LIMIT = as<ast::LimitClause>(stmt.limit.get());
        errno = 0;
        graph_->limit_.limit = strtoull(LIMIT->limit.text, nullptr, 0);
        M_insist(errno == 0);
        if (LIMIT->offset) {
            graph_->limit_.offset = strtoull(LIMIT->offset.text, nullptr, 0);
            M_insist(errno == 0);
        }
    }
}


/*======================================================================================================================
 * QueryGraph
 *====================================================================================================================*/

QueryGraph::QueryGraph() { }

QueryGraph::~QueryGraph() { }

std::unique_ptr<QueryGraph> QueryGraph::Build(const ast::Stmt &stmt)
{
    GraphBuilder builder;
    builder(stmt);
    return builder.get();
}

bool QueryGraph::is_correlated() const {
    // if (not correlation_info2_->is_correlated()) return true;
    // if (not correlation_info_->empty()) return true;
    // for (auto &ds : sources_) {
    //     if (ds->is_correlated()) // recurse into data sources
    //         return true;
    // }
    return false;
}

void QueryGraph::compute_adjacency_matrix() const
{
    adjacency_matrix_ = std::make_unique<AdjacencyMatrix>(num_sources());

    /* Iterate over all joins in the query graph. */
    for (auto &join : joins()) {
        if (join->sources().size() != 2)
            throw std::invalid_argument("building adjacency matrix for non-binary join");
        /* Take both join inputs and set the appropriate bit in the adjacency matrix. */
        auto i = join->sources()[0].get().id(); // first join input
        auto j = join->sources()[1].get().id(); // second join input
        adjacency_matrix_->at(i, j) = adjacency_matrix_->at(j, i) = true;
    }
}

void QueryGraph::dot(std::ostream &out) const
{
    out << "graph query_graph\n{\n"
        << "    forcelabels=true;\n"
        << "    overlap=false;\n"
        << "    labeljust=\"l\";\n"
        << "    graph [compound=true];\n"
        << "    graph [fontname = \"DejaVu Sans\"];\n"
        << "    node [fontname = \"DejaVu Sans\"];\n"
        << "    edge [fontname = \"DejaVu Sans\"];\n";

    dot_recursive(out);

    out << '}' << std::endl;
}

void QueryGraph::dot_recursive(std::ostream &out) const
{
#define q(X) '"' << X << '"' // quote
#define id(X) q(std::hex << &X << std::dec) // convert virtual address to identifier
    for (auto &ds : sources()) {
        if (auto q = cast<Query>(ds.get()))
            q->query_graph().dot_recursive(out);
    }

    out << "\n  subgraph cluster_" << this << " {\n";

    for (auto &ds : sources()) {
        out << "    " << id(*ds) << " [label=<";
        out << "<B>" << ds->name() << "</B>";
        if (ds->filter().size())
            out << "<BR/><FONT COLOR=\"0.0 0.0 0.25\" POINT-SIZE=\"10\">"
                << html_escape(to_string(ds->filter()))
                << "</FONT>";
        out << ">,style=filled,fillcolor=\"0.0 0.0 0.8\"];\n";
        if (auto q = cast<Query>(ds.get()))
            out << "  " << id(*ds) << " -- \"cluster_" << &q->query_graph() << "\";\n";
    }

    for (auto &j : joins()) {
        out << "    " << id(*j) << " [label=<" << html_escape(to_string(j->condition())) << ">,style=filled,fillcolor=\"0.0 0.0 0.95\"];\n";
        for (auto ds : j->sources())
            out << "    " << id(*j) << " -- " << id(ds.get()) << ";\n";
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
            out << html_escape(to_string(it->first.get()));
            out << ' ' << (it->second ? "ASC" : "DESC");
        }
        out << "</FONT>\n"
            << "             </TD></TR>\n";
    }

    /* Projections */
    if (not projections_.empty()) {
        out << "             <TR><TD ALIGN=\"LEFT\">\n"
            << "               <B>π</B><FONT POINT-SIZE=\"9\">";
        for (auto it = projections_.begin(), end = projections_.end(); it != end; ++it) {
            if (it != projections_.begin())
                out << ", ";
            out << html_escape(to_string(it->first.get()));
            if (it->second)
                out << " AS " << html_escape(it->second);
        }
        out << "</FONT>\n"
            << "             </TD></TR>\n";
    }

    /* Group by and aggregates */
    if (not group_by_.empty() or not aggregates_.empty()) {
        out << "             <TR><TD ALIGN=\"LEFT\">\n"
            << "               <B>γ</B><FONT POINT-SIZE=\"9\">";
        for (auto it = group_by_.begin(), end = group_by_.end(); it != end; ++it) {
            if (it != group_by_.begin()) out << ", ";
            out << html_escape(to_string(it->first.get()));
            if (it->second)
                out << " AS " << html_escape(it->second);
        }
        if (not group_by_.empty() and not aggregates_.empty())
            out << ", ";
        for (auto it = aggregates_.begin(), end = aggregates_.end(); it != end; ++it) {
            if (it != aggregates_.begin()) out << ", ";
            out << html_escape(to_string(it->get()));
        }
        out << "</FONT>\n"
            << "             </TD></TR>\n";
    }

    out << "           </TABLE>\n"
        << "          >;\n"
        << "  }\n";
#undef id
#undef q
}

void QueryGraph::sql(std::ostream &out) const
{
    QueryGraph2SQL t(out);
    t(*this);
    out << ';' << std::endl;
}

M_LCOV_EXCL_START
void QueryGraph::dump(std::ostream &out) const
{
    out << "QueryGraph {\n  sources:";

    /*----- Print sources. -------------------------------------------------------------------------------------------*/
    for (auto &src : sources()) {
        out << "\n    ";
        if (auto q = cast<Query>(src.get())) {
            out << "(...)";
        } else {
            auto bt = as<BaseTable>(*src);
            out << bt.table().name;
        }
        if (src->alias())
            out << " AS " << src->alias();
        if (not src->filter().empty())
            out << " WHERE " << src->filter();
    }

    /*----- Print joins. ---------------------------------------------------------------------------------------------*/
    if (joins().empty()) {
        out << "\n  no joins";
    } else {
        out << "\n  joins:";
        for (auto &j : joins()) {
            out << "\n    {";
            auto &srcs = j->sources();
            for (auto it = srcs.begin(), end = srcs.end(); it != end; ++it) {
                if (it != srcs.begin()) out << ' ';
                if (it->get().alias()) {
                    out << it->get().alias();
                } else {
                    auto bt = as<const BaseTable>(it->get());
                    out << bt.name();
                }
            }
            out << "}  " << j->condition();
        }
    }

    /*----- Print grouping and aggregation information.  -------------------------------------------------------------*/
    if (group_by().empty() and aggregates().empty()) {
        out << "\n  no grouping";
    } else {
        if (not group_by().empty()) {
            out << "\n  group by: ";
            for (auto it = group_by().begin(), end = group_by().end(); it != end; ++it) {
                if (it != group_by().begin())
                    out << ", ";
                out << it->first.get();
                if (it->second)
                    out << " AS " << it->second;
            }
        }
        if (not aggregates().empty()) {
            out << "\n  aggregates: ";
            for (auto it = aggregates().begin(), end = aggregates().end(); it != end; ++it) {
                if (it != aggregates().begin())
                    out << ", ";
                out << it->get();
            }
        }
    }

    /*----- Print ordering information. ------------------------------------------------------------------------------*/
    if (order_by().empty()) {
        out << "\n  no order";
    } else {
        out << "\n  order by: ";
        for (auto it = order_by().begin(), end = order_by().end(); it != end; ++it) {
            if (it != order_by().begin())
                out << ", ";
            out << it->first.get() << ' ' << (it->second ? "ASC" : "DESC");
        }
    }

    /*----- Print projections. ---------------------------------------------------------------------------------------*/
    out << "\n  projections: ";
    for (auto [expr, alias] : projections()) {
        if (alias) {
            out << "\n    AS " << alias;
            ast::ASTDumper P(out, 3);
            P(expr.get());
        } else {
            ast::ASTDumper P(out, 2);
            P(expr.get());
        }
    }

    out << "\n}" << std::endl;
}
void QueryGraph::dump() const { dump(std::cerr); }
M_LCOV_EXCL_STOP

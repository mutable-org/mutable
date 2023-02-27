#pragma once

#include <mutable/catalog/Catalog.hpp>
#include <mutable/catalog/DatabaseCommand.hpp>
#include <mutable/parse/AST.hpp>
#include <mutable/util/Diagnostic.hpp>
#include <sstream>
#include <unordered_map>
#include <vector>


namespace m {

namespace ast {

struct M_EXPORT Sema : ASTVisitor
{
    /** Holds context information used by semantic analysis of a single statement. */
    struct SemaContext
    {
        ///> if the statement that is being analyzed is a nested query, this is its alias in the outer statement
        const char *alias = nullptr;

        ///> the statement that is currently being analyzed and for which this `SemaContext` is used
        Stmt &stmt;
        enum stage_t {
            S_From,
            S_Where,
            S_GroupBy,
            S_Having,
            S_Select,
            S_OrderBy,
            S_Limit,
        } stage = S_From; ///< current stage

        bool needs_grouping = false;

        struct result_t
        {
            std::reference_wrapper<Expr> expr_;
            ///> the order of this result column in the result set
            unsigned order;
            ///> alias of the expression; may be `nullptr`
            const char *alias = nullptr;

            result_t(Expr &expr, unsigned order) : expr_(expr), order(order) { }
            result_t(Expr &expr, unsigned order, const char *alias) : expr_(expr), order(order), alias(alias) { }

            Expr & expr() { return expr_.get(); }
            const Expr & expr() const { return expr_.get(); }
        };

        ///> list of all computed expressions along with their order
        using named_expr_table = std::unordered_multimap<const char*, std::pair<std::reference_wrapper<Expr>, unsigned>>;
        ///> the type of a source of data: either a database table or a nested query with named results
        using source_type = std::variant<std::monostate, std::reference_wrapper<const Table>, named_expr_table>;
        ///> associative container mapping source name to data source and its order
        using source_table = std::unordered_map<const char*, std::pair<source_type, unsigned>>;
        ///> list of all sources along with their order
        source_table sources;
        ///> list of all results computed by this statement along with their order
        std::unordered_multimap<const char*, result_t> results;
        ///> list of grouping keys
        std::unordered_multimap<const char*, std::reference_wrapper<Expr>> grouping_keys;

        SemaContext(Stmt &stmt) : stmt(stmt) { }

        /* Returns the `sources` in a sorted manner according to their order. */
        std::vector<std::pair<const char*, source_type>> sorted_sources() const {
            std::vector<std::pair<const char*, source_type>> res(sources.size());

            for (auto &src : sources)
                res[src.second.second] = std::make_pair(src.first, src.second.first);
            return res;
        }
    };

    private:
    /** Helper class to create a context when one is required but does not yet exist.  Automatically disposes of the
     * context when the instance goes out of scope. */
    struct RequireContext
    {
        private:
        Sema &sema_;
        bool needs_context_ = false;

        public:
        RequireContext(Sema *sema, Stmt &stmt)
            : sema_(*M_notnull(sema))
            , needs_context_(sema_.contexts_.empty())
        {
            if (needs_context_)
                sema_.push_context(stmt);
        }

        ~RequireContext() {
            if (needs_context_)
                sema_.pop_context();
        }
    };

    public:
    Diagnostic &diag;
    private:
    ///> a stack of sema contexts; one per statement; grows by nesting statements
    using context_stack_t = std::vector<SemaContext*>;
    context_stack_t contexts_;
    ///> used to create textual representation of complex AST objects, e.g. expressions
    std::ostringstream oss;
    ///> the command to execute when semantic analysis completes without errors
    std::unique_ptr<DatabaseCommand> command_;

    public:
    Sema(Diagnostic &diag) : diag(diag) { }

    /** Perform semantic analysis of an `ast::Command`. Returns an `m::DatabaseCommand` to execute when no semantic
     * errors occurred, `nullptr` otherwise. */
    std::unique_ptr<DatabaseCommand> analyze(std::unique_ptr<ast::Command> ast);

    using ASTExprVisitor::operator();
    using ASTClauseVisitor::operator();
    using ASTCommandVisitor::operator();
#define DECLARE(CLASS) void operator()(CLASS&) override;
    M_AST_EXPR_LIST(DECLARE)
    M_AST_CLAUSE_LIST(DECLARE)
    M_AST_COMMAND_LIST(DECLARE)
#undef DECLARE

    private:
    SemaContext & push_context(Stmt &stmt, const char *alias = nullptr) {
        auto &ref = contexts_.emplace_back(new SemaContext(stmt));
        ref->alias = alias;
        return *ref;
    }
    SemaContext pop_context() {
        auto ctx = *contexts_.back();
        delete contexts_.back();
        contexts_.pop_back();
        return ctx;
    }
    SemaContext & get_context() {
        M_insist(not contexts_.empty());
        return *contexts_.back();
    }
    const SemaContext & get_context() const {
        M_insist(not contexts_.empty());
        return *contexts_.back();
    }

    /** Returns true iff the current statement, that is being analyzed, is a nested statement. */
    bool is_nested() const;


    /*------------------------------------------------------------------------------------------------------------------
     * Sema Designator Helpers
     *----------------------------------------------------------------------------------------------------------------*/

    /** Creates a fresh `Designator` with the given \p name at location \p tok and with target \p target. */
    std::unique_ptr<Designator> create_designator(const char *name, Token tok, const Expr &target);

    /** Creates a fresh `Designator` with the same syntactical representation as \p name (except the table name if
     * \p drop_table_name) and with target \p target. */
    std::unique_ptr<Designator> create_designator(const Expr &name, const Expr &target, bool drop_table_name = false);

    /** Creates an entirely new `Designator`.  This method is used to introduce artificial `Designator`s to *expand* an
     * anti-projection (`SELECT` with asterisk `*`). */
    std::unique_ptr<Designator> create_designator(Position pos, const char *table_name, const char *attr_name,
                                                  typename Designator::target_type target, const Type *type)
    {
        auto &C = Catalog::Get();
        Token dot(pos, C.pool("."), TK_DOT);
        Token table(pos, table_name, TK_IDENTIFIER);
        Token attr(pos, attr_name, TK_IDENTIFIER);
        auto d = std::make_unique<Designator>(dot, table, attr);
        d->type_ = type;
        d->target_ = target;
        return d;
    }

    /** Replaces \p to_replace by a fresh `Designator`, that has the same syntactical representation as \p to_replace
     * and targets \p target. */
    void replace_by_fresh_designator_to(std::unique_ptr<Expr> &to_replace, const Expr &target);


    /*------------------------------------------------------------------------------------------------------------------
     * Other Sema Helpers
     *----------------------------------------------------------------------------------------------------------------*/

    /** Computes whether the bound parts of \p expr are composable of elements in \p components. */
    bool is_composable_of(const ast::Expr &expr, const std::vector<std::reference_wrapper<ast::Expr>> components);

    /** Recursively analyzes the `ast::Expr` referenced by \p ptr and replaces subexpressions that can be composed of
     * the elements in \p components. */
    void compose_of(std::unique_ptr<ast::Expr> &ptr, const std::vector<std::reference_wrapper<ast::Expr>> components);

    /** Creates a unique ID from a sequence of `SemaContext`s by concatenating their aliases. */
    const char * make_unique_id_from_binding_path(context_stack_t::reverse_iterator current_ctx,
                                                  context_stack_t::reverse_iterator binding_ctx);
};

}

}

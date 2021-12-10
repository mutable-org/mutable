#pragma once

#include "catalog/Schema.hpp"
#include <mutable/parse/AST.hpp>
#include <mutable/util/Diagnostic.hpp>
#include <unordered_map>
#include <vector>


namespace m {

struct Sema : ASTExprVisitor, ASTClauseVisitor, ASTStmtVisitor
{
    /** Holds context information used by semantic analysis of a single statement. */
    struct SemaContext
    {
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

        ///> list of all computed expressions along with their order
        using named_expr_table = std::unordered_multimap<const char*, std::pair<Expr*, unsigned>>;
        using source_type = std::variant<const Table*, named_expr_table>;
        using source_table = std::unordered_map<const char*, std::pair<source_type, unsigned>>;
        source_table sources; ///< list of all sources along with their order
        ///> list of all results computed by this statement along with their order
        std::unordered_multimap<const char*, std::pair<Expr*, unsigned>> results;

        std::vector<Expr*> group_keys; ///< list of group keys

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
    std::vector<SemaContext*> contexts_; ///> a stack of sema contexts; one per statement; grows by nesting statements

    public:
    Sema(Diagnostic &diag) : diag(diag) { }

    using ASTExprVisitor::Const;
    using ASTExprVisitor::operator();
    using ASTClauseVisitor::operator();
    using ASTStmtVisitor::operator();
#define DECLARE(CLASS) void operator()(Const<CLASS>&) override;
    M_AST_EXPR_LIST(DECLARE)
    M_AST_CLAUSE_LIST(DECLARE)
    M_AST_STMT_LIST(DECLARE)
#undef DECLARE

    private:
    SemaContext & push_context(Stmt &stmt) {
        contexts_.emplace_back(new SemaContext(stmt));
        return *contexts_.back();
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

    /** Creates a new designator that has the same textual representation as `from` and has `to` as target. */
    Designator * make_designator(const Expr *from, const Expr *to) {
        auto &C = Catalog::Get();
        std::ostringstream oss;
        oss << *from;
        Token tok(from->tok.pos, C.pool(oss.str().c_str()), TK_IDENTIFIER);
        auto d = new Designator(tok);
        d->type_ = to->type();
        d->target_ = to;
        return d;
    }

    Designator * make_designator(Position pos, const char *table_name, const char *attr_name,
                                 typename Designator::target_type target, const Type *type)
    {
        auto &C = Catalog::Get();
        Token dot(pos, C.pool("."), TK_DOT);
        Token table(pos, table_name, TK_IDENTIFIER);
        Token attr(pos, attr_name, TK_IDENTIFIER);
        auto d = new Designator(dot, table, attr);
        d->type_ = type;
        d->target_ = target;
        return d;
    }
};

}

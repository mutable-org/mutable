#pragma once

#include "catalog/Schema.hpp"
#include "parse/ASTVisitor.hpp"
#include "util/Diagnostic.hpp"
#include <unordered_map>
#include <vector>


namespace db {

struct Sema : ASTVisitor
{
    using ASTVisitor::operator();

    /** Holds context information used by semantic analysis of a single statement. */
    struct SemaContext
    {
        enum stage_t {
            S_From,
            S_Where,
            S_GroupBy,
            S_Having,
            S_Select,
            S_OrderBy,
            S_Limit,
        } stage = S_From;

        using named_expr_table = std::unordered_map<const char*, Expr*>;
        using source_type = std::variant<const Table*, named_expr_table>;
        using source_table = std::unordered_map<const char*, source_type>;
        source_table sources; ///> list of all sources
        std::unordered_map<const char*, Expr*> results; ///> list of all results computed by this statement

        std::vector<Expr*> group_keys; ///> list of group keys
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
        RequireContext(Sema *sema) : sema_(*notnull(sema)), needs_context_(sema_.contexts_.empty()) {
            if (needs_context_)
                sema_.push_context();
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

    /* Expressions */
    void operator()(Const<ErrorExpr> &e);
    void operator()(Const<Designator> &e);
    void operator()(Const<Constant> &e);
    void operator()(Const<FnApplicationExpr> &e);
    void operator()(Const<UnaryExpr> &e);
    void operator()(Const<BinaryExpr> &e);

    /* Clauses */
    void operator()(Const<ErrorClause> &c);
    void operator()(Const<SelectClause> &c);
    void operator()(Const<FromClause> &c);
    void operator()(Const<WhereClause> &c);
    void operator()(Const<GroupByClause> &c);
    void operator()(Const<HavingClause> &c);
    void operator()(Const<OrderByClause> &c);
    void operator()(Const<LimitClause> &c);

    /* Statements */
    void operator()(Const<ErrorStmt> &s);
    void operator()(Const<EmptyStmt> &s);
    void operator()(Const<CreateDatabaseStmt> &s);
    void operator()(Const<UseDatabaseStmt> &s);
    void operator()(Const<CreateTableStmt> &s);
    void operator()(Const<SelectStmt> &s);
    void operator()(Const<InsertStmt> &s);
    void operator()(Const<UpdateStmt> &s);
    void operator()(Const<DeleteStmt> &s);
    void operator()(Const<DSVImportStmt> &s);

    private:
    SemaContext & push_context() {
        contexts_.emplace_back(new SemaContext());
        return *contexts_.back();
    }
    SemaContext pop_context() {
        auto ctx = *contexts_.back();
        delete contexts_.back();
        contexts_.pop_back();
        return ctx;
    }
    SemaContext & get_context() {
        insist(not contexts_.empty());
        return *contexts_.back();
    }
    const SemaContext & get_context() const {
        insist(not contexts_.empty());
        return *contexts_.back();
    }
};

}

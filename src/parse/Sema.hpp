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
        std::unordered_map<const char*, const Relation*> sources; ///> lists all data sources of a statement
    };

    public:
    Diagnostic &diag;
    private:
    std::vector<SemaContext> contexts_; ///> a stack of sema contexts; one per statement; grows by nesting statements

    public:
    Sema(Diagnostic &diag) : diag(diag) { }

    /* Expressions */
    void operator()(Const<ErrorExpr> &e);
    void operator()(Const<Designator> &e);
    void operator()(Const<Constant> &e);
    void operator()(Const<FnApplicationExpr> &e);
    void operator()(Const<UnaryExpr> &e);
    void operator()(Const<BinaryExpr> &e);

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
};

}

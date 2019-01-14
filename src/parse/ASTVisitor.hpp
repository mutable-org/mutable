#pragma once

#include "parse/AST.hpp"


namespace db {

template<bool C>
struct TheASTVisitor
{
    static constexpr bool is_constant = C;

    template<typename T>
    using Const = std::conditional_t<is_constant, const T, T>;

    virtual ~TheASTVisitor() { }

    /* Expressions */
    void operator()(Const<Expr> &e) { e.accept(*this); }
    virtual void operator()(Const<ErrorExpr> &e) = 0;
    virtual void operator()(Const<Designator> &e) = 0;
    virtual void operator()(Const<Constant> &e) = 0;
    virtual void operator()(Const<FnApplicationExpr> &e) = 0;
    virtual void operator()(Const<UnaryExpr> &e) = 0;
    virtual void operator()(Const<BinaryExpr> &e) = 0;

    /* Clauses */
    void operator()(Const<Clause> &c) { c.accept(*this); }
    virtual void operator()(Const<ErrorClause> &c) = 0;
    virtual void operator()(Const<SelectClause> &c) = 0;
    virtual void operator()(Const<FromClause> &c) = 0;
    virtual void operator()(Const<WhereClause> &c) = 0;
    virtual void operator()(Const<GroupByClause> &c) = 0;
    virtual void operator()(Const<HavingClause> &c) = 0;
    virtual void operator()(Const<OrderByClause> &c) = 0;
    virtual void operator()(Const<LimitClause> &c) = 0;

    /* Statements */
    void operator()(Const<Stmt> &s) { s.accept(*this); }
    virtual void operator()(Const<ErrorStmt> &s) = 0;
    virtual void operator()(Const<EmptyStmt> &s) = 0;
    virtual void operator()(Const<CreateDatabaseStmt> &s) = 0;
    virtual void operator()(Const<UseDatabaseStmt> &s) = 0;
    virtual void operator()(Const<CreateTableStmt> &s) = 0;
    virtual void operator()(Const<SelectStmt> &s) = 0;
    virtual void operator()(Const<InsertStmt> &s) = 0;
    virtual void operator()(Const<UpdateStmt> &s) = 0;
    virtual void operator()(Const<DeleteStmt> &s) = 0;
};

using ASTVisitor = TheASTVisitor<false>;
using ConstASTVisitor = TheASTVisitor<true>;

}

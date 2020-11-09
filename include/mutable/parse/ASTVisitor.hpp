#pragma once

#include "mutable/parse/AST.hpp"


namespace m {

template<bool C>
struct TheASTExprVisitor
{
    static constexpr bool is_constant = C;

    template<typename T>
    using Const = std::conditional_t<is_constant, const T, T>;

    virtual ~TheASTExprVisitor() { }

    void operator()(Const<Expr> &e) { e.accept(*this); }
#define DECLARE(CLASS) virtual void operator()(Const<CLASS> &expr) = 0;
    DB_AST_EXPR_LIST(DECLARE)
#undef DECLARE
};
using ASTExprVisitor = TheASTExprVisitor<false>;
using ConstASTExprVisitor = TheASTExprVisitor<true>;

template<bool C>
struct TheASTClauseVisitor
{
    static constexpr bool is_constant = C;

    template<typename T>
    using Const = std::conditional_t<is_constant, const T, T>;

    virtual ~TheASTClauseVisitor() { }

    void operator()(Const<Clause> &e) { e.accept(*this); }
#define DECLARE(CLASS) virtual void operator()(Const<CLASS> &clause) = 0;
    DB_AST_CLAUSE_LIST(DECLARE)
#undef DECLARE
};
using ASTClauseVisitor = TheASTClauseVisitor<false>;
using ConstASTClauseVisitor = TheASTClauseVisitor<true>;

template<bool C>
struct TheASTConstraintVisitor
{
    static constexpr bool is_constant = C;

    template<typename T>
    using Const = std::conditional_t<is_constant, const T, T>;

    virtual ~TheASTConstraintVisitor() { }

    void operator()(Const<Constraint> &e) { e.accept(*this); }
#define DECLARE(CLASS) virtual void operator()(Const<CLASS> &constraint) = 0;
    DB_AST_CONSTRAINT_LIST(DECLARE)
#undef DECLARE
};
using ASTConstraintVisitor = TheASTConstraintVisitor<false>;
using ConstASTConstraintVisitor = TheASTConstraintVisitor<true>;

template<bool C>
struct TheASTStmtVisitor
{
    static constexpr bool is_constant = C;

    template<typename T>
    using Const = std::conditional_t<is_constant, const T, T>;

    virtual ~TheASTStmtVisitor() { }

    void operator()(Const<Stmt> &e) { e.accept(*this); }
#define DECLARE(CLASS) virtual void operator()(Const<CLASS> &stmt) = 0;
    DB_AST_STMT_LIST(DECLARE)
#undef DECLARE
};
using ASTStmtVisitor = TheASTStmtVisitor<false>;
using ConstASTStmtVisitor = TheASTStmtVisitor<true>;

template<bool C>
struct TheASTVisitor : TheASTExprVisitor<C>, TheASTClauseVisitor<C>, TheASTConstraintVisitor<C>, TheASTStmtVisitor<C>
{
    static constexpr bool is_constant = C;

    template<typename T>
    using Const = std::conditional_t<is_constant, const T, T>;

    virtual ~TheASTVisitor() { }

    using TheASTExprVisitor<C>::operator();
    using TheASTClauseVisitor<C>::operator();
    using TheASTConstraintVisitor<C>::operator();
    using TheASTStmtVisitor<C>::operator();
};
using ASTVisitor = TheASTVisitor<false>;
using ConstASTVisitor = TheASTVisitor<true>;

}

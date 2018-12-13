#pragma once

#include "lex/Token.hpp"
#include "util/assert.hpp"
#include <iostream>
#include <vector>


namespace db {

/*======================================================================================================================
 * Expressions
 *====================================================================================================================*/

/** An expression. */
struct Expr
{
    virtual ~Expr() { }

    virtual void print(std::ostream &out) const = 0;
};

/** The error expression.  Used when the parser encountered a syntactical error. */
struct ErrorExpr : Expr
{
    Token tok;

    explicit ErrorExpr(Token tok) : tok(tok) { }

    void print(std::ostream &out) const;
};

/** A designator.  Identifies an attribute, optionally preceeded by a table name. */
struct Designator : Expr
{
    Token table_name;
    Token attr_name;

    explicit Designator(Token attr_name) : attr_name(attr_name) { }

    Designator(Token table_name, Token attr_name) : table_name(table_name), attr_name(attr_name) { }

    bool has_table_name() const { return bool(table_name); }

    void print(std::ostream &out) const;
};

/** A constant: a string literal or a numeric constant. */
struct Constant : Expr
{
    Token tok;

    Constant(Token tok) : tok(tok) { }

    bool is_number() const;
    bool is_integer() const;
    bool is_float() const;
    bool is_string() const;

    void print(std::ostream &out) const;
};

/** A postfix expression. */
struct PostfixExpr : Expr
{
};

/** A function application. */
struct FnApplicationExpr : PostfixExpr
{
    Expr *fn;
    std::vector<Expr*> args;

    FnApplicationExpr(Expr *fn, std::vector<Expr*> args);

    void print(std::ostream &out) const;
};

/** A unary expression: "+e", "-e", "~e", "NOT e". */
struct UnaryExpr : Expr
{
    Token op;
    Expr *expr;

    UnaryExpr(Token op, Expr *expr) : op(op), expr(notnull(expr)) { }

    void print(std::ostream &out) const;
};

/** A binary expression.  This includes all arithmetic and logical binary operations. */
struct BinaryExpr : Expr
{
    Token op;
    Expr *lhs;
    Expr *rhs;

    BinaryExpr(Token op, Expr *lhs, Expr *rhs) : op(op), lhs(notnull(lhs)), rhs(notnull(rhs)) { }

    void print(std::ostream &out) const;
};

/*======================================================================================================================
 * Clauses
 *====================================================================================================================*/

/** A select clause. */
struct SelectClause
{
};

/** A from clause. */
struct FromClause
{
};

/** A where clause. */
struct WhereClause
{
};

/** A group-by clause. */
struct GroupByClause
{
};

/** A order-by clause. */
struct OrderByClause
{
};

/** A limit clause. */
struct LimitClause
{
};

/*======================================================================================================================
 * Statements
 *====================================================================================================================*/

/** A SQL statement. */
struct Stmt
{
};

/** A SQL select statement. */
struct SelectStmt : Stmt
{
};

/** A SQL insert statement. */
struct InsertStmt : Stmt
{
};

/** A SQL update statement. */
struct UpdateStmt : Stmt
{
};

/** A SQL delete statement. */
struct DeleteStmt : Stmt
{
};

}

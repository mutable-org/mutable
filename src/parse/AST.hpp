#pragma once

#include "lex/Token.hpp"
#include "util/assert.hpp"
#include <iostream>
#include <vector>


namespace db {

struct Type;

/*======================================================================================================================
 * Expressions
 *====================================================================================================================*/

/** An expression. */
struct Expr
{
    virtual ~Expr() { }

    virtual void print(std::ostream &out) const = 0;
    virtual void dump(std::ostream &out, int indent = 0) const = 0;
    void dump() const __attribute__((noinline)) { dump(std::cerr); }

    friend std::ostream & operator<<(std::ostream &out, const Expr &e) {
        e.print(out);
        return out;
    }
};

/** The error expression.  Used when the parser encountered a syntactical error. */
struct ErrorExpr : Expr
{
    Token tok;

    explicit ErrorExpr(Token tok) : tok(tok) { }

    void print(std::ostream &out) const;
    void dump(std::ostream &out, int indent) const;
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
    void dump(std::ostream &out, int indent) const;
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
    void dump(std::ostream &out, int indent) const;
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
    void dump(std::ostream &out, int indent) const;
};

/** A unary expression: "+e", "-e", "~e", "NOT e". */
struct UnaryExpr : Expr
{
    Token op;
    Expr *expr;

    UnaryExpr(Token op, Expr *expr) : op(op), expr(notnull(expr)) { }

    void print(std::ostream &out) const;
    void dump(std::ostream &out, int indent) const;
};

/** A binary expression.  This includes all arithmetic and logical binary operations. */
struct BinaryExpr : Expr
{
    Token op;
    Expr *lhs;
    Expr *rhs;

    BinaryExpr(Token op, Expr *lhs, Expr *rhs) : op(op), lhs(notnull(lhs)), rhs(notnull(rhs)) { }

    void print(std::ostream &out) const;
    void dump(std::ostream &out, int indent) const;
};

/*======================================================================================================================
 * Statements
 *====================================================================================================================*/

/** A SQL statement. */
struct Stmt
{
    virtual ~Stmt() { }

    virtual void print(std::ostream &out) const = 0;
    virtual void dump(std::ostream &out, int indent = 0) const = 0;
    void dump() const __attribute__((noinline)) { dump(std::cerr); }

    friend std::ostream & operator<<(std::ostream &out, const Stmt &s) {
        s.print(out);
        return out;
    }
};

/** The error statement.  Used when the parser encountered a syntactical error. */
struct ErrorStmt : Stmt
{
    Token tok;

    explicit ErrorStmt(Token tok) : tok(tok) { }

    void print(std::ostream &out) const;
    void dump(std::ostream &out, int indent) const;
};

struct CreateTableStmt : Stmt
{
    using attribute_type = std::pair<Token, const Type*>;

    Token table_name;
    std::vector<attribute_type> attributes;

    CreateTableStmt(Token table_name, std::vector<attribute_type> attributes)
        : table_name(table_name)
        , attributes(attributes)
    { }

    void print(std::ostream &out) const;
    void dump(std::ostream &out, int indent) const;
};

/** A SQL select statement. */
struct SelectStmt : Stmt
{
    bool select_all; ///> for SELECT *
    std::vector<std::pair<Expr*, Token>> select; ///> the list of selections
    std::vector<std::pair<Token, Token>> from; ///> the list of data sources
    Expr *where = nullptr; ///> the where condition
    std::vector<Expr*> group_by; ///> a list of what to group by
    Expr *having = nullptr; ///> the having condition
    std::vector<std::pair<Expr*, bool>> order_by; ///> true means ascending, false means descending
    std::pair<Expr*, Expr*> limit; ///> limit and offset

    void print(std::ostream &out) const;
    void dump(std::ostream &out, int indent) const;
};

/** A SQL insert statement. */
struct InsertStmt : Stmt
{
    enum kind_t { I_Default, I_Null, I_Expr };
    struct value_type {
        kind_t kind;
        Expr *expr = nullptr;
    };

    Token table_name;
    std::vector<value_type> values;

    void print(std::ostream &out) const;
    void dump(std::ostream &out, int indent) const;
};

/** A SQL update statement. */
struct UpdateStmt : Stmt
{
    Token table_name;
    std::vector<std::pair<Token, Expr*>> set;
    Expr *where = nullptr;

    void print(std::ostream &out) const;
    void dump(std::ostream &out, int indent) const;
};

/** A SQL delete statement. */
struct DeleteStmt : Stmt
{
    Token table_name;
    Expr *where = nullptr;

    void print(std::ostream &out) const;
    void dump(std::ostream &out, int indent) const;
};

}

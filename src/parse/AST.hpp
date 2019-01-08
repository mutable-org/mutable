#pragma once

#include "lex/Token.hpp"
#include <iostream>
#include <vector>


namespace db {

// forward declare the AST visitor
template<bool C>
struct TheASTVisitor;
using ASTVisitor = TheASTVisitor<false>;
using ConstASTVisitor = TheASTVisitor<true>;

struct Type;

/*======================================================================================================================
 * Expressions
 *====================================================================================================================*/

/** An expression. */
struct Expr
{
    virtual ~Expr() { }

    virtual void accept(ASTVisitor &v) = 0;
    virtual void accept(ConstASTVisitor &v) const = 0;

    void dump(std::ostream &out) const;
    void dump() const;

    friend std::ostream & operator<<(std::ostream &out, const Expr &e);
};

/** The error expression.  Used when the parser encountered a syntactical error. */
struct ErrorExpr : Expr
{
    Token tok;

    explicit ErrorExpr(Token tok) : tok(tok) { }

    void accept(ASTVisitor &v);
    void accept(ConstASTVisitor &v) const;
};

/** A designator.  Identifies an attribute, optionally preceeded by a table name. */
struct Designator : Expr
{
    Token table_name;
    Token attr_name;

    explicit Designator(Token attr_name) : attr_name(attr_name) { }

    Designator(Token table_name, Token attr_name) : table_name(table_name), attr_name(attr_name) { }

    void accept(ASTVisitor &v);
    void accept(ConstASTVisitor &v) const;

    bool has_table_name() const { return bool(table_name); }
};

/** A constant: a string literal or a numeric constant. */
struct Constant : Expr
{
    Token tok;

    Constant(Token tok) : tok(tok) { }

    void accept(ASTVisitor &v);
    void accept(ConstASTVisitor &v) const;

    bool is_number() const;
    bool is_integer() const;
    bool is_float() const;
    bool is_string() const;
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

    FnApplicationExpr(Expr *fn, std::vector<Expr*> args) : fn(fn), args(args) { }
    ~FnApplicationExpr();

    void accept(ASTVisitor &v);
    void accept(ConstASTVisitor &v) const;
};

/** A unary expression: "+e", "-e", "~e", "NOT e". */
struct UnaryExpr : Expr
{
    Token op;
    Expr *expr;

    UnaryExpr(Token op, Expr *expr) : op(op), expr(notnull(expr)) { }
    ~UnaryExpr() { delete expr; }

    void accept(ASTVisitor &v);
    void accept(ConstASTVisitor &v) const;
};

/** A binary expression.  This includes all arithmetic and logical binary operations. */
struct BinaryExpr : Expr
{
    Token op;
    Expr *lhs;
    Expr *rhs;

    BinaryExpr(Token op, Expr *lhs, Expr *rhs) : op(op), lhs(notnull(lhs)), rhs(notnull(rhs)) { }
    ~BinaryExpr() { delete lhs; delete rhs; }

    void accept(ASTVisitor &v);
    void accept(ConstASTVisitor &v) const;
};

/*======================================================================================================================
 * Statements
 *====================================================================================================================*/

/** A SQL statement. */
struct Stmt
{
    virtual ~Stmt() { }

    virtual void accept(ASTVisitor &v) = 0;
    virtual void accept(ConstASTVisitor &v) const = 0;

    void dump(std::ostream &out) const;
    void dump() const;

    friend std::ostream & operator<<(std::ostream &out, const Stmt &s);
};

/** The error statement.  Used when the parser encountered a syntactical error. */
struct ErrorStmt : Stmt
{
    Token tok;

    explicit ErrorStmt(Token tok) : tok(tok) { }

    void accept(ASTVisitor &v);
    void accept(ConstASTVisitor &v) const;
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

    void accept(ASTVisitor &v);
    void accept(ConstASTVisitor &v) const;
};

/** A SQL select statement. */
struct SelectStmt : Stmt
{
    using selection_type = std::pair<Expr*, Token>;
    using source_type = std::pair<Token, Token>;
    using order_type = std::pair<Expr*, bool>; ///> true means ascending, false means descending
    using limit_type = std::pair<Expr*, Expr*>; ///> limit and offset

    bool select_all = false; ///> for SELECT *
    std::vector<selection_type> select; ///> the list of selections
    std::vector<source_type> from; ///> the list of data sources
    Expr *where = nullptr; ///> the where condition
    std::vector<Expr*> group_by; ///> a list of what to group by
    Expr *having = nullptr; ///> the having condition
    std::vector<order_type> order_by; ///> the ordering
    limit_type limit; ///> limit and offset

    SelectStmt(bool select_all,
               std::vector<selection_type> select,
               std::vector<source_type> from,
               Expr *where,
               std::vector<Expr*> group_by,
               Expr *having,
               std::vector<order_type> order_by,
               limit_type limit)
        : select_all(select_all)
        , select(select)
        , from(from)
        , where(where)
        , group_by(group_by)
        , having(having)
        , order_by(order_by)
        , limit(limit)
    { }

    void accept(ASTVisitor &v);
    void accept(ConstASTVisitor &v) const;

    ~SelectStmt();
};

/** A SQL insert statement. */
struct InsertStmt : Stmt
{
    enum kind_t { I_Default, I_Null, I_Expr };
    using element_type = std::pair<kind_t, Expr*>;
    using value_type = std::vector<element_type>;

    Token table_name;
    std::vector<value_type> values;

    InsertStmt(Token table_name, std::vector<value_type> values) : table_name(table_name), values(values) { }
    ~InsertStmt();

    void accept(ASTVisitor &v);
    void accept(ConstASTVisitor &v) const;
};

/** A SQL update statement. */
struct UpdateStmt : Stmt
{
    using set_type = std::pair<Token, Expr*>;

    Token table_name;
    std::vector<set_type> set;
    Expr *where = nullptr;

    UpdateStmt(Token table_name, std::vector<set_type> set, Expr *where)
        : table_name(table_name)
        , set(set)
        , where(where)
    { }

    ~UpdateStmt();

    void accept(ASTVisitor &v);
    void accept(ConstASTVisitor &v) const;
};

/** A SQL delete statement. */
struct DeleteStmt : Stmt
{
    Token table_name;
    Expr *where = nullptr;

    DeleteStmt(Token table_name, Expr *where) : table_name(table_name), where(where) { }
    ~DeleteStmt();

    void accept(ASTVisitor &v);
    void accept(ConstASTVisitor &v) const;
};

}

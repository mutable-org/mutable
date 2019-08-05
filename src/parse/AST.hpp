#pragma once

#include "lex/Token.hpp"
#include <iostream>
#include <variant>
#include <vector>


namespace db {

// forward declare the AST visitor
template<bool C>
struct TheASTVisitor;
using ASTVisitor = TheASTVisitor<false>;
using ConstASTVisitor = TheASTVisitor<true>;

struct Type;
struct Sema;
struct Attribute;
struct Relation;
struct Stmt;

/*======================================================================================================================
 * Expressions
 *====================================================================================================================*/

/** An expression. */
struct Expr
{
    friend struct Sema;

    private:
    const Type *type_ = nullptr; ///> the type of an expression, determined by the semantic analysis

    public:
    virtual ~Expr() { }

    const Type * type() const { return notnull(type_); }
    bool has_type() const { return type_ != nullptr; }

    virtual bool operator==(const Expr &other) const = 0;
    bool operator!=(const Expr &other) const { return not operator==(other); }

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

    bool operator==(const Expr &other) const;

    void accept(ASTVisitor &v);
    void accept(ConstASTVisitor &v) const;
};

/** A designator.  Identifies an attribute, optionally preceeded by a table name. */
struct Designator : Expr
{
    friend struct Sema;

    Token table_name;
    Token attr_name;
    private:
    using target_type = std::variant<std::monostate, const Expr*, const Attribute*>;
    target_type target_; ///< the target that is referenced by this designator

    public:
    explicit Designator(Token attr_name) : attr_name(attr_name) { }

    Designator(Token table_name, Token attr_name) : table_name(table_name), attr_name(attr_name) { }

    bool operator==(const Expr &other) const;

    void accept(ASTVisitor &v);
    void accept(ConstASTVisitor &v) const;

    bool has_explicit_table_name() const { return bool(table_name); }
    bool is_identifier() const { return not has_explicit_table_name(); }

    target_type target() const { return target_; }
};

/** A constant: a string literal or a numeric constant. */
struct Constant : Expr
{
    Token tok;

    Constant(Token tok) : tok(tok) { }

    bool operator==(const Expr &other) const;

    void accept(ASTVisitor &v);
    void accept(ConstASTVisitor &v) const;

    bool is_number() const { return is_integer() or is_float(); }
    bool is_integer() const {
        return tok.type == TK_OCT_INT or
               tok.type == TK_DEC_INT or
               tok.type == TK_HEX_FLOAT;
    }
    bool is_float() const { return tok.type == TK_DEC_FLOAT or tok.type == TK_HEX_FLOAT; }
    bool is_string() const { return tok.type == TK_STRING_LITERAL; }
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

    bool operator==(const Expr &other) const;

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

    bool operator==(const Expr &other) const;

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

    bool operator==(const Expr &other) const;

    void accept(ASTVisitor &v);
    void accept(ConstASTVisitor &v) const;
};

/*======================================================================================================================
 * Clauses
 *====================================================================================================================*/

struct Clause
{
    Token tok;

    Clause(Token tok) : tok(tok) { }
    virtual ~Clause() { }

    virtual void accept(ASTVisitor &v) = 0;
    virtual void accept(ConstASTVisitor &v) const = 0;

    void dump(std::ostream &out) const;
    void dump() const;

    friend std::ostream & operator<<(std::ostream &out, const Clause &c);
};

struct ErrorClause : Clause
{
    ErrorClause(Token tok) : Clause(tok) { }

    void accept(ASTVisitor &v);
    void accept(ConstASTVisitor &v) const;
};

struct SelectClause : Clause
{
    using select_type = std::pair<Expr*, Token>; ///> list of selected elements; expr AS name

    std::vector<select_type> select;
    bool select_all;

    SelectClause(Token tok, std::vector<select_type> select, bool select_all)
        : Clause(tok)
        , select(select)
        , select_all(select_all)
    { }
    ~SelectClause();

    void accept(ASTVisitor &v);
    void accept(ConstASTVisitor &v) const;
};

struct FromClause : Clause
{
    struct from_type
    {
        friend struct Sema;

        std::variant<Token, Stmt*> source;
        Token alias;

        from_type(Token name, Token alias) : source(name), alias(alias) { }
        from_type(Stmt *S, Token alias) : source(S), alias(alias) { }

        const Relation & relation() const { return *notnull(relation_); }
        bool has_relation() const { return relation_ != nullptr; }

        private:
        const Relation *relation_ = nullptr; ///< the referenced relation
    };

    std::vector<from_type> from;

    FromClause(Token tok, std::vector<from_type> from) : Clause(tok), from(from) { }
    ~FromClause();

    void accept(ASTVisitor &v);
    void accept(ConstASTVisitor &v) const;
};

struct WhereClause : Clause
{
    Expr *where;

    WhereClause(Token tok, Expr *where) : Clause(tok), where(notnull(where)) { }
    ~WhereClause();

    void accept(ASTVisitor &v);
    void accept(ConstASTVisitor &v) const;
};

struct GroupByClause : Clause
{
    std::vector<Expr*> group_by; ///> a list of expressions to group by

    GroupByClause(Token tok, std::vector<Expr*> group_by) : Clause(tok), group_by(group_by) { }
    ~GroupByClause();

    void accept(ASTVisitor &v);
    void accept(ConstASTVisitor &v) const;
};

struct HavingClause : Clause
{
    Expr *having;

    HavingClause(Token tok, Expr *having) : Clause(tok), having(having) { }
    ~HavingClause();

    void accept(ASTVisitor &v);
    void accept(ConstASTVisitor &v) const;
};

struct OrderByClause : Clause
{
    using order_type = std::pair<Expr*, bool>; ///> true means ascending, false means descending

    std::vector<order_type> order_by;

    OrderByClause(Token tok, std::vector<order_type> order_by) : Clause(tok), order_by(order_by) { }
    ~OrderByClause();

    void accept(ASTVisitor &v);
    void accept(ConstASTVisitor &v) const;
};

struct LimitClause : Clause
{
    Token limit;
    Token offset;

    LimitClause(Token tok, Token limit, Token offset) : Clause(tok), limit(limit), offset(offset) { }

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

struct EmptyStmt : Stmt
{
    Token tok;

    explicit EmptyStmt(Token tok) : tok(tok) { }

    void accept(ASTVisitor &v);
    void accept(ConstASTVisitor &v) const;
};

struct CreateDatabaseStmt : Stmt
{
    Token database_name;

    explicit CreateDatabaseStmt(Token database_name) : database_name(database_name) { }

    void accept(ASTVisitor &v);
    void accept(ConstASTVisitor &v) const;
};


struct UseDatabaseStmt : Stmt
{
    Token database_name;

    explicit UseDatabaseStmt(Token database_name) : database_name(database_name) { }

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
    Clause *select;
    Clause *from;
    Clause *where;
    Clause *group_by;
    Clause *having;
    Clause *order_by;
    Clause *limit;

    SelectStmt(Clause *select,
               Clause *from,
               Clause *where,
               Clause *group_by,
               Clause *having,
               Clause *order_by,
               Clause *limit)
        : select(notnull(select))
        , from(notnull(from))
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
    Clause *where = nullptr;

    UpdateStmt(Token table_name, std::vector<set_type> set, Clause *where)
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
    Clause *where = nullptr;

    DeleteStmt(Token table_name, Clause *where) : table_name(table_name), where(where) { }
    ~DeleteStmt();

    void accept(ASTVisitor &v);
    void accept(ConstASTVisitor &v) const;
};

}

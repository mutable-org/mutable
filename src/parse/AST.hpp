#pragma once

#include "catalog/Schema.hpp"
#include "lex/Token.hpp"
#include <iostream>
#include <variant>
#include <vector>


namespace db {

// forward declare the AST visitor
template<bool C> struct TheASTVisitor;
using ASTVisitor = TheASTVisitor<false>;
using ConstASTVisitor = TheASTVisitor<true>;

struct Type;
struct Function;
struct Sema;
struct Attribute;
struct Table;
struct Stmt;

/*======================================================================================================================
 * Expressions
 *====================================================================================================================*/

/** An expression. */
struct Expr
{
    friend struct Sema;

    Token tok;

    private:
    const Type *type_ = nullptr; ///> the type of an expression, determined by the semantic analysis

    public:
    Expr(Token tok) : tok(tok) { }
    virtual ~Expr() { }

    const Type * type() const { return notnull(type_); }
    bool has_type() const { return type_ != nullptr; }

    virtual bool is_constant() const = 0;

    virtual bool operator==(const Expr &other) const = 0;
    bool operator!=(const Expr &other) const { return not operator==(other); }

    virtual void accept(ASTVisitor &v) = 0;
    virtual void accept(ConstASTVisitor &v) const = 0;

    /** Returns a `Schema` instance containing all required definitions (of `Attribute`s and other `Designator`s). */
    Schema get_required() const;

    void dot(std::ostream &out) const;

    void dump(std::ostream &out) const;
    void dump() const;

    friend std::ostream & operator<<(std::ostream &out, const Expr &e);
    friend std::string to_string(const Expr &e) {
        std::ostringstream oss;
        oss << e;
        return oss.str();
    }
};

/** The error expression.  Used when the parser encountered a syntactical error. */
struct ErrorExpr : Expr
{
    explicit ErrorExpr(Token tok) : Expr(tok) { }

    bool is_constant() const { return false; }

    bool operator==(const Expr &other) const;

    void accept(ASTVisitor &v);
    void accept(ConstASTVisitor &v) const;
};

/** A designator.  Identifies an attribute, optionally preceeded by a table name, a named expression, or a function. */
struct Designator : Expr
{
    friend struct Sema;

    Token table_name;
    Token attr_name;
    private:
    using target_type = std::variant<std::monostate, const Expr*, const Attribute*>;
    target_type target_; ///< the target that is referenced by this designator

    public:
    explicit Designator(Token attr_name) : Expr(attr_name), attr_name(attr_name) { }

    Designator(Token dot, Token table_name, Token attr_name) : Expr(dot), table_name(table_name), attr_name(attr_name) { }

    bool is_constant() const {
        if (auto e = std::get_if<const Expr*>(&target_))
            return (*e)->is_constant();
        return false;
    }

    bool operator==(const Expr &other) const;

    void accept(ASTVisitor &v);
    void accept(ConstASTVisitor &v) const;

    bool has_explicit_table_name() const { return bool(table_name); }
    bool is_identifier() const { return not has_explicit_table_name(); }

    bool has_table_name() const { return table_name.text != nullptr; }
    const char * get_table_name() const {
        insist(table_name.text != nullptr,
               "if the table name was not explicitly provided, semantic analysis must deduce it first");
        return table_name.text;
    }

    target_type target() const { return target_; }
};

/** A constant: a string literal or a numeric constant. */
struct Constant : Expr
{
    Constant(Token tok) : Expr(tok) { }

    bool is_constant() const { return true; }

    bool operator==(const Expr &other) const;

    void accept(ASTVisitor &v);
    void accept(ConstASTVisitor &v) const;

    bool is_null() const { return tok.type == TK_Null; }
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
    PostfixExpr(Token tok) : Expr(tok) { }
};

/** A function application. */
struct FnApplicationExpr : PostfixExpr
{
    friend struct Sema;

    Expr *fn;
    std::vector<Expr*> args;
    private:
    const Function *func_ = nullptr;

    public:
    FnApplicationExpr(Token lpar, Expr *fn, std::vector<Expr*> args) : PostfixExpr(lpar), fn(fn), args(args) { }
    ~FnApplicationExpr();

    bool is_constant() const { return false; }

    bool operator==(const Expr &other) const;

    bool has_function() const { return func_; }
    const Function & get_function() const { insist(func_); return *func_; }

    void accept(ASTVisitor &v);
    void accept(ConstASTVisitor &v) const;
};

/** A unary expression: "+e", "-e", "~e", "NOT e". */
struct UnaryExpr : Expr
{
    Expr *expr;

    UnaryExpr(Token op, Expr *expr) : Expr(op), expr(notnull(expr)) { }
    ~UnaryExpr() { delete expr; }

    bool is_constant() const { return expr->is_constant(); }
    Token op() const { return tok; }

    bool operator==(const Expr &other) const;

    void accept(ASTVisitor &v);
    void accept(ConstASTVisitor &v) const;
};

/** A binary expression.  This includes all arithmetic and logical binary operations. */
struct BinaryExpr : Expr
{
    Expr *lhs;
    Expr *rhs;

    BinaryExpr(Token op, Expr *lhs, Expr *rhs) : Expr(op), lhs(notnull(lhs)), rhs(notnull(rhs)) { }
    ~BinaryExpr() { delete lhs; delete rhs; }

    bool is_constant() const { return lhs->is_constant() and rhs->is_constant(); }
    Token op() const { return tok; }

    bool operator==(const Expr &other) const;

    void accept(ASTVisitor &v);
    void accept(ConstASTVisitor &v) const;
};

#define DB_AST_EXPR_LIST(X) \
    X(ErrorExpr) \
    X(Designator) \
    X(Constant) \
    X(FnApplicationExpr) \
    X(UnaryExpr) \
    X(BinaryExpr)


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

    void dot(std::ostream &out) const;

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
    Token select_all;
    std::vector<Expr*> expansion; ///> list of expressions expanded from `SELECT *`

    SelectClause(Token tok, std::vector<select_type> select, Token select_all)
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

        public:
        std::variant<Token, Stmt*> source;
        Token alias;

        private:
        const Table *table_ = nullptr; ///< the referenced table

        public:
        from_type(Token name, Token alias) : source(name), alias(alias) { }
        from_type(Stmt *S, Token alias) : source(S), alias(alias) { }

        const Table & table() const { return *notnull(table_); }
        bool has_table() const { return table_ != nullptr; }
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

#define DB_AST_CLAUSE_LIST(X) \
    X(ErrorClause) \
    X(SelectClause) \
    X(FromClause) \
    X(WhereClause) \
    X(GroupByClause) \
    X(HavingClause) \
    X(OrderByClause) \
    X(LimitClause)


/*======================================================================================================================
 * Constraints
 *====================================================================================================================*/

/** Abstract class to represent constraints attached to attributes of a table. */
struct Constraint
{
    Token tok;

    Constraint(Token tok) : tok(tok) { }

    virtual ~Constraint() { }
};

struct PrimaryKeyConstraint : Constraint
{
    PrimaryKeyConstraint(Token tok) : Constraint(tok) { }
};

struct UniqueConstraint : Constraint
{
    UniqueConstraint(Token tok) : Constraint(tok) { }
};

struct NotNullConstraint : Constraint
{
    NotNullConstraint(Token tok) : Constraint(tok) { }
};

struct CheckConditionConstraint : Constraint
{
    Expr *cond;

    CheckConditionConstraint(Token tok, Expr *cond) : Constraint(tok), cond(notnull(cond)) { }

    ~CheckConditionConstraint() { delete cond; }
};

struct ReferenceConstraint : Constraint
{
    enum OnDeleteAction
    {
        ON_DELETE_RESTRICT,
        ON_DELETE_CASCADE,
    };

    Token table_name;
    Token attr_name;
    OnDeleteAction on_delete;

    ReferenceConstraint(Token tok, Token table_name, Token attr_name, OnDeleteAction action)
        : Constraint(tok)
        , table_name(table_name)
        , attr_name(attr_name)
        , on_delete(action)
    { }
};

#define DB_AST_CONSTRAINT_LIST(X) \
    X(PrimaryKeyConstraint) \
    X(UniqueConstraint) \
    X(NotNullConstraint) \
    X(CheckConditionConstraint) \
    X(ReferenceConstraint)


/*======================================================================================================================
 * Statements
 *====================================================================================================================*/

/** A SQL statement. */
struct Stmt
{
    virtual ~Stmt() { }

    virtual void accept(ASTVisitor &v) = 0;
    virtual void accept(ConstASTVisitor &v) const = 0;

    void dot(std::ostream &out) const;

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
    struct attribute_definition
    {
        Token name;
        const Type *type;
        std::vector<Constraint*> constraints;

        attribute_definition(Token name, const Type *type, std::vector<Constraint*> constraints)
            : name(name)
            , type(type)
            , constraints(constraints)
        { }

        ~attribute_definition() {
            for (auto c : constraints)
                delete c;
        }
    };

    Token table_name;
    std::vector<attribute_definition*> attributes;

    CreateTableStmt(Token table_name, std::vector<attribute_definition*> attributes)
        : table_name(table_name)
        , attributes(attributes)
    { }

    ~CreateTableStmt() {
        for (auto a : attributes)
            delete a;
    }

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
    using tuple_t = std::vector<element_type>;

    Token table_name;
    std::vector<tuple_t> tuples;

    InsertStmt(Token table_name, std::vector<tuple_t> tuples) : table_name(table_name), tuples(tuples) { }
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

/** A SQL import statement. */
struct ImportStmt : Stmt
{
    Token table_name;
};

/** An import statement for a delimiter separated values (DSV) file. */
struct DSVImportStmt : ImportStmt
{
    Token path;
    Token delimiter;
    Token escape;
    Token quote;
    bool has_header = false;
    bool skip_header = false;

    void accept(ASTVisitor &v);
    void accept(ConstASTVisitor &v) const;
};

#define DB_AST_STMT_LIST(X) \
    X(ErrorStmt) \
    X(EmptyStmt) \
    X(CreateDatabaseStmt) \
    X(UseDatabaseStmt) \
    X(CreateTableStmt) \
    X(SelectStmt) \
    X(InsertStmt) \
    X(UpdateStmt) \
    X(DeleteStmt) \
    X(ImportStmt) \
    X(DSVImportStmt)

#define DB_AST_LIST(X) \
    DB_AST_EXPR_LIST(X) \
    DB_AST_CLAUSE_LIST(X) \
    DB_AST_CONSTRAINT_LIST(X) \
    DB_AST_STMT_LIST(X)

}

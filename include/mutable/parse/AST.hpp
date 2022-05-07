#pragma once

#include <iostream>
#include <mutable/catalog/Catalog.hpp>
#include <mutable/lex/Token.hpp>
#include <mutable/mutable-config.hpp>
#include <variant>
#include <vector>


namespace m {

/*----- forward declarations -----------------------------------------------------------------------------------------*/
// AST visitors
struct ASTExprVisitor;
struct ConstASTExprVisitor;
struct ASTClauseVisitor;
struct ConstASTClauseVisitor;
struct ASTConstraintVisitor;
struct ConstASTConstraintVisitor;
struct ASTStmtVisitor;
struct ConstASTStmtVisitor;

// classes
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
struct M_EXPORT Expr
{
    friend struct Sema;
    friend struct GetCorrelationInfo;

    Token tok; ///< the token of the expression; serves as an anchor to locate the expression in the source

    private:
    const Type *type_ = nullptr; ///< the type of an expression, determined by the semantic analysis

    public:
    explicit Expr(Token tok) : tok(tok) { }
    Expr(Token tok, const Type *type) : tok(tok), type_(type) { }
    virtual ~Expr() { }

    /** Returns the `Type` of this `Expr`.  Assumes that the `Expr` has been assigned a `Type` by the `Sema`. */
    virtual const Type * type() const { return M_notnull(type_); }
    /** Returns true iff this `Expr` has been assigned a `Type`, most likely by `Sema`. */
    bool has_type() const { return type_ != nullptr; }

    /** Returns true iff this `Expr` is constant, i.e. consists only of constatns and can be evaluated at compilation
     * time. */
    virtual bool is_constant() const = 0;

    /** Returns true iff this `Expr` is correlated, i.e. contains a free variable.  A free variable is a `Designator`
     * that is not bound to an `Attribute` or `Value` within the query but defined by an outer, enclosing query. */
    virtual bool is_correlated() const = 0;

    virtual bool operator==(const Expr &other) const = 0;
    bool operator!=(const Expr &other) const { return not operator==(other); }

    virtual void accept(ASTExprVisitor &v) = 0;
    virtual void accept(ConstASTExprVisitor &v) const = 0;

    /** Returns a `Schema` instance containing all required definitions (of `Attribute`s and other `Designator`s). */
    Schema get_required() const;

    /** Writes a Graphivz dot representation of this `Expr` to `out`.  Used to render ASTs with Graphivz. */
    void dot(std::ostream &out) const;

    friend std::ostream & M_EXPORT operator<<(std::ostream &out, const Expr &e);

M_LCOV_EXCL_START
    friend std::string to_string(const Expr &e) {
        std::ostringstream oss;
        oss << e;
        return oss.str();
    }
M_LCOV_EXCL_STOP

    void dump(std::ostream &out) const;
    void dump() const;
};

/** The error expression.  Used when the parser encountered a syntactical error. */
struct M_EXPORT ErrorExpr : Expr
{
    explicit ErrorExpr(Token tok) : Expr(tok) {}

    bool is_constant() const override { return false; }
    bool is_correlated() const override { return false; }

    bool operator==(const Expr &other) const override;

    void accept(ASTExprVisitor &v) override;
    void accept(ConstASTExprVisitor &v) const override;
};

/** A designator.  Identifies an attribute, optionally preceeded by a table name, a named expression, or a function. */
struct M_EXPORT Designator : Expr
{
    friend struct Sema;

    using target_type = std::variant<std::monostate, const Expr*, const Attribute*>;
    Token table_name;
    Token attr_name;
    private:
    target_type target_; ///< the target that is referenced by this designator
    bool is_correlated_ = false; ///< indicates whether this designator is correlated

    public:
    explicit Designator(Token attr_name) : Expr(attr_name), attr_name(attr_name) { }
    Designator(Token dot, Token table_name, Token attr_name) : Expr(dot), table_name(table_name), attr_name(attr_name) { }
    Designator(Token dot, Token table_name, Token attr_name, const Type *type, target_type target)
        : Expr(dot, type), table_name(table_name), attr_name(attr_name), target_(target) { }

    /** Return the type of this designator. Change into its scalar version iff correlated. */
    const Type * type() const override {
        if (auto pt = cast<const PrimitiveType>(Expr::type()); pt and is_correlated_)
            return pt->as_scalar();
        else
            return Expr::type();
    }

    bool is_constant() const override {
        if (auto e = std::get_if<const Expr*>(&target_))
            return (*e)->is_constant();
        return false;
    }

    bool is_correlated() const override { return is_correlated_; }
    /** Removes `is_correlated` flag to indicate that this designator has been decorrelated. */
    void decorrelate() { is_correlated_ = false; }

    bool operator==(const Expr &other) const override;

    void accept(ASTExprVisitor &v) override;
    void accept(ConstASTExprVisitor &v) const override;

    bool has_explicit_table_name() const { return bool(table_name); }
    bool is_identifier() const { return not has_explicit_table_name(); }

    bool has_table_name() const { return table_name.text != nullptr; }
    const char *get_table_name() const {
        M_insist(table_name.text != nullptr,
               "if the table name was not explicitly provided, semantic analysis must deduce it first");
        return table_name.text;
    }

    target_type target() const { return target_; }
};

/** A constant: a string literal or a numeric constant. */
struct M_EXPORT Constant : Expr
{
    Constant(Token tok) : Expr(tok) {}

    bool is_constant() const override { return true; }
    bool is_correlated() const override { return false; }

    bool operator==(const Expr &other) const override;

    void accept(ASTExprVisitor &v) override;
    void accept(ConstASTExprVisitor &v) const override;

    bool is_null() const { return tok.type == TK_Null; }

    bool is_number() const { return is_integer() or is_float(); }

    bool is_integer() const {
        return tok.type == TK_OCT_INT or
               tok.type == TK_DEC_INT or
               tok.type == TK_HEX_FLOAT;
    }

    bool is_float() const { return tok.type == TK_DEC_FLOAT or tok.type == TK_HEX_FLOAT; }

    bool is_string() const { return tok.type == TK_STRING_LITERAL; }

    bool is_date() const { return tok.type == TK_DATE; }

    bool is_datetime() const { return tok.type == TK_DATE_TIME; }
};

/** A postfix expression. */
struct M_EXPORT PostfixExpr : Expr
{
    PostfixExpr(Token tok) : Expr(tok) {}
};

/** A function application. */
struct M_EXPORT FnApplicationExpr : PostfixExpr
{
    friend struct Sema;

    Expr *fn;
    std::vector<Expr *> args;
    private:
    const Function *func_ = nullptr;

    public:
    FnApplicationExpr(Token lpar, Expr *fn, std::vector<Expr *> args) : PostfixExpr(lpar), fn(fn), args(args) {}
    ~FnApplicationExpr();

    bool is_constant() const override { return false; }
    bool is_correlated() const override { return false; } // TODO correlated function arguments are not yet supported

    bool operator==(const Expr &other) const override;

    bool has_function() const { return func_; }
    const Function &get_function() const {
        if (not func_)
            throw runtime_error("no function provided");
        return *func_;
    }

    void accept(ASTExprVisitor &v) override;
    void accept(ConstASTExprVisitor &v) const override;
};

/** A unary expression: "+e", "-e", "~e", "NOT e". */
struct M_EXPORT UnaryExpr : Expr
{
    Expr *expr;

    UnaryExpr(Token op, Expr *expr) : Expr(op), expr(M_notnull(expr)) {}
    ~UnaryExpr() { delete expr; }

    bool is_constant() const override { return expr->is_constant(); }
    bool is_correlated() const override { return expr->is_correlated(); }
    Token op() const { return tok; }

    bool operator==(const Expr &other) const override;

    void accept(ASTExprVisitor &v) override;
    void accept(ConstASTExprVisitor &v) const override;
};

/** A binary expression.  This includes all arithmetic and logical binary operations. */
struct M_EXPORT BinaryExpr : Expr
{
    Expr *lhs;
    Expr *rhs;

    BinaryExpr(Token op, Expr *lhs, Expr *rhs) : Expr(op), lhs(M_notnull(lhs)), rhs(M_notnull(rhs)) {}
    ~BinaryExpr() {
        delete lhs;
        delete rhs;
    }

    bool is_constant() const override { return lhs->is_constant() and rhs->is_constant(); }
    bool is_correlated() const override { return lhs->is_correlated() or rhs->is_correlated(); }
    Token op() const { return tok; }

    bool operator==(const Expr &other) const override;

    void accept(ASTExprVisitor &v) override;
    void accept(ConstASTExprVisitor &v) const override;
};

/** A query expression for nested queries. */
struct M_EXPORT QueryExpr : Expr
{
    Stmt *query;

    private:
    const char *alias_; ///> the alias that is used for this query expression

    public:
    QueryExpr(Token op, Stmt *query) : Expr(op), query(M_notnull(query)), alias_(make_unique_alias()) { }
    ~QueryExpr();

    bool is_constant() const override;
    bool is_correlated() const override;

    bool operator==(const Expr &other) const override;

    void accept(ASTExprVisitor &v) override;
    void accept(ConstASTExprVisitor &v) const override;

    const char * alias() const { return alias_; }

    private:
    static const char * make_unique_alias() {
        static uint64_t id(0);
        std::ostringstream oss;
        oss << "q_" << id++;
        Catalog &C = Catalog::Get();
        return C.pool(oss.str().c_str());
    }
};

#define M_AST_EXPR_LIST(X) \
    X(ErrorExpr) \
    X(Designator) \
    X(Constant) \
    X(FnApplicationExpr) \
    X(UnaryExpr) \
    X(BinaryExpr) \
    X(QueryExpr)

M_DECLARE_VISITOR(ASTExprVisitor, Expr, M_AST_EXPR_LIST)
M_DECLARE_VISITOR(ConstASTExprVisitor, const Expr, M_AST_EXPR_LIST)


/*======================================================================================================================
 * Clauses
 *====================================================================================================================*/

struct M_EXPORT Clause
{
    Token tok;

    Clause(Token tok) : tok(tok) { }
    virtual ~Clause() { }

    virtual void accept(ASTClauseVisitor &v) = 0;
    virtual void accept(ConstASTClauseVisitor &v) const = 0;

    void dot(std::ostream &out) const;

    void dump(std::ostream &out) const;
    void dump() const;

    friend std::ostream & M_EXPORT operator<<(std::ostream &out, const Clause &c);
};

struct M_EXPORT ErrorClause : Clause
{
    ErrorClause(Token tok) : Clause(tok) { }

    void accept(ASTClauseVisitor &v) override;
    void accept(ConstASTClauseVisitor &v) const override;
};

struct M_EXPORT SelectClause : Clause
{
    using select_type = std::pair<Expr*, Token>; ///> list of selected elements; expr AS name

    std::vector<select_type> select;
    Token select_all;
    std::vector<const Expr*> expansion; ///> list of expressions expanded from `SELECT *`

    SelectClause(Token tok, std::vector<select_type> select, Token select_all)
            : Clause(tok)
            , select(select)
            , select_all(select_all)
    { }
    ~SelectClause();

    void accept(ASTClauseVisitor &v) override;
    void accept(ConstASTClauseVisitor &v) const override;
};

struct M_EXPORT FromClause : Clause
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

        const Table & table() const { return *M_notnull(table_); }
        bool has_table() const { return table_ != nullptr; }
    };

    std::vector<from_type> from;

    FromClause(Token tok, std::vector<from_type> from) : Clause(tok), from(from) { }
    ~FromClause();

    void accept(ASTClauseVisitor &v) override;
    void accept(ConstASTClauseVisitor &v) const override;
};

struct M_EXPORT WhereClause : Clause
{
    Expr *where;

    WhereClause(Token tok, Expr *where) : Clause(tok), where(M_notnull(where)) { }
    ~WhereClause();

    void accept(ASTClauseVisitor &v) override;
    void accept(ConstASTClauseVisitor &v) const override;
};

struct M_EXPORT GroupByClause : Clause
{
    std::vector<Expr*> group_by; ///> a list of expressions to group by

    GroupByClause(Token tok, std::vector<Expr*> group_by) : Clause(tok), group_by(group_by) { }
    ~GroupByClause();

    void accept(ASTClauseVisitor &v) override;
    void accept(ConstASTClauseVisitor &v) const override;
};

struct M_EXPORT HavingClause : Clause
{
    Expr *having;

    HavingClause(Token tok, Expr *having) : Clause(tok), having(having) { }
    ~HavingClause();

    void accept(ASTClauseVisitor &v) override;
    void accept(ConstASTClauseVisitor &v) const override;
};

struct M_EXPORT OrderByClause : Clause
{
    using order_type = std::pair<Expr*, bool>; ///> true means ascending, false means descending

    std::vector<order_type> order_by;

    OrderByClause(Token tok, std::vector<order_type> order_by) : Clause(tok), order_by(order_by) { }
    ~OrderByClause();

    void accept(ASTClauseVisitor &v) override;
    void accept(ConstASTClauseVisitor &v) const override;
};

struct M_EXPORT LimitClause : Clause
{
    Token limit;
    Token offset;

    LimitClause(Token tok, Token limit, Token offset) : Clause(tok), limit(limit), offset(offset) { }

    void accept(ASTClauseVisitor &v) override;
    void accept(ConstASTClauseVisitor &v) const override;
};

#define M_AST_CLAUSE_LIST(X) \
    X(ErrorClause) \
    X(SelectClause) \
    X(FromClause) \
    X(WhereClause) \
    X(GroupByClause) \
    X(HavingClause) \
    X(OrderByClause) \
    X(LimitClause)

M_DECLARE_VISITOR(ASTClauseVisitor, Clause, M_AST_CLAUSE_LIST)
M_DECLARE_VISITOR(ConstASTClauseVisitor, const Clause, M_AST_CLAUSE_LIST)


/*======================================================================================================================
 * Constraints
 *====================================================================================================================*/

/** Abstract class to represent constraints attached to attributes of a table. */
struct M_EXPORT Constraint
{
    Token tok;

    Constraint(Token tok) : tok(tok) { }

    virtual ~Constraint() { }

    virtual void accept(ASTConstraintVisitor &v) = 0;
    virtual void accept(ConstASTConstraintVisitor &v) const = 0;
};

struct M_EXPORT PrimaryKeyConstraint : Constraint
{
    PrimaryKeyConstraint(Token tok) : Constraint(tok) { }

    void accept(ASTConstraintVisitor &v) override;
    void accept(ConstASTConstraintVisitor &v) const override;
};

struct M_EXPORT UniqueConstraint : Constraint
{
    UniqueConstraint(Token tok) : Constraint(tok) { }

    void accept(ASTConstraintVisitor &v) override;
    void accept(ConstASTConstraintVisitor &v) const override;
};

struct M_EXPORT NotNullConstraint : Constraint
{
    NotNullConstraint(Token tok) : Constraint(tok) { }

    void accept(ASTConstraintVisitor &v) override;
    void accept(ConstASTConstraintVisitor &v) const override;
};

struct M_EXPORT CheckConditionConstraint : Constraint
{
    Expr *cond;

    CheckConditionConstraint(Token tok, Expr *cond) : Constraint(tok), cond(M_notnull(cond)) { }

    ~CheckConditionConstraint() { delete cond; }

    void accept(ASTConstraintVisitor &v) override;
    void accept(ConstASTConstraintVisitor &v) const override;
};

struct M_EXPORT ReferenceConstraint : Constraint
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

    void accept(ASTConstraintVisitor &v) override;
    void accept(ConstASTConstraintVisitor &v) const override;
};

#define M_AST_CONSTRAINT_LIST(X) \
    X(PrimaryKeyConstraint) \
    X(UniqueConstraint) \
    X(NotNullConstraint) \
    X(CheckConditionConstraint) \
    X(ReferenceConstraint)

M_DECLARE_VISITOR(ASTConstraintVisitor, Constraint, M_AST_CONSTRAINT_LIST)
M_DECLARE_VISITOR(ConstASTConstraintVisitor, const Constraint, M_AST_CONSTRAINT_LIST)


/*======================================================================================================================
 * Statements
 *====================================================================================================================*/

/** A SQL statement. */
struct M_EXPORT Stmt
{
    virtual ~Stmt() { }

    virtual void accept(ASTStmtVisitor &v) = 0;
    virtual void accept(ConstASTStmtVisitor &v) const = 0;

    void dot(std::ostream &out) const;

    void dump(std::ostream &out) const;
    void dump() const;

    friend std::ostream & M_EXPORT operator<<(std::ostream &out, const Stmt &s);
};

/** The error statement.  Used when the parser encountered a syntactical error. */
struct M_EXPORT ErrorStmt : Stmt
{
    Token tok;

    explicit ErrorStmt(Token tok) : tok(tok) { }

    void accept(ASTStmtVisitor &v) override;
    void accept(ConstASTStmtVisitor &v) const override;
};

struct M_EXPORT EmptyStmt : Stmt
{
    Token tok;

    explicit EmptyStmt(Token tok) : tok(tok) { }

    void accept(ASTStmtVisitor &v) override;
    void accept(ConstASTStmtVisitor &v) const override;
};

struct M_EXPORT CreateDatabaseStmt : Stmt
{
    Token database_name;

    explicit CreateDatabaseStmt(Token database_name) : database_name(database_name) { }

    void accept(ASTStmtVisitor &v) override;
    void accept(ConstASTStmtVisitor &v) const override;
};

struct M_EXPORT UseDatabaseStmt : Stmt
{
    Token database_name;

    explicit UseDatabaseStmt(Token database_name) : database_name(database_name) { }

    void accept(ASTStmtVisitor &v) override;
    void accept(ConstASTStmtVisitor &v) const override;
};

struct M_EXPORT CreateTableStmt : Stmt
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

    void accept(ASTStmtVisitor &v) override;
    void accept(ConstASTStmtVisitor &v) const override;
};

/** A SQL select statement. */
struct M_EXPORT SelectStmt : Stmt
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
            : select(M_notnull(select))
            , from(from)
            , where(where)
            , group_by(group_by)
            , having(having)
            , order_by(order_by)
            , limit(limit)
    { }

    void accept(ASTStmtVisitor &v) override;
    void accept(ConstASTStmtVisitor &v) const override;

    ~SelectStmt();
};

/** A SQL insert statement. */
struct M_EXPORT InsertStmt : Stmt
{
    enum kind_t { I_Default, I_Null, I_Expr };
    using element_type = std::pair<kind_t, Expr*>;
    using tuple_t = std::vector<element_type>;

    Token table_name;
    std::vector<tuple_t> tuples;

    InsertStmt(Token table_name, std::vector<tuple_t> tuples) : table_name(table_name), tuples(tuples) { }
    ~InsertStmt();

    void accept(ASTStmtVisitor &v) override;
    void accept(ConstASTStmtVisitor &v) const override;
};

/** A SQL update statement. */
struct M_EXPORT UpdateStmt : Stmt
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

    void accept(ASTStmtVisitor &v) override;
    void accept(ConstASTStmtVisitor &v) const override;
};

/** A SQL delete statement. */
struct M_EXPORT DeleteStmt : Stmt
{
    Token table_name;
    Clause *where = nullptr;

    DeleteStmt(Token table_name, Clause *where) : table_name(table_name), where(where) { }
    ~DeleteStmt();

    void accept(ASTStmtVisitor &v) override;
    void accept(ConstASTStmtVisitor &v) const override;
};

/** A SQL import statement. */
struct M_EXPORT ImportStmt : Stmt
{
    Token table_name;
};

/** An import statement for a delimiter separated values (DSV) file. */
struct M_EXPORT DSVImportStmt : ImportStmt
{
    Token path;
    Token delimiter;
    Token escape;
    Token quote;
    Token rows;
    bool has_header = false;
    bool skip_header = false;

    void accept(ASTStmtVisitor &v) override;
    void accept(ConstASTStmtVisitor &v) const override;
};

#define M_AST_STMT_LIST(X) \
    X(ErrorStmt) \
    X(EmptyStmt) \
    X(CreateDatabaseStmt) \
    X(UseDatabaseStmt) \
    X(CreateTableStmt) \
    X(SelectStmt) \
    X(InsertStmt) \
    X(UpdateStmt) \
    X(DeleteStmt) \
    X(DSVImportStmt)

M_DECLARE_VISITOR(ASTStmtVisitor, Stmt, M_AST_STMT_LIST)
M_DECLARE_VISITOR(ConstASTStmtVisitor, const Stmt, M_AST_STMT_LIST)

#define M_AST_LIST(X) \
    M_AST_EXPR_LIST(X) \
    M_AST_CLAUSE_LIST(X) \
    M_AST_CONSTRAINT_LIST(X) \
    M_AST_STMT_LIST(X)


struct M_EXPORT ASTVisitor : ASTExprVisitor, ASTClauseVisitor, ASTConstraintVisitor, ASTStmtVisitor
{
    template<typename T> using Const = ASTExprVisitor::Const<T>; // resolve ambiguous name lookup
    using ASTExprVisitor::operator();
    using ASTClauseVisitor::operator();
    using ASTConstraintVisitor::operator();
    using ASTStmtVisitor::operator();
};
struct M_EXPORT ConstASTVisitor : ConstASTExprVisitor, ConstASTClauseVisitor, ConstASTConstraintVisitor, ConstASTStmtVisitor
{
    template<typename T> using Const = ConstASTExprVisitor::Const<T>; // resolve ambiguous name lookup
    using ConstASTExprVisitor::operator();
    using ConstASTClauseVisitor::operator();
    using ConstASTConstraintVisitor::operator();
    using ConstASTStmtVisitor::operator();
};

}

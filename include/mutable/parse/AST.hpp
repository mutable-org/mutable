#pragma once

#include <iostream>
#include <memory>
#include <mutable/catalog/Schema.hpp>
#include <mutable/lex/Token.hpp>
#include <mutable/mutable-config.hpp>
#include <variant>
#include <vector>


namespace m {

struct Type;
struct Function;
struct Attribute;
struct Table;

namespace ast {

struct ASTClauseVisitor;
struct ASTCommandVisitor;
struct ASTConstraintVisitor;
struct ASTExprVisitor;
struct ConstASTClauseVisitor;
struct ConstASTCommandVisitor;
struct ConstASTConstraintVisitor;
struct ConstASTExprVisitor;
struct Sema;
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
    ///> the type of an expression, determined by the semantic analysis
    const Type *type_ = nullptr;
    /** Whether this `Expr` is correlated within its query, i.e. whether this `Expr` contains free variables that are
     * bound by an outside query. */
    bool is_correlated_ = false;

    public:
    explicit Expr(Token tok) : tok(tok) { }
    Expr(Token tok, const Type *type) : tok(tok), type_(type) { }
    virtual ~Expr() { }

    /** Returns the `Type` of this `Expr`.  Assumes that the `Expr` has been assigned a `Type` by the `Sema`. */
    const Type * type() const { return M_notnull(type_); }
    /** Returns true iff this `Expr` has been assigned a `Type`, most likely by `Sema`. */
    bool has_type() const { return type_ != nullptr; }

    /** Returns true iff this `Expr` is constant, i.e. consists only of constants and can be evaluated at compilation
     * time. */
    virtual bool is_constant() const = 0;
    /** Returns true iff this `Expr` is nullable, i.e. may evaluate to `NULL` at runtime. */
    virtual bool can_be_null() const = 0;

    /** Returns `true` iff this `Expr` contains a free variable.  A free variable is a `Designator` that is not bound to
     * an `Attribute` or another `Expr` within the query but defined by an outer, enclosing statement. */
    virtual bool contains_free_variables() const = 0;
    /** Returns `true` iff this `Expr` contains a bound variable.  A bound variable is a `Designator` that is bound to
     * an `Attribute` or another `Expr` within the query. */
    virtual bool contains_bound_variables() const  = 0;

    /** Computes a hash of `this`, considering only syntactic properties.  Other properties are ignored, e.g. type or
     * designator target. */
    virtual uint64_t hash() const = 0;

    /** Returns `true` iff \p other is *syntactically* equal to `this`. */
    virtual bool operator==(const Expr &other) const = 0;
    /** Returns `false` iff \p other is *syntactically* equal to `this`. */
    bool operator!=(const Expr &other) const { return not operator==(other); }

    virtual void accept(ASTExprVisitor &v) = 0;
    virtual void accept(ConstASTExprVisitor &v) const = 0;

    /** Returns a `Schema` instance containing all required definitions.  Recursively traverses the `Expr` to find all
     * `Designators`.  Note, that `FnApplicationExpr`s require special handling: a `FnApplicationExpr` that is an
     * aggregation is stringified to a `Schema::Identifier` and not processed further recursively.  The reason for this
     * special treatment is that aggregations are performed during grouping and the aggregate value is referenced by the
     * stringified expression (or alias, if given). */
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
    bool can_be_null() const override { return false; }
    bool contains_free_variables() const override { return false; }
    bool contains_bound_variables() const override { return false; }

    uint64_t hash() const override { return 8546018603292329429UL; }

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
    unsigned binding_depth_ = 0; ///< at which level above this designator is bound; 0 for bound variables, ≥ 1 for free
    const char *unique_id_ = nullptr; ///< a unique ID created for Designators in nested queries (for decorrelation)

    public:
    explicit Designator(Token attr_name) : Expr(attr_name), attr_name(attr_name) { }

    Designator(Token dot, Token table_name, Token attr_name)
        : Expr(dot)
        , table_name(table_name)
        , attr_name(attr_name)
    { }

    Designator(Token dot, Token table_name, Token attr_name, const Type *type, target_type target)
        : Expr(dot, type)
        , table_name(table_name)
        , attr_name(attr_name)
        , target_(target)
    { }

    bool is_constant() const override {
        if (auto e = std::get_if<const Expr*>(&target_))
            return (*e)->is_constant();
        return false;
    }
    bool can_be_null() const override {
        return std::visit(overloaded {
            [](const Expr *e) -> bool { return e->can_be_null(); },
            [](const Attribute *a) -> bool { return not a->not_nullable; },
            [](std::monostate) -> bool { return true; }
        }, target_);
    }

    /** Returns `true` iff this `Designator` is a free variable. */
    bool contains_free_variables() const override { return binding_depth_ != 0; }
    /** Returns `false` iff this `Designator` is a free variable. */
    bool contains_bound_variables() const override { return binding_depth_ == 0; }

    unsigned binding_depth() const { return binding_depth_; }

    uint64_t hash() const override;

    bool operator==(const Expr &other) const override;

    void accept(ASTExprVisitor &v) override;
    void accept(ConstASTExprVisitor &v) const override;

    bool has_explicit_table_name() const { return bool(table_name); }
    /** Returns `true` iff this `Designator` has *no* table name, neither explicitly nor implicitly (by sema). */
    bool is_identifier() const { return not has_table_name(); }

    bool has_table_name() const { return table_name.text != nullptr; }
    const char * get_table_name() const {
        M_insist(table_name.text != nullptr,
               "if the table name was not explicitly provided, semantic analysis must deduce it first");
        return table_name.text;
    }

    const target_type & target() const { return target_; }

    private:
    /** Marks this `Designator` as free variable, i.e. *not* being bound by the query. */
    void set_binding_depth(unsigned depth) { binding_depth_ = depth; }
    void decrease_binding_depth() { M_insist(binding_depth_ > 0); --binding_depth_; }
};

/** A constant: a string literal or a numeric constant. */
struct M_EXPORT Constant : Expr
{
    Constant(Token tok) : Expr(tok) {}

    bool is_constant() const override { return true; }
    bool can_be_null() const override { return is_null(); }
    bool contains_free_variables() const override { return false; }
    bool contains_bound_variables() const override { return false; }

    uint64_t hash() const override;

    bool operator==(const Expr &other) const override;

    void accept(ASTExprVisitor &v) override;
    void accept(ConstASTExprVisitor &v) const override;

    bool is_null() const { return tok.type == TK_Null; }
    bool is_bool() const { return tok.type == TK_True or tok.type == TK_False; }
    bool is_number() const { return is_integer() or is_float(); }
    bool is_integer() const { return tok.type == TK_OCT_INT or tok.type == TK_DEC_INT or tok.type == TK_HEX_FLOAT; }
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

    std::unique_ptr<Expr> fn;
    std::vector<std::unique_ptr<Expr>> args;
    private:
    const Function *func_ = nullptr;

    public:
    FnApplicationExpr(Token lpar, std::unique_ptr<Expr> fn, std::vector<std::unique_ptr<Expr>> args)
        : PostfixExpr(lpar)
        , fn(M_notnull(std::move(fn)))
        , args(std::move(args))
    {
#ifndef NDEBUG
        for (auto &arg : args) M_insist(bool(arg));
#endif
    }

    bool is_constant() const override { return false; }
    bool can_be_null() const override {
        if (not func_)
            return true;
        switch (func_->fnid) {
            default:
                M_unreachable("function kind not implemented");

            case m::Function::FN_UDF:
                return true;

            case Function::FN_COUNT:
            case Function::FN_ISNULL:
                return false;

            case Function::FN_MIN:
            case Function::FN_MAX:
            case Function::FN_SUM:
            case Function::FN_AVG:
            case Function::FN_INT:
                M_insist(args.size() == 1);
                return args[0]->can_be_null();
        }
    }

    /* A `FnApplicationExpr` is correlated iff at least one argument is correlated. */
    bool contains_free_variables() const override {
        // if (fn->contains_free_variables()) return true;
        for (auto &arg : args)
            if (arg->contains_free_variables())
                return true;
        return false;
    }

    bool contains_bound_variables() const override {
        if (fn->contains_bound_variables()) return true;
        for (auto &arg : args)
            if (arg->contains_bound_variables())
                return true;
        return false;
    }

    uint64_t hash() const override;

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
    std::unique_ptr<Expr> expr;

    UnaryExpr(Token op, std::unique_ptr<Expr> expr)
        : Expr(op)
        , expr(M_notnull(std::move(expr)))
    { }

    bool is_constant() const override { return expr->is_constant(); }
    bool can_be_null() const override { return expr->can_be_null(); }
    bool contains_free_variables() const override { return expr->contains_free_variables(); }
    bool contains_bound_variables() const override { return expr->contains_bound_variables(); }
    Token op() const { return tok; }

    uint64_t hash() const override;

    bool operator==(const Expr &other) const override;

    void accept(ASTExprVisitor &v) override;
    void accept(ConstASTExprVisitor &v) const override;
};

/** A binary expression.  This includes all arithmetic and logical binary operations. */
struct M_EXPORT BinaryExpr : Expr
{
    std::unique_ptr<Expr> lhs;
    std::unique_ptr<Expr> rhs;
    const Numeric *common_operand_type = nullptr;

    BinaryExpr(Token op, std::unique_ptr<Expr> lhs, std::unique_ptr<Expr> rhs)
        : Expr(op)
        , lhs(M_notnull(std::move(lhs)))
        , rhs(M_notnull(std::move(rhs)))
    { }

    bool is_constant() const override { return lhs->is_constant() and rhs->is_constant(); }
    bool can_be_null() const override {
        if (tok.type == TK_And or tok.type == TK_Or) { // special case handling for ternary logic of `AND` and `OR`
            /* TODO: use `is_constant()` and check whether one side *evaluates* to dominating element */
            const auto dominating_element = tok.type == TK_And ? TK_False : TK_True;
            const auto lhs_const = cast<Constant>(lhs.get());
            const auto rhs_const = cast<Constant>(rhs.get());
            const bool lhs_dominates = lhs_const and lhs_const->tok.type == dominating_element;
            const bool rhs_dominates = rhs_const and rhs_const->tok.type == dominating_element;
            return not lhs_dominates and not rhs_dominates and (lhs->can_be_null() or rhs->can_be_null());
        }
        return lhs->can_be_null() or rhs->can_be_null();
    }

    bool contains_free_variables() const override
    { return lhs->contains_free_variables() or rhs->contains_free_variables(); }
    bool contains_bound_variables() const override
    { return lhs->contains_bound_variables() or rhs->contains_bound_variables(); }
    Token op() const { return tok; }

    uint64_t hash() const override;

    bool operator==(const Expr &other) const override;

    void accept(ASTExprVisitor &v) override;
    void accept(ConstASTExprVisitor &v) const override;
};

/** A query expression for nested queries. */
struct M_EXPORT QueryExpr : Expr
{
    std::unique_ptr<Stmt> query;

    private:
    const char *alias_; ///< the alias that is used for this query expression

    public:
    QueryExpr(Token op, std::unique_ptr<Stmt> query)
        : Expr(op)
        , query(M_notnull(std::move(query)))
        , alias_(M_notnull(make_unique_alias()))
    { }

    bool is_constant() const override;
    bool can_be_null() const override;

    /** Conceptually, `QueryExpr`s have no variables at all.  They only contain another `SelectStmt`.  The correlation
     * of expressions within this `SelectStmt` does not matter here.  Therefore, `QueryExpr`s never have free variables
     * and this method always returns `false`. */
    bool contains_free_variables() const override { return false; }
    /** Conceptually, `QueryExpr`s have no variables at all.  They only contain another `SelectStmt`.  The correlation
     * of expressions within this `SelectStmt` does not matter here.  Therefore, conceptually, all variables within the
     * `QueryExpr` are bound and this method always returns `true`. */
    bool contains_bound_variables() const override { return true; }

    uint64_t hash() const override;

    bool operator==(const Expr &other) const override;

    void accept(ASTExprVisitor &v) override;
    void accept(ConstASTExprVisitor &v) const override;

    const char * alias() const { return M_notnull(alias_); }

    private:
    static const char * make_unique_alias();
};

#define M_AST_EXPR_LIST(X) \
    X(m::ast::ErrorExpr) \
    X(m::ast::Designator) \
    X(m::ast::Constant) \
    X(m::ast::FnApplicationExpr) \
    X(m::ast::UnaryExpr) \
    X(m::ast::BinaryExpr) \
    X(m::ast::QueryExpr)

M_DECLARE_VISITOR(ASTExprVisitor, Expr, M_AST_EXPR_LIST)
M_DECLARE_VISITOR(ConstASTExprVisitor, const Expr, M_AST_EXPR_LIST)

/** A generic base class for implementing recursive `ast::Expr` visitors. */
template<bool C>
struct TheRecursiveExprVisitorBase : std::conditional_t<C, ConstASTExprVisitor, ASTExprVisitor>
{
    using super = std::conditional_t<C, ConstASTExprVisitor, ASTExprVisitor>;
    template<typename T> using Const = typename super::template Const<T>;

    virtual ~TheRecursiveExprVisitorBase() { }

    using super::operator();
    void operator()(Const<FnApplicationExpr> &e) override {
        (*this)(*e.fn);
        for (auto &arg : e.args)
            (*this)(*arg);
    }
    void operator()(Const<UnaryExpr> &e) override { (*this)(*e.expr); }
    void operator()(Const<BinaryExpr> &e) override { (*this)(*e.lhs); (*this)(*e.rhs); }
};

using RecursiveExprVisitorBase = TheRecursiveExprVisitorBase<false>;
using RecursiveConstExprVisitorBase = TheRecursiveExprVisitorBase<true>;

template<bool C>
struct M_EXPORT ThePreOrderExprVisitor : std::conditional_t<C, ConstASTExprVisitor, ASTExprVisitor>
{
    using super = std::conditional_t<C, ConstASTExprVisitor, ASTExprVisitor>;
    template<typename T> using Const = typename super::template Const<T>;

    virtual ~ThePreOrderExprVisitor() { }

    void operator()(Const<Expr> &e);
};

template<bool C>
struct M_EXPORT ThePostOrderExprVisitor : std::conditional_t<C, ConstASTExprVisitor, ASTExprVisitor>
{
    using super = std::conditional_t<C, ConstASTExprVisitor, ASTExprVisitor>;
    template<typename T> using Const = typename super::template Const<T>;

    virtual ~ThePostOrderExprVisitor() { }

    void operator()(Const<Expr> &e);
};

using PreOrderExprVisitor = ThePreOrderExprVisitor<false>;
using ConstPreOrderExprVisitor = ThePreOrderExprVisitor<true>;
using PostOrderExprVisitor = ThePostOrderExprVisitor<false>;
using ConstPostOrderExprVisitor = ThePostOrderExprVisitor<true>;

M_MAKE_STL_VISITABLE(PreOrderExprVisitor, Expr, M_AST_EXPR_LIST)
M_MAKE_STL_VISITABLE(ConstPreOrderExprVisitor, const Expr, M_AST_EXPR_LIST)
M_MAKE_STL_VISITABLE(PostOrderExprVisitor, Expr, M_AST_EXPR_LIST)
M_MAKE_STL_VISITABLE(ConstPostOrderExprVisitor, const Expr, M_AST_EXPR_LIST)


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
    using select_type = std::pair<std::unique_ptr<Expr>, Token>; ///> list of selected elements; expr AS name

    std::vector<select_type> select;
    Token select_all;
    std::vector<std::unique_ptr<Expr>> expanded_select_all; ///> list of expressions expanded from `SELECT *`

    SelectClause(Token tok, std::vector<select_type> select, Token select_all)
            : Clause(tok)
            , select(std::move(select))
            , select_all(select_all)
    { }

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
        from_type(std::unique_ptr<Stmt> S, Token alias) : source(S.release()), alias(alias) { }

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
    std::unique_ptr<Expr> where;

    WhereClause(Token tok, std::unique_ptr<Expr> where)
        : Clause(tok)
        , where(M_notnull(std::move(where)))
    { }

    void accept(ASTClauseVisitor &v) override;
    void accept(ConstASTClauseVisitor &v) const override;
};

struct M_EXPORT GroupByClause : Clause
{
    using group_type = std::pair<std::unique_ptr<Expr>, Token>;
    std::vector<group_type> group_by; ///> a list of expressions to group by

    GroupByClause(Token tok, std::vector<group_type> group_by)
        : Clause(tok)
        , group_by(std::move(group_by))
    { }

    void accept(ASTClauseVisitor &v) override;
    void accept(ConstASTClauseVisitor &v) const override;
};

struct M_EXPORT HavingClause : Clause
{
    std::unique_ptr<Expr> having;

    HavingClause(Token tok, std::unique_ptr<Expr> having)
        : Clause(tok)
        , having(M_notnull(std::move(having)))
    { }

    void accept(ASTClauseVisitor &v) override;
    void accept(ConstASTClauseVisitor &v) const override;
};

struct M_EXPORT OrderByClause : Clause
{
    using order_type = std::pair<std::unique_ptr<Expr>, bool>; ///> true means ascending, false means descending

    std::vector<order_type> order_by;

    OrderByClause(Token tok, std::vector<order_type> order_by)
        : Clause(tok),
        order_by(std::move(order_by))
    { }

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
    X(m::ast::ErrorClause) \
    X(m::ast::SelectClause) \
    X(m::ast::FromClause) \
    X(m::ast::WhereClause) \
    X(m::ast::GroupByClause) \
    X(m::ast::HavingClause) \
    X(m::ast::OrderByClause) \
    X(m::ast::LimitClause)

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
    std::unique_ptr<Expr> cond;

    CheckConditionConstraint(Token tok, std::unique_ptr<Expr> cond)
        : Constraint(tok)
        , cond(std::move(cond))
    {
        M_insist(bool(this->cond));
    }

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
    X(m::ast::PrimaryKeyConstraint) \
    X(m::ast::UniqueConstraint) \
    X(m::ast::NotNullConstraint) \
    X(m::ast::CheckConditionConstraint) \
    X(m::ast::ReferenceConstraint)

M_DECLARE_VISITOR(ASTConstraintVisitor, Constraint, M_AST_CONSTRAINT_LIST)
M_DECLARE_VISITOR(ConstASTConstraintVisitor, const Constraint, M_AST_CONSTRAINT_LIST)


/*======================================================================================================================
 * Command
 *====================================================================================================================*/

struct M_EXPORT Command
{
    virtual ~Command() = default;

    virtual void accept(ASTCommandVisitor &v) = 0;
    virtual void accept(ConstASTCommandVisitor &v) const = 0;

    void dump(std::ostream &out) const;
    void dump() const;

    friend std::ostream & M_EXPORT operator<<(std::ostream &out, const Command &cmd);
};


/*======================================================================================================================
 * Instruction
 *====================================================================================================================*/

struct M_EXPORT Instruction : Command
{
    ///> the token of the `Instruction`; starts with `\`
    Token tok;
    ///> the name of the `Instruction` (without leading `\`)
    const char *name;
    ///> the arguments to the `Instruction`; may be empty
    std::vector<std::string> args;

    Instruction(Token tok, const char *name, std::vector<std::string> args)
        : tok(tok)
        , name(name)
        , args(std::move(args))
    { }

    void accept(ASTCommandVisitor &v) override;
    void accept(ConstASTCommandVisitor &v) const override;
};


/*======================================================================================================================
 * Statements
 *====================================================================================================================*/

/** A SQL statement. */
struct M_EXPORT Stmt : Command
{
    virtual ~Stmt() { }

    /** Writes a Graphivz dot representation of this `Stmt` to `out`.  Used to render ASTs with Graphivz. */
    void dot(std::ostream &out) const;
};

/** The error statement.  Used when the parser encountered a syntactical error. */
struct M_EXPORT ErrorStmt : Stmt
{
    Token tok;

    explicit ErrorStmt(Token tok) : tok(tok) { }

    void accept(ASTCommandVisitor &v) override;
    void accept(ConstASTCommandVisitor &v) const override;
};

struct M_EXPORT EmptyStmt : Stmt
{
    Token tok;

    explicit EmptyStmt(Token tok) : tok(tok) { }

    void accept(ASTCommandVisitor &v) override;
    void accept(ConstASTCommandVisitor &v) const override;
};

struct M_EXPORT CreateDatabaseStmt : Stmt
{
    Token database_name;

    explicit CreateDatabaseStmt(Token database_name) : database_name(database_name) { }

    void accept(ASTCommandVisitor &v) override;
    void accept(ConstASTCommandVisitor &v) const override;
};

struct M_EXPORT UseDatabaseStmt : Stmt
{
    Token database_name;

    explicit UseDatabaseStmt(Token database_name) : database_name(database_name) { }

    void accept(ASTCommandVisitor &v) override;
    void accept(ConstASTCommandVisitor &v) const override;
};

struct M_EXPORT CreateTableStmt : Stmt
{
    struct attribute_definition
    {
        Token name;
        const Type *type;
        std::vector<std::unique_ptr<Constraint>> constraints;

        attribute_definition(Token name, const Type *type, std::vector<std::unique_ptr<Constraint>> constraints)
                : name(name)
                , type(type)
                , constraints(std::move(constraints))
        { }
    };

    Token table_name;
    std::vector<std::unique_ptr<attribute_definition>> attributes;

    CreateTableStmt(Token table_name, std::vector<std::unique_ptr<attribute_definition>> attributes)
            : table_name(table_name)
            , attributes(std::move(attributes))
    { }

    void accept(ASTCommandVisitor &v) override;
    void accept(ConstASTCommandVisitor &v) const override;
};

/** A SQL select statement. */
struct M_EXPORT SelectStmt : Stmt
{
    std::unique_ptr<Clause> select;
    std::unique_ptr<Clause> from;
    std::unique_ptr<Clause> where;
    std::unique_ptr<Clause> group_by;
    std::unique_ptr<Clause> having;
    std::unique_ptr<Clause> order_by;
    std::unique_ptr<Clause> limit;

    SelectStmt(std::unique_ptr<Clause> select,
               std::unique_ptr<Clause> from,
               std::unique_ptr<Clause> where,
               std::unique_ptr<Clause> group_by,
               std::unique_ptr<Clause> having,
               std::unique_ptr<Clause> order_by,
               std::unique_ptr<Clause> limit)
            : select(M_notnull(std::move(select)))
            , from(std::move(from))
            , where(std::move(where))
            , group_by(std::move(group_by))
            , having(std::move(having))
            , order_by(std::move(order_by))
            , limit(std::move(limit))
    { }

    void accept(ASTCommandVisitor &v) override;
    void accept(ConstASTCommandVisitor &v) const override;
};

/** A SQL insert statement. */
struct M_EXPORT InsertStmt : Stmt
{
    enum kind_t { I_Default, I_Null, I_Expr };
    using element_type = std::pair<kind_t, std::unique_ptr<Expr>>;
    using tuple_t = std::vector<element_type>;

    Token table_name;
    std::vector<tuple_t> tuples;

    InsertStmt(Token table_name, std::vector<tuple_t> tuples)
        : table_name(table_name)\
        , tuples(std::move(tuples))
    { }

    void accept(ASTCommandVisitor &v) override;
    void accept(ConstASTCommandVisitor &v) const override;
};

/** A SQL update statement. */
struct M_EXPORT UpdateStmt : Stmt
{
    using set_type = std::pair<Token, std::unique_ptr<Expr>>;

    Token table_name;
    std::vector<set_type> set;
    std::unique_ptr<Clause> where;

    UpdateStmt(Token table_name, std::vector<set_type> set, std::unique_ptr<Clause> where)
        : table_name(table_name)
        , set(std::move(set))
        , where(std::move(where))
    { }

    void accept(ASTCommandVisitor &v) override;
    void accept(ConstASTCommandVisitor &v) const override;
};

/** A SQL delete statement. */
struct M_EXPORT DeleteStmt : Stmt
{
    Token table_name;
    std::unique_ptr<Clause> where = nullptr;

    DeleteStmt(Token table_name, std::unique_ptr<Clause> where)
        : table_name(table_name)
        , where(std::move(where))
    { }

    void accept(ASTCommandVisitor &v) override;
    void accept(ConstASTCommandVisitor &v) const override;
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

    void accept(ASTCommandVisitor &v) override;
    void accept(ConstASTCommandVisitor &v) const override;
};

#define M_AST_COMMAND_LIST(X) \
    X(m::ast::Instruction) \
    X(m::ast::ErrorStmt) \
    X(m::ast::EmptyStmt) \
    X(m::ast::CreateDatabaseStmt) \
    X(m::ast::UseDatabaseStmt) \
    X(m::ast::CreateTableStmt) \
    X(m::ast::SelectStmt) \
    X(m::ast::InsertStmt) \
    X(m::ast::UpdateStmt) \
    X(m::ast::DeleteStmt) \
    X(m::ast::DSVImportStmt)

M_DECLARE_VISITOR(ASTCommandVisitor, Command, M_AST_COMMAND_LIST)
M_DECLARE_VISITOR(ConstASTCommandVisitor, const Command, M_AST_COMMAND_LIST)


#define M_AST_LIST(X) \
    M_AST_EXPR_LIST(X) \
    M_AST_CLAUSE_LIST(X) \
    M_AST_CONSTRAINT_LIST(X) \
    M_AST_COMMAND_LIST(X)

struct M_EXPORT ASTVisitor : ASTExprVisitor, ASTClauseVisitor, ASTConstraintVisitor, ASTCommandVisitor
{
    template<typename T> using Const = ASTExprVisitor::Const<T>; // resolve ambiguous name lookup

    using ASTExprVisitor::operator();
    using ASTClauseVisitor::operator();
    using ASTConstraintVisitor::operator();
    using ASTCommandVisitor::operator();
};

struct M_EXPORT ConstASTVisitor : ConstASTExprVisitor, ConstASTClauseVisitor, ConstASTConstraintVisitor, ConstASTCommandVisitor
{
    template<typename T> using Const = ConstASTExprVisitor::Const<T>; // resolve ambiguous name lookup

    using ConstASTExprVisitor::operator();
    using ConstASTClauseVisitor::operator();
    using ConstASTConstraintVisitor::operator();
    using ConstASTCommandVisitor::operator();
};

M_MAKE_STL_VISITABLE(ASTVisitor, Expr, M_AST_EXPR_LIST)
M_MAKE_STL_VISITABLE(ASTVisitor, Clause, M_AST_CLAUSE_LIST)
M_MAKE_STL_VISITABLE(ASTVisitor, Constraint, M_AST_CONSTRAINT_LIST)
M_MAKE_STL_VISITABLE(ASTVisitor, Command, M_AST_COMMAND_LIST)

M_MAKE_STL_VISITABLE(ConstASTVisitor, const Expr, M_AST_LIST)
M_MAKE_STL_VISITABLE(ConstASTVisitor, const Clause, M_AST_LIST)
M_MAKE_STL_VISITABLE(ConstASTVisitor, const Constraint, M_AST_LIST)
M_MAKE_STL_VISITABLE(ConstASTVisitor, const Command, M_AST_LIST)

}

}

namespace std {

/** Specialization of `std::hash` to `m::ast::Expr`. */
template<>
struct hash<m::ast::Expr>
{
    std::size_t operator()(const m::ast::Expr &e) const { return e.hash(); }
};

}

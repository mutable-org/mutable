#pragma once

#include "backend/PhysicalOperator.hpp"
#include "backend/WasmDSL.hpp"
#include <functional>
#include <mutable/catalog/Schema.hpp>
#include <mutable/parse/AST.hpp>
#include <mutable/util/concepts.hpp>
#include <optional>
#include <variant>


namespace m {

namespace wasm {

// forward declarations
struct Environment;
template<bool IsGlobal> struct Buffer;
template<bool IsGlobal> struct buffer_load_proxy_t;
template<bool IsGlobal> struct buffer_store_proxy_t;
template<bool IsGlobal> struct buffer_swap_proxy_t;


/*======================================================================================================================
 * Declare SQL helper type for character sequences
 *====================================================================================================================*/

struct NChar : Ptr<Charx1>
{
    private:
    bool can_be_null_;
    const CharacterSequence *type_;

    public:
    NChar(Ptr<Charx1> ptr, bool can_be_null, const CharacterSequence *type)
        : Ptr<Charx1>(ptr), can_be_null_(can_be_null), type_(type)
    { }
    NChar(Ptr<Charx1> ptr, bool can_be_null, std::size_t length, bool guarantees_terminating_nul)
        : Ptr<Charx1>(ptr)
        , can_be_null_(can_be_null)
        , type_(guarantees_terminating_nul ? Type::Get_Varchar(Type::TY_Scalar, length)
                                           : Type::Get_Char(Type::TY_Scalar, length))
    { }

    NChar(NChar&) = default;
    NChar(NChar&&) = default;

    NChar clone() const { return NChar(Ptr<Charx1>::clone(), can_be_null_, type_); }

    Ptr<Charx1> val() { return *this; }

    Boolx1 is_null() {
        if (can_be_null()) {
            return Ptr<Charx1>::is_null();
        } else {
            discard();
            return Boolx1(false);
        }
    }
    Boolx1 not_null() {
        if (can_be_null()) {
            return Ptr<Charx1>::not_null();
        } else {
            discard();
            return Boolx1(true);
        }
    }

    bool can_be_null() const { return can_be_null_; }
    std::size_t length() const { return type_->length; }
    uint64_t size_in_bytes() const { return type_->size() / 8; }
    bool guarantees_terminating_nul() const { return type_->is_varying; }
};


/*======================================================================================================================
 * Decimal
 *====================================================================================================================*/

template<signed_integral Base>
struct Decimal : Expr<Base>
{
    using expr_type = Expr<Base>;
    using arithmetic_type = Base;

    private:
    ///> the number of decimal digits right of the decimal point
    uint32_t scale_;

    public:
    /** Constructs a `Decimal` from a given \p value at the given \p scale.  For example, a \p value of `142` at a \p
     * scale of `2` would represent `1.42`. */
    Decimal(Base value, uint32_t scale) : expr_type(value), scale_(scale) {
        M_insist(scale != 0);
    }

    /** Constructs a `Decimal` from a given \p value at the given \p scale.  For example, a \p value of `142` at a \p
     * scale of `2` would represent `1.42`. */
    Decimal(expr_type value, uint32_t scale) : expr_type(value), scale_(scale) { }

    Decimal(Decimal &other) : Decimal(other.val(), other.scale_) { }
    Decimal(Decimal &&other) : Decimal(other.val(), other.scale_) { }

    /** Constructs a `Decimal` from a given \p value at the given \p scale.  For example, a \p value of `142` at a \p
     * scale of `2` would represent `142.00`. */
    static Decimal Scaled(expr_type value, uint32_t scale) {
        const arithmetic_type scaling_factor = powi(arithmetic_type(10), scale);
        return Decimal(value * scaling_factor, scale);
    }


    public:
    expr_type val() { return *this; }

    uint32_t scale() const { return scale_; }

    template<signed_integral To>
    Expr<To> to() { return val().template to<To>() / powi(To(10), To(scale_)); }

    Decimal clone() const { return Decimal(expr_type::clone(), scale_); }


    /*------------------------------------------------------------------------------------------------------------------
     * Unary operations
     *----------------------------------------------------------------------------------------------------------------*/

    /** Bitwise negation is not supported by `Decimal` type. */
    Decimal operator~()
    requires false
    { M_unreachable("modulo division on decimals is not defined"); }


    /*------------------------------------------------------------------------------------------------------------------
     * Binary operations
     *----------------------------------------------------------------------------------------------------------------*/

    Decimal operator+(Decimal other) {
        if (this->scale_ == other.scale_)
            return Decimal(this->val() + other.val(), scale_); // fall back to regular integer arithmetic

        if (this->scale() > other.scale()) {
            const arithmetic_type scaling_factor = powi(arithmetic_type(10), this->scale() - other.scale());
            return Decimal(this->val() + (other * scaling_factor).val() , this->scale());
        } else {
            const arithmetic_type scaling_factor = powi(arithmetic_type(10), other.scale() - this->scale());
            return Decimal((*this * scaling_factor).val() + other.val() , other.scale());
        }
    }

    template<typename T>
    requires expr_convertible<T> and signed_integral<typename expr_t<T>::type>
    Decimal operator+(T &&other) { return Decimal(*this + Scaled(expr_t<T>(other), scale_)); }

    Decimal operator-(Decimal other) {
        if (this->scale_ == other.scale_)
            return Decimal(this->val() - other.val(), scale_); // fall back to regular integer arithmetic

        if (this->scale() > other.scale()) {
            const arithmetic_type scaling_factor = powi(arithmetic_type(10), this->scale() - other.scale());
            return Decimal(this->val() - (other * scaling_factor).val() , this->scale());
        } else {
            const arithmetic_type scaling_factor = powi(arithmetic_type(10), other.scale() - this->scale());
            return Decimal((*this * scaling_factor).val() - other.val() , other.scale());
        }
    }

    template<typename T>
    requires expr_convertible<T> and signed_integral<typename expr_t<T>::type>
    Decimal operator-(T &&other) { return Decimal(*this - Scaled(expr_t<T>(other), scale_)); }

    Decimal operator*(Decimal other) {
        uint32_t smaller_scale = this->scale();
        uint32_t higher_scale = other.scale();
        if (smaller_scale > higher_scale)
            std::swap(smaller_scale, higher_scale);
        M_insist(smaller_scale <= higher_scale);
        const arithmetic_type scaling_factor = powi(arithmetic_type(10), smaller_scale);
        return Decimal((this->val() * other.val()) / scaling_factor, higher_scale);
    }

    template<typename T>
    requires expr_convertible<T> and signed_integral<typename expr_t<T>::type>
    Decimal operator*(T &&other) { return Decimal(this->val() * expr_t<T>(other), scale()); }

    Decimal operator/(Decimal other) {
        const uint32_t scale_res = std::max(this->scale(), other.scale());
        const arithmetic_type scaling_factor = powi(arithmetic_type(10), scale_res + other.scale() - this->scale());
        return Decimal((this->val() * scaling_factor) / other.val(), scale_res);
    }

    template<typename T>
    requires expr_convertible<T> and signed_integral<typename expr_t<T>::type>
    Decimal operator/(T &&other) { return Decimal(this->val() / expr_t<T>(other), scale()); }

    /** Modulo division is not supported by `Decimal` type. */
    template<typename T>
    requires false
    Decimal operator%(T&&) { M_unreachable("modulo division on decimals is not defined"); }

    /** Bitwise and is not supported by `Decimal` type. */
    template<typename T>
    requires false
    Decimal operator bitand(T&&) { M_unreachable("bitwise and on decimals is not defined"); }

    /** Bitwise or is not supported by `Decimal` type. */
    template<typename T>
    requires false
    Decimal operator bitor(T&&) { M_unreachable("bitwise or on decimals is not defined"); }

    /** Bitwise xor (exclusive or) is not supported by `Decimal` type. */
    template<typename T>
    requires false
    Decimal operator xor(T&&) { M_unreachable("exclusive or on decimals is not defined"); }

    /** Shift left is not supported by `Decimal` type. */
    template<typename T>
    requires false
    Decimal operator<<(T&&) { M_unreachable("shift left on decimals is not defined"); }

    /** Shift right is not supported by `Decimal` type. */
    template<typename T>
    requires false
    Decimal operator>>(T&&) { M_unreachable("shift right on decimals is not defined"); }

    /** Shift right is not supported by `Decimal` type. */
    template<typename T>
    requires false
    Decimal rotl(T&&) { M_unreachable("rotate left on decimals is not defined"); }

    /** Shift right is not supported by `Decimal` type. */
    template<typename T>
    requires false
    Decimal rotr(T&&) { M_unreachable("rotate right on decimals is not defined"); }


    friend std::ostream & operator<<(std::ostream &out, const Decimal &d) {
        return out << "Decimal: " << static_cast<const expr_type&>(d) << ", scale = " << d.scale_;
    }

    void dump(std::ostream &out) const { out << *this << std::endl; }
    void dump() const { dump(std::cerr); }
};

/*----------------------------------------------------------------------------------------------------------------------
 * Binary operations
 *--------------------------------------------------------------------------------------------------------------------*/

template<typename Base>
requires expr_convertible<Base> and signed_integral<typename expr_t<Base>::type>
Decimal<typename expr_t<Base>::type> operator+(Base &&left, Decimal<typename expr_t<Base>::type> right)
{
    return right + left;
}

template<typename Base>
requires expr_convertible<Base> and signed_integral<typename expr_t<Base>::type>
Decimal<typename expr_t<Base>::type> operator-(Base &&left, Decimal<typename expr_t<Base>::type> right)
{
    return Decimal<typename expr_t<Base>::type>::Scaled(expr_t<Base>(left), right.scale()) - right;
}

template<typename Base>
requires expr_convertible<Base> and signed_integral<typename expr_t<Base>::type>
Decimal<typename expr_t<Base>::type> operator*(Base &&left, Decimal<typename expr_t<Base>::type> right)
{
    return right * left;
}

template<typename Base>
requires expr_convertible<Base> and signed_integral<typename expr_t<Base>::type>
Decimal<typename expr_t<Base>::type> operator/(Base &&left, Decimal<typename expr_t<Base>::type> right)
{
    return Decimal<typename expr_t<Base>::type>(expr_t<Base>(left), 0) / right;
}

using Decimal32 = Decimal<int32_t>;
using Decimal64 = Decimal<int64_t>;



/*======================================================================================================================
 * Declare valid SQL types
 *====================================================================================================================*/

template<typename>
struct is_sql_type;

#define SQL_TYPES_SCALAR(X) \
    X(_Boolx1) \
    X(_I8x1) \
    X(_I16x1) \
    X(_I32x1) \
    X(_I64x1) \
    X(_Floatx1) \
    X(_Doublex1) \
    X(NChar)

#define SQL_TYPES(X) \
    SQL_TYPES_SCALAR(X) \
    X(_Boolx16) \
    X(_I8x16) \
    X(_I16x8) \
    X(_I16x16) \
    X(_I32x4) \
    X(_I32x8) \
    X(_I32x16) \
    X(_I64x2) \
    X(_I64x4) \
    X(_I64x8) \
    X(_I64x16) \
    X(_Floatx4) \
    X(_Floatx8) \
    X(_Floatx16) \
    X(_Doublex2) \
    X(_Doublex4) \
    X(_Doublex8) \
    X(_Doublex16)

#define ADD_EXPR_SQL_TYPE(TYPE) template<> struct is_sql_type<TYPE>{};
SQL_TYPES(ADD_EXPR_SQL_TYPE)
#undef ADD_EXPR_SQL_TYPE

template<typename T>
concept sql_type = requires { is_sql_type<T>{}; };

using SQL_t = std::variant<
    std::monostate
#define ADD_TYPE(TYPE) , TYPE
    SQL_TYPES(ADD_TYPE)
#undef ADD_TYPE
>;


template<typename T>
concept sql_boolean_type = sql_type<T> and boolean<typename T::type>;

using SQL_boolean_t = std::variant<std::monostate, _Boolx1, _Boolx16>;


/*======================================================================================================================
 * Helper functions for SQL types
 *====================================================================================================================*/

inline void discard(SQL_t &variant)
{
    std::visit(overloaded {
        []<sql_type T>(T actual) -> void { actual.discard(); },
        [](std::monostate) -> void { M_unreachable("invalid variant"); },
    }, variant);
}

template<sql_type To>
inline To convert(SQL_t &variant)
{
    using type = typename To::type;
    static constexpr std::size_t num_simd_lanes = To::num_simd_lanes;

    return std::visit(overloaded {
        [](auto actual) -> To requires requires { actual.template to<type, num_simd_lanes>(); } {
            return actual.template to<type, num_simd_lanes>();
        },
        [](auto actual) -> To requires (not requires { actual.template to<type, num_simd_lanes>(); }) {
            M_unreachable("illegal conversion");
        },
        [](std::monostate) -> To { M_unreachable("invalid variant"); },
    }, variant);
}

inline bool can_be_null(const SQL_t &variant)
{
    return std::visit(overloaded {
        []<sql_type T>(const T &actual) -> bool { return actual.can_be_null(); },
        [](std::monostate) -> bool { M_unreachable("invalid variant"); },
    }, variant);
}

template<std::size_t L = 1>
inline Bool<L> is_null(SQL_t &variant)
{
    return std::visit(overloaded {
        []<sql_type T>(T actual) -> Bool<L> requires requires { { actual.is_null() } -> std::same_as<Bool<L>>; } {
            return actual.is_null();
        },
        []<sql_type T>(T actual) -> Bool<L> requires (not requires { { actual.is_null() } -> std::same_as<Bool<L>>; }) {
            M_unreachable("invalid type for given number of SIMD lanes");
        },
        [](std::monostate) -> Bool<L> { M_unreachable("invalid variant"); },
    }, variant);
}

template<std::size_t L = 1>
inline Bool<L> not_null(SQL_t &variant)
{
    return std::visit(overloaded {
        []<sql_type T>(T actual) -> Bool<L> requires requires { { actual.not_null() } -> std::same_as<Bool<L>>; } {
            return actual.not_null();
        },
        []<sql_type T>(T actual) -> Bool<L> requires (not requires { { actual.not_null() } -> std::same_as<Bool<L>>; }) {
            M_unreachable("invalid type for given number of SIMD lanes");
        },
        [](std::monostate) -> Bool<L> { M_unreachable("invalid variant"); },
    }, variant);
}


/*======================================================================================================================
 * ExprCompiler
 *====================================================================================================================*/

/** Compiles AST expressions `m::Expr` to Wasm ASTs `m::wasm::Expr<T>`.  Also supports compiling `m::cnf::CNF`s. */
struct ExprCompiler : ast::ConstASTExprVisitor
{
    private:
    ///> current intermediate results during AST traversal and compilation
    SQL_t intermediate_result_;
    ///> the environment to use for resolving designators to `Expr<T>`s
    const Environment &env_;

    public:
    ExprCompiler(const Environment &env) : env_(env) { }

    ///> Compiles a `m::Expr` \p e of statically unknown type to a `SQL_t`.
    SQL_t compile(const m::ast::Expr &e) {
        (*this)(e);
        return std::move(intermediate_result_);
    }
    ///> Compile a `m::Expr` \p e of statically known type to \tparam T.
    template<sql_type T>
    T compile(const m::ast::Expr &e) {
        (*this)(e);
        M_insist(std::holds_alternative<T>(intermediate_result_));
        return *std::get_if<T>(&intermediate_result_);
    }

    ///> Compile a `m::cnf::CNF` \p cnf of statically unknown type to a `SQL_t`.
    SQL_boolean_t compile(const cnf::CNF &cnf);

    ///> Compile a `m::cnf::CNF` \p cnf of statically known type to \tparam T.
    template<sql_boolean_type T>
    T compile(const cnf::CNF &cnf) {
        auto result = compile(cnf);
        M_insist(std::holds_alternative<T>(result));
        return *std::get_if<T>(&result);
    }

    private:
    using ConstASTExprVisitor::operator();
    void operator()(const ast::ErrorExpr&) override;
    void operator()(const ast::Designator &op) override;
    void operator()(const ast::Constant &op) override;
    void operator()(const ast::UnaryExpr &op) override;
    void operator()(const ast::BinaryExpr &op) override;
    void operator()(const ast::FnApplicationExpr &op) override;
    void operator()(const ast::QueryExpr &op) override;

    SQL_t get() { return std::move(intermediate_result_); }

    template<sql_type T>
    T get() {
        M_insist(std::holds_alternative<T>(intermediate_result_));
        return *std::get_if<T>(&intermediate_result_);
    }

    void set(SQL_t &&value) {
        intermediate_result_.~SQL_t(); // destroy old
        new (&intermediate_result_) SQL_t(std::move(value)); // placement-new
    }

    template<sql_type T>
    void set(T &&value) { set(SQL_t(std::forward<T>(value))); }
};


/*======================================================================================================================
 * Environment
 *====================================================================================================================*/

/** Binds `Schema::Identifier`s to `Expr<T>`s. */
struct Environment
{
    private:
    ///> maps `Schema::Identifier`s to `Expr<T>`s that evaluate to the current expression
    std::unordered_map<Schema::Identifier, SQL_t> exprs_;
    ///> optional predicate if predication is used
    SQL_boolean_t predicate_;

    public:
    Environment() = default;
    Environment(const Environment&) = delete;
    Environment(Environment&&) = default;

    ~Environment() {
        for (auto &p : exprs_)
            discard(p.second);
        /* do not discard `predicate_` to make sure predication predicate is used if it was set */
    }

    /*----- Access methods -------------------------------------------------------------------------------------------*/
    /** Returns `true` iff this `Environment` contains \p id. */
    bool has(Schema::Identifier id) const { return exprs_.find(id) != exprs_.end(); }
    /** Returns `true` iff the entry for identifier \p id has `sql_type` \tparam T. */
    template<sql_type T>
    bool is(Schema::Identifier id) const {
        auto it = exprs_.find(id);
        M_insist(it != exprs_.end(), "identifier not found");
        return std::holds_alternative<T>(it->second);
    }
    /** Returns `true` iff this `Environment` is empty. */
    bool empty() const { return exprs_.empty(); }

    ///> Adds a mapping from \p id to \p expr.
    template<sql_type T>
    void add(Schema::Identifier id, T &&expr) {
        auto res = exprs_.emplace(id, std::forward<T>(expr));
        M_insist(res.second, "duplicate ID");
    }
    ///> Adds a mapping from \p id to \p expr.
    void add(Schema::Identifier id, SQL_t &&expr) {
        auto res = exprs_.emplace(id, std::move(expr));
        M_insist(res.second, "duplicate ID");
    }

    ///> **Copies** all entries of \p other into `this`.
    void add(const Environment &other) {
        for (auto &p : other.exprs_) {
            std::visit(overloaded {
                [](std::monostate) -> void { M_unreachable("invalid expression"); },
                [this, &p](auto &e) -> void { this->add(p.first, e.clone()); },
            }, p.second);
        }
    }
    ///> **Moves** all entries of \p other into `this`.
    void add(Environment &&other) {
        this->exprs_.merge(other.exprs_);
        M_insist(other.exprs_.empty(), "duplicate ID not moved from other to this");
    }

    ///> Returns the **moved** entry for identifier \p id.
    SQL_t extract(Schema::Identifier id) {
        auto it = exprs_.find(id);
        M_insist(it != exprs_.end(), "identifier not found");
        auto nh = exprs_.extract(it);
        return std::move(nh.mapped());
    }
    ///> Returns the **moved** entry for identifier \p id.
    template<sql_type T>
    T extract(Schema::Identifier id) {
        auto it = exprs_.find(id);
        M_insist(it != exprs_.end(), "identifier not found");
        auto nh = exprs_.extract(it);
        M_insist(std::holds_alternative<T>(nh.mapped()));
        return *std::get_if<T>(&nh.mapped());
    }

    ///> Returns the **copied** entry for identifier \p id.
    SQL_t get(Schema::Identifier id) const {
        auto it = exprs_.find(id);
        M_insist(it != exprs_.end(), "identifier not found");
        return std::visit(overloaded {
            [](auto &e) -> SQL_t { return e.clone(); },
            [](std::monostate) -> SQL_t { M_unreachable("invalid expression"); },
        }, it->second);
    }
    ///> Returns the **copied** entry for identifier \p id.
    template<sql_type T>
    T get(Schema::Identifier id) const {
        auto it = exprs_.find(id);
        M_insist(it != exprs_.end(), "identifier not found");
        M_insist(std::holds_alternative<T>(it->second));
        return std::get_if<T>(&it->second)->clone();
    }
    ///> Returns the **copied** entry for identifier \p id.
    SQL_t operator[](Schema::Identifier id) const { return get(id); }


    /*----- Expression and CNF compilation ---------------------------------------------------------------------------*/
    ///> Compile \p t by delegating compilation to an `ExprCompiler` for `this` `Environment`.
    template<typename T>
    requires requires (ExprCompiler C, T &&t) { C.compile(std::forward<T>(t)); }
    auto compile(T &&t) const {
        ExprCompiler C(*this);
        return C.compile(std::forward<T>(t));
    }
     ///> Compile \p t by delegating compilation to an `ExprCompiler` for `this` `Environment`.
    template<typename T, typename U>
    requires requires (ExprCompiler C, U &&u) { C.compile<T>(std::forward<U>(u)); }
    auto compile(U &&u) const {
        ExprCompiler C(*this);
        return C.compile<T>(std::forward<U>(u));
    }

    /*----- Predication ----------------------------------------------------------------------------------------------*/
    ///> Returns `true` iff `this` `Environment` uses predication.
    bool predicated() const { return not std::holds_alternative<std::monostate>(predicate_); }

    ///> Adds the predicate \p pred to the predication predicate.
    void add_predicate(SQL_boolean_t &&pred) {
        std::visit(overloaded {
            [](std::monostate) -> void { M_unreachable("invalid predicate"); },
            [this]<sql_boolean_type T>(T &&e) -> void { this->add_predicate<T>(std::forward<T>(e)); },
        }, std::forward<SQL_boolean_t>(pred));
    }
    ///> Adds the predicate \p pred to the predication predicate.
    template<sql_boolean_type T>
    void add_predicate(T &&pred) {
        if (predicated()) {
            auto old = extract_predicate<T>();
            new (&predicate_) SQL_boolean_t(old and std::forward<T>(pred)); // placement-new
        } else {
            new (&predicate_) SQL_boolean_t(std::forward<T>(pred)); // placement-new
        }
    }
    ///> Adds the predicate compiled from the `cnf::CNF` \p cnf to the predication predicate.
    void add_predicate(const cnf::CNF &cnf) { add_predicate(compile(cnf)); }

    ///> Returns the **moved** current predication predicate.
    SQL_boolean_t extract_predicate() {
        M_insist(predicated(), "cannot access an undefined or already extracted predicate");
        auto tmp = std::move(predicate_);
        new (&predicate_) SQL_boolean_t(std::monostate()); // placement-new
        return tmp;
    }
    ///> Returns the **moved** current predication predicate.
    template<sql_boolean_type T>
    T extract_predicate() {
        M_insist(predicated(), "cannot access an undefined or already extracted predicate");
        M_insist(std::holds_alternative<T>(predicate_));
        auto tmp = *std::get_if<T>(&predicate_);
        predicate_.~SQL_boolean_t(); // destroy old
        new (&predicate_) SQL_boolean_t(std::monostate()); // placement-new
        return tmp;
    }

    ///> Returns the **copied** current predication predicate.
    SQL_boolean_t get_predicate() const {
        return std::visit(overloaded {
            [](const std::monostate&) -> SQL_boolean_t {
                M_unreachable("cannot access an undefined or already extracted predicate");
            },
            [](const auto &e) -> SQL_boolean_t { return e.clone(); },
        }, predicate_);
    }
    ///> Returns the **copied** current predication predicate.
    template<sql_boolean_type T>
    T get_predicate() const {
        M_insist(predicated(), "cannot access an undefined or already extracted predicate");
        M_insist(std::holds_alternative<T>(predicate_));
        return std::get_if<T>(&predicate_)->clone();
    }

    void dump(std::ostream &out) const;
    void dump() const;
};


/*======================================================================================================================
 * CodeGenContext
 *====================================================================================================================*/

struct Scope
{
    friend struct CodeGenContext;

    private:
    Environment inner_; ///< environment active during `this` `Scope`s lifetime
    Environment *outer_; ///< environment active before and after `this` `Scope`s lifetime

    Scope() = delete;
    Scope(Environment inner);

    Scope(const Scope&) = delete;
    Scope(Scope&&) = default;

    public:
    ~Scope();

    /** Ends `this` `Scope` by returning the currently active environment and setting the former one active again. */
    Environment extract();
};

/** The Wasm `CodeGenContext` provides context information necessary for code generation.
 *
 * The context contains:
 * - an `Environment` of named values, e.g. SQL attribute values
 * - an `ExprCompiler` to compile expressions within the current `Environment`
 * - the number of tuples written to the result set
 * / the number of SIMD lanes currently used
 */
struct CodeGenContext
{
    friend struct Scope;

    private:
    Environment *env_ = nullptr; ///< environment for locally bound identifiers
    Global<U32x1> num_tuples_; ///< variable to hold the number of result tuples produced
    std::unordered_map<const char*, NChar> literals_; ///< maps each literal to its address at which it is stored
    ///> number of SIMD lanes currently used, i.e. 1 for scalar and at least 2 for vectorial values
    std::size_t num_simd_lanes_ = 1;
    ///> number of SIMD lanes currently preferred, i.e. 1 for scalar and at least 2 for vectorial values
    std::size_t num_simd_lanes_preferred_ = 1;

    public:
    CodeGenContext() = default;
    CodeGenContext(const CodeGenContext&) = delete;

    ~CodeGenContext() {
#ifdef M_ENABLE_SANITY_FIELDS
        num_tuples_.val().discard();  // artificial use of `num_tuples_` to silence diagnostics if unittests are executed
#endif
        for (auto &p : literals_)
            p.second.discard();
    }

    /*----- Thread-local instance ------------------------------------------------------------------------------------*/
    private:
    static thread_local std::unique_ptr<CodeGenContext> the_context_;

    public:
    static void Init() {
        M_insist(not the_context_, "must not have a context yet");
        the_context_ = std::make_unique<CodeGenContext>();
    }
    static void Dispose() {
        M_insist(bool(the_context_), "must have a context");
        the_context_ = nullptr;
    }
    static CodeGenContext & Get() {
        M_insist(bool(the_context_), "must have a context");
        return *the_context_;
    }

    /** Creates a new, *scoped* `Environment`.  The new `Environment` is immediately used by the `CodeGenContext`.  When
     * the `Scope` is destroyed (i.e. when it goes out of scope or its method `extract()` is called), the *old*
     * `Environment` is used again by the `CodeGenContext`. */
    Scope scoped_environment() { return Scope(Environment()); }
    /** Creates a new `Scope` using the `Environment` `env` which is immediately used by the `CodeGenContext`.  When
     * the `Scope` is destroyed (i.e. when it goes out of scope or its method `extract()` is called), the *old*
     * `Environment` is used again by the `CodeGenContext`. */
    Scope scoped_environment(Environment env) { return Scope(std::move(env)); }

    /*----- Access methods -------------------------------------------------------------------------------------------*/
    /** Returns the current `Environment`. */
    Environment & env() { M_insist(bool(env_)); return *env_; }
    /** Returns the current `Environment`. */
    const Environment & env() const { M_insist(bool(env_)); return *env_; }

    /** Returns the number of result tuples produced. */
    U32x1 num_tuples() const { return num_tuples_; }
    /** Set the number of result tuples produced to `n`. */
    void set_num_tuples(U32x1 n) { num_tuples_ = n; }
    /** Increments the number of result tuples produced by `n`. */
    void inc_num_tuples(U32x1 n = U32x1(1)) { num_tuples_ += n; }

    /** Adds the string literal `literal` located at pointer offset `ptr`. */
    void add_literal(const char *literal, uint32_t ptr) {
        auto [_, inserted] = literals_.emplace(literal, NChar(Ptr<Charx1>(U32x1(ptr)), false, strlen(literal) + 1, true));
        M_insist(inserted);
    }
    /** Returns the address at which `literal` is stored. */
    NChar get_literal_address(const char *literal) const {
        auto it = literals_.find(literal);
        M_insist(it != literals_.end(), "unknown literal");
        return it->second.clone();
    }

    /** Returns the number of SIMD lanes used. */
    std::size_t num_simd_lanes() const { return num_simd_lanes_; }
    /** Sets the number of SIMD lanes used to `n`. */
    void set_num_simd_lanes(std::size_t n) { num_simd_lanes_ = n; }

    /** Returns the number of SIMD lanes preferred by other operators. */
    std::size_t num_simd_lanes_preferred() const { return num_simd_lanes_preferred_; }
    /** Updates the number of SIMD lanes preferred by `n`. */
    void update_num_simd_lanes_preferred(std::size_t n) {
        num_simd_lanes_preferred_ = std::max(num_simd_lanes_preferred_, n);
    }
};

inline Scope::Scope(Environment inner)
    : inner_(std::move(inner))
{
    outer_ = std::exchange(CodeGenContext::Get().env_, &inner_);
}

inline Scope::~Scope()
{
    CodeGenContext::Get().env_ = outer_;
}

inline Environment Scope::extract()
{
    CodeGenContext::Get().env_ = outer_;
    return std::move(inner_);
}


/*======================================================================================================================
 * compile data layout
 *====================================================================================================================*/

/** Compiles the data layout \p layout containing tuples of schema \p layout_schema such that it sequentially stores
 * tuples of schema \p tuple_schema starting at memory address \p base_address and tuple ID \p tuple_id.  The store
 * does *not* have to be done in a single pass, i.e. the returned code may be emitted into a function which can be
 * called multiple times and each call starts storing at exactly the point where it has ended in the last call.  The
 * given variable \p tuple_id will be incremented automatically before advancing to the next tuple (i.e. code for
 * this will be emitted at the start of the block returned as third element).  Predication is supported and emitted
 * respectively.  SIMDfication is supported and will be emitted iff \p num_simd_lanes is greater than 1.
 *
 * Does not emit any code but returns three `wasm::Block`s containing code: the first one initializes all needed
 * variables, the second one stores one tuple, and the third one advances to the next tuple. */
template<VariableKind Kind>
std::tuple<Block, Block, Block>
compile_store_sequential(const Schema &tuple_schema, Ptr<void> base_address, const storage::DataLayout &layout,
                          std::size_t num_simd_lanes, const Schema &layout_schema,
                          Variable<uint32_t, Kind, false> &tuple_id);

/** Compiles the data layout \p layout containing tuples of schema \p layout_schema such that it sequentially stores
 * tuples of schema \p tuple_schema starting at memory address \p base_address and tuple ID \p tuple_id.  The store
 * has to be done in a single pass, i.e. the execution of the returned code must *not* be split among multiple
 * function calls.  The given variable \p tuple_id will be incremented automatically before advancing to the next
 * tuple (i.e. code for this will be emitted at the start of the block returned as third element).  Predication is
 * supported and emitted respectively.  SIMDfication is supported and will be emitted iff \p num_simd_lanes is greater
 * than 1.
 *
 * Does not emit any code but returns three `wasm::Block`s containing code: the first one initializes all needed
 * variables, the second one stores one tuple, and the third one advances to the next tuple. */
template<VariableKind Kind>
std::tuple<Block, Block, Block>
compile_store_sequential_single_pass(const Schema &tuple_schema, Ptr<void> base_address,
                                     const storage::DataLayout &layout, std::size_t num_simd_lanes,
                                     const Schema &layout_schema, Variable<uint32_t, Kind, false> &tuple_id);

/** Compiles the data layout \p layout containing tuples of schema \p layout_schema such that it sequentially loads
 * tuples of schema \p tuple_schema starting at memory address \p base_address and tuple ID \p tuple_id.  The given
 * variable \p tuple_id will be incremented automatically before advancing to the next tuple (i.e. code for this will
 * be emitted at the start of the block returned as third element).  SIMDfication is supported and will be emitted
 * iff \p num_simd_lanes is greater than 1.
 *
 * Does not emit any code but returns three `wasm::Block`s containing code: the first one initializes all needed
 * variables, the second one loads one tuple, and the third one advances to the next tuple. */
template<VariableKind Kind>
std::tuple<Block, Block, Block>
compile_load_sequential(const Schema &tuple_schema, Ptr<void> base_address, const storage::DataLayout &layout,
                         std::size_t num_simd_lanes, const Schema &layout_schema,
                         Variable<uint32_t, Kind, false> &tuple_id);

/** Compiles the data layout \p layout starting at memory address \p base_address and containing tuples of schema
 * \p layout_schema such that it stores the single tuple with schema \p tuple_schema and ID \p tuple_id.
 *
 * Emits the storing code into the current block. */
void compile_store_point_access(const Schema &tuple_schema, Ptr<void> base_address, const storage::DataLayout &layout,
                                const Schema &layout_schema, U32x1 tuple_id);

/** Compiles the data layout \p layout starting at memory address \p base_address and containing tuples of schema
 * \p layout_schema such that it loads the single tuple with schema \p tuple_schema and ID \p tuple_id.
 *
 * Emits the loading code into the current block and adds the loaded values into the current environment. */
void compile_load_point_access(const Schema &tuple_schema, Ptr<void> base_address, const storage::DataLayout &layout,
                               const Schema &layout_schema, U32x1 tuple_id);


/*======================================================================================================================
 * Buffer
 *====================================================================================================================*/

template<bool IsGlobal>
class buffer_storage;

template<>
class buffer_storage<false> {};

template<>
class buffer_storage<true>
{
    friend struct Buffer<true>;

    Global<Ptr<void>> base_address_; ///< global backup for base address of buffer
    Global<U32x1> size_; ///< global backup for current size of buffer, default initialized to 0
    ///> global backup for dynamic capacity of infinite buffer, default initialized to 0
    std::optional<Global<U32x1>> capacity_;
};

/** Buffers tuples by materializing them into memory. */
template<bool IsGlobal>
struct Buffer
{
    private:
    ///> parameter type for proxy creation and pipeline resuming methods
    using param_t = std::optional<std::reference_wrapper<const Schema>>;

    std::reference_wrapper<const Schema> schema_; ///< schema of buffer
    storage::DataLayout layout_; ///< data layout of buffer
    bool load_simdfied_ = false; ///< flag whether to load from the buffer in SIMDfied manner
    std::optional<Var<Ptr<void>>> base_address_; ///< base address of buffer
    std::optional<Var<U32x1>> size_; ///< current size of buffer, default initialized to 0
    std::optional<Var<U32x1>> capacity_; ///< dynamic capacity of infinite buffer, default initialized to 0
    std::optional<Var<Boolx1>> first_iteration_; ///< flag to indicate first loop iteration for infinite buffer
    buffer_storage<IsGlobal> storage_; ///< if `IsGlobal`, contains backups for base address, capacity, and size
    setup_t setup_; ///< remaining pipeline initializations
    pipeline_t pipeline_; ///< remaining actual pipeline
    teardown_t teardown_; ///< remaining pipeline post-processing
    ///> function to resume pipeline for entire buffer; expects base address and size of buffer as parameters
    std::optional<FunctionProxy<void(void*, uint32_t)>> resume_pipeline_;

    public:
    /** Creates a buffer for \p num_tuples tuples (0 means infinite) of schema \p schema using the data layout
     * created by \p factory to temporarily materialize tuples before resuming with the remaining pipeline
     * initializations \p setup, the actual pipeline \p pipeline, and the post-processing \p teardown.  For global
     * finite buffers, emits code to pre-allocate entire buffer into the **current** block. */
    Buffer(const Schema &schema, const storage::DataLayoutFactory &factory, bool load_simdfied = false,
           std::size_t num_tuples = 0, setup_t setup = setup_t::Make_Without_Parent(),
           pipeline_t pipeline = pipeline_t(), teardown_t teardown = teardown_t::Make_Without_Parent());

    Buffer(const Buffer&) = delete;
    Buffer(Buffer&&) = default;

    ~Buffer();

    Buffer & operator=(Buffer&&) = default;

    /** Returns the schema of the buffer. */
    const Schema & schema() const { return schema_; }
    /** Returns the layout of the buffer. */
    const storage::DataLayout & layout() const { return layout_; }
    /** Returns the base address of the buffer. */
    Ptr<void> base_address() const {
        if constexpr (IsGlobal) {
            return base_address_ ? base_address_->val() : storage_.base_address_.val(); // since global may be outdated
        } else {
            M_insist(bool(base_address_));
            return *base_address_;
        }
    }
    /** Returns the current size of the buffer. */
    U32x1 size() const {
        if constexpr (IsGlobal) {
            return size_ ? size_->val() : storage_.size_.val(); // since global may be outdated
        } else {
            M_insist(bool(size_));
            return *size_;
        }
    }

    /** Creates and returns a proxy object to load tuples of schema \p tuple_schema (default: entire tuples) from the
     * buffer. */
    buffer_load_proxy_t<IsGlobal> create_load_proxy(param_t tuple_schema = param_t()) const;
    /** Creates and returns a proxy object to store tuples of schema \p tuple_schema (default: entire tuples) to the
     * buffer. */
    buffer_store_proxy_t<IsGlobal> create_store_proxy(param_t tuple_schema = param_t()) const;
    /** Creates and returns a proxy object to swap tuples of schema \p tuple_schema (default: entire tuples) in the
     * buffer. */
    buffer_swap_proxy_t<IsGlobal> create_swap_proxy(param_t tuple_schema = param_t()) const;

    /** Performs the setup of all local variables of this buffer (by reading them from the global backups iff
     * \tparam IsGlobal) for a write access.  Must be called before any call to `consume()`. */
    void setup();
    /** Performs the teardown of all local variables of this buffer (by storing them into the global backups iff
     * \tparam IsGlobal) for a write access.  Must be called after all calls to `consume()`. */
    void teardown();

    /** Performs the setup of the local base address of this buffer by reading it from the global backup. */
    void setup_base_address() requires IsGlobal {
        M_insist(not base_address_, "must not call `setup_base_address()` twice");
        if (not layout_.is_finite()) {
            M_insist(bool(storage_.capacity_));
            Wasm_insist(*storage_.capacity_ != 0U, "buffer must be already allocated");
        }
        base_address_.emplace(storage_.base_address_);
    }
    /** Performs the teardown of the local base address of this buffer by destroying it but *without* storing it into
     * the global backup. */
    void teardown_base_address() requires IsGlobal {
        M_insist(bool(base_address_), "must call `setup_base_address()` before");
        base_address_.reset();
    }

    /** Emits code into a separate function to resume the pipeline for each tuple of schema \p tuple_schema (default:
     * entire tuples) in the  buffer.  Used to explicitly resume pipeline for infinite or partially filled buffers. */
    void resume_pipeline(param_t tuple_schema = param_t());
    /** Emits code inline to resume the pipeline for each tuple of schema \p tuple_schema (default: entire tuples) in
     * the buffer.  Due to inlining the current `Environment` must not be cleared and this method should be used for
     * n-ary operators.  Used to explicitly resume pipeline for infinite or partially filled buffers.  Predication is
     * supported, i.e. if the predication predicate is not fulfilled, no tuples will be loaded and thus the pipeline
     * will not be resumed. */
    void resume_pipeline_inline(param_t tuple_schema = param_t()) const;

    /** Emits code to store the current tuple into the buffer.  The behaviour depends on whether the buffer is finite:
     * - **finite:** If the buffer is full, resumes the pipeline for each tuple in the buffer and clears the buffer
     *               afterwards.
     * - **infinite:**  Potentially resizes the buffer but never resumes the pipeline (must be done explicitly by
     *                  calling `resume_pipeline()`).
     * Predication is supported, i.e. the current tuple is always written in the buffer but can only loaded from it
     * later iff the predication predicate is fulfilled. */
    void consume();
};

using LocalBuffer = Buffer<false>;
using GlobalBuffer = Buffer<true>;


/*======================================================================================================================
 * buffer accesses
 *====================================================================================================================*/

/** Proxy to implement loads from a buffer. */
template<bool IsGlobal>
struct buffer_load_proxy_t
{
    friend struct Buffer<IsGlobal>;

    private:
    std::reference_wrapper<const Buffer<IsGlobal>> buffer_; ///< buffer to load from
    std::reference_wrapper<const Schema> schema_; ///< entries to load

    buffer_load_proxy_t(const Buffer<IsGlobal> &buffer, const Schema &schema)
        : buffer_(std::cref(buffer))
        , schema_(std::cref(schema))
    { }

    public:
    buffer_load_proxy_t(const buffer_load_proxy_t&) = delete;
    buffer_load_proxy_t(buffer_load_proxy_t&&) = default;

    buffer_load_proxy_t & operator=(buffer_load_proxy_t&&) = default;

    /** Returns the entries to load. */
    const Schema & schema() const { return schema_; }

    /** Loads tuple with ID \p tuple_id into the current environment. */
    void operator()(U32x1 tuple_id) {
        Wasm_insist(tuple_id.clone() < buffer_.get().size(), "tuple ID out of bounds");
        compile_load_point_access(schema_, buffer_.get().base_address(), buffer_.get().layout(),
                                  buffer_.get().schema(), tuple_id);
    }
};

/** Proxy to implement stores to a buffer. */
template<bool IsGlobal>
struct buffer_store_proxy_t
{
    friend struct Buffer<IsGlobal>;

    private:
    std::reference_wrapper<const Buffer<IsGlobal>> buffer_; ///< buffer to store to
    std::reference_wrapper<const Schema> schema_; ///< entries to store

    buffer_store_proxy_t(const Buffer<IsGlobal> &buffer, const Schema &schema)
        : buffer_(std::cref(buffer))
        , schema_(std::cref(schema))
    { }

    public:
    buffer_store_proxy_t(const buffer_store_proxy_t&) = delete;
    buffer_store_proxy_t(buffer_store_proxy_t&&) = default;

    buffer_store_proxy_t & operator=(buffer_store_proxy_t&&) = default;

    /** Returns the entries to store. */
    const Schema & schema() const { return schema_; }

    /** Stores values from the current environment to tuple with ID \p tuple_id. */
    void operator()(U32x1 tuple_id) {
        Wasm_insist(tuple_id.clone() < buffer_.get().size(), "tuple ID out of bounds");
        compile_store_point_access(schema_, buffer_.get().base_address(), buffer_.get().layout(),
                                   buffer_.get().schema(), tuple_id);
    }
};

/** Proxy to implement swaps in a buffer. */
template<bool IsGlobal>
struct buffer_swap_proxy_t
{
    friend struct Buffer<IsGlobal>;

    private:
    std::reference_wrapper<const Buffer<IsGlobal>> buffer_; ///< buffer in which swaps are performed
    std::reference_wrapper<const Schema> schema_; ///< entries to swap

    buffer_swap_proxy_t(const Buffer<IsGlobal> &buffer, const Schema &schema)
        : buffer_(std::cref(buffer))
        , schema_(std::cref(schema))
    { }

    public:
    /** Returns the entries to swap. */
    const Schema & schema() const { return schema_; }

    /** Swaps tuples with IDs \p first and \p second. */
    void operator()(U32x1 first, U32x1 second);
    /** Swaps tuples with IDs \p first and \p second where the first one is already loaded and accessible through
     * \p env_first.  Note that environments are also swapped afterwards, i.e. \p env_first contains still the values
     * of the former tuple with ID \p first which is located at ID \p second after the call, except for `NChar`s
     * since they are only pointers to the actual values, i.e. \p env_first contains still the addresses of the
     * former tuple with ID \p first where the values of tuple with ID \p second are stored after the call. */
    void operator()(U32x1 first, U32x1 second, const Environment &env_first);
    /** Swaps tuples with IDs \p first and \p second which are already loaded and accessible through \p env_first and
     * \p env_second.  Note that environments are also swapped afterwards, i.e. \p env_first contains still the values
     * of the former tuple with ID \p first which is located at ID \p second after the call and vice versa, except
     * for `NChar`s since they are only pointers to the actual values, i.e. \p env_first contains still the addresses
     * of the former tuple with ID \p first where the values of tuple with ID \p second are stored after the call and
     * vice versa. */
    void operator()(U32x1 first, U32x1 second, const Environment &env_first, const Environment &env_second);
};


/*======================================================================================================================
 * bit operations
 *====================================================================================================================*/

/** Sets the \p n -th bit of the value pointed to by \p bytes to \p value. */
template<typename T, std::size_t L>
requires integral<typename T::type> and (T::num_simd_lanes == L)
void setbit(Ptr<T> bytes, Bool<L> value, uint8_t n)
{
    *bytes ^= (-value.template to<typename T::type>() xor *bytes.clone()) bitand T(1 << n);
}
/** Sets the bit masked by \p mask of the value pointed to by \p bytes to \p value. */
template<typename T, std::size_t L>
requires integral<typename T::type> and (T::num_simd_lanes == L)
void setbit(Ptr<T> bytes, Bool<L> value, T mask)
{
    *bytes ^= (-value.template to<typename T::type>() xor *bytes.clone()) bitand mask;
}


/*======================================================================================================================
 * string comparison
 *====================================================================================================================*/

///> comparison operations, e.g. for string comparison
enum cmp_op
{
    EQ, NE, LT, LE, GT, GE
};

/** Compares two strings \p left and \p right.  Has similar semantics to `strncmp` of libc. */
_I32x1 strncmp(NChar left, NChar right, U32x1 len);
/** Compares two strings \p left and \p right.  Has similar semantics to `strcmp` of libc. */
_I32x1 strcmp(NChar left, NChar right);
/** Compares two strings \p left and \p right.  Has similar semantics to `strncmp` of libc. */
_Boolx1 strncmp(NChar left, NChar right, U32x1 len, cmp_op op);
/** Compares two strings \p left and \p right.  Has similar semantics to `strcmp` of libc. */
_Boolx1 strcmp(NChar left, NChar right, cmp_op op);


/*======================================================================================================================
 * string copy
 *====================================================================================================================*/

/** Copies the contents of \p src to \p dst, but no more than \p count characters.  The function returns a `Ptr<Charx1>`
 * to the *end* of the copied sequence in \p dst, i.e. to the copied NUL-byte or to the character *after* the lastly
 * copied character.  If the first \p count characters of \p src are *not* NUL-terminated, \p dst will not be
 * NUL-terminated, too. */
Ptr<Charx1> strncpy(Ptr<Charx1> dst, Ptr<Charx1> src, U32x1 count);


/*======================================================================================================================
 * SQL LIKE
 *====================================================================================================================*/

/** Compares whether the string \p str matches the pattern \p pattern regarding SQL LIKE semantics using escape
 * character \p escape_char. */
_Boolx1 like(NChar str, NChar pattern, const char escape_char = '\\');


/*======================================================================================================================
 * signum and comparator
 *====================================================================================================================*/

/** Returns the signum of \p value, i.e. -1 for negative values, 0 for zero, and 1 for positive values.. */
template<typename T>
requires arithmetic<typename T::type>
T signum(T value)
{
    using type = typename T::type;
    return (value.clone() > type(0)).template to<type>() - (value < type(0)).template to<type>();
}

/** Compares two tuples, which must be already loaded into the environments \p env_left and \p env_right, according to
 * the ordering \p order (the second element of each pair is `true` iff the corresponding sorting should be
 * ascending).  Note that the value NULL is always considered smaller regardless of the ordering.
 *
 * Returns a negative number if \p left is smaller than \p right, 0 if both are equal, and a positive number if
 * \p left is greater than \p right, according to the ordering. */
I32x1 compare(const Environment &env_left, const Environment &env_right,
            const std::vector<SortingOperator::order_type> &order);


/*======================================================================================================================
 * explicit instantiation declarations
 *====================================================================================================================*/

template void Environment::add_predicate(_Boolx1 &&);
template void Environment::add_predicate(_Boolx16&&);
template _Boolx1  Environment::extract_predicate();
template _Boolx16 Environment::extract_predicate();
template _Boolx1  Environment::get_predicate() const;
template _Boolx16 Environment::get_predicate() const;
extern template std::tuple<Block, Block, Block> compile_store_sequential(
    const Schema&, Ptr<void>, const storage::DataLayout&, std::size_t, const Schema&, Var<U32x1>&
);
extern template std::tuple<Block, Block, Block> compile_store_sequential(
    const Schema&, Ptr<void>, const storage::DataLayout&, std::size_t, const Schema&, Global<U32x1>&
);
extern template std::tuple<Block, Block, Block> compile_store_sequential(
    const Schema&, Ptr<void>, const storage::DataLayout&, std::size_t, const Schema&,
    Variable<uint32_t, VariableKind::Param, false>&
);
extern template std::tuple<Block, Block, Block> compile_store_sequential_single_pass(
    const Schema&, Ptr<void>, const storage::DataLayout&, std::size_t, const Schema&, Var<U32x1>&
);
extern template std::tuple<Block, Block, Block> compile_store_sequential_single_pass(
    const Schema&, Ptr<void>, const storage::DataLayout&, std::size_t, const Schema&, Global<U32x1>&
);
extern template std::tuple<Block, Block, Block> compile_store_sequential_single_pass(
    const Schema&, Ptr<void>, const storage::DataLayout&, std::size_t, const Schema&,
    Variable<uint32_t, VariableKind::Param, false>&
);
extern template std::tuple<Block, Block, Block> compile_load_sequential(
    const Schema&, Ptr<void>, const storage::DataLayout&, std::size_t, const Schema&, Var<U32x1>&
);
extern template std::tuple<Block, Block, Block> compile_load_sequential(
    const Schema&, Ptr<void>, const storage::DataLayout&, std::size_t, const Schema&, Global<U32x1>&
);
extern template std::tuple<Block, Block, Block> compile_load_sequential(
    const Schema&, Ptr<void>, const storage::DataLayout&, std::size_t, const Schema&,
    Variable<uint32_t, VariableKind::Param, false>&
);
extern template struct Buffer<false>;
extern template struct Buffer<true>;
extern template struct buffer_swap_proxy_t<false>;
extern template struct buffer_swap_proxy_t<true>;

}

}

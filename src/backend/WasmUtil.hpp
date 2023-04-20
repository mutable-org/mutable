#pragma once

#include "backend/PhysicalOperator.hpp"
#include "backend/WasmDSL.hpp"
#include <mutable/catalog/Schema.hpp>
#include <mutable/parse/AST.hpp>
#include <functional>
#include <optional>
#include <variant>


namespace m {

namespace wasm {

// forward declarations
struct Environment;
template<bool IsGlobal> struct buffer_load_proxy_t;
template<bool IsGlobal> struct buffer_store_proxy_t;
template<bool IsGlobal> struct buffer_swap_proxy_t;


/*======================================================================================================================
 * Declare SQL helper type for character sequences
 *====================================================================================================================*/

struct NChar : Ptr<Char>
{
    private:
    bool can_be_null_;
    const CharacterSequence *type_;

    public:
    NChar(Ptr<Char> ptr, bool can_be_null, const CharacterSequence *type)
        : Ptr<Char>(ptr), can_be_null_(can_be_null), type_(type)
    { }
    NChar(Ptr<Char> ptr, bool can_be_null, std::size_t length, bool guarantees_terminating_nul)
        : Ptr<Char>(ptr)
        , can_be_null_(can_be_null)
        , type_(guarantees_terminating_nul ? Type::Get_Varchar(Type::TY_Scalar, length)
                                           : Type::Get_Char(Type::TY_Scalar, length))
    { }

    NChar(NChar&) = default;
    NChar(NChar&&) = default;

    NChar clone() const { return NChar(Ptr<Char>::clone(), can_be_null_, type_); }

    Ptr<Char> val() { return *this; }

    Bool is_null() {
        if (can_be_null()) {
            return Ptr<Char>::is_null();
        } else {
            discard();
            return Bool(false);
        }
    }
    Bool not_null() {
        if (can_be_null()) {
            return Ptr<Char>::not_null();
        } else {
            discard();
            return Bool(true);
        }
    }

    bool can_be_null() const { return can_be_null_; }
    std::size_t length() const { return type_->length; }
    uint64_t size_in_bytes() const { return type_->size() / 8; }
    bool guarantees_terminating_nul() const { return type_->is_varying; }
};

/*======================================================================================================================
 * Declare valid SQL types
 *====================================================================================================================*/

template<typename>
struct is_sql_type;

#define SQL_TYPES(X) \
    X(_Bool) \
    X(_I8) \
    X(_I16) \
    X(_I32) \
    X(_I64) \
    X(_Float) \
    X(_Double) \
    X(NChar)

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


/*======================================================================================================================
 * Helper functions for SQL types
 *====================================================================================================================*/

template<sql_type To>
inline To convert(SQL_t &&variant)
{
    using type = typename To::type;

    return std::visit(overloaded {
        [](auto &&actual) -> To requires requires { actual.template to<type>(); } { return actual.template to<type>(); },
        [](auto &&actual) -> To requires (not requires { actual.template to<type>(); }) {
            M_unreachable("illegal conversion");
        },
        [](std::monostate&&) -> To { M_unreachable("invalid variant"); },
    }, variant);
}

inline Bool is_null(SQL_t &&variant)
{
    return std::visit(overloaded {
        []<typename T>(Expr<T> value) -> Bool { return value.is_null(); },
        [](NChar value) -> Bool { return value.is_null(); },
        [](std::monostate) -> Bool { M_unreachable("invalid variant"); },
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

    ///> Compile a `m::Expr` of statically known type to an `Expr<T>`.
    template<sql_type T>
    T compile(const m::ast::Expr &e) {
        (*this)(e);
        M_insist(std::holds_alternative<T>(intermediate_result_));
        return *std::get_if<T>(&intermediate_result_);
    }

    ///> Compile a `m::cnf::CNF` \p cnf to an `Expr<bool>`.
    _Bool compile(const cnf::CNF &cnf);

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
};


/*======================================================================================================================
 * Environment
 *====================================================================================================================*/

/** Binds `Schema::Identifier`s to `Expr<T>`s. */
struct Environment
{
    private:
    /** Discards the held expression of `expr`. */
    static void discard(SQL_t &expr) {
        std::visit(overloaded {
            [](auto &e) { e.discard(); },
            [](std::monostate) { M_unreachable("invalid variant"); },
        }, expr);
    }

    private:
    ///> maps `Schema::Identifier`s to `Expr<T>`s that evaluate to the current expression
    std::unordered_map<Schema::Identifier, SQL_t> exprs_;
    ///> optional predicate if predication is used
    std::optional<_Bool> predicate_;

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
    /** Returns `true` iff this `Environment` contains `id`. */
    bool has(Schema::Identifier id) const { return exprs_.find(id) != exprs_.end(); }
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


    /*----- Expression compilation -----------------------------------------------------------------------------------*/
    ///> Compile \p t by delegating compilation to an `ExprCompiler` for `this` `Environment`.
    template<typename T>
    requires requires (ExprCompiler C, T &&t) { C.compile(std::forward<T>(t)); }
    auto compile(T &&t) const {
        ExprCompiler C(*this);
        return C.compile(std::forward<T>(t));
    }
     ///> Compile \p t by delegating compilation to an `ExprCompiler` for `this` `Environment`.
    template<typename T, typename U>
    requires requires (ExprCompiler C, U &&u) { C.compile(std::forward<U>(u)); }
    auto compile(U &&u) const {
        ExprCompiler C(*this);
        return C.compile<T>(std::forward<U>(u));
    }

    /*----- Predication ----------------------------------------------------------------------------------------------*/
    ///> Returns `true` iff `this` `Environment` uses predication.
    bool predicated() const { return bool(predicate_); }
    ///> Adds the predicate \p pred to the predication predicate.
    void add_predicate(_Bool pred) {
        if (predicate_)
            predicate_.emplace(*predicate_ and pred);
        else
            predicate_.emplace(pred);
    }
    ///> Adds the predicate compiled from the `cnf::CNF` \p cnf to the predication predicate.
    void add_predicate(const cnf::CNF &cnf) { add_predicate(compile(cnf)); }
    ///> Returns the **moved** current predication predicate.
    _Bool extract_predicate() {
        M_insist(predicated(), "cannot access an undefined or already extracted predicate");
        auto tmp = *predicate_;
        predicate_.reset();
        return tmp;
    }
    ///> Returns the **copied** current predication predicate.
    _Bool get_predicate() const {
        M_insist(predicated(), "cannot access an undefined or already extracted predicate");
        return predicate_->clone();
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
 */
struct CodeGenContext
{
    friend struct Scope;

    private:
    Environment *env_ = nullptr; ///< environment for locally bound identifiers
    Global<U32> num_tuples_; ///< variable to hold the number of result tuples produced
    std::unordered_map<const char*, NChar> literals_; ///< maps each literal to its address at which it is stored

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
     * the `Scope` is destroyed (i.e. when it goes out of scope or its methods `extract()` is called), the *old*
     * `Environment` is used again by the `CodeGenContext`. */
    Scope scoped_environment() { return Scope(Environment()); }
    /** Creates a new `Scope` using the `Environment` `env` which is immediately used by the `CodeGenContext`.  When
     * the `Scope` is destroyed (i.e. when it goes out of scope or its methods `extract()` is called), the *old*
     * `Environment` is used again by the `CodeGenContext`. */
    Scope scoped_environment(Environment env) { return Scope(std::move(env)); }

    /*----- Access methods -------------------------------------------------------------------------------------------*/
    /** Returns the current `Environment`. */
    Environment & env() { M_insist(bool(env_)); return *env_; }
    /** Returns the current `Environment`. */
    const Environment & env() const { M_insist(bool(env_)); return *env_; }

    /** Returns the number of result tuples produced. */
    U32 num_tuples() const { return num_tuples_; }
    /** Set the number of result tuples produced to `n`. */
    void set_num_tuples(U32 n) { num_tuples_ = n; }
    /** Increments the number of result tuples produced by `n`. */
    void inc_num_tuples(U32 n = U32(1)) { num_tuples_ += n; }

    /** Adds the string literal `literal` located at pointer offset `ptr`. */
    void add_literal(const char *literal, uint32_t ptr) {
        auto [_, inserted] = literals_.emplace(literal, NChar(Ptr<Char>(U32(ptr)), false, strlen(literal) + 1, true));
        M_insist(inserted);
    }
    /** Returns the address at which `literal` is stored. */
    NChar get_literal_address(const char *literal) const {
        auto it = literals_.find(literal);
        M_insist(it != literals_.end(), "unknown literal");
        return it->second.clone();
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
 * tuples of schema \p tuple_schema starting at memory address \p base_address and tuple ID \p initial_tuple_id.  The
 * store does *not* have to be done in a single pass, i.e. the returned code may be emitted into a function which can
 * be called multiple times and each call starts storing at exactly the point where it has ended in the last call.
 * The caller has to provide a variable \p tuple_id which must be initialized to \p initial_tuple_id and will be
 * incremented automatically after storing each tuple (i.e. code for this will be emitted at the end of the block
 * returned as second element).  Predication is supported and emitted respectively.
 *
 * Does not emit any code but returns three `wasm::Block`s containing code: the first one initializes all needed
 * variables, the second one stores one tuple, and the third one advances to the next tuple. */
template<VariableKind Kind>
std::tuple<Block, Block, Block>
compile_store_sequential(const Schema &tuple_schema, Ptr<void> base_address, const storage::DataLayout &layout,
                         const Schema &layout_schema, Variable<uint32_t, Kind, false> &tuple_id,
                         uint32_t initial_tuple_id = 0);

/** Compiles the data layout \p layout containing tuples of schema \p layout_schema such that it sequentially stores
 * tuples of schema \p tuple_schema starting at memory address \p base_address and tuple ID \p initial_tuple_id.  The
 * store has to be done in a single pass, i.e. the execution of the returned code must *not* be split among multiple
 * function calls.
 * The caller has to provide a variable \p tuple_id which must be initialized to \p initial_tuple_id and will be
 * incremented automatically after storing each tuple (i.e. code for this will be emitted at the end of the block
 * returned as second element).  Predication is supported and emitted respectively.
 *
 * Does not emit any code but returns three `wasm::Block`s containing code: the first one initializes all needed
 * variables, the second one stores one tuple, and the third one advances to the next tuple. */
template<VariableKind Kind>
std::tuple<Block, Block, Block>
compile_store_sequential_single_pass(const Schema &tuple_schema, Ptr<void> base_address,
                                     const storage::DataLayout &layout, const Schema &layout_schema,
                                     Variable<uint32_t, Kind, false> &tuple_id, uint32_t initial_tuple_id = 0);

/** Compiles the data layout \p layout containing tuples of schema \p layout_schema such that it sequentially loads
 * tuples of schema \p tuple_schema starting at memory address \p base_address and tuple ID \p initial_tuple_id.
 * The caller has to provide a variable \p tuple_id which must be initialized to \p initial_tuple_id and will be
 * incremented automatically after loading each tuple (i.e. code for this will be emitted at the end of the block
 * returned as second element).
 *
 * Does not emit any code but returns three `wasm::Block`s containing code: the first one initializes all needed
 * variables, the second one loads one tuple, and the third one advances to the next tuple. */
template<VariableKind Kind>
std::tuple<Block, Block, Block>
compile_load_sequential(const Schema &tuple_schema, Ptr<void> base_address, const storage::DataLayout &layout,
                        const Schema &layout_schema, Variable<uint32_t, Kind, false> &tuple_id,
                        uint32_t initial_tuple_id = 0);

/** Compiles the data layout \p layout starting at memory address \p base_address and containing tuples of schema
 * \p layout_schema such that it stores the single tuple with schema \p tuple_schema and ID \p tuple_id.
 *
 * Emits the storing code into the current block. */
void compile_store_point_access(const Schema &tuple_schema, Ptr<void> base_address, const storage::DataLayout &layout,
                                const Schema &layout_schema, U32 tuple_id);

/** Compiles the data layout \p layout starting at memory address \p base_address and containing tuples of schema
 * \p layout_schema such that it loads the single tuple with schema \p tuple_schema and ID \p tuple_id.
 *
 * Emits the loading code into the current block and adds the loaded values into the current environment. */
void compile_load_point_access(const Schema &tuple_schema, Ptr<void> base_address, const storage::DataLayout &layout,
                               const Schema &layout_schema, U32 tuple_id);


/*======================================================================================================================
 * Buffer
 *====================================================================================================================*/

/** Buffers tuples by materializing them into memory. */
template<bool IsGlobal>
struct Buffer
{
    private:
    ///> variable type dependent on whether buffer should be globally usable
    template<typename T>
    using var_t = std::conditional_t<IsGlobal, Global<T>, Var<T>>;
    ///> parameter type for proxy creation and pipeline resuming methods
    using param_t = std::optional<std::reference_wrapper<const Schema>>;

    std::reference_wrapper<const Schema> schema_; ///< schema of buffer
    storage::DataLayout layout_; ///< data layout of buffer
    var_t<Ptr<void>> base_address_; ///< base address of buffer
    std::optional<var_t<U32>> capacity_; ///< optional dynamic capacity of buffer, default initialized to 0
    var_t<U32> size_; ///< current size of buffer, default initialized to 0
    MatchBase::callback_t Setup_; ///< remaining pipeline initializations
    MatchBase::callback_t Pipeline_; ///< remaining actual pipeline
    MatchBase::callback_t Teardown_; ///< remaining pipeline post-processing
    ///> function to resume pipeline for entire buffer; expects base address and size of buffer as parameters
    std::optional<FunctionProxy<void(void*, uint32_t)>> resume_pipeline_;

    public:
    /** Creates a buffer for \p num_tuples tuples (0 means infinite) of schema \p schema using the data layout
     * created by \p factory to temporarily materialize tuples before resuming with the remaining pipeline
     * initializations \p Setup, the actual pipeline \p Pipeline, and the post-processing \p Teardown.  For finite
     * buffers, emits code to allocate entire buffer into the **current** block. */
    Buffer(const Schema &schema, const storage::DataLayoutFactory &factory, std::size_t num_tuples = 0,
           MatchBase::callback_t Setup = MatchBase::DoNothing, MatchBase::callback_t Pipeline = MatchBase::DoNothing,
           MatchBase::callback_t Teardown = MatchBase::DoNothing);

    Buffer(const Buffer&) = delete;
    Buffer(Buffer&&) = default;

    ~Buffer();

    Buffer & operator=(Buffer&&) = default;

    /** Returns the schema of the buffer. */
    const Schema & schema() const { return schema_; }
    /** Returns the layout of the buffer. */
    const storage::DataLayout & layout() const { return layout_; }
    /** Returns the base address of the buffer. */
    Ptr<void> base_address() const { return base_address_; }
    /** Returns the current size of the buffer. */
    U32 size() const { return size_; }

    /** Creates and returns a proxy object to load tuples of schema \p tuple_schema (default: entire tuples) from the
     * buffer. */
    buffer_load_proxy_t<IsGlobal> create_load_proxy(param_t tuple_schema = param_t()) const;
    /** Creates and returns a proxy object to store tuples of schema \p tuple_schema (default: entire tuples) to the
     * buffer. */
    buffer_store_proxy_t<IsGlobal> create_store_proxy(param_t tuple_schema = param_t()) const;
    /** Creates and returns a proxy object to swap tuples of schema \p tuple_schema (default: entire tuples) in the
     * buffer. */
    buffer_swap_proxy_t<IsGlobal> create_swap_proxy(param_t tuple_schema = param_t()) const;

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
    void operator()(U32 tuple_id) {
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
    void operator()(U32 tuple_id) {
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
    void operator()(U32 first, U32 second);
};


/*======================================================================================================================
 * bit operations
 *====================================================================================================================*/

/** Sets the \p n -th bit of the value pointed to by \p bytes to \p value. */
template<typename T>
requires integral<typename T::type>
void setbit(Ptr<T> bytes, Bool value, uint8_t n)
{
    *bytes ^= (-value.to<typename T::type>() xor *bytes.clone()) bitand T(1 << n);
}
/** Sets the bit masked by \p mask of the value pointed to by \p bytes to \p value. */
template<typename T>
requires integral<typename T::type>
void setbit(Ptr<T> bytes, Bool value, T mask)
{
    *bytes ^= (-value.to<typename T::type>() xor *bytes.clone()) bitand mask;
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
_I32 strncmp(NChar left, NChar right, U32 len);
/** Compares two strings \p left and \p right.  Has similar semantics to `strcmp` of libc. */
_I32 strcmp(NChar left, NChar right);
/** Compares two strings \p left and \p right.  Has similar semantics to `strncmp` of libc. */
_Bool strncmp(NChar left, NChar right, U32 len, cmp_op op);
/** Compares two strings \p left and \p right.  Has similar semantics to `strcmp` of libc. */
_Bool strcmp(NChar left, NChar right, cmp_op op);


/*======================================================================================================================
 * string copy
 *====================================================================================================================*/

/** Copies the contents of \p src to \p dst, but no more than \p count characters.  The function returns a `Ptr<Char>`
 * to the *end* of the copied sequence in \p dst, i.e. to the copied NUL-byte or to the character *after* the lastly
 * copied character.  If the first \p count characters of \p src are *not* NUL-terminated, \p dst will not be
 * NUL-terminated, too. */
Ptr<Char> strncpy(Ptr<Char> dst, Ptr<Char> src, U32 count);


/*======================================================================================================================
 * SQL LIKE
 *====================================================================================================================*/

/** Compares whether the string \p str matches the pattern \p pattern regarding SQL LIKE semantics using escape
 * character \p escape_char. */
_Bool like(NChar str, NChar pattern, const char escape_char = '\\');


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

/** Compares two tuples with IDs \p left and \p right, loadable using the buffer load handle \p load, according to
 * the ordering \p order (the second element of each pair is `true` iff the corresponding sorting should be
 * ascending).  Note that the value NULL is always considered smaller regardless of the ordering.
 *
 * Returns a negative number if \p left is smaller than \p right, 0 if both are equal, and a positive number if
 * \p left is greater than \p right, according to the ordering. */
template<bool IsGlobal>
I32 compare(buffer_load_proxy_t<IsGlobal> &load, U32 left, U32 right,
            const std::vector<SortingOperator::order_type> &order);


/*======================================================================================================================
 * explicit instantiation declarations
 *====================================================================================================================*/

extern template struct Buffer<false>;
extern template struct Buffer<true>;
extern template struct buffer_swap_proxy_t<false>;
extern template struct buffer_swap_proxy_t<true>;
extern template I32 compare(buffer_load_proxy_t<false> &load, U32 left, U32 right,
                            const std::vector<SortingOperator::order_type> &order);
extern template I32 compare(buffer_load_proxy_t<true> &load, U32 left, U32 right,
                            const std::vector<SortingOperator::order_type> &order);

}

}

#pragma once

#include "backend/PhysicalOperator.hpp"
#include "backend/WasmDSL.hpp"
#include <mutable/catalog/Schema.hpp>
#include <mutable/parse/AST.hpp>
#include <optional>
#include <variant>


namespace m {

namespace wasm {

// forward declarations
struct Environment;


/*======================================================================================================================
 * Declare valid SQL types
 *====================================================================================================================*/

template<typename>
struct is_sql_type : std::false_type {};

#define SQL_TYPES(X) \
    X(_Bool) \
    X(_I8) \
    X(_I16) \
    X(_I32) \
    X(_I64) \
    X(_Float) \
    X(_Double) \
    X(Ptr<Char>) \

#define ADD_EXPR_SQL_TYPE(TYPE) template<> struct is_sql_type<TYPE> : std::true_type {};
SQL_TYPES(ADD_EXPR_SQL_TYPE)
#undef ADD_EXPR_SQL_TYPE

template<typename T>
static constexpr bool is_sql_type_v = is_sql_type<T>::value;

using SQL_t = std::variant<
    std::monostate
#define ADD_TYPE(TYPE) , TYPE
    SQL_TYPES(ADD_TYPE)
#undef ADD_TYPE
>;
#undef SQL_TYPES


/*======================================================================================================================
 * ExprCompiler
 *====================================================================================================================*/

/** Compiles AST expressions `m::Expr` to Wasm ASTs `m::wasm::Expr<T>`.  Also supports compiling `m::cnf::CNF`s. */
struct ExprCompiler : ConstASTExprVisitor
{
    private:
    ///> current intermediate results during AST traversal and compilation
    SQL_t intermediate_result_;
    ///> the environment to use for resolving designators to `Expr<T>`s
    const Environment &env_;

    public:
    ExprCompiler(const Environment &env) : env_(env) { }

    ///> Compiles a `m::Expr` \p e of statically unknown type to a `SQL_t`.
    SQL_t compile(const m::Expr &e) {
        (*this)(e);
        return std::move(intermediate_result_);
    }

    ///> Compile a `m::Expr` of statically known type to an `Expr<T>`.
    template<typename T>
    requires is_sql_type_v<T>
    T compile(const m::Expr &e) {
        (*this)(e);
        M_insist(std::holds_alternative<T>(intermediate_result_));
        return *std::get_if<T>(&intermediate_result_);
    }

    ///> Compile a `m::cnf::CNF` \p cnf to an `Expr<bool>`.
    _Bool compile(const cnf::CNF &cnf);

    private:
    using ConstASTExprVisitor::operator();
    void operator()(const ErrorExpr&) override;
    void operator()(const Designator &op) override;
    void operator()(const Constant &op) override;
    void operator()(const UnaryExpr &op) override;
    void operator()(const BinaryExpr &op) override;
    void operator()(const FnApplicationExpr &op) override;
    void operator()(const QueryExpr &op) override;

    SQL_t get() { return std::move(intermediate_result_); }

    template<typename T>
    requires is_sql_type_v<T>
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
    friend void swap(Environment &first, Environment &second) {
        using std::swap;
        swap(first.exprs_, second.exprs_);
    }

    private:
    /** Discards the held expression of `expr`. */
    static void discard(SQL_t &expr) {
        std::visit(overloaded {
            [](std::monostate) { },
            [](auto &e) { e.discard(); },
        }, expr);
    }

    private:
    ///> maps `Schema::Identifier`s to `Expr<T>`s that evaluate to the current expression
    std::unordered_map<Schema::Identifier, SQL_t> exprs_;

    public:
    Environment() = default;
    Environment(const Environment&) = delete;
    Environment(Environment &&other) : Environment() { swap(*this, other); }

    ~Environment() {
        for (auto &p : exprs_)
            discard(p.second);
    }

    Environment & operator=(Environment &&other) { swap(*this, other); return *this; }

    /*----- Access methods -------------------------------------------------------------------------------------------*/
    /** Returns `true` iff this `Environment` contains `id`. */
    bool has(Schema::Identifier id) const { return exprs_.find(id) != exprs_.end(); }
    /** Returns `true` iff this `Environment` is empty. */
    bool empty() const { return exprs_.empty(); }

    ///> Adds a mapping from \p id to \p expr.
    template<typename T>
    requires is_sql_type_v<T>
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
    template<typename T>
    requires is_sql_type_v<T>
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
            [](std::monostate) -> SQL_t { M_unreachable("invalid expression"); },
            [](auto &e) -> SQL_t { return e.clone(); },
        }, it->second);
    }
    ///> Returns the **copied** entry for identifier \p id.
    template<typename T>
    requires is_sql_type_v<T>
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
    auto compile(T &&t) {
        ExprCompiler C(*this);
        return C.compile(std::forward<T>(t));
    }
     ///> Compile \p t by delegating compilation to an `ExprCompiler` for `this` `Environment`.
    template<typename T, typename U>
    requires requires (ExprCompiler C, U &&u) { C.compile(std::forward<U>(u)); }
    auto compile(U &&u) {
        ExprCompiler C(*this);
        return C.compile<T>(std::forward<U>(u));
    }

    void dump(std::ostream &out) const;
    void dump() const;
};


/*======================================================================================================================
 * CodeGenContext
 *====================================================================================================================*/

struct Scope
{
    private:
    Environment outer_;

    public:
    Scope(Environment inner);
    ~Scope();

    Scope(const Scope&) = delete;
    Scope(Scope&&) = default;
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
    Environment env_; ///< environment for locally bound identifiers
    Global<U32> num_tuples_; ///< variable to hold the number of result tuples produced
    std::unordered_map<const char*, Ptr<Char>> literals_; ///< maps each literal to its address at which it is stored

    public:
    CodeGenContext() = default;
    CodeGenContext(const CodeGenContext&) = delete;

    ~CodeGenContext() {
        for (auto &p : literals_)
            p.second.discard();
    }

    /*----- Thread-local instance ------------------------------------------------------------------------------------*/
    private:
    static inline thread_local std::unique_ptr<CodeGenContext> the_context_;

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
     * the `Scope` is destroyed (i.e. when it goes out of scope), the *old* `Environment` is used again by the
     * `CodeGenContext`. */
    Scope scoped_environment() { return Scope(Environment()); }

    /*----- Access methods -------------------------------------------------------------------------------------------*/
    /** Returns the current `Environment`. */
    Environment & env() { return env_; }
    /** Returns the current `Environment`. */
    const Environment & env() const { return env_; }

    /** Returns the number of result tuples produced. */
    U32 num_tuples() const { return num_tuples_; }
    /** Increments the number of result tuples produced by `n`. */
    void inc_num_tuples(uint32_t n = 1) { num_tuples_ += n; }

    /** Adds the string literal `literal` located at pointer offset `ptr`. */
    void add_literal(const char *literal, uint32_t ptr) {
        auto [_, inserted] = literals_.emplace(literal, Ptr<Char>(U32(ptr)));
        M_insist(inserted);
    }
    /** Returns the address at which `literal` is stored. */
    Ptr<Char> get_literal_address(const char *literal) {
        auto it = literals_.find(literal);
        M_insist(it != literals_.end(), "unknown literal");
        return it->second.clone();
    }
};

inline Scope::Scope(Environment inner)
{
    outer_ = std::exchange(CodeGenContext::Get().env_, std::move(inner));
}

inline Scope::~Scope() {
    CodeGenContext::Get().env_ = std::move(outer_);
}


/*======================================================================================================================
 * compile data layout
 *====================================================================================================================*/

/** Compiles the data layout \p layout containing tuples of schema \p layout_schema such that it sequentially stores
 * tuples of schema \p tuple_schema starting at memory address \p base_address and tuple ID \p initial_tuple_id.
 * The caller has to provide a variable \p tuple_id which must be initialized to \p initial_tuple_id and will be
 * incremented automatically after storing each tuple (i.e. code for this will be emitted at the end of the block
 * returned as second element).
 *
 * Does not emit any code but returns three `wasm::Block`s containing code: the first one initializes all needed
 * variables, the second one stores one tuple, and the third one advances to the next tuple. */
template<VariableKind Kind>
std::tuple<Block, Block, Block>
compile_store_sequential(const Schema &tuple_schema, Ptr<void> base_address, const storage::DataLayout &layout,
                         const Schema &layout_schema, Variable<uint32_t, Kind, false> &tuple_id,
                         uint32_t initial_tuple_id = 0);

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


/*======================================================================================================================
 * Buffer
 *====================================================================================================================*/

/** Buffers tuples by materializing them into memory. */
template<bool IsGlobal>
struct Buffer
{
    ///> variable type dependent on whether buffer should be globally usable
    template<typename T>
    using var_t = std::conditional_t<IsGlobal, Global<T>, Var<T>>;
    ///> function type for resuming pipeline dependent on whether buffer should be globally usable
    using fn_t = std::conditional_t<IsGlobal, void(void), void(void*, uint32_t)>;

    private:
    const Schema *schema_; ///< schema of buffer
    storage::DataLayout layout_; ///< data layout of buffer
    var_t<Ptr<void>> base_address_; ///< base address of buffer
    std::optional<var_t<U32>> capacity_; ///< optional dynamic capacity of buffer, in number of tuples
    var_t<U32> size_; ///< current size of buffer, default initialized to 0
    MatchBase::callback_t Pipeline_; ///< remaining pipeline
    ///> function to resume pipeline for entire buffer; for local buffer, expects its base address and size as parameters
    std::optional<FunctionProxy<fn_t>> resume_pipeline_;

    public:
    /** Creates a buffer for \p num_tuples tuples (0 means infinite) of schema \p schema using the data layout
     * created by \p factory to temporarily materialize tuples before resuming with the remaining pipeline
     * \p Pipeline.  For finite buffers, emits code to allocate entire buffer into the **current** block. */
    Buffer(const Schema &schema, const storage::DataLayoutFactory &factory, std::size_t num_tuples = 0,
           MatchBase::callback_t Pipeline = MatchBase::callback_t());

    Buffer(Buffer&&) = default;

    Buffer & operator=(Buffer&&) = default;

    /** Returns the schema of the buffer. */
    const Schema & schema() const { return *schema_; }
    /** Returns the layout of the buffer. */
    const storage::DataLayout & layout() const { return layout_; }
    /** Returns the base address of the buffer. */
    Ptr<void> base_address() const { return base_address_; }
    /** Returns the current size of the buffer. */
    U32 size() const { return size_; }

    /** Emits code into a separate function to resume the pipeline for each tuple in the buffer.  Used to explicitly
     * resume pipeline for infinite or partially filled buffers. */
    void resume_pipeline();
    /** Emits code inline to resume the pipeline for each tuple in the buffer.  Due to inlining the current
     * `Environment` must not be cleared and this method should be used for n-ary operators.  Used to explicitly resume
     * pipeline for infinite or partially filled buffers. */
    void resume_pipeline_inline();

    /** Emits code to store the current tuple into the buffer.  The behaviour depends on whether the buffer is finite:
     * - **finite:** If the buffer is full, resumes the pipeline for each tuple in the buffer and clears the buffer
     *               afterwards.
     * - **infinite:**  Potentially resizes the buffer but never resumes the pipeline (must be done explicitly by
     *                  calling `resume_pipeline()`). */
    void consume();
};

using LocalBuffer = Buffer<false>;
using GlobalBuffer = Buffer<true>;


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

/** Compares two strings \p left and \p right of type \p ty_left and \p ty_right, respectively.  Has similar semantics
 * to `strncmp` of libc. */
_I32 strncmp(const CharacterSequence &ty_left, const CharacterSequence &ty_right, Ptr<Char> left, Ptr<Char> right,
             U32 len);
/** Compares two strings \p left and \p right of type \p ty_left and \p ty_right, respectively.  Has similar semantics
 * to `strcmp` of libc. */
_I32 strcmp(const CharacterSequence &ty_left, const CharacterSequence &ty_right, Ptr<Char> left, Ptr<Char> right);
/** Compares two strings \p left and \p right of type \p ty_left and \p ty_right, respectively.  Has similar semantics
 * to `strncmp` of libc. */
_Bool strncmp(const CharacterSequence &ty_left, const CharacterSequence &ty_right, Ptr<Char> left, Ptr<Char> right,
              U32 len, cmp_op op);
/** Compares two strings \p left and \p right of type \p ty_left and \p ty_right, respectively.  Has similar semantics
 * to `strcmp` of libc. */
_Bool strcmp(const CharacterSequence &ty_left, const CharacterSequence &ty_right, Ptr<Char> left, Ptr<Char> right,
             cmp_op op);


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

/** Compares whether the string \p str of type \p ty_str matches the pattern \p pattern of type \p ty_pattern
 * regarding SQL LIKE semantics using escape character \p escape_char. */
_Bool like(const CharacterSequence &ty_str, const CharacterSequence &ty_pattern, Ptr<Char> str, Ptr<Char> pattern,
           const char escape_char = '\\');


/*======================================================================================================================
 * explicit instantiation declarations
 *====================================================================================================================*/

extern template struct Buffer<false>;
extern template struct Buffer<true>;

}

}

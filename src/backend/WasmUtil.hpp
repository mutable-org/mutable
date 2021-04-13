#pragma once

#include "backend/RuntimeStruct.hpp"
#include "backend/WebAssembly.hpp"
#include "catalog/Schema.hpp"
#include <binaryen-c.h>
#include <cstring>
#include <iostream>
#include <mutable/catalog/Type.hpp>
#include <mutable/IR/CNF.hpp>
#include <mutable/IR/OperatorVisitor.hpp>
#include <sstream>
#include <utility>
#include <vector>


namespace {

/** Create a unique name from `name` by appending a unique identifier.  If `name` is `nullptr`, use `fallback` instead.
 */
inline const char * mkname(const char *name, const char *fallback) {
    insist(fallback, "fallback name must not be nullptr");
    static uint32_t id = 0;
    std::ostringstream oss;
    if (name) oss << name;
    else      oss << fallback;
    oss << '_' << id++;
    return strdup(oss.str().c_str());
}

}

namespace m {

/*----- Common forward declarations ----------------------------------------------------------------------------------*/
struct Type;
struct FunctionBuilder;
struct WasmStrcmp;
struct WasmModuleCG;

/*----- WasmAlgo forward declarations --------------------------------------------------------------------------------*/
struct WasmPartition;
struct WasmQuickSort;
struct WasmHash;
struct WasmHashTable;

/** Returns the `BinaryenType` that corresponds to mu*t*able's `Type` `ty`. */
inline BinaryenType get_binaryen_type(const Type *ty)
{
    insist(not ty->is_error());

    if (ty->is_boolean()) return BinaryenTypeInt32();

    if (auto n = cast<const Numeric>(ty)) {
        if (n->kind == Numeric::N_Float) {
            if (n->size() == 32) return BinaryenTypeFloat32();
            else                 return BinaryenTypeFloat64();
        }

        switch (n->size()) {
            case 8:  /* not supported, fall through */
            case 16: /* not supported, fall through */
            case 32: return BinaryenTypeInt32();
            case 64: return BinaryenTypeInt64();
        }
    }

    if (ty->is_character_sequence()) return BinaryenTypeInt32();

    if (ty->is_date()) return BinaryenTypeInt32();
    if (ty->is_date_time()) return BinaryenTypeInt64();

    unreachable("unsupported type");
}

/** Manages the lifetime of a `BinaryenExpressionRef`.  Ensures that the `BinaryenExpressionRef` has at most one user.
 * The semantics of this class are similar to that of `std::unique_ptr`.  */
struct WasmTemporary
{
    friend void swap(WasmTemporary &first, WasmTemporary &second) {
        std::swap(first.ref_, second.ref_);
    }

    private:
    BinaryenExpressionRef ref_ = nullptr;

    public:
    WasmTemporary() { }
    WasmTemporary(BinaryenExpressionRef ref) : ref_(notnull(ref)) { }
    ~WasmTemporary() { /* nothing to be done */ }

    WasmTemporary(const WasmTemporary&) = delete;
    WasmTemporary(WasmTemporary &&other) : WasmTemporary() { swap(*this, other); }

    WasmTemporary & operator=(WasmTemporary other) {
        insist(other.ref_, "cannot assign nullptr");
        swap(*this, other);
        return *this;
    }

    operator BinaryenExpressionRef() { insist(ref_); auto tmp = ref_; ref_ = nullptr; return tmp; }

    /** Returns true iff there is a `BinaryenExpressionRef` attached to `this`. */
    bool is() const { return ref_ != nullptr; }

    /** Returns the type of the attached `BinaryenExpressionRef`. */
    BinaryenType type() const { insist(ref_); return BinaryenExpressionGetType(ref_); }

    /** Returns a fresh `WasmTemporary` with a *deep copy* of the attached `BinaryenExpressionRef`. */
    WasmTemporary clone(WasmModuleCG &module) const;

    void dump() const;
};

struct WasmVariable
{
    private:
    BinaryenModuleRef module_;
    BinaryenType ty_;
    std::size_t var_idx_;

    public:
    WasmVariable(FunctionBuilder &fn, BinaryenType ty);

    WasmVariable(BinaryenModuleRef module, BinaryenType ty, std::size_t idx)
        : module_(module)
        , ty_(ty)
        , var_idx_(idx)
    { }

    WasmVariable(const WasmVariable&) = delete;
    WasmVariable(WasmVariable&&) = default;

    WasmTemporary set(WasmTemporary expr) const {
        return BinaryenLocalSet(
            /* module= */ module_,
            /* index=  */ var_idx_,
            /* value=  */ expr
        );
    }

    WasmTemporary get() const { return BinaryenLocalGet(module_, var_idx_, ty_); }

    operator WasmTemporary() const { return get(); }
    operator BinaryenExpressionRef() const { return get(); }
};

inline WasmTemporary convert(BinaryenModuleRef module, WasmTemporary expr, const Type *original, const Type *target)
{
#define CONVERT(CONVERSION) BinaryenUnary(module, Binaryen##CONVERSION(), expr)
    auto O = as<const Numeric>(original);
    auto T = as<const Numeric>(target);
    if (O->as_vectorial() == T->as_vectorial()) return expr; // no conversion required

    if (T->is_double()) {
        if (O->is_float())
            return CONVERT(PromoteFloat32); // f32 to f64
        if (O->is_integral()) {
            if (O->size() == 64)
                return CONVERT(ConvertSInt64ToFloat64); // i64 to f64
            else
                return CONVERT(ConvertSInt32ToFloat64); // i32 to f64
        }
        if (O->is_decimal()) {
            WasmTemporary val = O->size() == 64 ? CONVERT(ConvertSInt64ToFloat64)  // i64 to f64
                                                : CONVERT(ConvertSInt32ToFloat64); // i32 to f64
            return BinaryenBinary(
                /* module= */ module,
                /* op=     */ BinaryenDivFloat64(),
                /* left=   */ val,
                /* right=  */ BinaryenConst(module, BinaryenLiteralFloat64(pow(10., O->scale)))
            );
        }
    }

    if (T->is_float()) {
        if (O->is_integral()) {
            if (O->size() == 64)
                return CONVERT(ConvertSInt64ToFloat32); // i64 to f32
            else
                return CONVERT(ConvertSInt32ToFloat32); // i32 to f32
        }
        if (O->is_decimal()) {
            unreachable("not implemented");
        }
    }

    if (T->is_integral()) {
        if (T->size() == 64) {
            if (O->is_integral()) {
                if (O->size() == 64)
                    return expr; // i64 to i64; no conversion required
                else
                    return CONVERT(ExtendSInt32); // i32 to i64
            }
        }
        if (T->size() == 32) {
            if (O->is_integral()) {
                if (O->size() == 64)
                    return CONVERT(WrapInt64); // i64 to i32
                else
                    return expr; // i32 to i32; no conversion required
            }
        }
    }

    if (T->is_decimal()) {
        if (O->is_decimal()) {
            if (O->size() < T->size())
                expr = CONVERT(ExtendSInt32);
            if (T->scale > O->scale) {
                const auto delta = T->scale - O->scale;
                auto factor = BinaryenConst(module, T->size() == 64 ? BinaryenLiteralInt64(powi(10L, delta))
                                                                    : BinaryenLiteralInt32(powi(10,  delta)));
                return BinaryenBinary(
                    /* module= */ module,
                    /* op=     */ T->size() == 64 ? BinaryenMulInt64() : BinaryenMulInt32(),
                    /* left=   */ expr,
                    /* right=  */ factor
                );
            } else if (T->scale < O->scale) {
                const auto delta =  O->scale - T->scale;
                auto factor = BinaryenConst(module, T->size() == 64 ? BinaryenLiteralInt64(powi(10L, delta))
                                                                    : BinaryenLiteralInt32(powi(10,  delta)));
                return BinaryenBinary(
                    /* module= */ module,
                    /* op=     */ T->size() == 64 ? BinaryenDivSInt64() : BinaryenDivSInt32(),
                    /* left=   */ expr,
                    /* right=  */ factor
                );
            }

            insist(T->scale == O->scale);
            return expr;
        }

        if (O->is_integral()) {
            if (O->size() < T->size())
                expr = CONVERT(ExtendSInt32);
            if (T->scale) {
                auto factor = BinaryenConst(module, T->size() == 64 ? BinaryenLiteralInt64(powi(10L, T->scale))
                                                                    : BinaryenLiteralInt32(powi(10,  T->scale)));
                return BinaryenBinary(
                    /* module= */ module,
                    /* op=     */ T->size() == 64 ? BinaryenMulInt64() : BinaryenMulInt32(),
                    /* left=   */ expr,
                    /* right=  */ factor
                );
            }
            return expr;
        }

        if (O->is_floating_point()) {
            if (O->size() == 64) {
                expr = BinaryenBinary(
                    /* module= */ module,
                    /* op=     */ BinaryenMulFloat64(),
                    /* left=   */ expr,
                    /* right=  */ BinaryenConst(module, BinaryenLiteralFloat64(pow(10., O->scale)))
                );
                return T->size() == 64 ? CONVERT(TruncSFloat64ToInt64) : CONVERT(TruncSFloat64ToInt32);
            } else {
                expr = BinaryenBinary(
                    /* module= */ module,
                    /* op=     */ BinaryenMulFloat32(),
                    /* left=   */ expr,
                    /* right=  */ BinaryenConst(module, BinaryenLiteralFloat32(powf(10.f, O->scale)))
                );
                return T->size() == 64 ? CONVERT(TruncSFloat32ToInt64) : CONVERT(TruncSFloat32ToInt32);
            }
        }
    }

    unreachable("unsupported conversion");
#undef CONVERT
};

inline WasmTemporary reinterpret(BinaryenModuleRef module, WasmTemporary expr, const BinaryenType target)
{
#define CONVERT(CONVERSION, EXPR) BinaryenUnary(module, Binaryen##CONVERSION(), EXPR)
    const BinaryenType original = expr.type();
    if (original == target) return expr;

    if (target == BinaryenTypeInt64()) {
        if (original == BinaryenTypeInt32())
            return CONVERT(ExtendUInt32, expr); // i32 to i64
        if (original == BinaryenTypeFloat32())
            return CONVERT(ExtendUInt32, CONVERT(ReinterpretFloat32, expr)); // f32 to i64
        if (original == BinaryenTypeFloat64())
            return CONVERT(ReinterpretFloat64, expr); // f64 to i64
    }

    unreachable("unsupported reinterpretation");
#undef CONVERT
}

/** Helper class to construct WASM blocks. */
struct BlockBuilder
{
    friend void swap(BlockBuilder &first, BlockBuilder &second) {
        using std::swap;
        swap(first.module_,      second.module_);
        swap(first.name_,        second.name_);
        swap(first.exprs_,       second.exprs_);
        swap(first.return_type_, second.return_type_);
    }

    private:
    WasmModuleCG *module_ = nullptr; ///< the WebAssembly module
    const char *name_ = nullptr; ///< the block name
    std::vector<BinaryenExpressionRef> exprs_; ///< list of expressions in the block
    BinaryenType return_type_; ///< the result type of the block (i.e. the type of the last expression)

    public:
    BlockBuilder(WasmModuleCG &module, const char *name = nullptr)
        : module_(&module)
        , name_(mkname(name, "block"))
        , return_type_(BinaryenTypeAuto())
    { }

    BlockBuilder(const BlockBuilder&) = delete;
    BlockBuilder(BlockBuilder &&other) { swap(*this, other); }
    ~BlockBuilder() { free((void*) name_); }

    WasmModuleCG & module() const { return *module_; }

    BlockBuilder & operator=(BlockBuilder other) { swap(*this, other); return *this; }

    BlockBuilder & add(WasmTemporary expr) { exprs_.emplace_back(expr); return *this; }
    BlockBuilder & operator+=(WasmTemporary expr) { return add(std::move(expr)); }
    BlockBuilder & operator<<(WasmTemporary expr) { return add(std::move(expr)); }

    /** Returns the block's name. */
    const char * name() const { return name_; }

    void set_return_type(BinaryenType ty) { return_type_ = ty; }

    WasmTemporary finalize();

    void dump(std::ostream &out) const;
    void dump() const;
};

/** Helper class to construct WASM functions. */
struct FunctionBuilder
{
    private:
    WasmModuleCG *module_ = nullptr; ///< the WebAssembly module
    const char *name_ = nullptr; ///< the name of this function
    BinaryenType result_type_; ///< the result type
    BinaryenType parameter_type_; ///< the compound type of all parameters
    std::vector<BinaryenType> locals_; ///< the types of local variables
    BlockBuilder block_; ///< the function body

    public:
    FunctionBuilder(WasmModuleCG &module, const char *name,
                    BinaryenType result_type, std::vector<BinaryenType> parameter_types)
        : module_(&module)
        , name_(strdup(notnull(name)))
        , result_type_(result_type)
        , parameter_type_(BinaryenTypeCreate(&parameter_types[0], parameter_types.size()))
        , block_(module, (std::string(name) + ".body").c_str())
    { }

    FunctionBuilder(const FunctionBuilder&) = delete;

    ~FunctionBuilder() { free((void*) name_); }

    /** Returns the module. */
    WasmModuleCG & module() const { return *module_; }

    /** Returns the function body. */
    BlockBuilder & block() { return block_; }
    /** Returns the function body. */
    const BlockBuilder & block() const { return block_; }

    const char * name() const { return name_; }

    /** Adds a fresh local, unnamed variable to the function and returns its index. */
    std::size_t add_local_anonymous(BinaryenType ty) {
        std::size_t idx = BinaryenTypeArity(parameter_type_) + locals_.size();
        locals_.push_back(ty);
        return idx;
    }

    /** Adds a fresh local, named variable to the function. */
    WasmVariable add_local(BinaryenType ty);

    /** Create a `BinaryenFunctionRef` with the current block and locals. */
    BinaryenFunctionRef finalize();

    void dump(std::ostream &out) const;
    void dump() const;
};

/** An abstract class to provide a codegen context for compilation of expressions. */
struct WasmExprCompiler : ConstASTExprVisitor
{
    private:
    mutable FunctionBuilder *fn_ = nullptr;
    mutable BlockBuilder *block_ = nullptr;
    mutable WasmTemporary value_;

    protected:
    /** Sets the current `FunctionBuilder`. */
    void fn(FunctionBuilder &fn) const { fn_ = &fn; }
    /** Returns the current `FunctionBuilder`. */
    FunctionBuilder & fn() const { return *notnull(fn_); }
    /** Sets the current `BlockBuilder`. */
    void block(BlockBuilder &block) const { block_ = &block; }
    /** Retunrs the target `BlockBuilder` for auxiliary code. */
    BlockBuilder & block() const { return *notnull(block_); }
    /** Returns the current `WasmModuleCG`. */
    WasmModuleCG & module() const { return fn().module(); }

    WasmTemporary get() const { insist(value_.is()); return std::move(value_); }
    void set(WasmTemporary value) { value_ = std::move(value); }

    using ConstASTExprVisitor::operator();
    void operator()(const ErrorExpr&) override { unreachable("no errors at this stage"); }
    void operator()(const Constant &op) override;
    void operator()(const UnaryExpr &op) override;
    void operator()(const BinaryExpr &op) override;

    WasmTemporary compile(const Expr &e) const {
        (*const_cast<WasmExprCompiler*>(this))(e);
        return get();
    }
};

/** Provides a codegen context for compilation of expressions using locally bound identifiers. */
struct WasmEnvironment : WasmExprCompiler
{
    private:
    ///> Maps `Schema::Identifier`s to `WasmTemporary`s that evaluate to 0 if NULL and 1 otherwise
    std::unordered_map<Schema::Identifier, WasmTemporary> nulls_;
    ///> Maps `Schema::Identifier`s to `WasmTemporary`s that evaluate to the current value
    std::unordered_map<Schema::Identifier, WasmTemporary> values_;

    public:
    WasmEnvironment(FunctionBuilder &fn) { this->fn(fn); }
    WasmEnvironment(const WasmEnvironment&) = delete;
    WasmEnvironment(WasmEnvironment&&) = default;

    /** Returns `true` iff this `WasmEnvironment` contains `id`. */
    bool has(Schema::Identifier id) const { return values_.find(id) != values_.end(); }

    /** Adds a mapping from `id` to `val` to this `WasmEnvironment`. */
    void add(Schema::Identifier id, WasmTemporary val) {
        auto res = values_.emplace(id, std::move(val));
        insist(res.second, "duplicate ID");
    }

    /** Returns a `WasmTemporary` that evaluates to `true` iff the value of `id` is `NULL`. */
    WasmTemporary get_null(Schema::Identifier id) const;
    /** Returns a `WasmTemporary` that evaluates to the value of `id`. */
    WasmTemporary get_value(Schema::Identifier id) const;
    /** Returns a `WasmTemporary` that evaluates to the value of `id`. */
    WasmTemporary operator[](Schema::Identifier id) const { return get_value(id); }

    /** Compiles the AST `Expr` `e` in this `WasmEnvironment` and returns a `WasmTemporary` evaluating to the value of
     * `e`.  Auxiliary code is emitted into `block`. */
    WasmTemporary compile(BlockBuilder &block, const Expr &e) const {
        this->block(block);
        return WasmExprCompiler::compile(e);
    }

    /** Compiles a `cnf::CNF` in this `WasmEnvironment` and returns a `WasmTemporary` evaluating to the value of `cnf`.
     * (Compiles `cnf` without short-circuit evaluation!)  Auxiliary code is emitted into `block`. */
    WasmTemporary compile(BlockBuilder &block, const cnf::CNF &cnf) const;

    void dump(std::ostream &out) const;
    void dump() const;

    private:
    using WasmExprCompiler::operator();
    void operator()(const Designator &op) override;
    void operator()(const FnApplicationExpr &op) override;
    void operator()(const QueryExpr &op) override;
};

/** A helper class to generate accesses into a structure. */
struct WasmStruct : RuntimeStruct
{
    public:
    const Schema &schema; ///< the schema of the struct

    WasmStruct(const Schema &schema, std::initializer_list<const Type*> additional_fields = {})
        : RuntimeStruct(schema, additional_fields)
        , schema(schema)
    { }

    WasmStruct(const WasmStruct&) = delete;

    /** Creates a `WasmEnvironment` for the `WasmStruct` at address `ptr` and offset `struc_offset`. */
    WasmEnvironment create_load_context(FunctionBuilder &fn, WasmTemporary ptr, std::size_t struc_offset = 0) const;

    /** Loads the value of the `idx`-th field from the `WasmStruct` at address `ptr` and offset `struc_offset`.  If the
     * field is a C-stype primitive type, the value is returned.  If the field is a character sequence, the address of
     * the first character is returned. */
    WasmTemporary load(FunctionBuilder &fn, WasmTemporary ptr, std::size_t idx, std::size_t struc_offset = 0) const;

    /** Emits code into `block` that stores the value `val` to the `idx`-th field to the `WasmStruct` at address
     * `ptr` and offset `struc_offset`.  */
    void store(FunctionBuilder &fn, BlockBuilder &block, WasmTemporary ptr, std::size_t idx, WasmTemporary val,
               std::size_t struc_offset = 0) const;

    void dump(std::ostream &out) const;
    void dump() const;
};

/** Provides a codegen context for compilation of expressions accessing fields of a `WasmStruct`. */
struct WasmStructCGContext : WasmExprCompiler
{
    const WasmStruct &struc; ///< the `WasmStruct` to access
    private:
    ///> maps each `Schema::Identifier` to its index in the `WasmStruct`
    std::unordered_map<Schema::Identifier, WasmStruct::index_type> indices_;
    WasmStruct::offset_type struc_offset_; ///< the offset of the `WasmStruct` `struc`

    mutable WasmTemporary base_ptr_;

    public:
    WasmStructCGContext(const WasmStruct &struc, WasmStruct::offset_type struc_offset = 0)
        : struc(struc)
        , struc_offset_(struc_offset)
    { }
    WasmStructCGContext(const WasmStructCGContext&) = delete;
    WasmStructCGContext(WasmStructCGContext&&) = default;

    /** Returns `true` iff the `WasmStruct` `struc` has a field for `id`. */
    bool has(Schema::Identifier id) const { return indices_.find(id) != indices_.end(); }

    /** Adds an entry for field `id` at `index` of the `WasmStruct` `struc`. */
    void add(Schema::Identifier id, WasmStruct::index_type index) {
        auto res = indices_.emplace(id, index);
        insist(res.second, "duplicate ID");
    }

    WasmStruct::index_type index(Schema::Identifier id) const { return indices_.at(id); }

    WasmTemporary get(WasmTemporary ptr, Schema::Identifier id) const {
        return struc.load(fn(), std::move(ptr), index(id), struc_offset_);
    }

    WasmTemporary compile(FunctionBuilder &fn, BlockBuilder &block, WasmTemporary ptr, const Expr &e) const {
        this->fn(fn);
        this->block(block);
        base_ptr_ = std::move(ptr);
        return WasmExprCompiler::compile(e);
    }

    private:
    using WasmExprCompiler::get;
    using WasmExprCompiler::operator();
    void operator()(const Designator &op) override;
    void operator()(const FnApplicationExpr &op) override;
    void operator()(const QueryExpr &op) override;
};

struct WasmLoop
{
    private:
    const char *name_ = nullptr;
    BlockBuilder body_;

    public:
    WasmLoop(WasmModuleCG &module, const char *name = nullptr)
        : name_(mkname(name, "loop"))
        , body_(module, (std::string(name_) + ".body").c_str())
    { }

    virtual ~WasmLoop() { free((void*) name_); }

    BlockBuilder & body() { return body_; }
    const BlockBuilder & body() const { return body_; }

    /** Return the loop's name. */
    const char * name() const { return name_; }

    operator BlockBuilder&() { return body(); }

    WasmLoop & add(WasmTemporary expr) { body().add(std::move(expr)); return *this; }
    WasmLoop & operator+=(WasmTemporary expr) { return add(std::move(expr)); }
    WasmLoop & operator<<(WasmTemporary expr) { return add(std::move(expr)); }

    WasmTemporary continu(WasmTemporary condition = WasmTemporary());

    virtual WasmTemporary finalize();
};

struct WasmDoWhile : WasmLoop
{
    private:
    WasmTemporary condition_;

    public:
    WasmDoWhile(WasmModuleCG &module, const char *name, WasmTemporary condition)
        : WasmLoop(module, name)
        , condition_(std::move(condition))
    { }

    WasmTemporary condition() const;

    virtual WasmTemporary finalize() override {
        body() += continu(condition());
        return WasmLoop::finalize();
    }
};

struct WasmWhile : WasmDoWhile
{
    public:
    WasmWhile(WasmModuleCG &module, const char *name, WasmTemporary condition)
        : WasmDoWhile(module, name, std::move(condition))
    { }

    WasmTemporary finalize() override;
};

/** Compares primitive C-style types.  To compare two C-strings use `WasmStrcmp`. */
struct WasmCompare
{
    friend WasmStrcmp;

    using order_type = std::pair<const Expr*, bool>;

    private:
    /** The available comparison operations. */
    enum cmp_op {
        EQ, NE, LT, LE, GT, GE
    };

    FunctionBuilder &fn_; ///< the function in which to compare two structs
    public:
    const std::vector<order_type> &order; ///< the struct's attributes to compare by

    WasmCompare(FunctionBuilder &fn, const std::vector<order_type> &order)
        : fn_(fn)
        , order(order)
    { }

    FunctionBuilder & fn() const { return fn_; }

    /** Emit code to compare structs at positions `left` and `right`. */
    WasmTemporary emit(BlockBuilder &block, const WasmStructCGContext &context,
                       WasmTemporary left, WasmTemporary right);

    static WasmTemporary Eq(FunctionBuilder &fn, const Type &ty, WasmTemporary left, WasmTemporary right) {
        return Cmp(fn, ty, std::move(left), std::move(right), EQ);
    }
    static WasmTemporary Ne(FunctionBuilder &fn, const Type &ty, WasmTemporary left, WasmTemporary right) {
        return Cmp(fn, ty, std::move(left), std::move(right), NE);
    }
    static WasmTemporary Lt(FunctionBuilder &fn, const Type &ty, WasmTemporary left, WasmTemporary right) {
        return Cmp(fn, ty, std::move(left), std::move(right), LT);
    }
    static WasmTemporary Le(FunctionBuilder &fn, const Type &ty, WasmTemporary left, WasmTemporary right) {
        return Cmp(fn, ty, std::move(left), std::move(right), LE);
    }
    static WasmTemporary Gt(FunctionBuilder &fn, const Type &ty, WasmTemporary left, WasmTemporary right) {
        return Cmp(fn, ty, std::move(left), std::move(right), GT);
    }
    static WasmTemporary Ge(FunctionBuilder &fn, const Type &ty, WasmTemporary left, WasmTemporary right) {
        return Cmp(fn, ty, std::move(left), std::move(right), GE);
    }

    private:
    static WasmTemporary Cmp(FunctionBuilder &fn, const Type &ty, WasmTemporary left, WasmTemporary right, cmp_op op);
};

/** Compares two c-style strings (i.e. strings terminated by a NUL-byte). */
struct WasmStrcmp
{
    static WasmTemporary Eq(FunctionBuilder &fn, BlockBuilder &block,
                            const CharacterSequence &ty_left, const CharacterSequence &ty_right,
                            WasmTemporary left, WasmTemporary right) {
        return Cmp(fn, block, ty_left, ty_right, std::move(left), std::move(right), WasmCompare::EQ);
    }
    static WasmTemporary Ne(FunctionBuilder &fn, BlockBuilder &block,
                            const CharacterSequence &ty_left, const CharacterSequence &ty_right,
                            WasmTemporary left, WasmTemporary right) {
        return Cmp(fn, block, ty_left, ty_right, std::move(left), std::move(right), WasmCompare::NE);
    }
    static WasmTemporary Lt(FunctionBuilder &fn, BlockBuilder &block,
                            const CharacterSequence &ty_left, const CharacterSequence &ty_right,
                            WasmTemporary left, WasmTemporary right) {
        return Cmp(fn, block, ty_left, ty_right, std::move(left), std::move(right), WasmCompare::LT);
    }
    static WasmTemporary Gt(FunctionBuilder &fn, BlockBuilder &block,
                            const CharacterSequence &ty_left, const CharacterSequence &ty_right,
                            WasmTemporary left, WasmTemporary right) {
        return Cmp(fn, block, ty_left, ty_right, std::move(left), std::move(right), WasmCompare::GT);
    }
    static WasmTemporary Le(FunctionBuilder &fn, BlockBuilder &block,
                            const CharacterSequence &ty_left, const CharacterSequence &ty_right,
                            WasmTemporary left, WasmTemporary right) {
        return Cmp(fn, block, ty_left, ty_right, std::move(left), std::move(right), WasmCompare::LE);
    }
    static WasmTemporary Ge(FunctionBuilder &fn, BlockBuilder &block,
                            const CharacterSequence &ty_left, const CharacterSequence &ty_right,
                            WasmTemporary left, WasmTemporary right) {
        return Cmp(fn, block, ty_left, ty_right, std::move(left), std::move(right), WasmCompare::GE);
    }

    private:
    static WasmTemporary Cmp(FunctionBuilder &fn, BlockBuilder &block,
                             const CharacterSequence &ty_left, const CharacterSequence &ty_right,
                             WasmTemporary left, WasmTemporary right,
                             WasmCompare::cmp_op op);
};

struct WasmStrncpy
{
    FunctionBuilder &fn;

    WasmStrncpy(FunctionBuilder &fn) : fn(fn) { }

    void emit(BlockBuilder &block, WasmTemporary dest, WasmTemporary src, std::size_t count);
};

struct WasmSwap
{
    FunctionBuilder &fn;
    std::unordered_map<BinaryenType, WasmVariable> swap_temp;

    WasmSwap(FunctionBuilder &fn) : fn(fn) { }

    void emit(BlockBuilder &block, const WasmStruct &struc, WasmTemporary first, WasmTemporary second);

    void swap_string(BlockBuilder &block, const CharacterSequence &ty, WasmTemporary first, WasmTemporary second);
};

struct WasmLimits
{
    static BinaryenLiteral min(const Type &type);
    static BinaryenLiteral lowest(const Type &type);
    static BinaryenLiteral max(const Type &type);
    static BinaryenLiteral NaN(const Type &type);
    static BinaryenLiteral infinity(const Type &type);
};

template<typename T>
BinaryenLiteral wasm_constant(const T &val, const Type &ty)
{
    BinaryenLiteral literal;
    visit(overloaded {
        [&val, &literal](const Boolean&) { literal = BinaryenLiteralInt32(bool(val)); },
        [&val, &literal](const Numeric &n) {
            switch (n.kind) {
                case Numeric::N_Int:
                    if (n.size() <= 32)
                        literal = BinaryenLiteralInt32(int32_t(val));
                    else
                        literal = BinaryenLiteralInt64(int64_t(val));
                    break;

                case Numeric::N_Decimal:
                    unreachable("not supported");

                case Numeric::N_Float:
                    if (n.size() == 32)
                        literal = BinaryenLiteralFloat32(float(val));
                    else
                        literal = BinaryenLiteralFloat64(double(val));
                    break;
            }
        },
        [](auto&) { unreachable("unsupported type"); }
    }, ty);
    return literal;
}

struct WasmModuleCG
{
    private:
    WasmModule &module_; ///< the WASM module to emit code to
    FunctionBuilder main_; ///< the main function (or entry)
    WasmVariable head_of_heap_; ///< variable to hold the current offset of the heap's head
    WasmVariable num_tuples_; ///< variable to hold the number of result tuples produced
    ///> Maps each literal to its offset from the initial head of heap.  Used to access string literals.
    std::unordered_map<const char*, BinaryenIndex> literal_offsets_;
    WasmVariable literals_; ///< address of literals

    public:
    WasmModuleCG(WasmModule &module, const char *main)
        : module_(module)
        , main_(*this, main, BinaryenTypeInt32(), { /* module ID */ BinaryenTypeInt32() })
        , head_of_heap_(main_, BinaryenTypeInt32())
        , num_tuples_(main_, BinaryenTypeInt32())
        , literals_(main_, BinaryenTypeInt32())
    { }

    WasmModuleCG(const WasmModuleCG&) = delete;

    operator BinaryenModuleRef() { return module_.ref(); }
    operator const BinaryenModuleRef() const { return module_.ref(); }

    /** Returns the `FunctionBuilder` of the main function. */
    FunctionBuilder & main() { return main_; }
    /** Returns the `FunctionBuilder` of the main function. */
    const FunctionBuilder & main() const { return main_; }

    /** Returns the local variable holding the number of result tuples produced. */
    const WasmVariable & num_tuples() const { return num_tuples_; }

    /** Returns the local variable holding the address to the head of the heap. */
    const WasmVariable & head_of_heap() const { return head_of_heap_; }

    const WasmVariable & literals() const { return literals_; }

    void add_literal(const char *literal, BinaryenIndex offset) {
        literal_offsets_.emplace(literal, offset);
    }

    BinaryenIndex get_literal_offset(const char *literal) {
        auto it = literal_offsets_.find(literal);
        insist(it != literal_offsets_.end(), "unknown literal");
        return it->second;
    }

    /** Adds a global import to the module.  */
    void import(std::string name, BinaryenType ty) {
        if (not BinaryenGetGlobal(*this, name.c_str())) {
            BinaryenAddGlobalImport(
                /* module=             */ *this,
                /* internalName=       */ name.c_str(),
                /* externalModuleName= */ "env",
                /* externalBaseName=   */ name.c_str(),
                /* type=               */ ty,
                /* mutable=            */ false
            );
        }
    }

    /** Returns the value of the global with the given `name`. */
    WasmTemporary get_imported(const std::string &name, BinaryenType ty) const {
        return BinaryenGlobalGet(module_.ref(), name.c_str(), ty);
    }

    WasmTemporary inc_num_tuples(int32_t n = 1) {
        WasmTemporary inc = BinaryenBinary(
            /* module= */ *this,
            /* op=     */ BinaryenAddInt32(),
            /* lhs=    */ num_tuples_,
            /* rhs=    */ BinaryenConst(*this, BinaryenLiteralInt32(n))
        );
        return num_tuples_.set(std::move(inc));
    }

    WasmTemporary align_head_of_heap() {
        WasmTemporary head_inc = BinaryenBinary(
            /* module= */ *this,
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ head_of_heap(),
            /* right=  */ BinaryenConst(*this, BinaryenLiteralInt32(WasmPlatform::WASM_ALIGNMENT - 1))
        );
        WasmTemporary head_aligned = BinaryenBinary(
            /* module= */ *this,
            /* op=     */ BinaryenAndInt32(),
            /* left=   */ head_inc,
            /* right=  */ BinaryenConst(*this, BinaryenLiteralInt32(~(int32_t(WasmPlatform::WASM_ALIGNMENT) - 1)))
        );
        return head_of_heap().set(std::move(head_aligned));
    }

    void compile(const Operator &plan);
};

/** Compiles a physical plan to WebAssembly. */
struct WasmPlanCG : ConstOperatorVisitor
{
    private:
    WasmModuleCG &module_;

    public:
    WasmPlanCG(WasmModuleCG &module) : module_(module) { }
    WasmPlanCG(const WasmPlanCG&) = delete;

    /** Returns the current WASM module. */
    WasmModuleCG & module() const { return module_; }

    void compile(const Operator &plan) { (*this)(plan); }

    private:
    /*----- OperatorVisitor ------------------------------------------------------------------------------------------*/
    using ConstOperatorVisitor::operator();
#define DECLARE(CLASS) void operator()(const CLASS &op) override;
    DB_OPERATOR_LIST(DECLARE)
#undef DECLARE
};

/** Compiles a single pipeline.  Pipelines begin at producer nodes in the operator tree. */
struct WasmPipelineCG : ConstOperatorVisitor
{
    friend struct WasmStoreCG;
    friend struct WasmPlanCG;

    private:
    WasmPlanCG &plan_; ///< the current codegen context
    WasmEnvironment context_; ///< wasm context for compilation of expressions
    BlockBuilder block_; ///< used to construct the current block
    const char *name_ = nullptr; ///< name of this pipeline

    public:
    WasmPipelineCG(WasmPlanCG &plan, const char *name = nullptr)
        : plan_(plan)
        , context_(plan.module().main())
        , block_(module(), name)
        , name_(name)
    { }

    ~WasmPipelineCG() { }

    WasmPipelineCG(const WasmPipelineCG&) = delete;

    WasmPlanCG & plan() { return plan_; }
    const WasmPlanCG & plan() const { return plan_; }

    WasmModuleCG & module() { return plan().module(); }
    const WasmModuleCG & module() const { return plan().module(); }

    /** Compiles the pipeline of the given producer to a WASM block. */
    static WasmTemporary compile(const Producer &prod, WasmPlanCG &CG, const char *name) {
        WasmPipelineCG P(CG, name);
        P(prod);
        return P.block_.finalize();
    }

    WasmEnvironment & context() { return context_; }
    const WasmEnvironment & context() const { return context_; }

    const char * name() const { return name_; }

    private:
    void emit_write_results(const Schema &schema);

    /* Operators */
    using ConstOperatorVisitor::operator();
#define DECLARE(CLASS) void operator()(const CLASS &op) override;
    DB_OPERATOR_LIST(DECLARE)
#undef DECLARE
};

struct WasmStoreCG : ConstStoreVisitor
{
    WasmPipelineCG &pipeline;
    const Producer &op;

    WasmStoreCG(WasmPipelineCG &pipeline, const Producer &op)
        : pipeline(pipeline)
        , op(op)
    { }

    ~WasmStoreCG() { }

    using ConstStoreVisitor::operator();
    void operator()(const RowStore &store) override;
    void operator()(const ColumnStore &store) override;
};


/*======================================================================================================================
 * delayed definitions because of cyclic dependences
 *====================================================================================================================*/

/*----- WasmTemporary ------------------------------------------------------------------------------------------------*/
inline WasmTemporary WasmTemporary::clone(WasmModuleCG &module) const
{
    insist(ref_);
    return BinaryenExpressionCopy(ref_, module);
}

/*----- WasmVariable -------------------------------------------------------------------------------------------------*/
inline WasmVariable::WasmVariable(FunctionBuilder &fn, BinaryenType ty)
    : module_(fn.module())
    , ty_(ty)
    , var_idx_(fn.add_local_anonymous(ty))
{ }

/*----- WasmStruct ---------------------------------------------------------------------------------------------------*/
inline WasmEnvironment
WasmStruct::create_load_context(FunctionBuilder &fn, WasmTemporary ptr, std::size_t struc_offset) const
{
    WasmEnvironment context(fn);
    std::size_t idx = 0;
    for (auto &attr : schema)
        context.add(attr.id, load(fn, ptr.clone(fn.module()), idx++, struc_offset));
    return context;
}

inline WasmTemporary BlockBuilder::finalize()
{
    WasmTemporary blk = BinaryenBlock(
        /* module=      */ module(),
        /* name=        */ name_,
        /* children=    */ &exprs_[0],
        /* numChildren= */ exprs_.size(),
        /* type=        */ return_type_
    );
    exprs_.clear();
    return blk;
}

/*----- FunctionBuilder ----------------------------------------------------------------------------------------------*/
inline WasmVariable FunctionBuilder::add_local(BinaryenType ty)
{
    return WasmVariable(module(), ty, add_local_anonymous(ty));
}

inline BinaryenFunctionRef FunctionBuilder::finalize()
{
    return BinaryenAddFunction(
        /* module=      */ module(),
        /* name=        */ name_,
        /* params=      */ parameter_type_,
        /* results=     */ result_type_,
        /* varTypes=    */ &locals_[0],
        /* numVarTypes= */ locals_.size(),
        /* body=        */ block_.finalize()
    );
}

/*----- WasmLoop -----------------------------------------------------------------------------------------------------*/
inline WasmTemporary WasmLoop::continu(WasmTemporary condition)
{
    return BinaryenBreak(
        /* module=    */ body().module(),
        /* name=      */ name(),
        /* condition= */ condition.is() ? BinaryenExpressionRef(condition) : nullptr,
        /* value=     */ nullptr
    );
}

inline WasmTemporary WasmLoop::finalize()
{
    return BinaryenLoop(
        /* module= */ body().module(),
        /* name=   */ name(),
        /* body=   */ body().finalize()
    );
}

/*----- WasmEnvironment ----------------------------------------------------------------------------------------------*/
inline WasmTemporary WasmEnvironment::get_null(Schema::Identifier id) const { return nulls_.at(id).clone(module()); }
inline WasmTemporary WasmEnvironment::get_value(Schema::Identifier id) const { return values_.at(id).clone(module()); }

/*----- WasmDoWhile --------------------------------------------------------------------------------------------------*/
inline WasmTemporary WasmDoWhile::condition() const { return condition_.clone(body().module()); }

/*----- WasmWhile ----------------------------------------------------------------------------------------------------*/
inline WasmTemporary WasmWhile::finalize()
{
    auto loop = WasmDoWhile::finalize();
    return BinaryenIf(
        /* module=    */ body().module(),
        /* condition= */ condition(),
        /* ifTrue=    */ loop,
        /* ifFalse=   */ nullptr
    );
}

/*----- WasmModuleCG -------------------------------------------------------------------------------------------------*/
inline void WasmModuleCG::compile(const Operator &plan)
{
    WasmPlanCG CG(*this);
    CG.compile(plan);
}


/*======================================================================================================================
 * Codegen functions
 *====================================================================================================================*/

WasmTemporary wasm_emit_strhash(FunctionBuilder &fn, BlockBuilder &block,
                                WasmTemporary ptr, const CharacterSequence &ty);

}

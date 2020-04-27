#pragma once

#include "catalog/Schema.hpp"
#include "catalog/Type.hpp"
#include "IR/CNF.hpp"
#include <binaryen-c.h>
#include <cstring>
#include <utility>
#include <vector>


namespace db {

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

    unreachable("unsupported type");
}

inline BinaryenExpressionRef convert(BinaryenModuleRef module, BinaryenExpressionRef expr,
                                     const Type *original, const Type *target)
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
            unreachable("not implemented");
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

    unreachable("unsupported conversion");
#undef CONVERT
};

/** A helper class to provide a context for compilation of expressions. */
struct WasmCGContext : ConstASTExprVisitor
{
    private:
    BinaryenModuleRef module_; ///< the module
    ///> Maps `Schema::Identifier`s to `BinaryenExpressionRef`s that evaluate to 0 if NULL and 1 otherwise
    std::unordered_map<Schema::Identifier, BinaryenExpressionRef> nulls_;
    ///> Maps `Schema::Identifier`s to `BinaryenExpressionRef`s thatr evaluate to the current value
    std::unordered_map<Schema::Identifier, BinaryenExpressionRef> values_;

    BinaryenExpressionRef expr_; ///< a temporary used for recursive construction of expressions

    public:
    WasmCGContext(BinaryenModuleRef module) : module_(module) { }
    WasmCGContext(const WasmCGContext&) = delete;
    WasmCGContext(WasmCGContext&&) = default;

    BinaryenModuleRef module() const { return module_; }

    bool has(Schema::Identifier id) const { return values_.find(id) != values_.end(); }

    void add(Schema::Identifier id, BinaryenExpressionRef val) {
        auto res = values_.emplace(id, val);
        insist(res.second, "duplicate ID");
    }

    BinaryenExpressionRef get_null(Schema::Identifier id) const {
        auto it = nulls_.find(id);
        insist(it != nulls_.end(), "no entry for identifier");
        return it->second;
    }

    BinaryenExpressionRef get_value(Schema::Identifier id) const {
        auto it = values_.find(id);
        insist(it != values_.end(), "no entry for identifier");
        return it->second;
    }

    BinaryenExpressionRef operator[](Schema::Identifier id) const { return get_value(id); }

    /** Compiles an AST expression to a `BinaryenExpressionRef`. */
    BinaryenExpressionRef compile(const Expr &e) const {
        const_cast<WasmCGContext*>(this)->operator()(e);
        return expr_;
    }

    /** Compiles a `cnf::CNF` to a `BinaryenExpressionRef`.  (Without short-circuit evaluation!) */
    BinaryenExpressionRef compile(const cnf::CNF &cnf) const;

    private:
    using ConstASTExprVisitor::operator();
#define DECLARE(CLASS) void operator()(const CLASS &op) override;
    DB_AST_EXPR_LIST(DECLARE)
#undef DECLARE
};

/** A helper class to generate accesses into a structure. */
struct WasmStruct
{
    private:
    BinaryenModuleRef module_;
    std::size_t size_; ///< the size in bytes of the struct
    public:
    const Schema &schema; ///< the schema of the struct

    WasmStruct(BinaryenModuleRef module, const Schema &schema)
        : module_(module)
        , schema(schema)
    {
        /*----- Compute struct size. ---------------------------------------------------------------------------------*/
        std::size_t offset = 0;
        std::size_t alignment = 0;
        for (auto &attr : schema) {
            const std::size_t size_in_bytes = attr.type->size() < 8 ? 1 : attr.type->size() / 8;
            alignment = std::max(alignment, size_in_bytes);
            if (offset % size_in_bytes)
                offset += size_in_bytes - (offset % size_in_bytes); // self-align
            offset += size_in_bytes;
        }
        if (offset % alignment)
            offset += alignment - (offset & alignment);
        size_ = offset;
    }

    std::size_t size() const { return size_; }

    WasmCGContext create_load_context(BinaryenExpressionRef b_ptr) const {
        WasmCGContext context(module_);
        std::size_t offset = 0;
        std::size_t alignment = 0;
        for (auto &attr : schema) {
            const std::size_t size_in_bytes = attr.type->size() < 8 ? 1 : attr.type->size() / 8;
            alignment = std::max(alignment, size_in_bytes);
            if (offset % size_in_bytes)
                offset += size_in_bytes - (offset % size_in_bytes); // self-align

            BinaryenType b_attr_type = get_binaryen_type(attr.type);

            /*----- Load value from struct.  -------------------------------------------------------------------------*/
            auto b_val = BinaryenLoad(
                /* module= */ module_,
                /* bytes=  */ size_in_bytes,
                /* signed= */ true,
                /* offset= */ offset,
                /* align=  */ 0,
                /* type=   */ b_attr_type,
                /* ptr=    */ b_ptr
            );
            context.add(attr.id, b_val);

            offset += size_in_bytes;
        }
        return context;
    }

    BinaryenExpressionRef store(BinaryenExpressionRef b_ptr, Schema::Identifier id, BinaryenExpressionRef b_val) const {
        std::size_t offset = 0;
        std::size_t alignment = 0;
        for (auto &attr : schema) {
            const std::size_t size_in_bytes = attr.type->size() < 8 ? 1 : attr.type->size() / 8;
            alignment = std::max(alignment, size_in_bytes);
            if (offset % size_in_bytes)
                offset += size_in_bytes - (offset % size_in_bytes); // self-align

            if (attr.id == id) {
                return BinaryenStore(
                    /* module= */ module_,
                    /* bytes=  */ size_in_bytes,
                    /* offset= */ offset,
                    /* align=  */ 0,
                    /* ptr=    */ b_ptr,
                    /* value=  */ b_val,
                    /* type=   */ get_binaryen_type(attr.type)
                );
                break;
            }

            offset += size_in_bytes;
        }
        unreachable("unknown identifier");
    }
};

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
    BinaryenModuleRef module_; ///< the WebAssembly module
    const char *name_ = nullptr; ///< the block name
    std::vector<BinaryenExpressionRef> exprs_; ///< list of expressions in the block
    BinaryenType return_type_; ///< the result type of the block (i.e. the type of the last expression)

    public:
    BlockBuilder(BinaryenModuleRef module, const char *name = nullptr)
        : module_(module)
        , name_(strdupn(name))
        , return_type_(BinaryenTypeAuto())
    { }
    BlockBuilder(const BlockBuilder&) = delete;
    BlockBuilder(BlockBuilder &&other) { swap(*this, other); }
    ~BlockBuilder() { free((void*) name_); }

    BlockBuilder & operator=(BlockBuilder other) { swap(*this, other); return *this; }

    void add(BinaryenExpressionRef expr) { exprs_.push_back(expr); }
    BlockBuilder & operator+=(BinaryenExpressionRef expr) { add(expr); return *this; }

    void name(const char *name) { free((void*) name_); name_ = strdupn(name); }
    const char * name() const { return name_; }

    void set_return_type(BinaryenType ty) { return_type_ = ty; }

    BinaryenExpressionRef finalize() {
        return BinaryenBlock(
            /* module=      */ module_,
            /* name=        */ name_,
            /* children=    */ &exprs_[0],
            /* numChildren= */ exprs_.size(),
            /* type=        */ return_type_
        );
    }
};

/** Helper class to construct WASM functions. */
struct FunctionBuilder
{
    private:
    BinaryenModuleRef module_; ///< the WebAssembly module
    const char *name_ = nullptr; ///< the name of this function
    BinaryenType result_type_; ///< the result type
    BinaryenType parameter_type_; ///< the compound type of all parameters
    std::vector<BinaryenType> locals_; ///< the types of local variables
    BlockBuilder block_; ///< the function body

    public:
    FunctionBuilder(BinaryenModuleRef module, const char *name,
                    BinaryenType result_type, std::vector<BinaryenType> parameter_types)
        : module_(module)
        , name_(strdup(notnull(name)))
        , result_type_(result_type)
        , parameter_type_(BinaryenTypeCreate(&parameter_types[0], parameter_types.size()))
        , block_(module, (std::string(name) + ".body").c_str())
    { }

    FunctionBuilder(const FunctionBuilder&) = delete;

    ~FunctionBuilder() { free((void*) name_); }

    /** Create a `BinaryenFunctionRef` with the current block and locals. */
    BinaryenFunctionRef finalize() {
        return BinaryenAddFunction(
            /* module=      */ module_,
            /* name=        */ name_,
            /* params=      */ parameter_type_,
            /* results=     */ result_type_,
            /* varTypes=    */ &locals_[0],
            /* numVarTypes= */ locals_.size(),
            /* body=        */ block_.finalize()
        );
    }

    /** Returns the function body. */
    BlockBuilder & block() { return block_; }
    /** Returns the function body. */
    const BlockBuilder & block() const { return block_; }

    /** Add a fresh local variable to the function and return a `BinaryenLocalGet` expression to access it. */
    BinaryenExpressionRef add_local(BinaryenType ty) {
        std::size_t idx = BinaryenTypeArity(parameter_type_) + locals_.size();
        locals_.push_back(ty);
        return BinaryenLocalGet(module_, idx, ty);
    }
};

struct WasmCompare
{
    using order_type = std::pair<const Expr*, bool>;

    private:
    BinaryenModuleRef module_;
    public:
    const WasmStruct &struc;
    const std::vector<order_type> &order; ///< the attributes to sort by

    WasmCompare(BinaryenModuleRef module, const WasmStruct &struc, const std::vector<order_type> &order)
        : module_(module)
        , struc(struc)
        , order(order)
    { }

    BinaryenExpressionRef emit(FunctionBuilder &fn, BlockBuilder &block,
                               const WasmCGContext &left, const WasmCGContext &right);
};

struct WasmSwap
{
    BinaryenModuleRef module;
    FunctionBuilder &fn;
    std::unordered_map<BinaryenType, BinaryenExpressionRef> swap_temp;

    WasmSwap(BinaryenModuleRef module, FunctionBuilder &fn) : module(module) , fn(fn) { }

    void emit(BlockBuilder &block, const WasmStruct &struc,
              BinaryenExpressionRef b_first, BinaryenExpressionRef b_second);
};

}

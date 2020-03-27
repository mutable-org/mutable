#include "backend/WebAssembly.hpp"

#include "backend/Interpreter.hpp"
#include "IR/CNF.hpp"
#include "storage/ColumnStore.hpp"
#include "storage/RowStore.hpp"
#include "storage/Store.hpp"
#include <binaryen-c.h>
#include <exception>
#include <sstream>
#include <unordered_map>


using namespace db;


/*======================================================================================================================
 * Helper functions
 *====================================================================================================================*/

static BinaryenType get_binaryen_type(const Type *ty)
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

BinaryenExpressionRef convert(BinaryenModuleRef module, BinaryenExpressionRef expr,
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
                if (O->size() <= 32) {
                    return CONVERT(ExtendS32Int64); // i32 to i64
                }
            }
        }
        if (T->size() == 32) {
            if (O->is_integral()) {
                if (O->size() <= 32)
                    return expr; // i32 to i32; no conversion required
            }
        }
    }

    unreachable("unsupported conversion");
#undef CONVERT
};


/*======================================================================================================================
 * Code generation helper classes
 *====================================================================================================================*/

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
    BinaryenModuleRef module_;
    const char *name_;
    std::vector<BinaryenExpressionRef> exprs_;
    BinaryenType return_type_;

    public:
    BlockBuilder(BinaryenModuleRef module, const char *name = nullptr)
        : module_(module)
        , name_(name)
        , return_type_(BinaryenTypeAuto())
    { }
    BlockBuilder(const BlockBuilder&) = delete;
    BlockBuilder(BlockBuilder &&other) { swap(*this, other); }

    BlockBuilder & operator=(BlockBuilder other) { swap(*this, other); return *this; }

    void add(BinaryenExpressionRef expr) { exprs_.push_back(expr); }
    BlockBuilder & operator+=(BinaryenExpressionRef expr) { add(expr); return *this; }

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

/** Compiles a physical plan to WebAssembly. */
struct WasmCodeGen : ConstOperatorVisitor
{
    private:
    BinaryenModuleRef module_; ///< the WASM module
    std::size_t num_params_; ///< number of function parameters
    std::vector<BinaryenType> param_types; ///< types of function parameters
    std::vector<BinaryenType> locals_; ///< types of function locals
    BlockBuilder block_; ///< the global block
    BinaryenExpressionRef b_out_; ///< ptr to the output location

    WasmCodeGen(WasmModule &module) : module_(module.ref()), block_(module.ref(), "fn.body") { } // private c'tor

    public:
    static WasmModule compile(const Operator &op);

    /** Returns the current WASM module. */
    BinaryenModuleRef module() { return module_; }

    /** Creates a new local and returns an expression accessing this fresh local. */
    BinaryenExpressionRef add_local(BinaryenType ty) {
        std::size_t idx = num_params_ + locals_.size();
        locals_.push_back(ty);
        return BinaryenLocalGet(module(), idx, ty);
    }

    /** Returns an expression evaluating to the location where output (results) are written to. */
    BinaryenExpressionRef out() const { return b_out_; }

    private:
    /*----- OperatorVisitor ------------------------------------------------------------------------------------------*/
    using ConstOperatorVisitor::operator();
#define DECLARE(CLASS) void operator()(const CLASS &op) override;
    DB_OPERATOR_LIST(DECLARE)
#undef DECLARE
};

/** Compiles a single pipeline.  Pipelines begin at producer nodes in the operator tree. */
struct WasmPipelineCG : ConstOperatorVisitor, ConstASTExprVisitor
{
    friend struct WasmStoreCG;

    WasmCodeGen &CG; ///< the current codegen context

    private:
    BlockBuilder block_; ///< used to construct the current block
    BinaryenExpressionRef expr_; ///< used for recursive construction of expressions
    BinaryenExpressionRef b_induction_var; ///< the induction variable that is used as the row id
    std::unordered_map<Schema::Identifier, BinaryenExpressionRef> is_null_; ///< a map of intermediate results
    std::unordered_map<Schema::Identifier, BinaryenExpressionRef> intermediates_; ///< a map of intermediate results

    public:
    WasmPipelineCG(WasmCodeGen &CG) : CG(CG) , block_(CG.module()) { }

    /** Return the current WASM module. */
    BinaryenModuleRef module() { return CG.module(); }

    /** Compiles the pipeline of the given producer to a WASM block. */
    static BinaryenExpressionRef compile(const Producer &prod, WasmCodeGen &CG) {
        WasmPipelineCG P(CG);
        P(prod);
        return P.block_.finalize();
    }

    private:
    /** Compiles an AST expression to a `BinaryenExpressionRef`. */
    BinaryenExpressionRef compile(const Expr &expr);
    /** Compiles a `cnf::CNF` to a `BinaryenExpressionRef`.  (Without short-circuit evaluation!) */
    BinaryenExpressionRef compile(const cnf::CNF &cnf);

    /* Operators */
    using ConstOperatorVisitor::operator();
#define DECLARE(CLASS) void operator()(const CLASS &op) override;
    DB_OPERATOR_LIST(DECLARE)
#undef DECLARE

    using ConstASTExprVisitor::operator();
#define DECLARE(CLASS) void operator()(const CLASS &op) override;
    DB_AST_EXPR_LIST(DECLARE)
#undef DECLARE
};

struct WasmStoreCG : ConstStoreVisitor
{
    WasmPipelineCG &pipeline;
    const Schema &schema;

    WasmStoreCG(WasmPipelineCG &pipeline, const Schema &schema)
        : pipeline(pipeline)
        , schema(schema)
    { }

    ~WasmStoreCG() { }

    using ConstStoreVisitor::operator();
    void operator()(const RowStore &store) override;
    void operator()(const ColumnStore &store) override;
};


/*======================================================================================================================
 * WasmCodeGen
 *====================================================================================================================*/

WasmModule WasmCodeGen::compile(const Operator &op)
{
    WasmModule module; // fresh module
    WasmCodeGen codegen(module);

    /*----- Declare parameters. --------------------------------------------------------------------------------------*/
    std::vector<BinaryenType> param_types;
    param_types.push_back(BinaryenTypeInt32()); // module_id
    codegen.num_params_ = param_types.size();

    /*----- Add memory. ----------------------------------------------------------------------------------------------*/
    BinaryenSetMemory(
        /* module=         */ module.ref(),
        /* initial=        */ 1,
        /* maximum=        */ WasmPlatform::WASM_MAX_MEMORY / WasmPlatform::WASM_PAGE_SIZE, // allowed maximum
        /* exportName=     */ "memory",
        /* segments=       */ nullptr,
        /* segmentPassive= */ nullptr,
        /* segmentOffsets= */ nullptr,
        /* segmentSizes=   */ nullptr,
        /* numSegments=    */ 0,
        /* shared=         */ 0
    );

    /*----- Import callback to print i32. ----------------------------------------------------------------------------*/
    BinaryenAddFunctionImport(
        /* module=             */ module.ref(),
        /* internalName=       */ "print_i32",
        /* externalModuleName= */ "env",
        /* externalBaseName=   */ "print_i32",
        /* params=             */ BinaryenTypeInt32(),
        /* results=            */ BinaryenTypeNone()
    );

    /*----- Import output location.  ---------------------------------------------------------------------------------*/
    BinaryenAddGlobalImport(
        /* module=             */ module.ref(),
        /* internalName=       */ "out",
        /* externalModuleName= */ "env",
        /* externalBaseName=   */ "out",
        /* type=               */ BinaryenTypeInt32(),
        /* mutable=            */ false
    );

    /*----- Copy output location from global to local, s.t. it is mutable. -------------------------------------------*/
    codegen.b_out_ = codegen.add_local(BinaryenTypeInt32());
    auto b_out_global = BinaryenGlobalGet(
        /* module= */ module.ref(),
        /* name=   */ "out",
        /* type=   */ BinaryenTypeInt32()
    );
    codegen.block_ += BinaryenLocalSet(
        /* module= */ module.ref(),
        /* index=  */ BinaryenLocalGetGetIndex(codegen.b_out_),
        /* value=  */ b_out_global
    );

    /*----- Compile plan. --------------------------------------------------------------------------------------------*/
    codegen(op); // emit code

    /*----- Compute number of output records written. ----------------------------------------------------------------*/
    auto num_attrs = op.schema().num_entries();
    auto b_diff = BinaryenBinary(
        /* module= */ module.ref(),
        /* op=     */ BinaryenSubInt32(),
        /* left=   */ codegen.out(),
        /* right=  */ b_out_global
    );
    codegen.block_ += BinaryenBinary(
        /* module= */ module.ref(),
        /* op=     */ BinaryenDivSInt32(),
        /* left=   */ b_diff,
        /* right=  */ BinaryenConst(module.ref(), BinaryenLiteralInt32(8 * num_attrs))
    );

    /*----- Add function. --------------------------------------------------------------------------------------------*/
    BinaryenAddFunction(
        /* module=      */ module.ref(),
        /* name=        */ "run",
        /* params=      */ BinaryenTypeCreate(&param_types[0], param_types.size()),
        /* results=     */ BinaryenTypeInt32(),
        /* varTypes=    */ &codegen.locals_[0],
        /* numVarTypes= */ codegen.locals_.size(),
        /* body=        */ codegen.block_.finalize()
    );
    BinaryenAddFunctionExport(module.ref(), "run", "run");

    /*----- Validate and optimize module. ----------------------------------------------------------------------------*/
    if (not BinaryenModuleValidate(module.ref())) {
        module.dump();
        throw std::logic_error("invalid module");
    }
    BinaryenSetOptimizeLevel(2); // O2
    BinaryenSetShrinkLevel(0); // shrinking not required
    //BinaryenModuleOptimize(module.ref());

    return module;
}

void WasmCodeGen::operator()(const ScanOperator &op)
{
    block_ += WasmPipelineCG::compile(op, *this);
}

void WasmCodeGen::operator()(const CallbackOperator &op) { (*this)(*op.child(0)); }

void WasmCodeGen::operator()(const PrintOperator &op) { (*this)(*op.child(0)); }

void WasmCodeGen::operator()(const NoOpOperator &op) { (*this)(*op.child(0)); }

void WasmCodeGen::operator()(const FilterOperator &op)
{
    (*this)(*op.child(0));
}

void WasmCodeGen::operator()(const JoinOperator &op)
{
    // TODO implement
    for (auto c : op.children())
        (*this)(*c);
}

void WasmCodeGen::operator()(const ProjectionOperator &op)
{
    if (op.children().size()) {
        (*this)(*op.child(0));
    } else {
        // TODO implement
    }
}

void WasmCodeGen::operator()(const LimitOperator &op)
{
    (*this)(*op.child(0));
}

void WasmCodeGen::operator()(const GroupingOperator &op)
{
    (*this)(*op.child(0));
}

void WasmCodeGen::operator()(const SortingOperator &op)
{
    (*this)(*op.child(0));
}


/*======================================================================================================================
 * WasmPipelineCG
 *====================================================================================================================*/

BinaryenExpressionRef WasmPipelineCG::compile(const Expr &expr)
{
    (*this)(expr);
    return expr_;
}

BinaryenExpressionRef WasmPipelineCG::compile(const cnf::CNF &cnf)
{
    BinaryenExpressionRef b_cnf = nullptr;
    for (auto &clause : cnf) {
        BinaryenExpressionRef b_clause = nullptr;
        for (auto &pred : clause) {
            /* Generate code for the literal of the predicate. */
            auto b_pred = compile(*pred.expr());
            /* If the predicate is negative, negate the outcome by computing `1 - pred`. */
            if (pred.negative()) {
                b_pred = BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenSubInt32(),
                    /* left=   */ BinaryenConst(module(), BinaryenLiteralInt32(1)),
                    /* right=  */ b_pred
                );
            }
            /* Add the predicate to the clause with an `or`. */
            if (b_clause) {
                b_clause = BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenOrInt32(),
                    /* left=   */ b_clause,
                    /* right=  */ b_pred
                );
            } else {
                b_clause = b_pred;
            }
        }
        /* Add the clause to the CNF with an `and`. */
        if (b_cnf) {
            b_cnf = BinaryenBinary(
                /* module= */ module(),
                /* op=     */ BinaryenAndInt32(),
                /* left=   */ b_cnf,
                /* right=  */ b_clause
            );
        } else {
            b_cnf = b_clause;
        }
    }
    insist(b_cnf, "empty CNF?");

    return b_cnf;
}


/*======================================================================================================================
 * WasmPipelineCG / Operator
 *====================================================================================================================*/

void WasmPipelineCG::operator()(const ScanOperator &op)
{
    auto &table = op.store().table();
    std::ostringstream oss;

    oss.str("");
    oss << "loop_" << table.name;
    auto loop_name = oss.str();
    BlockBuilder loop_body(module(), "loop.body");

    /*----- Create induction variable. -------------------------------------------------------------------------------*/
    b_induction_var = CG.add_local(BinaryenTypeInt32());

    /*----- Import table size. ---------------------------------------------------------------------------------------*/
    oss.str("");
    oss << table.name << "_num_rows";
    auto table_size = oss.str();
    BinaryenAddGlobalImport(
        /* module=             */ module(),
        /* internalName=       */ table_size.c_str(),
        /* externalModuleName= */ "env",
        /* externalBaseName=   */ table_size.c_str(),
        /* type=               */ BinaryenTypeInt32(),
        /* mutable=            */ false
    );
    auto b_table_size = BinaryenGlobalGet(
        /* module= */ module(),
        /* name=   */ table_size.c_str(),
        /* type=   */ BinaryenTypeInt32()
    );

    swap(block_, loop_body);
    /*----- Generate code to access attributes. ----------------------------------------------------------------------*/
    WasmStoreCG store(*this, op.schema());
    store(op.store());

    /*----- Generate code for rest of the pipeline. ------------------------------------------------------------------*/
    (*this)(*op.parent());

    /*----- Increment induction variable. ----------------------------------------------------------------------------*/
    auto b_inc_induction_var = BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ b_induction_var,
        /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(1))
    );
    block_ += BinaryenLocalSet(
        /* module= */ module(),
        /* index=  */ BinaryenLocalGetGetIndex(b_induction_var),
        /* value=  */ b_inc_induction_var
    );

    /*----- Create loop header. --------------------------------------------------------------------------------------*/
    auto b_loop_cond = BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenLtUInt32(),
        /* left=   */ b_induction_var,
        /* right=  */ b_table_size
    );
    block_ += BinaryenBreak(
        /* module=    */ module(),
        /* name=      */ loop_name.c_str(),
        /* condition= */ b_loop_cond,
        /* value=     */ nullptr
    );
    swap(block_, loop_body);

    /*----- Create loop. ---------------------------------------------------------------------------------------------*/
    block_ += BinaryenLoop(
        /* module= */ module(),
        /* in=     */ loop_name.c_str(),
        /* body=   */ loop_body.finalize()
    );
}

void WasmPipelineCG::operator()(const CallbackOperator &op)
{
    std::size_t offset = 0;

    /*----- Write results. -------------------------------------------------------------------------------------------*/
    for (auto &e : op.schema()) {
        auto value = intermediates_.at(e.id);
        auto bytes = e.type->size() == 64 ? 8 : 4;
        block_ += BinaryenStore(
            /* module= */ module(),
            /* bytes=  */ bytes,
            /* offset= */ offset,
            /* align=  */ 0,
            /* ptr=    */ CG.out(),
            /* value=  */ value,
            /* type=   */ get_binaryen_type(e.type)
        );
        offset += 8;
    }

    auto inc = BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ CG.out(),
        /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(offset))
    );
    block_ += BinaryenLocalSet(
        /* module = */ module(),
        /* index=   */ BinaryenLocalGetGetIndex(CG.out()),
        /* value=   */ inc
    );
}

void WasmPipelineCG::operator()(const PrintOperator &op)
{
    // TODO
    unreachable("not implemented");
}

void WasmPipelineCG::operator()(const NoOpOperator&) { /* nothing to be done */ }

void WasmPipelineCG::operator()(const FilterOperator &op)
{
    BlockBuilder then_block(module(), "if.then");
    swap(this->block_, then_block);
    (*this)(*op.parent());
    swap(this->block_, then_block);

    block_ += BinaryenIf(
        /* module=    */ module(),
        /* condition= */ compile(op.filter()),
        /* ifTrue=    */ then_block.finalize(),
        /* ifFalse=   */ nullptr
    );
}

void WasmPipelineCG::operator()(const JoinOperator &op)
{
    (*this)(*op.parent());
}

void WasmPipelineCG::operator()(const ProjectionOperator &op)
{
    auto p = op.projections().begin();
    for (auto &e : op.schema())
        intermediates_[e.id] = compile(*p++->first); // FIXME: Does not work with duplicate Identifier
    (*this)(*op.parent());
}

void WasmPipelineCG::operator()(const LimitOperator &op)
{
    (*this)(*op.parent());
}

void WasmPipelineCG::operator()(const GroupingOperator &op)
{
    (*this)(*op.parent());
}

void WasmPipelineCG::operator()(const SortingOperator &op)
{
    (*this)(*op.parent());
}


/*======================================================================================================================
 * WasmPipelineCG / Expr
 *====================================================================================================================*/

void WasmPipelineCG::operator()(const ErrorExpr&) { unreachable("no errors at this stage"); }

void WasmPipelineCG::operator()(const Designator &e)
{
    auto t = e.target();
    if (auto pe = std::get_if<const Expr*>(&t)) {
        (*this)(**pe);
    } else if (auto pa = std::get_if<const Attribute*>(&t)) {
        auto &attr = **pa;
        Schema::Identifier id(attr.table.name, attr.name);
        auto it = intermediates_.find(id);
        insist(it != intermediates_.end(), "no intermediate result for the given designator");
        expr_ = it->second;
    } else {
        unreachable("designator must have a valid target");
    }
}

void WasmPipelineCG::operator()(const Constant &e)
{
    auto value = Interpreter::eval(e);
    struct BinaryenLiteral literal;

    auto ty = e.type();
    if (ty->is_boolean()) {
        literal = BinaryenLiteralInt32(value.as_b());
    } else if (ty->is_character_sequence()) {
        unreachable("not yet implemented");
    } else if (auto n = cast<const Numeric>(ty)) {
        switch (n->kind) {
            case Numeric::N_Int:
            case Numeric::N_Decimal: {
                if (n->size() <= 32)
                    literal = BinaryenLiteralInt32(value.as_i());
                else
                    literal = BinaryenLiteralInt64(value.as_i());
                break;
            }

            case Numeric::N_Float: {
                if (n->precision == 32)
                    literal = BinaryenLiteralFloat32(value.as_f());
                else
                    literal = BinaryenLiteralFloat64(value.as_d());
                break;
            }
        }
    } else
        unreachable("invalid type");

    expr_ = BinaryenConst(module(), literal);
}

void WasmPipelineCG::operator()(const FnApplicationExpr &e)
{
    (void) e;
    unreachable("not implemented");
}

void WasmPipelineCG::operator()(const UnaryExpr &e)
{
    (*this)(*e.expr);

    switch (e.op().type) {
        default:
            unreachable("illegal token type");

        case TK_PLUS:
            /* nothing to be done */
            break;

        case TK_MINUS: {
            auto n = as<const Numeric>(e.type());
            switch (n->kind) {
                case Numeric::N_Int:
                case Numeric::N_Decimal: {
                    /* In WebAssembly, negation of integral values is achieved by subtraction from zero (0 - x). */
                    if (n->size() <= 32) {
                        auto Zero = BinaryenConst(module(), BinaryenLiteralInt32(0));
                        expr_ = BinaryenBinary(/* module= */ module(),
                                               /* op=     */ BinaryenSubInt32(),
                                               /* left=   */ Zero,
                                               /* right=  */ expr_);
                    } else {
                        auto Zero = BinaryenConst(module(), BinaryenLiteralInt64(0));
                        expr_ = BinaryenBinary(/* module= */ module(),
                                               /* op=     */ BinaryenSubInt64(),
                                               /* left=   */ Zero,
                                               /* right=  */ expr_);
                    }
                    break;
                }

                case Numeric::N_Float:
                    if (n->precision == 32)
                        expr_ = BinaryenUnary(module(), BinaryenNegFloat32(), expr_);
                    else
                        expr_ = BinaryenUnary(module(), BinaryenNegFloat64(), expr_);
                    break;
            }
            break;
        }

        case TK_TILDE: {
            /* In WebAssembly, bitwise not of a value is achieved by exclusive or with the all 1 bits value. */
            auto n = as<const Numeric>(e.type());
            switch (n->kind) {
                case Numeric::N_Int:
                case Numeric::N_Decimal: {
                    if (n->size() <= 32) {
                        auto AllOnes = BinaryenConst(module(), BinaryenLiteralInt32(-1));
                        expr_ = BinaryenBinary(/* module= */ module(),
                                               /* op=     */ BinaryenXorInt32(),
                                               /* left=   */ AllOnes,
                                               /* right=  */ expr_);
                    } else {
                        auto AllOnes = BinaryenConst(module(), BinaryenLiteralInt64(-1L));
                        expr_ = BinaryenBinary(/* module= */ module(),
                                               /* op=     */ BinaryenXorInt64(),
                                               /* left=   */ AllOnes,
                                               /* right=  */ expr_);
                    }
                    break;
                }

                case Numeric::N_Float:
                    unreachable("bitwise not is not supported for floating-point types");
            }
            break;
        }

        case TK_Not: {
            /* In WebAssembly, booleans are represented as 32 bit integers.  Hence, logical not is achieved by computing
             * 1 - value. */
            insist(e.type()->is_boolean());
            auto One = BinaryenConst(module(), BinaryenLiteralInt32(1));
            expr_ = BinaryenBinary(/* module= */ module(),
                                   /* op=     */ BinaryenSubInt32(),
                                   /* left=   */ One,
                                   /* right=  */ expr_);
            break;
        }
    }
}

void WasmPipelineCG::operator()(const BinaryExpr &e)
{
    expr_ = nullptr;
    (*this)(*e.lhs);
    auto lhs = expr_;
    insist(lhs);
    expr_ = nullptr;
    (*this)(*e.rhs);
    auto rhs = expr_;
    insist(rhs);

#define BINARY_OP(OP, TYPE) \
    expr_ = BinaryenBinary(/* module= */ module(), \
                           /* op=     */ Binaryen ## OP ## TYPE(), \
                           /* left=   */ lhs, \
                           /* right=  */ rhs) \

#define BINARY(OP) \
{ \
    auto n = as<const Numeric>(e.type()); \
    lhs = convert(module(), lhs, as<const Numeric>(e.lhs->type()), n); \
    rhs = convert(module(), rhs, as<const Numeric>(e.rhs->type()), n); \
    switch (n->kind) { \
        case Numeric::N_Int: \
        case Numeric::N_Decimal: { \
            if (n->size() <= 32) \
                BINARY_OP(OP, Int32); \
            else \
                BINARY_OP(OP, Int64); \
            break; \
        } \
\
        case Numeric::N_Float: \
            if (n->precision == 32) \
                BINARY_OP(OP, Float32); \
            else \
                BINARY_OP(OP, Float64); \
            break; \
    } \
    break; \
}

#define CMP(OP) \
{ \
    auto n_lhs = as<const Numeric>(e.lhs->type()); \
    auto n_rhs = as<const Numeric>(e.rhs->type()); \
    auto n = arithmetic_join(n_lhs, n_rhs); \
    lhs = convert(module(), lhs, n_lhs, n); \
    rhs = convert(module(), rhs, n_rhs, n); \
    switch (n->kind) { \
        case Numeric::N_Int: \
        case Numeric::N_Decimal: { \
            if (n->size() <= 32) \
                BINARY_OP(OP##S, Int32); \
            else \
                BINARY_OP(OP##S, Int64); \
            break; \
        } \
\
        case Numeric::N_Float: \
            if (n->precision == 32) \
                BINARY_OP(OP, Float32); \
            else \
                BINARY_OP(OP, Float64); \
            break; \
    } \
    break; \
}

    switch (e.op().type) {
        default:
            unreachable("illegal token type");

        case TK_PLUS:       BINARY(Add); break;
        case TK_MINUS:      BINARY(Sub); break;
        case TK_ASTERISK:   BINARY(Mul); break;
        case TK_SLASH: {
            auto n = as<const Numeric>(e.type());
            switch (n->kind) {
                case Numeric::N_Int:
                case Numeric::N_Decimal: {
                    if (n->size() <= 32)
                        BINARY_OP(DivS, Int32);
                    else
                        BINARY_OP(DivS, Int64);
                    break;
                }

                case Numeric::N_Float:
                    if (n->precision == 32)
                        BINARY_OP(Div, Float32);
                    else
                        BINARY_OP(Div, Float64);
                    break;
            }
            break;
        }

        case TK_PERCENT: {
            auto n = as<const Numeric>(e.type());
            switch (n->kind) {
                case Numeric::N_Float:
                    unreachable("module with float not defined");

                case Numeric::N_Int:
                case Numeric::N_Decimal: {
                    auto bits = n->size();
                    if (bits <= 32)
                        BINARY_OP(RemS, Int32);
                    else
                        BINARY_OP(RemS, Int64);
                    break;
                }
            }
            break;
        }

        case TK_DOTDOT:
            unreachable("not yet supported");

        case TK_LESS:           CMP(Lt); break;
        case TK_GREATER:        CMP(Gt); break;
        case TK_LESS_EQUAL:     CMP(Le); break;
        case TK_GREATER_EQUAL:  CMP(Ge); break;
        case TK_EQUAL:          BINARY(Eq); break;
        case TK_BANG_EQUAL:     BINARY(Ne); break;

        case TK_And:
            insist(e.type()->is_boolean());
            BINARY_OP(And, Int32); // booleans are represented as 32 bit integers
            break;

        case TK_Or:
            insist(e.type()->is_boolean());
            BINARY_OP(Or, Int32); // booleans are represented as 32 bit integers
            break;
    }
#undef BINARY
#undef BINARY_OP
#undef CMP
}


/*======================================================================================================================
 * WasmStoreCG
 *====================================================================================================================*/

void WasmStoreCG::operator()(const RowStore &store)
{
    std::ostringstream oss;
    auto &table = store.table();

    BinaryenAddGlobalImport(
        /* module=             */ pipeline.module(),
        /* internalName=       */ table.name,
        /* externalModuleName= */ "env",
        /* externalBaseName=   */ table.name,
        /* globalType=         */ BinaryenTypeInt32(),
        /* mutable=            */ false
    );

    /*----- Generate code to compute row address. --------------------------------------------------------------------*/
    auto b_table_addr = BinaryenGlobalGet(
        /* module= */ pipeline.module(),
        /* name=   */ table.name,
        /* type=   */ BinaryenTypeInt32()
    );
    auto b_row_offset = BinaryenBinary(
        /* module= */ pipeline.module(),
        /* op=     */ BinaryenMulInt32(),
        /* left=   */ pipeline.b_induction_var,
        /* right=  */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(store.row_size() / 8))
    );
    auto b_row_addr = BinaryenBinary(
        /* module= */ pipeline.module(),
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ b_table_addr,
        /* right=  */ b_row_offset
    );

    /*----- Generate code to access null bitmap and value of all required attributes. --------------------------------*/
    const auto null_bitmap_offset = store.offset(table.size());
    for (auto &e : schema) {
        auto &attr = table[e.id.name];

        /*----- Generate code for null bit check. --------------------------------------------------------------------*/
        {
            const auto null_bit_offset = null_bitmap_offset + attr.id;
            const auto qwords = null_bit_offset / 32;
            const auto bits = null_bit_offset % 32;

            auto b_null_bitmap_addr = BinaryenBinary(
                /* module= */ pipeline.module(),
                /* op=     */ BinaryenAddInt32(),
                /* left=   */ b_row_addr,
                /* right=  */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(qwords))
            );
            auto b_qword = BinaryenLoad(
                /* module= */ pipeline.module(),
                /* bytes=  */ 4,
                /* signed= */ false,
                /* offset= */ 0,
                /* align=  */ 0,
                /* type=   */ BinaryenTypeInt32(),
                /* ptr=    */ b_null_bitmap_addr
            );
            auto b_shr = BinaryenBinary(
                /* module= */ pipeline.module(),
                /* op=     */ BinaryenShrUInt32(),
                /* left=   */ b_qword,
                /* right=  */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(bits))
            );
            auto b_isnull = BinaryenBinary(
                /* module= */ pipeline.module(),
                /* op=     */ BinaryenAndInt32(),
                /* left=   */ b_shr,
                /* right=  */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(1))
            );
            pipeline.is_null_.emplace(e.id, b_isnull);
        }

        /*----- Generate code for value access. ----------------------------------------------------------------------*/
        {
            auto b_value = BinaryenLoad(
                /* module= */ pipeline.module(),
                /* bytes=  */ attr.type->size() / 8,
                /* signed= */ true,
                /* offset= */ store.offset(attr.id) / 8,
                /* align=  */ 0,
                /* type=   */ get_binaryen_type(attr.type),
                /* ptr=    */ b_row_addr
            );
            pipeline.intermediates_.emplace(e.id, b_value);
        }
    }
}

void WasmStoreCG::operator()(const ColumnStore&)
{
    unreachable("not implemented"); // TODO implement
}


/*======================================================================================================================
 * WasmModule
 *====================================================================================================================*/

WasmModule::WasmModule() : ref_(BinaryenModuleCreate()) { }

WasmModule::~WasmModule() { BinaryenModuleDispose(ref_); }

std::pair<uint8_t*, std::size_t> WasmModule::binary() const
{
    auto result = BinaryenModuleAllocateAndWrite(ref_, nullptr);
    return std::make_pair(reinterpret_cast<uint8_t*>(result.binary), result.binaryBytes);
}

std::ostream & db::operator<<(std::ostream &out, const WasmModule &module)
{
    auto result = BinaryenModuleAllocateAndWriteText(module.ref_);
    out << result;
    free(result);
    return out;
}

void WasmModule::dump(std::ostream &out) const {
    out << *this;
    auto [buffer, length] = binary();
    out << '[' << std::hex;
    for (auto ptr = buffer, end = buffer + length; ptr != end; ++ptr) {
        if (ptr != buffer) out << ", ";
        out << "0x" << uint32_t(*ptr);
    }
    out << std::dec;
    out << ']' << std::endl;
    free(buffer);
}
void WasmModule::dump() const { dump(std::cerr); }


/*======================================================================================================================
 * WasmPlatform
 *====================================================================================================================*/

uint32_t WasmPlatform::wasm_counter_ = 0;
std::unordered_map<uint32_t, WasmPlatform::WasmContext> WasmPlatform::contexts_;

WasmModule WasmPlatform::compile(const Operator &op)
{
    return WasmCodeGen::compile(op);
}


/*======================================================================================================================
 * WasmBackend
 *====================================================================================================================*/

void WasmBackend::execute(const Operator &plan) const {
    auto module = WasmCodeGen::compile(plan);
    platform_->execute(module);
}

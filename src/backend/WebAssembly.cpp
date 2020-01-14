#include "backend/WebAssembly.hpp"

#include "backend/Interpreter.hpp"
#include "backend/V8Backend.hpp"
#include "binaryen-c.h"
#include <exception>


using namespace db;


/*======================================================================================================================
 * WASMModule
 *====================================================================================================================*/

WASMModule::WASMModule() : ref_(BinaryenModuleCreate()) { }

WASMModule::~WASMModule() { BinaryenModuleDispose(ref_); }

std::pair<uint8_t*, std::size_t> WASMModule::binary() const
{
    auto result = BinaryenModuleAllocateAndWrite(ref_, nullptr);
    return std::make_pair(reinterpret_cast<uint8_t*>(result.binary), result.binaryBytes);
}

std::ostream & db::operator<<(std::ostream &out, const WASMModule &module)
{
    auto result = BinaryenModuleAllocateAndWriteText(module.ref_);
    out << result;
    free(result);
    return out;
}

void WASMModule::dump(std::ostream &out) const {
    out << *this;
    auto [buffer, length] = binary();
    out << '[' << std::hex;
    for (auto ptr = buffer, end = buffer + length; ptr != end; ++ptr) {
        if (ptr != buffer) out << ", ";
        out << "0x" << uint32_t(*ptr);
    }
    out << std::dec;
    out << ']' << std::endl;
}
void WASMModule::dump() const { dump(std::cerr); }


/*======================================================================================================================
 * WASMCodeGen
 *====================================================================================================================*/

WASMModule WASMCodeGen::compile(const Operator &op)
{
    WASMModule module; // fresh module

    /*----- Add memory. ----------------------------------------------------------------------------------------------*/
    BinaryenSetMemory(/* module=         */ module.ref_,
                      /* initial=        */ 1,
                      /* maximum=        */ 1,
                      /* exportName=     */ nullptr,
                      /* segments=       */ nullptr,
                      /* segmentPassive= */ nullptr,
                      /* segmentOffsets= */ nullptr,
                      /* segmentSizes=   */ nullptr,
                      /* numSegments=    */ 0,
                      /* shared=         */ 0);
    BinaryenAddMemoryImport(/* module=             */ module.ref_,
                            /* internalName=       */ "mem",
                            /* externalModuleName= */ "env",
                            /* externalBaseName=   */ "mem",
                            /* shared=             */ 0);


    WASMCodeGen codegen(module);
    codegen(op); // emit code

    /*----- Patch invokable function into module and export it. ------------------------------------------------------*/
    std::vector<BinaryenType> param_types;
    //param_types.push_back(BinaryenTypeInt32());

    auto val = BinaryenLoad(/* module= */ module.ref_,
                            /* bytes=  */ 4,
                            /* signed= */ 1,
                            /* offset= */ 0,
                            /* align=  */ 0,
                            /* type=   */ BinaryenTypeInt32(),
                            /* ptr=    */ BinaryenConst(module.ref_, BinaryenLiteralInt32(20)));

    std::vector<BinaryenExpressionRef> block;
    block.emplace_back(val);

    auto fn_ty = BinaryenAddFunctionType(
            /* module=     */ module.ref_,
            /* name=       */ nullptr,
            /* result=     */ BinaryenTypeInt32(),
            /* paramTypes= */ &param_types[0],
            /* numParams=  */ param_types.size());
    auto fn_body = BinaryenBlock(
            /* module=      */ module.ref_,
            /* name=        */ nullptr,
            /* children=    */ &block[0],
            /* numChildren= */ block.size(),
            /* type=        */ BinaryenTypeAuto());
    auto fn = BinaryenAddFunction(
            /* module=      */ module.ref_,
            /* name=        */ "run",
            /* type=        */ fn_ty,
            /* varTypes=    */ nullptr,
            /* numVarTypes= */ 0,
            /* body=        */ fn_body);

    BinaryenAddFunctionExport(module.ref_, "run", "run");


    /*----- Validate and optimize module. ----------------------------------------------------------------------------*/
    if (not BinaryenModuleValidate(module.ref_))
        throw std::logic_error("invalid module");
    BinaryenSetOptimizeLevel(2); // O2
    BinaryenSetShrinkLevel(0); // shrinking not required
    //BinaryenModuleOptimize(module.ref_);

    return module;
}


/*======================================================================================================================
 * WASMCodeGen / Operators
 *====================================================================================================================*/

void WASMCodeGen::operator()(const ScanOperator &op)
{
    // TODO implement
}

void WASMCodeGen::operator()(const CallbackOperator &op)
{
    (*this)(*op.child(0));
}

void WASMCodeGen::operator()(const FilterOperator &op)
{
    // TODO implement
}

void WASMCodeGen::operator()(const JoinOperator &op)
{
    // TODO implement
}

void WASMCodeGen::operator()(const ProjectionOperator &op)
{
    if (op.children().size())
        (*this)(*op.child(0));

    std::vector<BinaryenExpressionRef> exprs;
    for (auto &p : op.projections()) {
        (*this)(*p.first);
        exprs.push_back(expr_);
    }

    expr_ = BinaryenBlock(/* module=      */ module_,
                          /* name=        */ nullptr,
                          /* children=    */ &exprs[0],
                          /* numChildren= */ exprs.size(),
                          /* type=        */ BinaryenTypeAuto());
}

void WASMCodeGen::operator()(const LimitOperator &op)
{
    // TODO implement
}

void WASMCodeGen::operator()(const GroupingOperator &op)
{
    // TODO implement
}

void WASMCodeGen::operator()(const SortingOperator &op)
{
    // TODO implement
}


/*======================================================================================================================
 * WASMCodeGen / Expressions
 *====================================================================================================================*/

void WASMCodeGen::operator()(Const<Designator> &e)
{
    // TODO implement
}

void WASMCodeGen::operator()(Const<Constant> &e)
{
    auto value = Interpreter::eval(e);
    struct BinaryenLiteral literal;

    auto ty = e.type();
    if (ty->is_boolean()) {
        bool b = std::get<bool>(value);
        literal = BinaryenLiteralInt32(b);
    } else if (ty->is_character_sequence()) {
        unreachable("not yet implemented");
    } else if (auto n = cast<const Numeric>(ty)) {
        switch (n->kind) {
            case Numeric::N_Int:
            case Numeric::N_Decimal: {
                int64_t i = std::get<int64_t>(value);
                if (n->size() <= 32)
                    literal = BinaryenLiteralInt32(i);
                else
                    literal = BinaryenLiteralInt64(i);
                break;
            }

            case Numeric::N_Float: {
                if (n->precision == 32) {
                    float f = std::get<float>(value);
                    literal = BinaryenLiteralFloat32(f);
                } else {
                    double d = std::get<double>(value);
                    literal = BinaryenLiteralFloat64(d);
                }
                break;
            }
        }
    } else
        unreachable("invalid type");

    expr_ = BinaryenConst(module_, literal);
}

void WASMCodeGen::operator()(Const<FnApplicationExpr> &e)
{
    (void) e;
    unreachable("not implemented");
}

void WASMCodeGen::operator()(Const<UnaryExpr> &e)
{
    (*this)(*e.expr);

    switch (e.op.type) {
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
                        auto Zero = BinaryenConst(module_, BinaryenLiteralInt32(0));
                        expr_ = BinaryenBinary(/* module= */ module_,
                                               /* op=     */ BinaryenSubInt32(),
                                               /* left=   */ Zero,
                                               /* right=  */ expr_);
                    } else {
                        auto Zero = BinaryenConst(module_, BinaryenLiteralInt64(0));
                        expr_ = BinaryenBinary(/* module= */ module_,
                                               /* op=     */ BinaryenSubInt64(),
                                               /* left=   */ Zero,
                                               /* right=  */ expr_);
                    }
                    break;
                }

                case Numeric::N_Float:
                    if (n->precision == 32)
                        expr_ = BinaryenUnary(module_, BinaryenNegFloat32(), expr_);
                    else
                        expr_ = BinaryenUnary(module_, BinaryenNegFloat64(), expr_);
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
                        auto AllOnes = BinaryenConst(module_, BinaryenLiteralInt32(-1));
                        expr_ = BinaryenBinary(/* module= */ module_,
                                               /* op=     */ BinaryenXorInt32(),
                                               /* left=   */ AllOnes,
                                               /* right=  */ expr_);
                    } else {
                        auto AllOnes = BinaryenConst(module_, BinaryenLiteralInt64(-1L));
                        expr_ = BinaryenBinary(/* module= */ module_,
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
            auto One = BinaryenConst(module_, BinaryenLiteralInt32(1));
            expr_ = BinaryenBinary(/* module= */ module_,
                                   /* op=     */ BinaryenSubInt32(),
                                   /* left=   */ One,
                                   /* right=  */ expr_);
            break;
        }
    }
}

void WASMCodeGen::operator()(Const<BinaryExpr> &e)
{
    (*this)(*e.lhs);
    auto lhs = expr_;
    (*this)(*e.rhs);
    auto rhs = expr_;

#define BINARY_OP(OP, TYPE) \
    expr_ = BinaryenBinary(/* module= */ module_, \
                           /* op=     */ Binaryen ## OP ## TYPE(), \
                           /* left=   */ lhs, \
                           /* right=  */ rhs) \

#define BINARY(OP) \
{ \
    auto n = as<const Numeric>(e.type()); \
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

#define BINARY_INT_FLOAT(OP_INT, OP_FLOAT) \
{ \
    auto n = as<const Numeric>(e.type()); \
    switch (n->kind) { \
        case Numeric::N_Int: \
        case Numeric::N_Decimal: { \
            if (n->size() <= 32) \
                BINARY_OP(OP_INT, Int32); \
            else \
                BINARY_OP(OP_INT, Int64); \
            break; \
        } \
\
        case Numeric::N_Float: \
            if (n->precision == 32) \
                BINARY_OP(OP_FLOAT, Float32); \
            else \
                BINARY_OP(OP_FLOAT, Float64); \
            break; \
    } \
    break; \
}

    switch (e.op.type) {
        default:
            unreachable("illegal token type");

        case TK_PLUS:       BINARY(Add); break;
        case TK_MINUS:      BINARY(Sub); break;
        case TK_ASTERISK:   BINARY(Mul); break;
        case TK_SLASH:      BINARY_INT_FLOAT(DivS, Div); break;

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

        case TK_LESS:           BINARY_INT_FLOAT(LtS, Lt); break;
        case TK_GREATER:        BINARY_INT_FLOAT(LtS, Gt); break;
        case TK_LESS_EQUAL:     BINARY_INT_FLOAT(LeS, Le); break;
        case TK_GREATER_EQUAL:  BINARY_INT_FLOAT(GtS, Gt); break;
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
#undef BINARY_INT_FLOAT
#undef BINARY
#undef BINARY_OP
}


/*======================================================================================================================
 * WASMBackend
 *====================================================================================================================*/

void WasmV8Backend::execute(const Operator &plan) const
{
    auto module = WASMCodeGen::compile(plan);
    V8Backend V8;
    V8.execute(module);
}

void WasmSpiderMonkeyBackend::execute(const Operator &plan) const
{
    auto module = WASMCodeGen::compile(plan);
    // TODO execute
}

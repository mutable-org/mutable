#include "backend/WasmUtil.hpp"

#include "backend/Interpreter.hpp"
#include "backend/WasmAlgo.hpp"
#include "storage/ColumnStore.hpp"
#include "storage/RowStore.hpp"
#include <limits>


using namespace m;


/*======================================================================================================================
 * WasmTemporary
 *====================================================================================================================*/

M_LCOV_EXCL_START
void WasmTemporary::dump() const { if (ref_) BinaryenExpressionPrint(ref_); }
M_LCOV_EXCL_STOP


/*======================================================================================================================
 * WasmExprCompiler
 *====================================================================================================================*/

void WasmExprCompiler::operator()(const Constant &e)
{
    auto value = Interpreter::eval(e);

    if (e.is_string()) {
        const char *s = value.as<const char*>();
        const auto offset = module().get_literal_offset(s);
        set(BinaryenBinary(
            /* module= */ module(),
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ module().literals(),
            /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(offset))
        ));
        return;
    }

    struct BinaryenLiteral literal;

    auto ty = e.type();
    if (ty->is_boolean()) {
        literal = BinaryenLiteralInt32(value.as_b());
    } else if (ty->is_character_sequence()) {
        M_unreachable("this is a string and should've been handled earlier");
    } else if (ty->is_date()) {
        literal = BinaryenLiteralInt32(value.as_i());
    } else if (ty->is_date_time()) {
        literal = BinaryenLiteralInt64(value.as_i());
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
        M_unreachable("invalid type");

    set(BinaryenConst(module(), literal));
}

void WasmExprCompiler::operator()(const UnaryExpr &e)
{
    (*this)(*e.expr);

    switch (e.op().type) {
        default:
            M_unreachable("illegal token type");

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
                        set(BinaryenBinary(/* module= */ module(),
                                           /* op=     */ BinaryenSubInt32(),
                                           /* left=   */ Zero,
                                           /* right=  */ get()));
                    } else {
                        auto Zero = BinaryenConst(module(), BinaryenLiteralInt64(0));
                        set(BinaryenBinary(/* module= */ module(),
                                           /* op=     */ BinaryenSubInt64(),
                                           /* left=   */ Zero,
                                           /* right=  */ get()));
                    }
                    break;
                }

                case Numeric::N_Float:
                    if (n->precision == 32)
                        set(BinaryenUnary(module(), BinaryenNegFloat32(), get()));
                    else
                        set(BinaryenUnary(module(), BinaryenNegFloat64(), get()));
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
                        set(BinaryenBinary(/* module= */ module(),
                                           /* op=     */ BinaryenXorInt32(),
                                           /* left=   */ AllOnes,
                                           /* right=  */ get()));
                    } else {
                        auto AllOnes = BinaryenConst(module(), BinaryenLiteralInt64(-1L));
                        set(BinaryenBinary(/* module= */ module(),
                                           /* op=     */ BinaryenXorInt64(),
                                           /* left=   */ AllOnes,
                                           /* right=  */ get()));
                    }
                    break;
                }

                case Numeric::N_Float:
                    M_unreachable("bitwise not is not supported for floating-point types");
            }
            break;
        }

        case TK_Not: {
            /* In WebAssembly, booleans are represented as 32 bit integers.  Hence, logical not is achieved by computing
             * 1 - value. */
            M_insist(e.type()->is_boolean());
            auto One = BinaryenConst(module(), BinaryenLiteralInt32(1));
            set(BinaryenBinary(/* module= */ module(),
                               /* op=     */ BinaryenSubInt32(),
                               /* left=   */ One,
                               /* right=  */ get()));
            break;
        }
    }
}

void WasmExprCompiler::operator()(const BinaryExpr &e)
{
    (*this)(*e.lhs);
    WasmTemporary lhs = get();
    M_insist(lhs.is());
    (*this)(*e.rhs);
    WasmTemporary rhs = get();
    M_insist(rhs.is());

    const PrimitiveType *ty_lhs = as<const PrimitiveType>(e.lhs->type());
    const PrimitiveType *ty_rhs = as<const PrimitiveType>(e.rhs->type());

#define BINARY_OP(OP, TYPE) \
    set(BinaryenBinary(/* module= */ module(), \
                       /* op=     */ Binaryen ## OP ## TYPE(), \
                       /* left=   */ lhs, \
                       /* right=  */ rhs))

#define BINARY(OP) \
{ \
    auto n = as<const Numeric>(e.type()); \
    lhs = convert(module(), std::move(lhs), as<const Numeric>(ty_lhs), n); \
    rhs = convert(module(), std::move(rhs), as<const Numeric>(ty_rhs), n); \
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
}

#define CMP(OP) \
{ \
    if (ty_lhs->is_character_sequence()) { \
        M_insist(ty_rhs->is_character_sequence()); \
        auto cs_lhs = as<const CharacterSequence>(ty_lhs); \
        auto cs_rhs = as<const CharacterSequence>(ty_rhs); \
        set(WasmStrcmp::OP(fn(), *block_, *cs_lhs, *cs_rhs, std::move(lhs), std::move(rhs))); \
    } else if (ty_lhs->is_numeric()) { \
        /* Convert numeric operands, if necessary. */ \
        M_insist(ty_rhs->is_numeric()); \
        auto n_lhs = as<const Numeric>(ty_lhs); \
        auto n_rhs = as<const Numeric>(ty_rhs); \
        auto n = arithmetic_join(n_lhs, n_rhs); \
        lhs = convert(module(), std::move(lhs), n_lhs, n); \
        rhs = convert(module(), std::move(rhs), n_rhs, n); \
        set(WasmCompare::OP(fn(), *n, std::move(lhs), std::move(rhs))); \
    } else { \
        M_insist(ty_lhs->as_vectorial() == ty_rhs->as_vectorial()); \
        set(WasmCompare::OP(fn(), *ty_lhs, std::move(lhs), std::move(rhs))); \
    } \
}

    switch (e.op().type) {
        default:
            M_unreachable("illegal token type");

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
                    M_unreachable("module with float not defined");

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

        case TK_Like: {
            auto cs_lhs = as<const CharacterSequence>(ty_lhs);
            auto cs_rhs = as<const CharacterSequence>(ty_rhs);
            set(WasmLike::Like(fn(), block(), *cs_lhs, *cs_rhs, std::move(lhs), std::move(rhs)));
            break;
        }

        case TK_DOTDOT:
            M_unreachable("not yet supported");

        case TK_LESS:           CMP(Lt); break;
        case TK_GREATER:        CMP(Gt); break;
        case TK_LESS_EQUAL:     CMP(Le); break;
        case TK_GREATER_EQUAL:  CMP(Ge); break;
        case TK_EQUAL:          CMP(Eq); break;
        case TK_BANG_EQUAL:     CMP(Ne); break;

        case TK_And:
            M_insist(e.type()->is_boolean());
            BINARY_OP(And, Int32); // booleans are represented as 32 bit integers
            break;

        case TK_Or:
            M_insist(e.type()->is_boolean());
            BINARY_OP(Or, Int32); // booleans are represented as 32 bit integers
            break;
    }

#undef CMP
#undef BINARY
#undef BINARY_OP
}


/*======================================================================================================================
 * WasmEnvironment
 *====================================================================================================================*/

WasmTemporary WasmEnvironment::compile(BlockBuilder &block, const cnf::CNF &cnf) const
{
    WasmTemporary wasm_cnf;
    for (auto &clause : cnf) {
        WasmTemporary wasm_clause;
        for (auto &pred : clause) {
            /* Generate code for the literal of the predicate. */
            WasmTemporary wasm_pred = compile(block, *pred.expr());
            /* If the predicate is negative, negate the outcome by computing `1 - pred`. */
            if (pred.negative()) {
                wasm_pred = BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenSubInt32(),
                    /* left=   */ BinaryenConst(module(), BinaryenLiteralInt32(1)),
                    /* right=  */ wasm_pred
                );
            }
            /* Add the predicate to the clause with an `or`. */
            if (wasm_clause.is()) {
                wasm_clause = BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenOrInt32(),
                    /* left=   */ wasm_clause,
                    /* right=  */ wasm_pred
                );
            } else {
                wasm_clause = std::move(wasm_pred);
            }
        }
        /* Add the clause to the CNF with an `and`. */
        if (wasm_cnf.is()) {
            wasm_cnf = BinaryenBinary(
                /* module= */ module(),
                /* op=     */ BinaryenAndInt32(),
                /* left=   */ wasm_cnf,
                /* right=  */ wasm_clause
            );
        } else {
            wasm_cnf = std::move(wasm_clause);
        }
    }
    M_insist(wasm_cnf.is(), "empty CNF?");

    return wasm_cnf;
}

M_LCOV_EXCL_START
void WasmEnvironment::dump(std::ostream &out) const
{
    out << "WasmEnvironment in function " << fn().name() << "\n` entries: { ";
    for (auto it = values_.begin(), end = values_.end(); it != end; ++it) {
        if (it != values_.begin()) out << ", ";
        out << it->first;
    }
    out << " }" << std::endl;
}
void WasmEnvironment::dump() const { dump(std::cerr); }
M_LCOV_EXCL_STOP

void WasmEnvironment::operator()(const Designator &e)
{
    try {
        /* Search with fully qualified name. */
        set(get_value({e.table_name.text, e.attr_name.text}));
    } catch (std::out_of_range) {
        M_unreachable("Designator could not be resolved");
    }
}

void WasmEnvironment::operator()(const FnApplicationExpr &e)
{
    auto &C = Catalog::Get();
    auto &fn = e.get_function();

    /* Load the arguments for the function call. */
    switch (fn.fnid) {
        default:
            M_unreachable("function kind not implemented");

        case Function::FN_UDF:
            M_unreachable("UDFs not yet supported");

        case Function::FN_ISNULL:
            M_unreachable("not yet supported");

        /*----- Type casts -------------------------------------------------------------------------------------------*/
        case Function::FN_INT: {
            auto arg_ty = e.args[0]->type();
            set(convert(module(), get(), arg_ty, arg_ty->size() <= 32 ? Type::Get_Integer(Type::TY_Vector, 4)
                                                                      : Type::Get_Integer(Type::TY_Vector, 8)));
            return;
        }

        /*----- Aggregate functions ----------------------------------------------------------------------------------*/
        case Function::FN_COUNT:
        case Function::FN_MIN:
        case Function::FN_MAX:
        case Function::FN_SUM:
        case Function::FN_AVG: {
            std::ostringstream oss;
            oss << e;
            auto name = C.pool(oss.str().c_str());
            set(get_value(Schema::Identifier(name)));
            return;
        }
    }
}

void WasmEnvironment::operator()(const QueryExpr &e)
{
    /* Search with fully qualified name. */
    Catalog &C = Catalog::Get();
    set(get_value({e.alias(), C.pool("$res")}));
}


/*======================================================================================================================
 * WasmStructCGContext
 *====================================================================================================================*/

void WasmStructCGContext::operator()(const Designator &e)
{
    try {
        /* Search with fully qualified name. */
        auto idx = index({e.table_name.text, e.attr_name.text});
        set(struc.load(fn(), base_ptr_.clone(module()), idx, struc_offset_));
    } catch (std::out_of_range) {
        M_unreachable("Designator could not be resolved");
    }
}

void WasmStructCGContext::operator()(const FnApplicationExpr &e)
{
    auto &C = Catalog::Get();
    auto &fn = e.get_function();

    /* Load the arguments for the function call. */
    switch (fn.fnid) {
        default:
            M_unreachable("function kind not implemented");

        case Function::FN_UDF:
            M_unreachable("UDFs not yet supported");

        case Function::FN_ISNULL:
            M_unreachable("not yet supported");

        /*----- Type casts -------------------------------------------------------------------------------------------*/
        case Function::FN_INT: {
            auto arg_ty = e.args[0]->type();
            set(convert(module(), get(), arg_ty, arg_ty->size() <= 32 ? Type::Get_Integer(Type::TY_Vector, 4)
                                                                      : Type::Get_Integer(Type::TY_Vector, 8)));
            return;
        }

        /*----- Aggregate functions ----------------------------------------------------------------------------------*/
        case Function::FN_COUNT:
        case Function::FN_MIN:
        case Function::FN_MAX:
        case Function::FN_SUM:
        case Function::FN_AVG: {
            std::ostringstream oss;
            oss << e;
            auto name = C.pool(oss.str().c_str());
            set(get(base_ptr_.clone(module()), Schema::Identifier(name)));
            return;
        }
    }
}

void WasmStructCGContext::operator()(const QueryExpr &e)
{
    /* Search with fully qualified name. */
    Catalog &C = Catalog::Get();
    set(get(base_ptr_.clone(module()), {e.alias(), C.pool("$res")}));
}


/*======================================================================================================================
 * WasmStruct
 *====================================================================================================================*/

WasmTemporary WasmStruct::load(FunctionBuilder &fn, WasmTemporary ptr, std::size_t idx, std::size_t struc_offset) const
{
    if (type(idx).is_boolean()) {
        const uint32_t byte_offset = offset(idx) / 8;
        const uint32_t bit = offset(idx) % 8;
        WasmTemporary byte = BinaryenLoad(
            /* module=  */ fn.module(),
            /* bytes=   */ 1,
            /* signed=  */ false,
            /* offset=  */ byte_offset + struc_offset,
            /* align=   */ 0,
            /* type=    */ BinaryenTypeInt32(),
            /* ptr=     */ ptr
        );
        WasmTemporary shifted_byte = BinaryenBinary(
            /* module= */ fn.module(),
            /* op=     */ BinaryenShrUInt32(),
            /* left=   */ byte,
            /* right=  */ BinaryenConst(fn.module(), BinaryenLiteralInt32(bit))
        );
        return BinaryenBinary(
            /* module= */ fn.module(),
            /* op=     */ BinaryenAndInt32(),
            /* left=   */ shifted_byte,
            /* right=  */ BinaryenConst(fn.module(), BinaryenLiteralInt32(1U))
        );
    } else if (type(idx).is_character_sequence()) {
        M_insist(offset(idx) % 8 == 0, "type must be byte-aligned");
        return BinaryenBinary(
            /* module= */ fn.module(),
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ ptr,
            /* right=  */ BinaryenConst(fn.module(), BinaryenLiteralInt32(offset(idx) / 8 + struc_offset))
        );
    } else {
        M_insist(offset(idx) % 8 == 0, "type must be byte-aligned");
        const std::size_t size_in_bytes = type(idx).size() / 8;
        return BinaryenLoad(
            /* module= */ fn.module(),
            /* bytes=  */ size_in_bytes,
            /* signed= */ true,
            /* offset= */ offset(idx) / 8 + struc_offset,
            /* align=  */ struc_offset % size_in_bytes ? 1 : 0,
            /* type=   */ get_binaryen_type(&type(idx)),
            /* ptr=    */ ptr
        );
    }
}

void WasmStruct::store(FunctionBuilder &fn, BlockBuilder &block, WasmTemporary ptr, std::size_t idx, WasmTemporary val,
                       std::size_t struc_offset) const
{
    if (type(idx).is_boolean()) {
        const uint32_t byte_offset = offset(idx) / 8;
        const uint32_t bit = offset(idx) % 8;
        WasmTemporary old_byte = BinaryenLoad(
            /* module=  */ fn.module(),
            /* bytes=   */ 1,
            /* signed=  */ false,
            /* offset=  */ byte_offset + struc_offset,
            /* align=   */ 0,
            /* type=    */ BinaryenTypeInt32(),
            /* ptr=     */ ptr.clone(fn.module())
        );
        WasmTemporary true_mask = BinaryenConst(fn.module(), BinaryenLiteralInt32(1U << bit));
        WasmTemporary false_mask = BinaryenConst(fn.module(), BinaryenLiteralInt32(~(1U << bit)));

        WasmTemporary byte_if_true = BinaryenBinary(
            /* module= */ fn.module(),
            /* op=     */ BinaryenOrInt32(),
            /* left=   */ old_byte.clone(fn.module()),
            /* right=  */ true_mask
        );
        WasmTemporary byte_if_false = BinaryenBinary(
            /* module= */ fn.module(),
            /* op=     */ BinaryenAndInt32(),
            /* left=   */ old_byte,
            /* right=  */ false_mask
        );

        WasmTemporary new_byte = BinaryenSelect(
            /* module=    */ fn.module(),
            /* condition= */ val,
            /* ifTrue=    */ byte_if_true,
            /* ifFalse=   */ byte_if_false,
            /* type=      */ BinaryenTypeInt32()
        );
        block += BinaryenStore(
            /* module= */ fn.module(),
            /* bytes=  */ 1,
            /* offset= */ byte_offset + struc_offset,
            /* align=  */ 0,
            /* ptr=    */ ptr,
            /* value=  */ new_byte,
            /* type=   */ BinaryenTypeInt32()
        );
    } else if (type(idx).is_character_sequence()) {
        M_insist(offset(idx) % 8 == 0, "type must be byte-aligned");
        /* Compute destination address. */
        WasmTemporary dest = BinaryenBinary(
            /* module= */ fn.module(),
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ ptr,
            /* right=  */ BinaryenConst(fn.module(), BinaryenLiteralInt32(offset(idx) / 8 + struc_offset))
        );
        /* Copy string to destination. */
        WasmStrncpy wasm_strncpy(fn);
        wasm_strncpy.emit(block, std::move(dest), std::move(val), as<const CharacterSequence>(type(idx)).length);
    } else {
        M_insist(offset(idx) % 8 == 0, "type must be byte-aligned");
        const std::size_t size_in_bytes = type(idx).size() / 8;
        block += BinaryenStore(
            /* module= */ fn.module(),
            /* bytes=  */ size_in_bytes,
            /* offset= */ offset(idx) / 8 + struc_offset,
            /* align=  */ struc_offset % size_in_bytes ? 1 : 0,
            /* ptr=    */ ptr,
            /* value=  */ val,
            /* type=   */ get_binaryen_type(&type(idx))
        );
    }
}

M_LCOV_EXCL_START
void WasmStruct::dump(std::ostream &out) const
{
    out << "WasmStruct of schema " << schema << " and size " << size_in_bits() << " bits";
    std::size_t idx = 0;
    for (auto &attr : schema) {
        out << "\n  " << idx << ": " << attr.id << " of type " << *attr.type << " at bit offset " << offset(idx);
        ++idx;
    }
    if (idx < num_entries()) {
        out << "\nadditional fields:";
        while (idx < num_entries()) {
            out << "\n  " << idx << ": bit offset " << offset(idx);
            ++idx;
        }
    }
    out << std::endl;
}
void WasmStruct::dump() const { dump(std::cerr); }
M_LCOV_EXCL_STOP


/*======================================================================================================================
 * BlockBuilder
 *====================================================================================================================*/

M_LCOV_EXCL_START
void BlockBuilder::dump(std::ostream &out) const
{
    out << "block \"" << name() << "\"";
    for (auto e : exprs_) {
        out << "\n    ";
        BinaryenExpressionPrint(e);
    }
    out << std::endl;
}
void BlockBuilder::dump() const { dump(std::cerr); }
M_LCOV_EXCL_STOP


/*======================================================================================================================
 * FunctionBuilder
 *====================================================================================================================*/

M_LCOV_EXCL_START
void FunctionBuilder::dump(std::ostream &out) const
{
    out << "function \"" << name() << "\"";
    block().dump(out);
}
void FunctionBuilder::dump() const { dump(std::cerr); }
M_LCOV_EXCL_STOP


/*======================================================================================================================
 * WasmCompare
 *====================================================================================================================*/

WasmTemporary WasmCompare::emit(BlockBuilder &block, const WasmStructCGContext &context,
                                WasmTemporary left, WasmTemporary right)
{
    WasmTemporary val_cmp;

    for (auto &o : order) {
        WasmVariable val_left (fn_, get_binaryen_type(o.first->type()));
        WasmVariable val_right(fn_, get_binaryen_type(o.first->type()));
        block += val_left .set(context.compile(fn(), block, left .clone(fn().module()), *o.first));
        block += val_right.set(context.compile(fn(), block, right.clone(fn().module()), *o.first));

        WasmTemporary val_sub;
        if (auto cs = cast<const CharacterSequence>(o.first->type())) {
            WasmTemporary delta = WasmStrcmp::Cmp(fn_, block, *cs, *cs, val_left, val_right);
            if (not o.second) { // to order descending invert sign
                delta = BinaryenBinary(
                    /* module= */ fn_.module(),
                    /* op=     */ BinaryenSubInt32(),
                    /* left=   */ BinaryenConst(fn_.module(), BinaryenLiteralInt32(0)),
                    /* right=  */ delta
                );
            }
            val_sub = wasm_emit_signum(fn_, block, std::move(delta));
        } else {
            WasmTemporary val_lt = Lt(fn_, *o.first->type(), val_left, val_right);
            WasmTemporary val_gt = Gt(fn_, *o.first->type(), val_left, val_right);

            /* Ascending: val_gt - val_lt, Descending: val_lt - val_gt */
            val_sub = BinaryenBinary(
                /* module= */ fn_.module(),
                /* op=     */ BinaryenSubInt32(),
                /* left=   */ o.second ? val_gt : val_lt,
                /* right=  */ o.second ? val_lt : val_gt
            );
        }
        if (val_cmp.is()) {
            /*----- Update the comparison variable. ------------------------------------------------------------------*/
            WasmTemporary val_shifted = BinaryenBinary(
                /* module= */ fn_.module(),
                /* op=     */ BinaryenShlInt32(),
                /* left=   */ val_cmp,
                /* right=  */ BinaryenConst(fn_.module(), BinaryenLiteralInt32(1))
            );
            val_cmp = BinaryenBinary(
                /* module= */ fn_.module(),
                /* op=     */ BinaryenAddInt32(),
                /* left=   */ val_shifted,
                /* right=  */ val_sub
            );
        } else {
            val_cmp =  std::move(val_sub);
        }
    }

    return val_cmp;
}

WasmTemporary WasmCompare::Cmp(FunctionBuilder &fn, const Type &ty, WasmTemporary left, WasmTemporary right, cmp_op op)
{
    WasmTemporary res;
#define BINARY_OP(OP, TYPE) \
    res = BinaryenBinary(/* module= */ fn.module(), \
                         /* op=     */ Binaryen ## OP ## TYPE(), \
                         /* left=   */ left, \
                         /* right=  */ right)

#define CMP(TYPE, SIGN, SIZE) \
    switch (op) { \
        case EQ: BINARY_OP(Eq, TYPE ## SIZE); break; \
        case NE: BINARY_OP(Ne, TYPE ## SIZE); break; \
        case LT: BINARY_OP(Lt ## SIGN, TYPE ## SIZE); break; \
        case LE: BINARY_OP(Le ## SIGN, TYPE ## SIZE); break; \
        case GT: BINARY_OP(Gt ## SIGN, TYPE ## SIZE); break; \
        case GE: BINARY_OP(Ge ## SIGN, TYPE ## SIZE); break; \
        default: M_unreachable("unknown comparison operator"); \
    }

#define CMP_SINT(SIZE)  CMP(Int,  S, SIZE)
#define CMP_UINT(SIZE)  CMP(Int,  U, SIZE)
#define CMP_FLOAT(SIZE) CMP(Float, , SIZE)

    visit(overloaded {
        [&fn, &left, &right, op, &res](const Boolean&) { CMP_SINT(32) },
        [&fn, &left, &right, op, &res](const Date&) { CMP_SINT(32) },
        [&fn, &left, &right, op, &res](const DateTime&) { CMP_SINT(64) },
        [&fn, &left, &right, op, &res](const Numeric &n) {
            switch (n.kind) {
                case Numeric::N_Int:
                case Numeric::N_Decimal:
                    if (n.size() <= 32)
                        CMP_SINT(32)
                    else
                        CMP_SINT(64)
                    break;

                case Numeric::N_Float:
                    if (n.size() == 32)
                        CMP_FLOAT(32)
                    else
                        CMP_FLOAT(64)
                    break;
            }
        },
        [](auto&) { M_unreachable("unsupported type"); }
    }, ty);

#undef CMP_FLOAT
#undef CMP_UINT
#undef CMP_SINT
#undef CMP
#undef BINARY_OP

    return res;
}


/*======================================================================================================================
 * WasmStrcmp
 *====================================================================================================================*/

WasmTemporary WasmStrcmp::Cmp(FunctionBuilder &fn, BlockBuilder &block,
                              const CharacterSequence &ty_left, const CharacterSequence &ty_right,
                              WasmTemporary _left, WasmTemporary _right)
{
    if (ty_left.length == 1 and ty_right.length == 1) {
        WasmTemporary byte_left = BinaryenLoad(
            /* module= */ fn.module(),
            /* bytes=  */ 1,
            /* signed= */ false,
            /* offset= */ 0,
            /* align=  */ 0,
            /* type=   */ BinaryenTypeInt32(),
            /* ptr=    */ _left
        );
        WasmTemporary byte_right = BinaryenLoad(
            /* module= */ fn.module(),
            /* bytes=  */ 1,
            /* signed= */ false,
            /* offset= */ 0,
            /* align=  */ 0,
            /* type=   */ BinaryenTypeInt32(),
            /* ptr=    */ _right
        );
        return BinaryenBinary(
            /* module= */ fn.module(),
            /* op=     */ BinaryenSubInt32(),
            /* left=   */ byte_left,
            /* right=  */ byte_right
        );
    }

    /* Create pointers to track locations of current characters to compare. */
    WasmVariable left(fn, BinaryenTypeInt32());
    block += left.set(std::move(_left));
    WasmVariable right(fn, BinaryenTypeInt32());
    block += right.set(std::move(_right));

    /* Compute ends of left and right. */
    WasmVariable end_left(fn, BinaryenTypeInt32());
    block += end_left.set(BinaryenBinary(
        /* module= */ fn.module(),
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ left,
        /* right=  */ BinaryenConst(fn.module(), BinaryenLiteralInt32(ty_left.length))
    ));
    WasmVariable end_right(fn, BinaryenTypeInt32());
    block += end_right.set(BinaryenBinary(
        /* module= */ fn.module(),
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ right,
        /* right=  */ BinaryenConst(fn.module(), BinaryenLiteralInt32(ty_right.length))
    ));

    /* Create a variable to store the "delta" of the first character where the sequences differ. */
    WasmVariable delta(fn, BinaryenTypeInt32()); // default initialized to 0

    WasmVariable byte_left(fn, BinaryenTypeInt32());
    WasmDoWhile loop(fn.module(), "strcmp.foreach", byte_left); // as long as the left byte is not NUL
    {
        /* Load next byte from left, if in bounds. */
        WasmTemporary is_left_in_bounds = BinaryenBinary(
            /* module= */ fn.module(),
            /* op=     */ BinaryenLtUInt32(),
            /* left=   */ left,
            /* right=  */ end_left
        );
        WasmTemporary load_left = BinaryenLoad(
            /* module= */ fn.module(),
            /* bytes=  */ 1,
            /* signed= */ false,
            /* offset= */ 0,
            /* align=  */ 0,
            /* type=   */ BinaryenTypeInt32(),
            /* ptr=    */ left
        );
        loop += byte_left.set(BinaryenSelect(
            /* module=    */ fn.module(),
            /* condition= */ is_left_in_bounds,
            /* ifTrue=    */ load_left,
            /* ifFalse=   */ BinaryenConst(fn.module(), BinaryenLiteralInt32(0)),
            /* type=      */ BinaryenTypeInt32()
        ));

        /* Load next byte from right, if in bounds. */
        WasmTemporary is_right_in_bounds = BinaryenBinary(
            /* module= */ fn.module(),
            /* op=     */ BinaryenLtUInt32(),
            /* left=   */ right,
            /* right=  */ end_right
        );
        WasmTemporary load_right = BinaryenLoad(
            /* module= */ fn.module(),
            /* bytes=  */ 1,
            /* signed= */ false,
            /* offset= */ 0,
            /* align=  */ 0,
            /* type=   */ BinaryenTypeInt32(),
            /* ptr=    */ right
        );
        WasmTemporary byte_right = BinaryenSelect(
            /* module=    */ fn.module(),
            /* condition= */ is_right_in_bounds,
            /* ifTrue=    */ load_right,
            /* ifFalse=   */ BinaryenConst(fn.module(), BinaryenLiteralInt32(0)),
            /* type=      */ BinaryenTypeInt32()
        );

        /* Compute delta and abort, if delta non-zero. */
        loop += delta.set(BinaryenBinary(
            /* module= */ fn.module(),
            /* op=     */ BinaryenSubInt32(),
            /* left=   */ byte_left,
            /* right=  */ byte_right
        ));
        loop += BinaryenBreak(
            /* module=    */ fn.module(),
            /* name=      */ loop.body().name(),
            /* condition= */ delta,
            /* value=     */ nullptr
        );

        /* Advance left and right to next byte. */
        loop += left.set(BinaryenBinary(
            /* module= */ fn.module(),
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ left,
            /* right=  */ BinaryenConst(fn.module(), BinaryenLiteralInt32(1))
        ));
        loop += right.set(BinaryenBinary(
            /* module= */ fn.module(),
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ right,
            /* right=  */ BinaryenConst(fn.module(), BinaryenLiteralInt32(1))
        ));
    }
    block += loop.finalize();

    return delta;
}

WasmTemporary WasmStrcmp::Cmp(FunctionBuilder &fn, BlockBuilder &block,
                              const CharacterSequence &ty_left, const CharacterSequence &ty_right,
                              WasmTemporary left, WasmTemporary right, WasmCompare::cmp_op op)
{
    if (ty_left.length == 1 and ty_right.length == 1) {
        WasmTemporary byte_left = BinaryenLoad(
            /* module= */ fn.module(),
            /* bytes=  */ 1,
            /* signed= */ false,
            /* offset= */ 0,
            /* align=  */ 0,
            /* type=   */ BinaryenTypeInt32(),
            /* ptr=    */ left
        );
        WasmTemporary byte_right = BinaryenLoad(
            /* module= */ fn.module(),
            /* bytes=  */ 1,
            /* signed= */ false,
            /* offset= */ 0,
            /* align=  */ 0,
            /* type=   */ BinaryenTypeInt32(),
            /* ptr=    */ right
        );
        switch (op) {
            case WasmCompare::EQ: return BinaryenBinary(fn.module(), BinaryenEqInt32(),  byte_left, byte_right);
            case WasmCompare::NE: return BinaryenBinary(fn.module(), BinaryenNeInt32(),  byte_left, byte_right);
            case WasmCompare::LT: return BinaryenBinary(fn.module(), BinaryenLtUInt32(), byte_left, byte_right);
            case WasmCompare::GT: return BinaryenBinary(fn.module(), BinaryenGtUInt32(), byte_left, byte_right);
            case WasmCompare::LE: return BinaryenBinary(fn.module(), BinaryenLeUInt32(), byte_left, byte_right);
            case WasmCompare::GE: return BinaryenBinary(fn.module(), BinaryenGeUInt32(), byte_left, byte_right);
        }
    }

    WasmTemporary delta = Cmp(fn, block, ty_left, ty_right, std::move(left), std::move(right));
    auto zero = BinaryenConst(fn.module(), BinaryenLiteralInt32(0));
    switch (op) {
        case WasmCompare::EQ: return BinaryenBinary(fn.module(), BinaryenEqInt32(),  delta, zero);
        case WasmCompare::NE: return BinaryenBinary(fn.module(), BinaryenNeInt32(),  delta, zero);
        case WasmCompare::LT: return BinaryenBinary(fn.module(), BinaryenLtSInt32(), delta, zero);
        case WasmCompare::GT: return BinaryenBinary(fn.module(), BinaryenGtSInt32(), delta, zero);
        case WasmCompare::LE: return BinaryenBinary(fn.module(), BinaryenLeSInt32(), delta, zero);
        case WasmCompare::GE: return BinaryenBinary(fn.module(), BinaryenGeSInt32(), delta, zero);
    }
}


/*======================================================================================================================
 * WasmStrncpy
 *====================================================================================================================*/

void WasmStrncpy::emit(BlockBuilder &block, WasmTemporary _dest, WasmTemporary _src, std::size_t count)
{
    if (count <= 8) {
        /* Create pointers to track source and destination location. */
        WasmVariable dest(fn, BinaryenTypeInt32());
        block += dest.set(std::move(_dest));
        WasmVariable src(fn, BinaryenTypeInt32());
        block += src.set(std::move(_src));

        for (std::size_t i = 0; i != count; ++i) {
            WasmTemporary byte = BinaryenLoad(
                /* module= */ fn.module(),
                /* bytes=  */ 1,
                /* signed= */ false,
                /* offset= */ i,
                /* align=  */ 0,
                /* type=   */ BinaryenTypeInt32(),
                /* ptr=    */ src
            );
            block += BinaryenStore(
                /* module= */ fn.module(),
                /* bytes=  */ 1,
                /* offset= */ i,
                /* align=  */ 0,
                /* ptr=    */ dest,
                /* value=  */ byte,
                /* type=   */ BinaryenTypeInt32()
            );
        }
        return;
    }

    if (BinaryenModuleGetFeatures(fn.module()) & BinaryenFeatureBulkMemory()) {
        block += BinaryenMemoryCopy(fn.module(), _dest, _src, BinaryenConst(fn.module(), BinaryenLiteralInt32(count)));
    } else {
        /* Create pointers to track source and destination location. */
        WasmVariable dest(fn, BinaryenTypeInt32());
        block += dest.set(std::move(_dest));
        WasmVariable src(fn, BinaryenTypeInt32());
        block += src.set(std::move(_src));

        /* Create abort condition for loop. */
        WasmVariable end(fn, BinaryenTypeInt32());
        block += end.set(BinaryenBinary(
            /* module= */ fn.module(),
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ src,
            /* right=  */ BinaryenConst(fn.module(), BinaryenLiteralInt32(count))
        ));
        WasmTemporary cond = BinaryenBinary(
            /* module= */ fn.module(),
            /* op=     */ BinaryenLtUInt32(),
            /* left=   */ src,
            /* right=  */ end
        );

        /* Create loop that iterates over all characters in the source string. */
        WasmWhile loop(fn.module(), "Strncpy.while", std::move(cond));

        /* Load next byte from source. */
        WasmTemporary byte = BinaryenLoad(
            /* module= */ fn.module(),
            /* bytes=  */ 1,
            /* signed= */ false,
            /* offset= */ 0,
            /* align=  */ 0,
            /* type=   */ BinaryenTypeInt32(),
            /* ptr=    */ src
        );
        /* Write loaded byte to next position in destination. */
        loop += BinaryenStore(
            /* module= */ fn.module(),
            /* bytes=  */ 1,
            /* offset= */ 0,
            /* algin=  */ 0,
            /* ptr=    */ dest,
            /* value=  */ byte,
            /* type=   */ BinaryenTypeInt32()
        );

        /* Increment source and destination pointers. */
        loop += src.set(BinaryenBinary(
            /* module= */ fn.module(),
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ src,
            /* right=  */ BinaryenConst(fn.module(), BinaryenLiteralInt32(1))
        ));
        loop += dest.set(BinaryenBinary(
            /* module= */ fn.module(),
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ dest,
            /* right=  */ BinaryenConst(fn.module(), BinaryenLiteralInt32(1))
        ));

        block += loop.finalize();
    }
}


/*======================================================================================================================
 * WasmLike
 *====================================================================================================================*/

WasmTemporary WasmLike::Like(FunctionBuilder &fn, BlockBuilder &block,
                             const CharacterSequence &ty_str, const CharacterSequence &ty_pattern,
                             WasmTemporary _str, WasmTemporary _pattern, const char escape_char)
{
    M_insist('_' != escape_char and '%' != escape_char, "illegal escape character");

    if (ty_str.length == 0 and ty_pattern.length == 0)
        return BinaryenConst(fn.module(), BinaryenLiteralInt32(1));

    /*----- Store old head of heap. ----------------------------------------------------------------------------------*/
    WasmVariable old_head_of_heap(fn, BinaryenTypeInt32());
    block += old_head_of_heap.set(fn.module().head_of_heap());

    /*----- Allocate space on the heap for the dynamic programming table. --------------------------------------------*/
    /* Row i and column j is located at old_head_of_heap + (i - 1) * (`ty_str.length` + 1) + (j - 1). */
    auto num_entries = (ty_str.length + 1) * (ty_pattern.length + 1);
    WasmVariable end(fn, BinaryenTypeInt32());
    block += end.set(create_table(fn, block, old_head_of_heap, num_entries));
    block += fn.module().head_of_heap().set(end);
    block += fn.module().align_head_of_heap();

    /*----- Initialize table with all entries set to false. ----------------------------------------------------------*/
    clear_table(fn, block, old_head_of_heap, end);

    /*----- Create pointer to track location of current entry. -------------------------------------------------------*/
    WasmVariable entry(fn, BinaryenTypeInt32());
    block += entry.set(old_head_of_heap);

    /*----- Create pointers to track locations of current characters of `_str` and `_pattern`. -----------------------*/
    WasmVariable str(fn, BinaryenTypeInt32());
    block += str.set(_str.clone(fn.module()));
    WasmVariable pattern(fn, BinaryenTypeInt32());
    block += pattern.set(_pattern.clone(fn.module()));

    /*----- Compute ends of str and pattern. -------------------------------------------------------------------------*/
    WasmVariable end_str(fn, BinaryenTypeInt32());
    block += end_str.set(BinaryenBinary(
        /* module= */ fn.module(),
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ str,
        /* right=  */ BinaryenConst(fn.module(), BinaryenLiteralInt32(ty_str.length))
    ));
    WasmVariable end_pattern(fn, BinaryenTypeInt32());
    block += end_pattern.set(BinaryenBinary(
        /* module= */ fn.module(),
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ pattern,
        /* right=  */ BinaryenConst(fn.module(), BinaryenLiteralInt32(ty_pattern.length))
    ));

    /*----- Create variables for the current byte of str and pattern. ------------------------------------------------*/
    WasmVariable byte_str(fn, BinaryenTypeInt32());
    WasmVariable byte_pattern(fn, BinaryenTypeInt32());

    /*----- Initialize first column. ---------------------------------------------------------------------------------*/
    /* Create loop iterating until current byte of pattern is not a `%`-wildcard. */
    WasmDoWhile init(fn.module(), "Like.init", BinaryenBinary(
        /* module= */ fn.module(),
        /* op=     */ BinaryenEqInt32(),
        /* left=   */ byte_pattern,
        /* right=  */ BinaryenConst(fn.module(), BinaryenLiteralInt32('%'))
    ));
    {
        /* Load next byte from pattern, if in bounds. */
        WasmTemporary is_pattern_in_bounds = BinaryenBinary(
            /* module= */ fn.module(),
            /* op=     */ BinaryenLtUInt32(),
            /* left=   */ pattern,
            /* right=  */ end_pattern
        );
        WasmTemporary load_pattern = BinaryenLoad(
            /* module= */ fn.module(),
            /* bytes=  */ 1,
            /* signed= */ false,
            /* offset= */ 0,
            /* align=  */ 0,
            /* type=   */ BinaryenTypeInt32(),
            /* ptr=    */ pattern
        );
        init += byte_pattern.set(BinaryenSelect(
            /* module=    */ fn.module(),
            /* condition= */ is_pattern_in_bounds,
            /* ifTrue=    */ load_pattern,
            /* ifFalse=   */ BinaryenConst(fn.module(), BinaryenLiteralInt32(0)),
            /* type=      */ BinaryenTypeInt32()
        ));

        /* Set current entry to true. */
        init += BinaryenStore(
            /* module= */ fn.module(),
            /* bytes=  */ 1,
            /* offset= */ 0,
            /* align=  */ 0,
            /* ptr=    */ entry,
            /* value=  */ BinaryenConst(fn.module(), BinaryenLiteralInt32(1)),
            /* type=   */ BinaryenTypeInt32()
        );

        /* Advance entry to next row. */
        init += entry.set(BinaryenBinary(
            /* module= */ fn.module(),
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ entry,
            /* right=  */ BinaryenConst(fn.module(), BinaryenLiteralInt32(ty_str.length + 1))
        ));

        /* Advance pattern to next byte. */
        init += pattern.set(BinaryenBinary(
            /* module= */ fn.module(),
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ pattern,
            /* right=  */ BinaryenConst(fn.module(), BinaryenLiteralInt32(1))
        ));
    }
    block += init.finalize();

    /*----- Compute entire table. ------------------------------------------------------------------------------------*/
    /* Create variable for the actual length of str. */
    WasmVariable len_str(fn, BinaryenTypeInt32()); // default initialized to 0

    /* Create flag whether the current byte of pattern is not escaped. */
    WasmVariable is_not_escaped(fn, BinaryenTypeInt32());
    block += is_not_escaped.set(BinaryenConst(fn.module(), BinaryenLiteralInt32(true)));

    /* Reset entry to second row and second column. */
    block += entry.set(BinaryenBinary(
        /* module= */ fn.module(),
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ old_head_of_heap,
        /* right=  */ BinaryenConst(fn.module(), BinaryenLiteralInt32(ty_str.length + 2))
    ));

    /* Reset pattern to first character. */
    block += pattern.set(std::move(_pattern));

    {
        /* Load first byte from pattern, if in bounds. */
        WasmTemporary is_pattern_in_bounds = BinaryenBinary(
            /* module= */ fn.module(),
            /* op=     */ BinaryenLtUInt32(),
            /* left=   */ pattern,
            /* right=  */ end_pattern
        );
        WasmTemporary load_pattern = BinaryenLoad(
            /* module= */ fn.module(),
            /* bytes=  */ 1,
            /* signed= */ false,
            /* offset= */ 0,
            /* align=  */ 0,
            /* type=   */ BinaryenTypeInt32(),
            /* ptr=    */ pattern
        );
        block += byte_pattern.set(BinaryenSelect(
            /* module=    */ fn.module(),
            /* condition= */ is_pattern_in_bounds,
            /* ifTrue=    */ load_pattern,
            /* ifFalse=   */ BinaryenConst(fn.module(), BinaryenLiteralInt32(0)),
            /* type=      */ BinaryenTypeInt32()
        ));
    }

    /* Create loop iterating as long as the current byte of pattern is not NUL. */
    WasmWhile outer(fn.module(), "Like.outer", byte_pattern);
    {
        /* If current byte of pattern is not escaped and equals `escape_char`, advance pattern to next byte and load
         * it. Additionally, mark this byte as escaped and check for invalid escape sequences. */
        WasmTemporary is_escape_char = BinaryenBinary(
            /* module= */ fn.module(),
            /* op=     */ BinaryenEqInt32(),
            /* left=   */ byte_pattern,
            /* right=  */ BinaryenConst(fn.module(), BinaryenLiteralInt32(escape_char))
        );
        WasmTemporary is_not_escaped_escape_char = BinaryenBinary(
            /* module= */ fn.module(),
            /* op=     */ BinaryenAndInt32(),
            /* left=   */ is_not_escaped,
            /* right=  */ is_escape_char
        );
        BlockBuilder block_if(fn.module(), "Like.if");
        {
            /* Advance pattern to next byte. */
            block_if += pattern.set(BinaryenBinary(
                /* module= */ fn.module(),
                /* op=     */ BinaryenAddInt32(),
                /* left=   */ pattern,
                /* right=  */ BinaryenConst(fn.module(), BinaryenLiteralInt32(1))
            ));

            /* Load next byte from pattern, if in bounds. */
            WasmTemporary is_pattern_in_bounds = BinaryenBinary(
                /* module= */ fn.module(),
                /* op=     */ BinaryenLtUInt32(),
                /* left=   */ pattern,
                /* right=  */ end_pattern
            );
            WasmTemporary load_pattern = BinaryenLoad(
                /* module= */ fn.module(),
                /* bytes=  */ 1,
                /* signed= */ false,
                /* offset= */ 0,
                /* align=  */ 0,
                /* type=   */ BinaryenTypeInt32(),
                /* ptr=    */ pattern
            );
            block_if += byte_pattern.set(BinaryenSelect(
                /* module=    */ fn.module(),
                /* condition= */ is_pattern_in_bounds,
                /* ifTrue=    */ load_pattern,
                /* ifFalse=   */ BinaryenConst(fn.module(), BinaryenLiteralInt32(0)),
                /* type=      */ BinaryenTypeInt32()
            ));

            /* Check whether current byte of pattern is a valid escaped character, i.e. `_`, `%` or `escape_char`.
             * If not, throw an exception. */
            WasmTemporary byte_pattern_not_equals_underscore = BinaryenBinary(
                /* module= */ fn.module(),
                /* op=     */ BinaryenNeInt32(),
                /* left=   */ byte_pattern,
                /* right=  */ BinaryenConst(fn.module(), BinaryenLiteralInt32('_'))
            );
            WasmTemporary byte_pattern_not_equals_percent = BinaryenBinary(
                /* module= */ fn.module(),
                /* op=     */ BinaryenNeInt32(),
                /* left=   */ byte_pattern,
                /* right=  */ BinaryenConst(fn.module(), BinaryenLiteralInt32('%'))
            );
            WasmTemporary byte_pattern_not_equals_escape_char = BinaryenBinary(
                /* module= */ fn.module(),
                /* op=     */ BinaryenNeInt32(),
                /* left=   */ byte_pattern,
                /* right=  */ BinaryenConst(fn.module(), BinaryenLiteralInt32(escape_char))
            );
            WasmTemporary byte_pattern_not_equals_underscore_and_percent_and_escape_char = BinaryenBinary(
                /* module= */ fn.module(),
                /* op=     */ BinaryenAndInt32(),
                /* left=   */ byte_pattern_not_equals_underscore,
                /* right=  */ BinaryenBinary(
                    /* module= */ fn.module(),
                    /* op=     */ BinaryenAndInt32(),
                    /* left=   */ byte_pattern_not_equals_percent,
                    /* right=  */ byte_pattern_not_equals_escape_char
                )
            );
            WasmTemporary call_throw_exception = BinaryenCall(
                /* module=      */ fn.module(),
                /* target=      */ "throw_invalid_escape_sequence",
                /* operands=    */ nullptr,
                /* numOperands= */ 0,
                /* returnType=  */ BinaryenTypeNone()
            );
            block_if += BinaryenIf(
                /* module=    */ fn.module(),
                /* condition= */ byte_pattern_not_equals_underscore_and_percent_and_escape_char,
                /* ifTrue=    */ call_throw_exception,
                /* ifFalse=   */ nullptr
            );

            /* Mark current byte of pattern as escaped. */
            block_if += is_not_escaped.set(BinaryenConst(fn.module(), BinaryenLiteralInt32(false)));
        }
        outer += BinaryenIf(
            /* module=    */ fn.module(),
            /* condition= */ is_not_escaped_escape_char,
            /* ifTrue=    */ block_if.finalize(),
            /* ifFalse=   */ nullptr
        );

        /* Reset actual length of str. */
        outer += len_str.set(BinaryenConst(fn.module(), BinaryenLiteralInt32(0)));

        {
            /* Load first byte from str, if in bounds. */
            WasmTemporary is_str_in_bounds = BinaryenBinary(
                /* module= */ fn.module(),
                /* op=     */ BinaryenLtUInt32(),
                /* left=   */ str,
                /* right=  */ end_str
            );
            WasmTemporary load_str = BinaryenLoad(
                /* module= */ fn.module(),
                /* bytes=  */ 1,
                /* signed= */ false,
                /* offset= */ 0,
                /* align=  */ 0,
                /* type=   */ BinaryenTypeInt32(),
                /* ptr=    */ str
            );
            outer += byte_str.set(BinaryenSelect(
                /* module=    */ fn.module(),
                /* condition= */ is_str_in_bounds,
                /* ifTrue=    */ load_str,
                /* ifFalse=   */ BinaryenConst(fn.module(), BinaryenLiteralInt32(0)),
                /* type=      */ BinaryenTypeInt32()
            ));
        }

        /* Create loop iterating as long as the current byte of str is not NUL. */
        WasmWhile inner(fn.module(), "Like.inner", byte_str);
        {
            /* Increment actual length of str. */
            inner += len_str.set(BinaryenBinary(
                /* module= */ fn.module(),
                /* op=     */ BinaryenAddInt32(),
                /* left=   */ len_str,
                /* right=  */ BinaryenConst(fn.module(), BinaryenLiteralInt32(1))
            ));

            /* Store above left entry. */
            WasmTemporary entry_above_left = BinaryenBinary(
                /* module= */ fn.module(),
                /* op=     */ BinaryenSubInt32(),
                /* left=   */ entry,
                /* right=  */ BinaryenConst(fn.module(), BinaryenLiteralInt32(ty_str.length + 2))
            );
            WasmTemporary value_above_left = BinaryenLoad(
                /* module= */ fn.module(),
                /* bytes=  */ 1,
                /* signed= */ false,
                /* offset= */ 0,
                /* align=  */ 0,
                /* type=   */ BinaryenTypeInt32(),
                /* ptr=    */ entry_above_left
            );
            WasmTemporary store_above_left = BinaryenStore(
                /* module= */ fn.module(),
                /* bytes=  */ 1,
                /* offset= */ 0,
                /* align=  */ 0,
                /* ptr=    */ entry,
                /* value=  */ value_above_left,
                /* type=   */ BinaryenTypeInt32()
            );

            /* Store disjunction of above and left entry. */
            WasmTemporary entry_above = BinaryenBinary(
                /* module= */ fn.module(),
                /* op=     */ BinaryenSubInt32(),
                /* left=   */ entry,
                /* right=  */ BinaryenConst(fn.module(), BinaryenLiteralInt32(ty_str.length + 1))
            );
            WasmTemporary value_above = BinaryenLoad(
                /* module= */ fn.module(),
                /* bytes=  */ 1,
                /* signed= */ false,
                /* offset= */ 0,
                /* align=  */ 0,
                /* type=   */ BinaryenTypeInt32(),
                /* ptr=    */ entry_above
            );
            WasmTemporary entry_left = BinaryenBinary(
                /* module= */ fn.module(),
                /* op=     */ BinaryenSubInt32(),
                /* left=   */ entry,
                /* right=  */ BinaryenConst(fn.module(), BinaryenLiteralInt32(1))
            );
            WasmTemporary value_left = BinaryenLoad(
                /* module= */ fn.module(),
                /* bytes=  */ 1,
                /* signed= */ false,
                /* offset= */ 0,
                /* align=  */ 0,
                /* type=   */ BinaryenTypeInt32(),
                /* ptr=    */ entry_left
            );
            WasmTemporary value_above_or_left = BinaryenBinary(
                /* module= */ fn.module(),
                /* op=     */ BinaryenOrInt32(),
                /* left=   */ value_above,
                /* right=  */ value_left
            );
            WasmTemporary store_above_or_left = BinaryenStore(
                /* module= */ fn.module(),
                /* bytes=  */ 1,
                /* offset= */ 0,
                /* align=  */ 0,
                /* ptr=    */ entry,
                /* value=  */ value_above_or_left,
                /* type=   */ BinaryenTypeInt32()
            );

            /* Check current bytes of str and pattern, and set entry respectively. */
            WasmTemporary byte_pattern_equals_underscore = BinaryenBinary(
                /* module= */ fn.module(),
                /* op=     */ BinaryenEqInt32(),
                /* left=   */ byte_pattern,
                /* right=  */ BinaryenConst(fn.module(), BinaryenLiteralInt32('_'))
            );
            WasmTemporary byte_pattern_equals_byte_str = BinaryenBinary(
                /* module= */ fn.module(),
                /* op=     */ BinaryenEqInt32(),
                /* left=   */ byte_pattern,
                /* right=  */ byte_str
            );
            WasmTemporary byte_pattern_equals_percent = BinaryenBinary(
                /* module= */ fn.module(),
                /* op=     */ BinaryenEqInt32(),
                /* left=   */ byte_pattern,
                /* right=  */ BinaryenConst(fn.module(), BinaryenLiteralInt32('%'))
            );
            WasmTemporary not_escaped_byte_pattern_equals_underscore = BinaryenBinary(
                /* module= */ fn.module(),
                /* op=     */ BinaryenAndInt32(),
                /* left=   */ is_not_escaped,
                /* right=  */ byte_pattern_equals_underscore
            );
            WasmTemporary not_escaped_byte_pattern_equals_underscore_or_byte_pattern_equals_byte_str = BinaryenBinary(
                /* module= */ fn.module(),
                /* op=     */ BinaryenOrInt32(),
                /* left=   */ not_escaped_byte_pattern_equals_underscore,
                /* right=  */ byte_pattern_equals_byte_str
            );
            WasmTemporary not_escaped_byte_pattern_equals_percent = BinaryenBinary(
                /* module= */ fn.module(),
                /* op=     */ BinaryenAndInt32(),
                /* left=   */ is_not_escaped,
                /* right=  */ byte_pattern_equals_percent
            );
            inner += BinaryenIf(
                /* module=    */ fn.module(),
                /* condition= */ not_escaped_byte_pattern_equals_percent,
                /* ifTrue=    */ store_above_or_left,
                /* ifFalse=   */ BinaryenIf(
                    /* module=    */ fn.module(),
                    /* condition= */ not_escaped_byte_pattern_equals_underscore_or_byte_pattern_equals_byte_str,
                    /* ifTrue=    */ store_above_left,
                    /* ifFalse=   */ nullptr
                )
            );

            /* Advance entry to next entry. */
            inner += entry.set(BinaryenBinary(
                /* module= */ fn.module(),
                /* op=     */ BinaryenAddInt32(),
                /* left=   */ entry,
                /* right=  */ BinaryenConst(fn.module(), BinaryenLiteralInt32(1))
            ));

            /* Advance str to next byte. */
            inner += str.set(BinaryenBinary(
                /* module= */ fn.module(),
                /* op=     */ BinaryenAddInt32(),
                /* left=   */ str,
                /* right=  */ BinaryenConst(fn.module(), BinaryenLiteralInt32(1))
            ));

            /* Load next byte from str, if in bounds. */
            WasmTemporary is_str_in_bounds = BinaryenBinary(
                /* module= */ fn.module(),
                /* op=     */ BinaryenLtUInt32(),
                /* left=   */ str,
                /* right=  */ end_str
            );
            WasmTemporary load_str = BinaryenLoad(
                /* module= */ fn.module(),
                /* bytes=  */ 1,
                /* signed= */ false,
                /* offset= */ 0,
                /* align=  */ 0,
                /* type=   */ BinaryenTypeInt32(),
                /* ptr=    */ str
            );
            inner += byte_str.set(BinaryenSelect(
                /* module=    */ fn.module(),
                /* condition= */ is_str_in_bounds,
                /* ifTrue=    */ load_str,
                /* ifFalse=   */ BinaryenConst(fn.module(), BinaryenLiteralInt32(0)),
                /* type=      */ BinaryenTypeInt32()
            ));
        }
        outer += inner.finalize();

        /* Advance entry to second column in the next row. */
        WasmTemporary offset = BinaryenBinary(
            /* module= */ fn.module(),
            /* op=     */ BinaryenSubInt32(),
            /* left=   */ BinaryenConst(fn.module(), BinaryenLiteralInt32(ty_str.length + 1)),
            /* right=  */ len_str
        );
        outer += entry.set(BinaryenBinary(
            /* module= */ fn.module(),
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ entry,
            /* right=  */ offset
        ));

        /* Reset str to first character. */
        outer += str.set(_str.clone(fn.module()));

        /* Advance pattern to next byte. */
        outer += pattern.set(BinaryenBinary(
            /* module= */ fn.module(),
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ pattern,
            /* right=  */ BinaryenConst(fn.module(), BinaryenLiteralInt32(1))
        ));

        /* Load next byte from pattern, if in bounds. */
        WasmTemporary is_pattern_in_bounds = BinaryenBinary(
            /* module= */ fn.module(),
            /* op=     */ BinaryenLtUInt32(),
            /* left=   */ pattern,
            /* right=  */ end_pattern
        );
        WasmTemporary load_pattern = BinaryenLoad(
            /* module= */ fn.module(),
            /* bytes=  */ 1,
            /* signed= */ false,
            /* offset= */ 0,
            /* align=  */ 0,
            /* type=   */ BinaryenTypeInt32(),
            /* ptr=    */ pattern
        );
        outer += byte_pattern.set(BinaryenSelect(
            /* module=    */ fn.module(),
            /* condition= */ is_pattern_in_bounds,
            /* ifTrue=    */ load_pattern,
            /* ifFalse=   */ BinaryenConst(fn.module(), BinaryenLiteralInt32(0)),
            /* type=      */ BinaryenTypeInt32()
        ));

        /* Reset is_not_escaped to true. */
        outer += is_not_escaped.set(BinaryenConst(fn.module(), BinaryenLiteralInt32(true)));
    }
    block += outer.finalize();

    /*----- Free allocated space. ------------------------------------------------------------------------------------*/
    block += fn.module().head_of_heap().set(old_head_of_heap);

    /*----- Return result. -------------------------------------------------------------------------------------------*/
    /* Entry points currently to the second column in the first row after the pattern has ended. Therefore, we have
     * to go one row up and len_str - 1 columns to the right, i.e. the result is located at
     * entry - (`ty_str.length` + 1) + len_str - 1 = entry + len_str - (`ty_str.length` + 2). */
    WasmTemporary offset = BinaryenBinary(
        /* module= */ fn.module(),
        /* op=     */ BinaryenSubInt32(),
        /* left=   */ len_str,
        /* right=  */ BinaryenConst(fn.module(), BinaryenLiteralInt32(ty_str.length + 2))
    );
    WasmTemporary result = BinaryenBinary(
        /* module= */ fn.module(),
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ entry,
        /* right=  */ offset
    );
    return BinaryenLoad(
        /* module= */ fn.module(),
        /* bytes=  */ 1,
        /* signed= */ false,
        /* offset= */ 0,
        /* align=  */ 0,
        /* type=   */ BinaryenTypeInt32(),
        /* ptr=    */ result
    );
}

WasmTemporary WasmLike::create_table(FunctionBuilder &fn, BlockBuilder &block, WasmTemporary addr, std::size_t num_entries)
{
    M_insist(num_entries <= 1UL << 31, "table size exceed uint32 type");
    return BinaryenBinary(
        /* module= */ fn.module(),
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ addr,
        /* right=  */ BinaryenConst(fn.module(), BinaryenLiteralInt32(num_entries))
    );
}

void WasmLike::clear_table(FunctionBuilder &fn, BlockBuilder &block, WasmTemporary begin, WasmTemporary end)
{
    WasmVariable induction(fn, BinaryenTypeInt32());
    block += induction.set(std::move(begin));

    WasmWhile loop(fn.module(), "Like.clear_table.loop", BinaryenBinary(
        /* module= */ fn.module(),
        /* op=     */ BinaryenLtUInt32(),
        /* left=   */ induction,
        /* right=  */ std::move(end)
    ));

    /* Clear entry. */
    loop += BinaryenStore(
        /* module= */ fn.module(),
        /* bytes=  */ 1,
        /* offset= */ 0,
        /* align=  */ 0,
        /* ptr=    */ induction,
        /* value=  */ BinaryenConst(fn.module(), BinaryenLiteralInt32(0)),
        /* type=   */ BinaryenTypeInt32()
    );

    /* Advance induction variable. */
    loop += induction.set(BinaryenBinary(
        /* module= */ fn.module(),
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ induction,
        /* right=  */ BinaryenConst(fn.module(), BinaryenLiteralInt32(1))
    ));

    block += loop.finalize();
}


/*======================================================================================================================
 * WasmSwap
 *====================================================================================================================*/

void WasmSwap::emit(BlockBuilder &block, const WasmStruct &struc, WasmTemporary _first, WasmTemporary _second)
{
    WasmVariable first(fn, BinaryenTypeInt32());
    block += first.set(std::move(_first));
    WasmVariable second(fn, BinaryenTypeInt32());
    block += second.set(std::move(_second));

    for (std::size_t idx = 0; idx != struc.num_entries(); ++idx) {
        auto &ty = struc.type(idx);
        if (auto cs = cast<const CharacterSequence>(&ty)) {
            swap_string(block, *cs, struc.load(fn, first, idx), struc.load(fn, second, idx));
            continue;
        }

        /* Introduce temporary for the swap. */
        BinaryenType b_ty = get_binaryen_type(&ty);
        auto it = swap_temp.find(b_ty);
        if (it == swap_temp.end())
            it = swap_temp.emplace_hint(it, b_ty, fn.add_local(b_ty));
        WasmVariable &tmp = it->second;

        /* tmp = *first */
        block += tmp.set(struc.load(fn, first, idx));
        /* *first = *second */
        struc.store(fn, block, first, idx, struc.load(fn, second, idx));
        /* *second = tmp */
        struc.store(fn, block, second, idx, tmp);
    }
}

void WasmSwap::swap_string(BlockBuilder &block, const CharacterSequence &ty,
                           WasmTemporary _first, WasmTemporary _second)
{
    /* Get temporary variable to swap single character. */
    auto it = swap_temp.find(BinaryenTypeInt32());
    if (it == swap_temp.end())
        it = swap_temp.emplace_hint(it, BinaryenTypeInt32(), fn.add_local(BinaryenTypeInt32()));
    WasmVariable &tmp = it->second;

    /* Create cursors. */
    WasmVariable first(fn, BinaryenTypeInt32());
    block += first.set(std::move(_first));
    WasmVariable second(fn, BinaryenTypeInt32());
    block += second.set(std::move(_second));

    /* Create abort condition. */
    WasmVariable end_first(fn, BinaryenTypeInt32());
    block += end_first.set(BinaryenBinary(
        /* module= */ fn.module(),
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ first,
        /* right=  */ BinaryenConst(fn.module(), BinaryenLiteralInt32(ty.length))
    ));
    WasmTemporary loop_cond = BinaryenBinary(
        /* module= */ fn.module(),
        /* op=     */ BinaryenLtUInt32(),
        /* left=   */ first,
        /* right=  */ end_first
    );

    WasmWhile loop(fn.module(), "swap.foreach", std::move(loop_cond));

    /* tmp = *first */
    loop += tmp.set(BinaryenLoad(
        /* module= */ fn.module(),
        /* bytes=  */ 1,
        /* signed= */ false,
        /* offset= */ 0,
        /* align=  */ 0,
        /* type=   */ BinaryenTypeInt32(),
        /* ptr=    */ first
    ));
    /* *first = *second */
    WasmTemporary byte_second = BinaryenLoad(
        /* module= */ fn.module(),
        /* bytes=  */ 1,
        /* signed= */ false,
        /* offset= */ 0,
        /* align=  */ 0,
        /* type=   */ BinaryenTypeInt32(),
        /* ptr=    */ second
    );
    loop += BinaryenStore(
        /* module= */ fn.module(),
        /* bytes=  */ 1,
        /* offset= */ 0,
        /* align=  */ 0,
        /* ptr=    */ first,
        /* value=  */ byte_second,
        /* type=   */ BinaryenTypeInt32()
    );
    /* *second = tmp */
    loop += BinaryenStore(
        /* module= */ fn.module(),
        /* bytes=  */ 1,
        /* offset= */ 0,
        /* align=  */ 0,
        /* ptr=    */ second,
        /* value=  */ tmp,
        /* type=   */ BinaryenTypeInt32()
    );

    /* Advance cursors. */
    loop += first.set(BinaryenBinary(
        /* module= */ fn.module(),
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ first,
        /* right=  */ BinaryenConst(fn.module(), BinaryenLiteralInt32(1))
    ));
    loop += second.set(BinaryenBinary(
        /* module= */ fn.module(),
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ second,
        /* right=  */ BinaryenConst(fn.module(), BinaryenLiteralInt32(1))
    ));

    block += loop.finalize();
}


/*======================================================================================================================
 * WasmLimits
 *====================================================================================================================*/

namespace {

template<typename Visitor>
BinaryenLiteral __limit(Visitor &&vis, const Type &ty)
{
    BinaryenLiteral literal;
#define LIMIT(WASM_TY, C_TY) \
    literal = BinaryenLiteral##WASM_TY(vis(std::numeric_limits<C_TY>{}))
    visit(overloaded {
        [&literal, &vis] (const Boolean&) { LIMIT(Int32, bool); },
        [&literal, &vis] (const Date&) { LIMIT(Int32, int32_t); },
        [&literal, &vis] (const DateTime&) { LIMIT(Int64, int64_t); },
        [&literal, &vis] (const Numeric &n) {
            switch (n.kind) {
                case Numeric::N_Int:
                    switch (n.size()) {
                        default:
                            M_unreachable("invalid integer type");
                        case 8:
                            LIMIT(Int32, int8_t);
                            break;
                        case 16:
                            LIMIT(Int32, int16_t);
                            break;
                        case 32:
                            LIMIT(Int32, int32_t);
                            break;
                        case 64:
                            LIMIT(Int64, int64_t);
                            break;
                    }
                    break;

                case Numeric::N_Decimal:
                    M_unreachable("not supported");

                case Numeric::N_Float:
                    if (n.size() <= 32)
                        LIMIT(Float32, float);
                    else
                        LIMIT(Float64, double);
                    break;
            }
        },
        [](auto&) { M_unreachable("invalid type"); },
    }, ty);
    return literal;
#undef LIMIT
}

}

BinaryenLiteral WasmLimits::min(const Type &ty) { return __limit([](auto limits) { return decltype(limits)::min(); }, ty); }
BinaryenLiteral WasmLimits::max(const Type &ty) { return __limit([](auto limits) { return decltype(limits)::max(); }, ty); }
BinaryenLiteral WasmLimits::lowest(const Type &ty) { return __limit([](auto limits) { return decltype(limits)::lowest(); }, ty); }
BinaryenLiteral WasmLimits::NaN(const Type &ty) { return __limit([](auto limits) { return decltype(limits)::quiet_NaN(); }, ty); }
BinaryenLiteral WasmLimits::infinity(const Type &ty) { return __limit([](auto limits) { return decltype(limits)::infinity(); }, ty); }


/*======================================================================================================================
 * Code generation helper classes
 *====================================================================================================================*/

namespace {

struct GroupingData : OperatorData
{
    WasmStruct *struc = nullptr;
    WasmHashTable *HT = nullptr;
    WasmVariable watermark_high; ///< stores the maximum number of entries before growing the table is required
    bool needs_running_count; ///< whether we must count the elements per group during grouping

    GroupingData(FunctionBuilder &fn)
        : watermark_high(fn, BinaryenTypeInt32())
    { }

    ~GroupingData() {
        delete HT;
        delete struc;
    }
};

struct AggregationData : OperatorData
{
    std::vector<WasmVariable> aggregates;

    AggregationData(FunctionBuilder &fn, const AggregationOperator &op) {
        aggregates.reserve(op.schema().num_entries() + 1);
        for (std::size_t i = 0; i != op.schema().num_entries(); ++i) {
            auto &e = op.schema()[i];
            M_insist(not e.type->is_character_sequence());
            aggregates.emplace_back(fn, get_binaryen_type(e.type));
        }
        aggregates.emplace_back(fn, BinaryenTypeInt32()); // running count
    }
};

struct SortingData : OperatorData
{
    WasmStruct *struc = nullptr;
    WasmVariable begin;

    SortingData(FunctionBuilder &fn)
        : begin(fn, BinaryenTypeInt32())
    { }

    ~SortingData() {
        delete struc;
    }
};

struct NestedLoopsJoinData : OperatorData
{
    std::vector<WasmStruct*> strucs;
    std::vector<WasmVariable> begins;
    std::vector<WasmVariable> ends;
    std::size_t active_child;

    NestedLoopsJoinData(FunctionBuilder &fn) { }

    ~NestedLoopsJoinData() {
        for (auto struc : strucs)
            delete struc;
    }
};

struct SimpleHashJoinData : OperatorData
{
    WasmStruct *struc = nullptr;
    WasmHashTable *HT = nullptr;
    WasmVariable watermark_high; ///< stores the maximum number of entries before growing the table is required
    bool is_build_phase = true;

    SimpleHashJoinData(FunctionBuilder &fn)
        : watermark_high(fn, BinaryenTypeInt32())
    { }

    ~SimpleHashJoinData() {
        delete HT;
        delete struc;
    }
};

}


/*======================================================================================================================
 * WasmPlanCG
 *====================================================================================================================*/

void WasmPlanCG::operator()(const ScanOperator &op)
{
    std::ostringstream oss;
    oss << "scan_" << op.alias();
    const std::string name = oss.str();
    module().main().block() += WasmPipelineCG::compile(op, *this, name.c_str());
}

void WasmPlanCG::operator()(const CallbackOperator &op) { (*this)(*op.child(0)); }

void WasmPlanCG::operator()(const PrintOperator &op) { (*this)(*op.child(0)); }

void WasmPlanCG::operator()(const NoOpOperator &op) { (*this)(*op.child(0)); }

void WasmPlanCG::operator()(const FilterOperator &op) { (*this)(*op.child(0)); }

void WasmPlanCG::operator()(const JoinOperator &op)
{
    switch (op.algo()) {
        default:
            M_unreachable("Undefined join algorithm.");

        case JoinOperator::J_Undefined:
        case JoinOperator::J_NestedLoops: {
            auto num_children = op.children().size();
            auto data = new NestedLoopsJoinData(module().main());
            data->strucs.reserve(num_children - 1);
            data->begins.reserve(num_children - 1);
            data->ends.reserve(num_children - 1);
            op.data(data);

            /* Process all children but the right-most one. */
            for (std::size_t i = 0; i != num_children - 1; ++i) {
                auto &c = *op.child(i);
                data->strucs.emplace_back(new WasmStruct(c.schema()));
                data->begins.emplace_back(module().main(), BinaryenTypeInt32());
                data->active_child = i;
                (*this)(c);

                /* Save the current head of heap as the end of the data to join. */
                data->ends.emplace_back(module().main(), BinaryenTypeInt32());
                module().main().block()
                    << data->ends[i].set(module().head_of_heap())
                    << module().align_head_of_heap();
            }

            /* Process right-most child. */
            data->active_child = num_children - 1;
            (*this)(*op.child(num_children - 1));

            break;
        }

        case JoinOperator::J_SimpleHashJoin: {
            M_insist(op.children().size() == 2, "SimpleHashJoin is a binary operation and expects exactly two children");
            auto &build = *op.child(0);
            auto &probe = *op.child(1);

            auto data = new SimpleHashJoinData(module().main());
            op.data(data);

            (*this)(build);

#if 0
            /*----- Verify the number of entries in the hash table. --------------------------------------------------*/
            /* Compute end of HT. */
            auto HT = as<WasmRefCountingHashTable>(data->HT);
            auto b_table_size = BinaryenBinary(module(), BinaryenAddInt32(), HT->mask(),
                                               BinaryenConst(module(), BinaryenLiteralInt32(1)));
            auto b_table_size_in_bytes = BinaryenBinary(module(), BinaryenMulInt32(), b_table_size,
                                                        BinaryenConst(module(), BinaryenLiteralInt32(HT->entry_size())));
            WasmVariable table_end(fn(), BinaryenTypeInt32());
            table_end.set(fn().block(), BinaryenBinary(module(), BinaryenAddInt32(), HT->addr(), b_table_size_in_bytes));

            /* Count occupied slots in HT. */
            WasmVariable counter(fn(), BinaryenTypeInt32());
            WasmVariable runner(fn(), BinaryenTypeInt32());
            runner.set(fn().block(), HT->addr());
            WasmWhile for_each(module(), "SHJ.for_each", BinaryenBinary(module(), BinaryenLtUInt32(), runner, table_end));
            auto b_is_occupied = BinaryenBinary(module(), BinaryenNeInt32(), HT->get_bucket_ref_count(runner), BinaryenConst(module(), BinaryenLiteralInt32(0)));
            counter.set(for_each, BinaryenBinary(module(), BinaryenAddInt32(), counter, b_is_occupied));
            runner.set(for_each, BinaryenBinary(module(), BinaryenAddInt32(), runner, BinaryenConst(module(), BinaryenLiteralInt32(HT->entry_size()))));
            fn().block() += for_each.finalize();

            /* Print number of occupied slots. */
            {
                BinaryenExpressionRef args[] = { counter() };
                fn().block() += BinaryenCall(module(), "print", args, 1, BinaryenTypeNone());
            }
#endif

            data->is_build_phase = false;
            (*this)(probe);
        }
    }
}

void WasmPlanCG::operator()(const ProjectionOperator &op)
{
    if (op.children().size())
        (*this)(*op.child(0));
    else
        module().main().block() += WasmPipelineCG::compile(op, *this, "projection");
}

void WasmPlanCG::operator()(const LimitOperator &op) { (*this)(*op.child(0)); }

void WasmPlanCG::operator()(const GroupingOperator &op)
{
    auto data = new GroupingData(module().main());
    op.data(data);

    data->needs_running_count = false;
    for (auto agg : op.aggregates()) {
        auto fe = as<const FnApplicationExpr>(agg);
        switch (fe->get_function().fnid) {
            default:
                continue;

            case Function::FN_AVG:
                data->needs_running_count = true;
                goto exit;
        }
    }
exit:

    if (data->needs_running_count)
        data->struc = new WasmStruct(op.schema(), { Type::Get_Integer(Type::TY_Scalar, 4) }); // with counter
    else
        data->struc = new WasmStruct(op.schema()); // without counter

    (*this)(*op.child(0));

    /*----- Create new pipeline starting at `GroupingOperator`. ------------------------------------------------------*/
    auto HT = as<WasmRefCountingHashTable>(data->HT);
    WasmPipelineCG pipeline(*this);

    /*----- Initialize runner at start of hash table. ----------------------------------------------------------------*/
    WasmVariable induction(module().main(), BinaryenTypeInt32());
    module().main().block() += induction.set(HT->addr());

    /*----- Compute end of hash table. -------------------------------------------------------------------------------*/
    WasmVariable end(module().main(), BinaryenTypeInt32());
    WasmTemporary size = BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ HT->mask(),
        /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(1))
    );
    WasmTemporary size_in_bytes = BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenMulInt32(),
        /* left=   */ size,
        /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(HT->entry_size()))
    );
    module().main().block() += end.set(BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ HT->addr(),
        /* right=  */ size_in_bytes
    ));

    /*----- Advance to first occupied slot. --------------------------------------------------------------------------*/
    {
        WasmLoop advance_to_first(module(), "group_by.advance_to_first");
        advance_to_first += induction.set(data->HT->compute_next_slot(induction));
        WasmTemporary is_in_bounds = BinaryenBinary(
            /* module= */ module(),
            /* op=     */ BinaryenLtUInt32(),
            /* left=   */ induction,
            /* right=  */ end
        );
        WasmTemporary is_slot_empty = data->HT->is_slot_empty(induction);
        advance_to_first += BinaryenIf(
            /* module=    */ module(),
            /* condition= */ is_in_bounds,
            /* ifTrue=    */ advance_to_first.continu(is_slot_empty.clone(module())),
            /* ifFalse=   */ nullptr
        );
        module().main().block() += BinaryenIf(
            /* module=    */ module(),
            /* condition= */ is_slot_empty.clone(module()),
            /* ifTrue=    */ advance_to_first.finalize(),
            /* ifFalse=   */ nullptr
        );
    }

    /*----- Iterate over all groups. ---------------------------------------------------------------------------------*/
    WasmWhile loop(module(), "group_by.foreach", BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenLtUInt32(),
        /* left=   */ induction,
        /* right=  */ end
    ));

    /*----- Emit code for data accesses. -----------------------------------------------------------------------------*/
    auto ld = data->HT->load_from_slot(induction);
    for (auto e : op.schema())
        pipeline.context().add(e.id, ld.get_value(e.id));

    /*----- Emit code for rest of the pipeline. ----------------------------------------------------------------------*/
    swap(pipeline.block_, loop);
    pipeline(*op.parent());
    swap(pipeline.block_, loop);

    /*----- Increment induction variable, then advance to next occupied slot. ----------------------------------------*/
    {
        WasmLoop advance(module(), "group_by.foreach.advace");
        advance += induction.set(data->HT->compute_next_slot(induction));
        WasmTemporary is_in_bounds = BinaryenBinary(
            /* module= */ module(),
            /* op=     */ BinaryenLtUInt32(),
            /* left=   */ induction,
            /* right=  */ end
        );
        WasmTemporary is_slot_empty = data->HT->is_slot_empty(induction);
        advance += BinaryenIf(
            /* module=    */ module(),
            /* condition= */ is_in_bounds,
            /* ifTrue=    */ advance.continu(std::move(is_slot_empty)),
            /* ifFalse=   */ nullptr
        );
        loop += advance.finalize();
    }

    pipeline.block_ += loop.finalize();
    module().main().block() += pipeline.block_.finalize();
}

void WasmPlanCG::operator()(const AggregationOperator &op)
{
    op.data(new AggregationData(module().main(), op));
    auto data = as<AggregationData>(op.data());

    (*this)(*op.child(0));

    WasmPipelineCG pipeline(*this);
    for (std::size_t i = 0; i != op.schema().num_entries(); ++i) {
        pipeline.context().add(op.schema()[i].id, data->aggregates[i]);
    }

    pipeline(*op.parent());
    module().main().block() += pipeline.block_.finalize();
}

void WasmPlanCG::operator()(const SortingOperator &op)
{
    auto &schema = op.child(0)->schema();
    auto data = new SortingData(module().main());
    data->struc = new WasmStruct(schema);
    op.data(data);

    (*this)(*op.child(0));

    /* Save the current head of heap as the end of the data to sort. */
    WasmVariable data_end(module().main(), BinaryenTypeInt32());
    module().main().block()
        << data_end.set(module().head_of_heap())
        << module().align_head_of_heap();

    /*----- Generate sorting algorithm and invoke with start and end of data segment. --------------------------------*/
    WasmQuickSort qsort(module(), op.order_by(), WasmPartitionBranchless{});
    BinaryenExpressionRef qsort_args[] = { data->begin, data_end };
    WasmStructCGContext context(*data->struc);
    for (std::size_t idx = 0; idx != schema.num_entries(); ++idx)
        context.add(schema[idx].id, idx);
    BinaryenFunctionRef b_qsort = qsort.emit(context);
    module().main().block() += BinaryenCall(
        /* module=      */ module(),
        /* target=      */ BinaryenFunctionGetName(b_qsort),
        /* operands=    */ qsort_args,
        /* numOperands= */ 2,
        /* returnType=  */ BinaryenTypeNone()
    );

    /*----- Create new pipeline starting at `SortingOperator`. -------------------------------------------------------*/
    WasmPipelineCG pipeline(*this);
    WasmWhile loop(module(), "order_by.foreach", BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenLtUInt32(),
        /* left=   */ data->begin,
        /* right=  */ data_end
    ));

    /*----- Emit code for data accesses. -----------------------------------------------------------------------------*/
    auto ld = data->struc->create_load_context(module().main(), data->begin);
    for (auto &attr : op.child(0)->schema())
        pipeline.context().add(attr.id, ld.get_value(attr.id));

    /*----- Emit code for rest of the pipeline. ----------------------------------------------------------------------*/
    swap(pipeline.block_, loop);
    pipeline(*op.parent());
    swap(pipeline.block_, loop);

    /*----- Increment induction variable. ----------------------------------------------------------------------------*/
    loop += data->begin.set(BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ data->begin,
        /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(data->struc->size_in_bytes()))
    ));

    module().main().block() += loop.finalize();
}


/*======================================================================================================================
 * WasmPipelineCG
 *====================================================================================================================*/

void WasmPipelineCG::emit_write_results(const Schema &schema)
{
    auto deduplicated_schema = schema.deduplicate();
    const WasmStruct layout(deduplicated_schema); // create Wasm struct used as data layout
    const WasmVariable &out = module().head_of_heap();
    std::size_t idx = 0;
    for (auto &attr : deduplicated_schema)
        layout.store(module().main(), block_, out, idx++, context()[attr.id]);

    WasmTemporary upd_out = BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ out,
        /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(layout.size_in_bytes()))
    );
    block_ += module().head_of_heap().set(std::move(upd_out));
}

void WasmPipelineCG::operator()(const ScanOperator &op)
{
    std::ostringstream oss;
    auto &table = op.store().table();
    auto &lin = op.store().linearization();

    /*----- Get the number of rows in the scanned table. -------------------------------------------------------------*/
    oss << table.name << "_num_rows";
    module().import(oss.str(), BinaryenTypeInt32());
    WasmTemporary num_rows = module().get_imported(oss.str(), BinaryenTypeInt32());

    /*----- Get the base addresses of the mapped memories. -----------------------------------------------------------*/
    std::vector<WasmTemporary> base_addresses;
    for (std::size_t idx = 0; idx < lin.num_sequences(); ++idx) {
        oss.str("");
        oss << table.name << "_mem_" << idx;
        module().import(oss.str(), BinaryenTypeInt32());
        base_addresses.push_back(module().get_imported(oss.str(), BinaryenTypeInt32()));
    }

    /*----- Generate code to access attributes and emit code for the rest of the pipeline. ---------------------------*/
    WasmStoreCG::compile_load(*this, op, lin, std::move(num_rows), base_addresses);
}

void WasmPipelineCG::operator()(const CallbackOperator &op)
{
    emit_write_results(op.schema());
}

void WasmPipelineCG::operator()(const PrintOperator &op)
{
#if 0
    /* Callback per result tuple. */
    std::vector<BinaryenExpressionRef> args;
    for (auto &e : op.schema()) {
        auto it = intermediates_.find(e.id);
        M_insist(it != intermediates_.end(), "unknown identifier");
        args.push_back(it->second);
    }
    block_ += BinaryenCall(
        /* module=      */ module(),
        /* target=      */ "print",
        /* operands=    */ &args[0],
        /* numOperands= */ args.size(),
        /* returnType=  */ BinaryenTypeNone()
    );
#else
    emit_write_results(op.schema());
#endif
    block_ += module().inc_num_tuples();
    // TODO issue a callback whenever NUM_TUPLES_OUTPUT_BUFFER tuples have been written, and reset counter
}

void WasmPipelineCG::operator()(const NoOpOperator&) { block_ += module().inc_num_tuples(); }

void WasmPipelineCG::operator()(const FilterOperator &op)
{
    BlockBuilder then_block(module(), "filter.accept");
    swap(this->block_, then_block);
    (*this)(*op.parent());
    swap(this->block_, then_block);

    block_ += BinaryenIf(
        /* module=  */ module(),
        /* cond=    */ context().compile(block_, op.filter()),
        /* ifTrue=  */ then_block.finalize(),
        /* ifFalse= */ nullptr
    );
}

void WasmPipelineCG::operator()(const JoinOperator &op)
{
    switch (op.algo()) {
        default:
            M_unreachable("Undefined join algorithm.");

        case JoinOperator::J_Undefined:
        case JoinOperator::J_NestedLoops: {
            auto num_children = op.children().size();
            auto data = as<NestedLoopsJoinData>(op.data());
            auto active_child = data->active_child;

            if (active_child == num_children - 1) {
                /* This is the right-most child. Combine its produced entry with all combinations of the buffered
                 * data. */

                /*----- Create variables to keep track of current positions in the buffered data. --------------------*/
                std::vector<WasmVariable> positions;
                positions.reserve(num_children - 1);
                for (std::size_t i = 0; i != active_child; ++i) {
                    positions.emplace_back(module().main(), BinaryenTypeInt32());
                    block_ += positions[i].set(data->begins[i]);
                }

                /*----- Create innermost loop. -----------------------------------------------------------------------*/
                std::size_t current_child = 0;
                M_insist(current_child < active_child);
                WasmWhile innermost(module(), "join.loop.child0", BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenLtUInt32(),
                    /* left=   */ positions[current_child],
                    /* right=  */ data->ends[current_child]
                ));

                /* Add entries from other children to context. */
                for (std::size_t i = 0; i != active_child; ++i) {
                    std::size_t idx = 0;
                    for (auto &attr : op.child(i)->schema()) {
                        context().add(attr.id, data->strucs[i]->load(module().main(), positions[i], idx));
                        ++idx;
                    }
                }

                /* Check whether combined entry is a match. If yes, resume with pipeline. */
                BlockBuilder match(module(), "join.match");
                swap(block_, match);
                (*this)(*op.parent());
                swap(block_, match);

                innermost += BinaryenIf(
                    /* module=  */ module(),
                    /* cond=    */ context().compile(block_, op.predicate()),
                    /* ifTrue=  */ match.finalize(),
                    /* ifFalse= */ nullptr
                );

                /* Advance position in buffered data to next entry for this child. */
                auto entry_size = data->strucs[current_child]->size_in_bytes();
                innermost += positions[current_child].set(BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenAddInt32(),
                    /* left=   */ positions[current_child],
                    /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(entry_size))
                ));

                WasmWhile *last_loop = &innermost; // to keep track of last inner loop
                ++current_child; // go to next child

                /*----- Create outer loops. --------------------------------------------------------------------------*/
                std::ostringstream oss;
                while (current_child != active_child) {
                    /* Reset position in buffered data for last child. */
                    *last_loop += positions[current_child - 1].set(data->begins[current_child - 1]);

                    /* Create next outer loop. */
                    oss.str("");
                    oss << "join.loop.child" << current_child;
                    WasmWhile current_loop(module(), oss.str().c_str(), BinaryenBinary(
                        /* module= */ module(),
                        /* op=     */ BinaryenLtUInt32(),
                        /* left=   */ positions[current_child],
                        /* right=  */ data->ends[current_child]
                    ));

                    /* Advance position in buffered data to next entry for this child. */
                    auto entry_size = data->strucs[current_child]->size_in_bytes();
                    current_loop += positions[current_child].set(BinaryenBinary(
                        /* module= */ module(),
                        /* op=     */ BinaryenAddInt32(),
                        /* left=   */ positions[current_child],
                        /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(entry_size))
                    ));

                    current_loop += last_loop->finalize();
                    last_loop = &current_loop;
                    ++current_child; // go to next child
                }

                /*----- Add outermost loop to the current block. -----------------------------------------------------*/
                block_ += last_loop->finalize();
            } else {
                /* This is not the right-most child. Save the current head of heap as the beginning of the data to
                 * join. */
                module().main().block() += data->begins[active_child].set(module().head_of_heap());

                /* Save the current results to the designated heap location. */
                std::size_t idx = 0;
                for (auto &attr : op.child(active_child)->schema())
                    data->strucs[active_child]->store(module().main(), block_, module().head_of_heap(),
                                                      idx++, context()[attr.id]);

                /* Advance head of heap. */
                block_ += module().head_of_heap().set(BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenAddInt32(),
                    /* left=   */ module().head_of_heap(),
                    /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(data->strucs[active_child]->size_in_bytes()))
                ));
            }
            break;
        }

        case JoinOperator::J_SimpleHashJoin: {
            WasmHashMumur3_64A hasher;
            auto data = as<SimpleHashJoinData>(op.data());

            auto &build_schema = op.child(0)->schema();
            auto &probe_schema = op.child(1)->schema();

            /*----- Decompose the join predicate of the form `A.x = B.y` into parts `A.x` and `B.y`. -----------------*/
            auto &pred = op.predicate();
            M_insist(pred.size() == 1, "invalid predicate for simple hash join");
            auto &clause = pred[0];
            M_insist(clause.size() == 1, "invalid predicate for simple hash join");
            auto &literal = clause[0];
            M_insist(not literal.negative(), "invalid predicate for simple hash join");
            auto binary = as<const BinaryExpr>(literal.expr());
            M_insist(binary->tok == TK_EQUAL, "invalid predicate for simple hash join");
            auto first = as<const Designator>(binary->lhs);
            auto second = as<const Designator>(binary->rhs);
            auto [build, probe] = build_schema.has({first->get_table_name(), first->attr_name.text}) ?
                                  std::make_pair(first, second) : std::make_pair(second, first);

            Schema::Identifier build_key_id(build->table_name.text, build->attr_name.text);
            Schema::Identifier probe_key_id(probe->table_name.text, probe->attr_name.text);

            if (data->is_build_phase) {
                data->struc = new WasmStruct(build_schema);
                std::vector<WasmTemporary> key;
                key.emplace_back(context().get_value(build_key_id));

                /*----- Compute payload ids. -------------------------------------------------------------------------*/
                std::vector<Schema::Identifier> payload_ids;
                for (auto attr : build_schema) {
                    if (attr.id != build_key_id)
                        payload_ids.push_back(attr.id);
                }

                /* Get key field index. */
                auto [key_index, _] = build_schema[build_key_id];

                /*----- Allocate hash table. -------------------------------------------------------------------------*/
                bool knows_capacity = false;
                std::size_t required_capacity = 0;

                if (op.child(0)->has_info()) {
                    required_capacity = op.child(0)->info().estimated_cardinality;
                    knows_capacity = true;
                }

                if (not knows_capacity) {
                    if (auto scan = cast<ScanOperator>(op.child(0))) { /// XXX: hack for pre-allocation
                        required_capacity = scan->store().num_rows();
                        knows_capacity = true;
                    }
                }

                if (not knows_capacity) {
                    required_capacity = 1024; // fallback
                    knows_capacity = true;
                }

                M_insist(knows_capacity, "at this point we must have determined an initial capacity for the hash table");
                std::size_t initial_capacity = ceil_to_pow_2<std::size_t>(required_capacity);
                if (required_capacity >= initial_capacity * .7)
                    initial_capacity *= 2;

                data->HT = new WasmRefCountingHashTable(module(), module().main(), *data->struc,
                                                        { WasmStruct::index_type(key_index) });
                auto HT = as<WasmRefCountingHashTable>(data->HT);

                WasmVariable end(module().main(), BinaryenTypeInt32());
                module().main().block() += end.set(HT->create_table(module().main().block(), module().head_of_heap(), initial_capacity));
                module().main().block() += data->watermark_high.set(BinaryenConst(module(), BinaryenLiteralInt32(8 * initial_capacity / 10)));

                /*----- Update head of heap. -------------------------------------------------------------------------*/
                module().main().block() += module().head_of_heap().set(end);
                module().main().block() += module().align_head_of_heap();

                /*----- Initialize hash table. -----------------------------------------------------------------------*/
                HT->clear_table(module().main().block(), HT->addr(), end);

                /*----- Create a counter for the number of entries. --------------------------------------------------*/
                WasmVariable num_entries(module().main(), BinaryenTypeInt32());
                module().main().block() += num_entries.set(BinaryenConst(module(), BinaryenLiteralInt32(0)));

                /*----- Create code block to resize the hash table when the high watermark is reached. ---------------*/
                BlockBuilder perform_rehash(module(), "join.build.rehash");

                /*----- Compute the mask for the new size. -----------------------------------------------------------*/
                WasmTemporary mask_x2 = BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenShlInt32(),
                    /* left=   */ HT->mask(),
                    /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(1))
                );
                WasmVariable mask_new(module().main(), BinaryenTypeInt32());
                perform_rehash += mask_new.set(BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenAddInt32(),
                    /* left=   */ mask_x2,
                    /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(1))
                ));

                /*----- Compute new hash table size. -----------------------------------------------------------------*/
                WasmTemporary size_new = BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenAddInt32(),
                    /* left=   */ mask_new,
                    /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(1))

                );
                WasmTemporary size_in_bytes_new = BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenMulInt32(),
                    /* left=   */ size_new.clone(module()),
                    /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(HT->entry_size()))
                );

                /*----- Allocate new hash table. ---------------------------------------------------------------------*/
                WasmVariable begin_new(module().main(), BinaryenTypeInt32());
                perform_rehash += begin_new.set(module().head_of_heap());
                perform_rehash += module().head_of_heap().set(BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenAddInt32(),
                    /* left=   */ begin_new,
                    /* right=  */ size_in_bytes_new
                ));
                perform_rehash += module().align_head_of_heap();

                /*----- Rehash to double size. -----------------------------------------------------------------------*/
                BinaryenFunctionRef b_fn_rehash = HT->rehash(hasher);
                BinaryenExpressionRef rehash_args[4] = {
                    /* addr_old= */ HT->addr(),
                    /* mask_old= */ HT->mask(),
                    /* addr_new= */ begin_new,
                    /* mask_new= */ mask_new
                };
                perform_rehash += BinaryenCall(
                    /* module=      */ module(),
                    /* target=      */ BinaryenFunctionGetName(b_fn_rehash),
                    /* operands=    */ rehash_args,
                    /* numOperands= */ M_ARR_SIZE(rehash_args),
                    /* returnType=  */ BinaryenTypeNone()
                );

                /*----- Update hash table. ---------------------------------------------------------------------------*/
                perform_rehash += HT->addr().set(begin_new);
                perform_rehash += HT->mask().set(mask_new);

                /*----- Update high watermark. -----------------------------------------------------------------------*/
                perform_rehash += data->watermark_high.set(BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenDivUInt32(),
                    /* left=   */ BinaryenBinary(
                                      /* module= */ module(),
                                      /* op=     */ BinaryenMulInt32(),
                                      /* left=   */ size_new,
                                      /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(8))
                    ),
                    /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(10))
                ));

                /*----- Perform resizing to twice the capacity when the high watermark is reached. -------------------*/
                WasmTemporary has_reached_watermark = BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenGeUInt32(),
                    /* left=   */ num_entries,
                    /* right=  */ data->watermark_high
                );
                block_ += BinaryenIf(
                    /* module=    */ module(),
                    /* condition= */ has_reached_watermark,
                    /* ifTrue=    */ perform_rehash.finalize(),
                    /* ifFalse=   */ nullptr
                );

                /*----- Compute hash. --------------------------------------------------------------------------------*/
                std::vector<WasmHash::element_type> hash_values;
                hash_values.emplace_back(key[0].clone(module()), *build->type());
                WasmTemporary hash = hasher.emit(module(), module().main(), block_, hash_values);
                WasmTemporary hash_i32 = BinaryenUnary(
                    /* module= */ module(),
                    /* op=     */ BinaryenWrapInt64(),
                    /* value=  */ hash
                );

                /*----- Insert the element. --------------------------------------------------------------------------*/
                WasmVariable slot_addr(module().main(), BinaryenTypeInt32());
                block_ += slot_addr.set(HT->insert_with_duplicates(block_, std::move(hash_i32), key));

                /*---- Write payload. --------------------------------------------------------------------------------*/
                for (std::size_t i = 0; i != build_schema.num_entries(); ++i) {
                    if (i != key_index) {
                        auto &e = build_schema[i];
                        HT->store_value_to_slot(block_, slot_addr, i, context().get_value(e.id));
                    }
                }

                block_ += num_entries.set(BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenAddInt32(),
                    /* left=   */ num_entries,
                    /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(1))
                ));
            } else {
                auto HT = as<WasmRefCountingHashTable>(data->HT);
                std::vector<WasmTemporary> key;
                key.emplace_back(context().get_value(probe_key_id));

                /*----- Compute hash. --------------------------------------------------------------------------------*/
                std::vector<WasmHash::element_type> hash_values;
                hash_values.emplace_back(key[0].clone(module()), *probe->type());
                WasmTemporary hash = hasher.emit(module(), module().main(), block_, hash_values);
                WasmTemporary hash_i32 = BinaryenUnary(
                    /* module= */ module(),
                    /* op=     */ BinaryenWrapInt64(),
                    /* value=  */ hash
                );

                /*----- Compute address of bucket. -------------------------------------------------------------------*/
                WasmVariable bucket_addr(module().main(), BinaryenTypeInt32());
                block_ += bucket_addr.set(HT->hash_to_bucket(std::move(hash_i32)));

                /*----- Iterate over all entries in the bucket and compare the key. ----------------------------------*/
                WasmVariable slot_addr(module().main(), BinaryenTypeInt32());
                block_ += slot_addr.set(bucket_addr);
                WasmVariable step(module().main(), BinaryenTypeInt32());
                block_ += step.set(BinaryenConst(module(), BinaryenLiteralInt32(0)));
                WasmTemporary ref_count = HT->get_bucket_ref_count(bucket_addr);
                WasmWhile loop(module(), "join.probe", BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenLtUInt32(),
                    /* left=   */ step,
                    /* right=  */ ref_count
                ));

                /*----- Load values from HT. -------------------------------------------------------------------------*/
                auto ld = HT->load_from_slot(slot_addr);
                for (auto &attr : build_schema)
                    context().add(attr.id, ld.get_value(attr.id));

                /*----- Check whether the key of the entry in the bucket -- identified by `build_key_id` -- equals the
                 * probe key. ----------------------------------------------------------------------------------------*/
                WasmTemporary is_key_equal = HT->compare_key(block_, slot_addr, key);
                BlockBuilder keys_equal(module(), "join.probe.match");

                swap(block_, keys_equal);
                (*this)(*op.parent());
                swap(block_, keys_equal);

                loop += BinaryenIf(
                    /* module=    */ module(),
                    /* condition= */ is_key_equal,
                    /* ifTrue=    */ keys_equal.finalize(),
                    /* ifFalse=   */ nullptr
                );

                /*----- Compute size and end of table to handle wrap around. -----------------------------------------*/
                WasmTemporary size = BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenAddInt32(),
                    /* left=   */ HT->mask(),
                    /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(1))
                );
                WasmVariable table_size_in_bytes(module().main(), BinaryenTypeInt32());
                block_ += table_size_in_bytes.set(BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenMulInt32(),
                    /* left=   */ size,
                    /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(HT->entry_size()))
                ));
                WasmTemporary table_end = BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenAddInt32(),
                    /* left=   */ HT->addr(),
                    /* right=  */ table_size_in_bytes
                );

                loop += step.set(BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenAddInt32(),
                    /* left=   */ step,
                    /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(HT->entry_size()))
                ));
                WasmVariable slot_addr_inc(module().main(), BinaryenTypeInt32());
                loop += slot_addr_inc.set(BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenAddInt32(),
                    /* left=   */ slot_addr,
                    /* right=  */ step
                ));
                WasmTemporary exceeds_table = BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenGeUInt32(),
                    /* left=   */ slot_addr_inc,
                    /* right=  */ table_end
                );
                WasmTemporary slot_addr_wrapped = BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenSubInt32(),
                    /* left=   */ slot_addr_inc,
                    /* right=  */ table_size_in_bytes
                );
                loop += slot_addr.set(BinaryenSelect(
                    /* module=    */ module(),
                    /* condition= */ exceeds_table,
                    /* ifTrue=    */ slot_addr_wrapped,
                    /* ifFalse=   */ slot_addr_inc,
                    /* type=      */ BinaryenTypeInt32()
                ));
                block_ += loop.finalize();
            }
        }
    }
}

void WasmPipelineCG::operator()(const ProjectionOperator &op)
{
    WasmEnvironment new_context(module().main());
    M_insist(op.projections().size() == op.schema().num_entries(), "projections must match the operator's schema");
    auto p = op.projections().begin();
    for (auto &e : op.schema()) {
        if (not new_context.has(e.id)) {
            if (context().has(e.id)) { // migrate compiled expression to new context
                new_context.add(e.id, context().get_value(e.id));
            } else {
                M_insist(p != op.projections().end());
                if (auto d = cast<const Designator>(p->first)) {
                    auto t = d->target(); // consider target of renamed identifier
                    if (auto expr = std::get_if<const Expr *>(&t)) {
                        new_context.add(e.id, context().compile(block_, **expr));
                    } else { // access renamed attribute
                        auto attr = std::get_if<const Attribute *>(&t);
                        M_insist(attr, "Target must be an expression or an attribute");
                        new_context.add(e.id, context().get_value({(*attr)->table.name, (*attr)->name}));
                    }
                } else { // compile entire expression
                    new_context.add(e.id, context().compile(block_, *p->first));
                }
            }
        }
        ++p;
    }
    swap(context(), new_context);
    (*this)(*op.parent());
    swap(context(), new_context);
}

void WasmPipelineCG::operator()(const LimitOperator &op)
{
    /* Emit code for the rest of the pipeline. */
    BlockBuilder within_limits_block(module(), "limit.within");
    swap(this->block_, within_limits_block);
    (*this)(*op.parent());
    swap(this->block_, within_limits_block);

    /* Declare a new counter.  Will be initialized to 0. */
    WasmVariable count(module().main(), BinaryenTypeInt32());

    /* Check whether the pipeline has exceeded the limit. */
    WasmTemporary cond_exceeds_limits = BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenGeSInt32(),
        /* lhs=    */ count,
        /* rhs=    */ BinaryenConst(module(), BinaryenLiteralInt32(op.limit() + op.offset()))
    );

    /* Abort the pipeline once the limit is exceeded. */
    block_ += BinaryenBreak(
        /* module= */ module(),
        /* name=   */ block_.name(),
        /* cond=   */ cond_exceeds_limits,
        /* value=  */ nullptr
    );

    /* Check whether the pipeline is within the limits. */
    WasmTemporary cond_within_limits = BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenGeSInt32(),
        /* lhs=    */ count,
        /* rhs=    */ BinaryenConst(module(), BinaryenLiteralInt32(op.offset()))
    );
    block_ += BinaryenIf(
        /* module=  */ module(),
        /* cond=    */ cond_within_limits,
        /* ifTrue=  */ within_limits_block.finalize(),
        /* ifFalse= */ nullptr
    );

    block_ += count.set(BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenAddInt32(),
        /* lhs=    */ count,
        /* rhs=    */ BinaryenConst(module(), BinaryenLiteralInt32(1))
    ));
}

void WasmPipelineCG::operator()(const GroupingOperator &op)
{
    WasmHashMumur3_64A hasher;
    auto data = as<GroupingData>(op.data());

    std::vector<Schema::Identifier> key_IDs;
    std::vector<WasmStruct::index_type> key_indizes;
    for (std::size_t i = 0; i != op.group_by().size(); ++i) {
        key_IDs.push_back(op.schema()[i].id);
        key_indizes.push_back(i);
    }

    /*----- Allocate hash table. -------------------------------------------------------------------------------------*/
    bool knows_capacity = false;
    std::size_t required_capacity = 0;

    if (op.has_info()) {
        required_capacity = op.info().estimated_cardinality;
        knows_capacity = true;
    }

    if (not knows_capacity) {
        /*--- XXX: Crude hack to do pre-allocation in benchmarks. ----------------------------------------------------*/
        std::regex reg_is_distinct("n\\d+");
        std::size_t num_groups_est = 1;
        for (auto expr : op.group_by()) {
            if (auto d = cast<const Designator>(expr)) {
                if (std::regex_match(d->attr_name.text, reg_is_distinct)) {
                    errno = 0;
                    unsigned num_distinct_values = strtol(d->attr_name.text + 1, nullptr, 10);
                    if (not errno) {
                        num_groups_est = mul_wo_overflow(num_groups_est, num_distinct_values);
                        continue;
                    }
                }
            }
            goto estimation_failed;
        }

        required_capacity = num_groups_est;
        knows_capacity = true;
estimation_failed:;
    }

    if (not knows_capacity) {
        required_capacity = 20; // fallback
        knows_capacity = true;
    }

    M_insist(knows_capacity, "at this point we must have determined an initial capacity for the hash table");
    std::size_t initial_capacity = ceil_to_pow_2<std::size_t>(required_capacity);
    if (required_capacity >= initial_capacity * .7)
        initial_capacity *= 2;

    data->HT = new WasmRefCountingHashTable(module(), module().main(), *data->struc, std::move(key_indizes));
    auto HT = as<WasmRefCountingHashTable>(data->HT);

    WasmVariable end(module().main(), BinaryenTypeInt32());
    module().main().block() += end.set(HT->create_table(module().main().block(), module().head_of_heap(), initial_capacity));
    module().main().block() += data->watermark_high.set(BinaryenConst(module(), BinaryenLiteralInt32(8 * initial_capacity / 10)));

    /*----- Update head of heap. -------------------------------------------------------------------------------------*/
    module().main().block() += module().head_of_heap().set(end);
    module().main().block() += module().align_head_of_heap();

    /*----- Initialize hash table. -----------------------------------------------------------------------------------*/
    HT->clear_table(module().main().block(), HT->addr(), end);

    /*----- Create a counter for the number of groups. ---------------------------------------------------------------*/
    WasmVariable num_groups(module().main(), BinaryenTypeInt32());
    module().main().block() += num_groups.set(BinaryenConst(module(), BinaryenLiteralInt32(0)));

    /*----- Create code block to resize the hash table. --------------------------------------------------------------*/
    BlockBuilder perform_rehash(module(), "group_by.rehash");
    {
        /*----- Compute the mask for the new size. -------------------------------------------------------------------*/
        WasmTemporary mask_x2 = BinaryenBinary(
            /* module= */ module(),
            /* op=     */ BinaryenShlInt32(),
            /* left=   */ HT->mask(),
            /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(1))
        );
        WasmVariable new_HT_mask(module().main(), BinaryenTypeInt32());
        perform_rehash += new_HT_mask.set(BinaryenBinary(
            /* module= */ module(),
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ mask_x2,
            /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(1))
        ));

        /*----- Compute new hash table size. -------------------------------------------------------------------------*/
        WasmVariable new_HT_size(module().main(), BinaryenTypeInt32());
        perform_rehash += new_HT_size.set(BinaryenBinary(
            /* module= */ module(),
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ new_HT_mask,
            /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(1))

        ));
        WasmTemporary new_HT_size_in_bytes = BinaryenBinary(
            /* module= */ module(),
            /* op=     */ BinaryenMulInt32(),
            /* left=   */ new_HT_size,
            /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(HT->entry_size()))
        );

        /*----- Allocate new hash table. -----------------------------------------------------------------------------*/
        WasmVariable new_HT_addr(module().main(), BinaryenTypeInt32());
        perform_rehash += new_HT_addr.set(module().head_of_heap());
        perform_rehash += module().head_of_heap().set(BinaryenBinary(
            /* module= */ module(),
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ new_HT_addr,
            /* right=  */ new_HT_size_in_bytes
        ));
        perform_rehash += module().align_head_of_heap();

        /*----- Rehash to double size by calling rehash function. ----------------------------------------------------*/
        BinaryenFunctionRef b_fn_rehash = HT->rehash(hasher);
        BinaryenExpressionRef rehash_args[4] = {
            /* addr_old= */ HT->addr(),
            /* mask_old= */ HT->mask(),
            /* addr_new= */ new_HT_addr,
            /* mask_new= */ new_HT_mask
        };
        perform_rehash += BinaryenCall(
            /* module=      */ module(),
            /* target=      */ BinaryenFunctionGetName(b_fn_rehash),
            /* operands=    */ rehash_args,
            /* numOperands= */ M_ARR_SIZE(rehash_args),
            /* returnType=  */ BinaryenTypeNone()
        );

        /*----- Update hash table and grouping data. -----------------------------------------------------------------*/
        perform_rehash += HT->addr().set(new_HT_addr);
        perform_rehash += HT->mask().set(new_HT_mask);

        /*----- Update high watermark. -------------------------------------------------------------------------------*/
        perform_rehash += data->watermark_high.set(BinaryenBinary(
            /* module= */ module(),
            /* op=     */ BinaryenDivUInt32(),
            /* left=   */ BinaryenBinary(
                              /* module= */ module(),
                              /* op=     */ BinaryenMulInt32(),
                              /* left=   */ new_HT_size,
                              /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(8))
            ),
            /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(10))
        ));

        /*----- Perform resizing to twice the capacity when the high watermark is reached. ---------------------------*/
        WasmTemporary has_reached_watermark = BinaryenBinary(
            /* module= */ module(),
            /* op=     */ BinaryenGeUInt32(),
            /* left=   */ num_groups,
            /* right=  */ data->watermark_high
        );
        block_ += BinaryenIf(
            /* module=    */ module(),
            /* condition= */ has_reached_watermark,
            /* ifTrue=    */ perform_rehash.finalize(),
            /* ifFalse=   */ nullptr
        );
    }

    /*----- Compute grouping key. ------------------------------------------------------------------------------------*/
    std::vector<WasmTemporary> key;
    std::vector<WasmHash::element_type> hash_values;
    for (auto grp : op.group_by()) {
        WasmTemporary value = context().compile(block_, *grp);
        hash_values.emplace_back(value.clone(module()), *grp->type());
        key.emplace_back(std::move(value));
    }

    /*----- Compute hash. --------------------------------------------------------------------------------------------*/
    WasmTemporary hash = hasher.emit(module(), module().main(), block_, hash_values);
    WasmTemporary hash_i32 = BinaryenUnary(
        /* module= */ module(),
        /* op=     */ BinaryenWrapInt64(),
        /* value=  */ hash
    );

    /*----- Compute address of bucket. -------------------------------------------------------------------------------*/
    WasmVariable bucket_addr(module().main(), BinaryenTypeInt32());
    block_ += bucket_addr.set(data->HT->hash_to_bucket(std::move(hash_i32)));

    /*----- Locate entry with key `key` in the bucket, or end of bucket if no such key exists. -----------------------*/
    auto [ slot_addr_found, steps_found ] = data->HT->find_in_bucket(block_, bucket_addr, key);
    WasmVariable slot_addr(module().main(), BinaryenTypeInt32());
    block_ += slot_addr.set(std::move(slot_addr_found));

    /*----- Create or update the group. ------------------------------------------------------------------------------*/
    BlockBuilder create_group(module(), "group_by.create_group");
    BlockBuilder update_group(module(), "group_by.update");

    create_group += num_groups.set(BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ num_groups,
        /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(1))
    ));

    auto ld_slot = data->HT->load_from_slot(slot_addr);
    data->HT->emplace(create_group, bucket_addr, std::move(steps_found), slot_addr, key);

    if (data->needs_running_count) {
        data->struc->store(
            /* fn=           */ module().main(),
            /* block=        */ create_group,
            /* ptr=          */ slot_addr,
            /* idx=          */ op.schema().num_entries(),
            /* val=          */ BinaryenConst(module(), BinaryenLiteralInt32(1)),
            /* struc_offset= */ WasmRefCountingHashTable::REFERENCE_SIZE
        );

        WasmTemporary old_count = data->struc->load(
            /* fn=           */ module().main(),
            /* ptr=          */ slot_addr,
            /* idx=          */ op.schema().num_entries(),
            /* struc_offset= */ WasmRefCountingHashTable::REFERENCE_SIZE
        );
        WasmTemporary new_count = BinaryenBinary(
            /* module= */ module(),
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ old_count,
            /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(1))
        );
        data->struc->store(
            /* fn=           */ module().main(),
            /* block=        */ update_group,
            /* ptr=          */ slot_addr,
            /* idx=          */ op.schema().num_entries(),
            /* val=          */ std::move(new_count),
            /* struc_offset= */ WasmRefCountingHashTable::REFERENCE_SIZE
        );
    }

    for (std::size_t i = 0; i != op.aggregates().size(); ++i) {
        auto idx = op.group_by().size() + i;
        auto e = op.schema()[idx];
        auto agg = op.aggregates()[i];

        auto fn_expr = as<const FnApplicationExpr>(agg);
        auto &fn = fn_expr->get_function();
        M_insist(fn.kind == Function::FN_Aggregate, "not an aggregation function");

        /*----- Emit code to evaluate arguments. ---------------------------------------------------------------------*/
        M_insist(fn_expr->args.size() <= 1, "unsupported aggregate with more than one argument");
        std::vector<WasmTemporary> args;
        for (auto arg : fn_expr->args)
            args.emplace_back(context().compile(block_, *arg));

        switch (fn.fnid) {
            default:
                M_unreachable("unsupported aggregate function");

            case Function::FN_MIN: {
                M_insist(args.size() == 1, "aggregate function expects exactly one argument");
                data->HT->store_value_to_slot(create_group, slot_addr, idx, args[0].clone(module()));
                WasmVariable val(module().main(), get_binaryen_type(e.type));
                update_group += val.set(args[0].clone(module()));
                WasmVariable old_val(module().main(), get_binaryen_type(e.type));
                update_group += old_val.set(ld_slot.get_value(e.id));
                auto n = as<const Numeric>(e.type);
                switch (n->kind) {
                    case Numeric::N_Int: {
                        WasmTemporary less = BinaryenBinary(
                            /* module= */ module(),
                            /* op=     */ n->size() <= 32 ? BinaryenLtSInt32() : BinaryenLtSInt64(),
                            /* left=   */ old_val,
                            /* right=  */ val
                        );
#if 1
                        /* Compute MIN via select. */
                        WasmTemporary new_val = BinaryenSelect(
                            /* module=    */ module(),
                            /* condition= */ less,
                            /* ifTrue=    */ old_val,
                            /* ifFalse=   */ val,
                            /* type=      */ n->size() <= 32 ? BinaryenTypeInt32() : BinaryenTypeInt64()
                        );
                        data->HT->store_value_to_slot(update_group, slot_addr, idx, std::move(new_val));
#else
                        /* Compute MIN via conditional branch. */
                        WasmTemporary upd = data->HT->store_value_to_slot(slot_addr, idx, std::move(val));
                        update_group += BinaryenIf(
                            /* module=    */ module(),
                            /* condition= */ less,
                            /* ifTrue=    */ upd,
                            /* ifFalse=   */ nullptr
                        );
#endif
                        break;
                    }

                    case Numeric::N_Decimal:
                        M_unreachable("not implemented");

                    case Numeric::N_Float: {
                        WasmTemporary new_val = BinaryenBinary(
                            /* module= */ module(),
                            /* op=     */ n->size() == 32 ? BinaryenMinFloat32() : BinaryenMinFloat64(),
                            /* left=   */ old_val,
                            /* right=  */ val
                        );
                        data->HT->store_value_to_slot(update_group, slot_addr, idx, std::move(new_val));
                        break;
                    }
                }
                break;
            }

            case Function::FN_MAX: {
                M_insist(args.size() == 1, "aggregate function expects exactly one argument");
                data->HT->store_value_to_slot(create_group, slot_addr, idx, args[0].clone(module()));
                WasmVariable val(module().main(), get_binaryen_type(e.type));
                update_group += val.set(args[0].clone(module()));
                WasmVariable old_val(module().main(), get_binaryen_type(e.type));
                update_group += old_val.set(ld_slot.get_value(e.id));
                auto n = as<const Numeric>(e.type);
                switch (n->kind) {
                    case Numeric::N_Int: {
                        WasmTemporary greater = BinaryenBinary(
                            /* module= */ module(),
                            /* op=     */ n->size() <= 32 ? BinaryenGtSInt32() : BinaryenGtSInt64(),
                            /* left=   */ old_val,
                            /* right=  */ val
                        );
#if 1
                        /* Compute MAX via select. */
                        WasmTemporary new_val = BinaryenSelect(
                            /* module=    */ module(),
                            /* condition= */ greater,
                            /* ifTrue=    */ old_val,
                            /* ifFalse=   */ val,
                            /* type=      */ n->size() <= 32 ? BinaryenTypeInt32() : BinaryenTypeInt64()
                        );
                        data->HT->store_value_to_slot(update_group, slot_addr, idx, std::move(new_val));
#else
                        /* Compute MAX via conditional branch. */
                        WasmTemporary upd = data->HT->store_value_to_slot(slot_addr, idx, std::move(val));
                        update_group += BinaryenIf(
                            /* module=    */ module(),
                            /* condition= */ greater,
                            /* ifTrue=    */ upd,
                            /* ifFalse=   */ nullptr
                        );
#endif
                        break;
                    }

                    case Numeric::N_Decimal:
                        M_unreachable("not implemented");

                    case Numeric::N_Float: {
                        WasmTemporary new_val = BinaryenBinary(
                            /* module= */ module(),
                            /* op=     */ n->size() == 32 ? BinaryenMaxFloat32() : BinaryenMaxFloat64(),
                            /* left=   */ old_val,
                            /* right=  */ val
                        );
                        data->HT->store_value_to_slot(update_group, slot_addr, idx, std::move(new_val));
                        break;
                    }
                }
                break;
            }

            case Function::FN_SUM: {
                M_insist(args.size() == 1, "aggregate function expects exactly one argument");
                auto old_val = ld_slot.get_value(e.id);
                auto n = as<const Numeric>(fn_expr->args[0]->type());
                switch (n->kind) {
                    case Numeric::N_Int: {
                        WasmTemporary extd = convert(module(), std::move(args[0]), n, Type::Get_Integer(Type::TY_Vector, 8));
                        data->HT->store_value_to_slot(create_group, slot_addr, idx, extd.clone(module()));
                        WasmTemporary new_val = BinaryenBinary(
                            /* module= */ module(),
                            /* op=     */ BinaryenAddInt64(),
                            /* left=   */ old_val,
                            /* right=  */ extd
                        );
                        data->HT->store_value_to_slot(update_group, slot_addr, idx, std::move(new_val));
                        break;
                    }

                    case Numeric::N_Decimal: {
                        /* Extend type to maximum precision DECIMAL. */
                        auto ty_extd = Type::Get_Decimal(Type::TY_Vector, Numeric::MAX_DECIMAL_PRECISION, n->scale);
                        /* Extend value to maximum precision DECIMAL. */
                        WasmTemporary extd = convert(module(), std::move(args[0]), n, ty_extd);
                        data->HT->store_value_to_slot(create_group, slot_addr, idx, extd.clone(module()));
                        WasmTemporary new_val = BinaryenBinary(
                            /* module= */ module(),
                            /* op=     */ BinaryenAddInt64(),
                            /* left=   */ old_val,
                            /* right=  */ extd
                        );
                        data->HT->store_value_to_slot(update_group, slot_addr, idx, std::move(new_val));
                        break;
                    }

                    case Numeric::N_Float:
                        WasmTemporary extd = convert(module(), std::move(args[0]), n, Type::Get_Double(Type::TY_Vector));
                        data->HT->store_value_to_slot(create_group, slot_addr, idx, extd.clone(module()));
                        WasmTemporary new_val = BinaryenBinary(
                            /* module= */ module(),
                            /* op=     */ BinaryenAddFloat64(),
                            /* left=   */ old_val,
                            /* right=  */ extd
                        );
                        data->HT->store_value_to_slot(update_group, slot_addr, idx, std::move(new_val));
                        break;
                }
                break;
            }

            case Function::FN_AVG: {
                M_insist(args.size() == 1, "aggregate function expects exactly one argument");
                /* Compute AVG as iterative mean as described in Knuth, The Art of Computer Programming Vol 2, section
                 * 4.2.2. */
                WasmTemporary argument = convert(module(), std::move(args[0]), fn_expr->args[0]->type(), Type::Get_Double(Type::TY_Scalar));
                data->HT->store_value_to_slot(create_group, slot_addr, idx, argument.clone(module()));
                WasmTemporary old_avg = ld_slot.get_value(e.id);
                WasmTemporary delta_absolute = BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenSubFloat64(),
                    /* left=   */ argument,
                    /* right=  */ old_avg.clone(module())
                );
                WasmTemporary running_count = data->struc->load(
                    /* fn=           */ module().main(),
                    /* ptr=          */ slot_addr,
                    /* idx=          */ op.schema().num_entries(),
                    /* struc_offset= */ WasmRefCountingHashTable::REFERENCE_SIZE
                );
                WasmTemporary delta_relative = BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenDivFloat64(),
                    /* left=   */ delta_absolute,
                    /* right=  */ BinaryenUnary(module(), BinaryenConvertSInt32ToFloat64(), running_count)
                );
                WasmTemporary new_avg = BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenAddFloat64(),
                    /* left=   */ old_avg,
                    /* right=  */ delta_relative
                );
                data->HT->store_value_to_slot(update_group, slot_addr, idx, std::move(new_avg));
                break;
            }

            case Function::FN_COUNT: {
                if (args.size() == 0) {
                    data->HT->store_value_to_slot(create_group, slot_addr, idx, BinaryenConst(module(),
                                                  BinaryenLiteralInt64(1)));
                    WasmTemporary old_val = ld_slot.get_value(e.id);
                    WasmTemporary new_val = BinaryenBinary(
                        /* module= */ module(),
                        /* op=     */ BinaryenAddInt64(),
                        /* left=   */ old_val,
                        /* right=  */ BinaryenConst(module(), BinaryenLiteralInt64(1))
                    );
                    data->HT->store_value_to_slot(update_group, slot_addr, idx, std::move(new_val));
                } else {
                    // TODO verify if NULL
                    M_unreachable("not yet supported");
                }
                break;
            }
        }
    }

    block_ += BinaryenIf(
        /* module=    */ module(),
        /* condition= */ data->HT->is_slot_empty(slot_addr),
        /* ifTrue=    */ create_group.finalize(),
        /* ifFalse=   */ update_group.finalize()
    );
}

void WasmPipelineCG::operator()(const AggregationOperator &op)
{
    auto data = as<AggregationData>(op.data());

    /*----- Increment running count. ---------------------------------------------------------------------------------*/
    WasmVariable &running_count = data->aggregates.back();
    block_ += running_count.set(BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ running_count,
        /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(1))
    ));

    /* Emit code to initialize and update aggregates. */
    for (std::size_t i = 0; i != op.schema().num_entries(); ++i) {
        auto fn_expr = as<const FnApplicationExpr>(op.aggregates()[i]);
        auto &fn = fn_expr->get_function();
        M_insist(fn.kind == Function::FN_Aggregate, "not an aggregation function");

        auto &agg_ty = *fn_expr->type();
        auto &agg_val = data->aggregates[i];

        switch (fn.fnid) {
            default:
                M_unreachable("unsupported aggregate function");

            case Function::FN_MIN: {
                auto &n = as<const Numeric>(agg_ty);
                auto &arg = *fn_expr->args[0];

                /*----- Initialize aggregate. ------------------------------------------------------------------------*/
                // TODO initialize with NULL
                module().main().block() += agg_val.set(BinaryenConst(module(), WasmLimits::max(*fn_expr->type())));

                /*----- Update aggregate. ----------------------------------------------------------------------------*/
                WasmVariable new_val(module().main(), get_binaryen_type(&agg_ty));
                block_ += new_val.set(context().compile(block_, arg));
                switch (n.kind) {
                    case Numeric::N_Int: {
                        WasmTemporary is_less = BinaryenBinary(
                            /* module= */ module(),
                            /* op=     */ n.size() <= 32 ? BinaryenLtSInt32() : BinaryenLtSInt64(),
                            /* left=   */ new_val,
                            /* right=  */ agg_val
                        );
                        block_ += agg_val.set(BinaryenSelect(
                            /* module=    */ module(),
                            /* condition= */ is_less,
                            /* ifTrue=    */ new_val,
                            /* ifFalse=   */ agg_val,
                            /* type=      */ n.size() <= 32 ? BinaryenTypeInt32() : BinaryenTypeInt64()
                        ));
                        break;
                    }

                    case Numeric::N_Decimal:
                        M_unreachable("not implemented");

                    case Numeric::N_Float:
                        block_ += agg_val.set(BinaryenBinary(
                            /* module= */ module(),
                            /* op=     */ n.size() <= 32 ? BinaryenMinFloat32() : BinaryenMinFloat64(),
                            /* left=   */ new_val,
                            /* right=  */ agg_val
                        ));
                        break;
                }
                break;
            }

            case Function::FN_MAX: {
                auto &n = as<const Numeric>(agg_ty);
                auto &arg = *fn_expr->args[0];

                /*----- Initialize aggregate. ------------------------------------------------------------------------*/
                // TODO initialize with NULL
                module().main().block() += agg_val.set(BinaryenConst(module(), WasmLimits::lowest(*fn_expr->type())));

                /*----- Update aggregate. ----------------------------------------------------------------------------*/
                WasmVariable new_val(module().main(), get_binaryen_type(&agg_ty));
                block_ += new_val.set(context().compile(block_, arg));
                switch (n.kind) {
                    case Numeric::N_Int: {
                        WasmTemporary is_greater = BinaryenBinary(
                            /* module= */ module(),
                            /* op=     */ n.size() <= 32 ? BinaryenGtSInt32() : BinaryenGtSInt64(),
                            /* left=   */ new_val,
                            /* right=  */ agg_val
                        );
                        block_ += agg_val.set(BinaryenSelect(
                            /* module=    */ module(),
                            /* condition= */ is_greater,
                            /* ifTrue=    */ new_val,
                            /* ifFalse=   */ agg_val,
                            /* type=      */ n.size() <= 32 ? BinaryenTypeInt32() : BinaryenTypeInt64()
                        ));
                        break;
                    }

                    case Numeric::N_Decimal:
                        M_unreachable("not implemented");

                    case Numeric::N_Float:
                        block_ += agg_val.set(BinaryenBinary(
                            /* module= */ module(),
                            /* op=     */ n.size() <= 32 ? BinaryenMaxFloat32() : BinaryenMaxFloat64(),
                            /* left=   */ new_val,
                            /* right=  */ agg_val
                        ));
                        break;
                }
                break;
            }

            case Function::FN_SUM: {
                auto &n = as<const Numeric>(agg_ty);
                auto &arg = *fn_expr->args[0];
                WasmTemporary new_val = context().compile(block_, arg);

                switch (n.kind) {
                    case Numeric::N_Int:
                    case Numeric::N_Decimal:
                        module().main().block() += agg_val.set(BinaryenConst(module(), BinaryenLiteralInt64(0)));
                        block_ += agg_val.set(BinaryenBinary(
                            /* module= */ module(),
                            /* op=     */ BinaryenAddInt64(),
                            /* left=   */ agg_val,
                            /* right=  */ convert(module(), std::move(new_val), arg.type(), &agg_ty)
                        ));
                        break;

                    case Numeric::N_Float:
                        module().main().block() += agg_val.set(BinaryenConst(module(), BinaryenLiteralFloat64(0.)));
                        block_ += agg_val.set(BinaryenBinary(
                            /* module= */ module(),
                            /* op=     */ BinaryenAddFloat64(),
                            /* left=   */ agg_val,
                            /* right=  */ convert(module(), std::move(new_val), arg.type(), Type::Get_Double(Type::TY_Vector))
                        ));
                        break;
                }
                break;
            }

            case Function::FN_AVG: {
                /* Compute AVG as iterative mean as described in Knuth, The Art of Computer Programming Vol 2, section
                 * 4.2.2. */
                auto &arg = *fn_expr->args[0];
                WasmTemporary new_val = context().compile(block_, arg);
                module().main().block() += agg_val.set(BinaryenConst(module(), BinaryenLiteralFloat64(0.)));
                WasmTemporary val_converted = convert(module(), std::move(new_val), arg.type(), Type::Get_Double(Type::TY_Vector));

                WasmTemporary absolute_delta = BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenSubFloat64(),
                    /* left=   */ val_converted,
                    /* right=  */ agg_val
                );
                WasmTemporary relative_delta = BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenDivFloat64(),
                    /* left=   */ absolute_delta,
                    /* right=  */ BinaryenUnary(module(), BinaryenConvertUInt32ToFloat64(), running_count)
                );
                block_ += agg_val.set(BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenAddFloat64(),
                    /* left=   */ agg_val,
                    /* right=  */ relative_delta
                ));
                break;
            }

            case Function::FN_COUNT: {
                auto &n = as<const Numeric>(agg_ty);
                module().main().block() += agg_val.set(BinaryenConst(module(), BinaryenLiteralInt64(0)));
                // TODO check for NULL values
                block_ += agg_val.set(BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenAddInt64(),
                    /* left=   */ agg_val,
                    /* right=  */ BinaryenConst(module(), BinaryenLiteralInt64(1))
                ));
                break;
            }
        }
    }
}

void WasmPipelineCG::operator()(const SortingOperator &op)
{
    auto data = as<SortingData>(op.data());

    /* Save the current head of heap as the beginning of the data to sort. */
    module().main().block() += data->begin.set(module().head_of_heap());

    std::size_t idx = 0;
    for (auto &attr : op.child(0)->schema())
        data->struc->store(module().main(), block_, module().head_of_heap(), idx++, context()[attr.id]);

    /* Advance head of heap. */
    block_ += module().head_of_heap().set(BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ module().head_of_heap(),
        /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(data->struc->size_in_bytes()))
    ));
}


/*======================================================================================================================
 * WasmStoreCG
 *====================================================================================================================*/

void WasmStoreCG::compile_load(WasmPipelineCG &pipeline, const Producer &op, const Linearization &L,
                               WasmTemporary num_rows, const std::vector<WasmTemporary> &root_offsets,
                               std::size_t row_id)
{
    compile_linearization<false>(pipeline, op, L, std::move(num_rows), root_offsets, row_id);
}

void WasmStoreCG::compile_store(WasmPipelineCG &pipeline, const Producer &op, const Linearization &L,
                                WasmTemporary num_rows, const std::vector<WasmTemporary> &root_offsets,
                                std::size_t row_id)
{
    compile_linearization<true>(pipeline, op, L, std::move(num_rows), root_offsets, row_id);
}

template<bool IsStore>
void WasmStoreCG::compile_linearization(WasmPipelineCG &pipeline, const Producer &op, const Linearization &L,
                                        WasmTemporary num_rows, const std::vector<WasmTemporary> &root_offsets,
                                        std::size_t row_id)
{
    struct {
        WasmVariable *ptr = nullptr; ///< pointer to the null bitmap
        ///> to keep track of current varying bit offset in case of a bit stride
        std::optional<WasmVariable> varying_bit_offset;
        uintptr_t bit_offset; ///< fixed offset, in bits
        std::size_t bit_stride; ///< stride, in bits
        const Linearization *lin; ///< `Linearization` in which the NULL bitmap is stored

        operator bool() const { return ptr != nullptr; }
        bool has_varying_offset() const { return *this and bit_stride != 0; }
    } null_bitmap_info;

    struct level_info_t
    {
        WasmVariable counter; ///< loop counter
        WasmWhile loop; ///< loop
        BlockBuilder block; ///< block containing code to emit at the end of loop's body

        level_info_t(WasmModuleCG &module, const char *name, WasmTemporary end)
            : counter(module.main(), BinaryenTypeInt32())
            , loop(module, name, BinaryenBinary(
                /* module= */ module,
                /* op=     */ BinaryenLtUInt32(),
                /* left=   */ counter,
                /* right=  */ end
            ))
            , block(module, name)
        { }

        level_info_t(level_info_t&&) = default;
    };
    std::vector<level_info_t> level_info_stack; // keep track of compiled code snippets per level

    struct stride_info_t
    {
        std::size_t num_tuples; ///< number of tuples of the `Linearization`
        std::size_t stride; ///< stride, in bytes
    };
    std::vector<stride_info_t> stride_info_stack; // keep track of all strides from root to leaf

    /* Keep track of greatest common divisors of all lengths of all `Linearization`s per level. */
    std::vector<std::size_t> gcd_stack;

    /* Keep track of pointers per `Linearization`-stride combination.
     * Since the root node's entries represent independent memory spaces and thus need individual pointers but may
     * have the same stride, duplicates may occur and a `std::unordered_multimap` must be used. */
    using hash = PairHash<const Linearization*, uint32_t>;
    std::unordered_multimap<std::pair<const Linearization*, uint32_t>, WasmVariable, hash> lin_stride2ptr;

    /* Keep track of masks per `Linearization`-bit_offset combination. */
    std::unordered_map<std::pair<const Linearization*, uint32_t>, WasmVariable, hash> lin_offset2mask;

    /*----- Compute location of NULL bitmap and initialize `gcd_stack`. ----------------------------------------------*/
    auto initialize = [&](const Linearization &L, std::size_t row_id) -> void {
        auto initialize_impl = [&](const Linearization &L, std::size_t level, WasmTemporary base, uintptr_t offset,
                                   std::size_t row_id, auto &initialize_impl_ref) -> void
        {
            if (level < gcd_stack.size()) {
                /* Update existing gcd level. */
                gcd_stack[level] = std::gcd(gcd_stack[level], L.num_tuples());
            } else {
                /* Create new gcd level. The gcd must divide the parent gcd if one is specified. The first level is
                 * initialized with gcd 0 to indicate an unspecified gcd. */
                M_insist(level == gcd_stack.size());
                gcd_stack.push_back(level == 0 ? 0UL : std::gcd(gcd_stack[level - 1], L.num_tuples()));
            }

            std::size_t idx = 0;
            for (const auto &e : L) {
                if (level == 0) {
                    if (root_offsets.empty()) {
                        /* Use offset provided in root node entry as base address. */
                        base = BinaryenConst(
                            /* module= */ pipeline.module(),
                            /* value=  */ BinaryenLiteralInt32(e.offset)
                        );
                    } else {
                        /* Use provided alternative offset as base address. */
                        M_insist(root_offsets.size() == L.num_sequences());
                        base = root_offsets[idx++].clone(pipeline.module());
                    }
                }

                if (e.is_null_bitmap()) {
                    M_insist(not null_bitmap_info, "there must be at most one null bitmap in the linearization");
                    const std::size_t ptr_byte_offset = (row_id * e.stride) / 8;
                    const std::size_t ptr_bit_offset = (row_id * e.stride) % 8;

                    /* Add pointer for this `Linearization`-`e.stride` combination if none exists. */
                    WasmVariable *ptr;
                    if (auto it = lin_stride2ptr.find({&L, e.stride}); it != lin_stride2ptr.end()) {
                        ptr = &it->second;
                    } else {
                        ptr = &lin_stride2ptr.emplace_hint(it, std::piecewise_construct,
                                                           std::forward_as_tuple(&L, e.stride),
                                                           std::forward_as_tuple(pipeline.module().main(), BinaryenTypeInt32())
                        )->second;
                        pipeline.block_ += ptr->set(BinaryenBinary(
                            /* module= */ pipeline.module(),
                            /* op=     */ BinaryenAddInt32(),
                            /* left=   */ base.clone(pipeline.module()),
                            /* right=  */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(offset + ptr_byte_offset))
                        ));
                    }

                    /* Initialize NULL bitmap info. */
                    null_bitmap_info.ptr = ptr;
                    null_bitmap_info.bit_offset = e.offset;
                    null_bitmap_info.bit_stride = e.stride;
                    null_bitmap_info.lin = &L;

                    if (null_bitmap_info.has_varying_offset()) {
                        /* Initialize varying NULL bitmap offset including pointer bit offset. */
                        null_bitmap_info.varying_bit_offset.emplace(pipeline.module().main(), BinaryenTypeInt32());
                        pipeline.block_ += null_bitmap_info.varying_bit_offset->set(BinaryenConst(
                            /* module= */ pipeline.module(),
                            /* value=  */ BinaryenLiteralInt32(null_bitmap_info.bit_offset + ptr_bit_offset)
                        ));
                    }
                } else if (e.is_linearization()) {
                    auto &lin = e.as_linearization();
                    M_insist(lin.num_tuples() != 0);
                    const std::size_t lin_id = row_id / lin.num_tuples();
                    const std::size_t inner_row_id = row_id % lin.num_tuples();
                    const auto additional_offset = level == 0 ? e.stride * lin_id : e.offset + e.stride * lin_id;
                    initialize_impl_ref(lin, level + 1, base.clone(pipeline.module()), offset + additional_offset,
                                        inner_row_id, initialize_impl_ref);
                }
            }
        };
        initialize_impl(L, 0, WasmTemporary(), 0, row_id, initialize_impl);
    };
    initialize(L, row_id);

    /*----- Initialize `level_info_stack`. Initialize counters and increment them in each block. ---------------------*/
    M_insist(not gcd_stack.empty());
    level_info_stack.reserve(gcd_stack.size());

    std::size_t inner_row_id = row_id;
    for (std::size_t level = 0; level < gcd_stack.size(); ++level) {
        const std::size_t current_gcd = gcd_stack[level];
        const std::size_t inner_gcd = level + 1 < gcd_stack.size() ? gcd_stack[level + 1] : 1UL;

        WasmTemporary end;
        if (level == 0) {
            /* Iterate in outermost loop until at least `num_rows` tuples are processed. The inner loop will process
             * `inner_gcd` tuples. */
            WasmTemporary num_rows_ceiled = BinaryenBinary(
                /* module= */ pipeline.module(),
                /* op=     */ BinaryenAddInt32(),
                /* lhs=    */ num_rows.clone(pipeline.module()),
                /* rhs=    */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(inner_gcd - 1U))
            );
            end = BinaryenBinary(
                /* module= */ pipeline.module(),
                /* op=     */ BinaryenDivUInt32(),
                /* lhs=    */ num_rows_ceiled,
                /* rhs=    */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(inner_gcd))
            );
        } else {
            /* Iterate in inner loops until exactly `current_gcd` tuples are processed. The inner loop will process
             * `inner_gcd` tuples. */
            end = BinaryenConst(pipeline.module(), BinaryenLiteralInt32(current_gcd / inner_gcd));
        }
        std::ostringstream oss;
        oss << "compile_linearization.level" << level;
        auto &level_info = level_info_stack.emplace_back(pipeline.module(), oss.str().c_str(), std::move(end));

        /* Initialize counter. */
        pipeline.block_ += level_info.counter.set(BinaryenConst(
            /* module= */ pipeline.module(),
            /* value=  */ BinaryenLiteralInt32(inner_row_id / inner_gcd)
        ));

        /* Increment counter. */
        level_info.block += level_info.counter.set(BinaryenBinary(
            /* module= */ pipeline.module(),
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ level_info.counter,
            /* right=  */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(1U))
        ));

        /* Update inner row id for next iteration. */
        inner_row_id = inner_row_id % inner_gcd;
    }
    M_insist(inner_row_id == 0);

    /*----- Compile code for aborting iteration at the end of the data. ----------------------------------------------*/
    auto &innermost = level_info_stack.back().block; // innermost block

    /* Initialize current row id. */
    WasmVariable current_row_id(pipeline.module().main(), BinaryenTypeInt32());
    pipeline.block_ += current_row_id.set(BinaryenConst(
        /* module= */ pipeline.module(),
        /* value=  */ BinaryenLiteralInt32(row_id)
    ));

    /* Break to end of outermost loop if `current_row_id` >= `num_rows`. */
    WasmTemporary cond = BinaryenBinary(
        /* module= */ pipeline.module(),
        /* op=     */ BinaryenGeUInt32(),
        /* lhs=    */ current_row_id,
        /* rhs=    */ num_rows
    );
    innermost += BinaryenBreak(
        /* module=    */ pipeline.module(),
        /* name=      */ level_info_stack.front().loop.body().name(), // body of outermost loop
        /* condition= */ cond,
        /* value=     */ nullptr
    );

    /* Increment current row id. */
    innermost += current_row_id.set(BinaryenBinary(
        /* module= */ pipeline.module(),
        /* op=     */ BinaryenAddInt32(),
        /* lhs=    */ current_row_id,
        /* rhs=    */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(1U))
    ));

    /*----- Compile entire pipeline for storing. ---------------------------------------------------------------------*/
    if constexpr (IsStore) {
        /* Compilation before attribute access to be able to store the computed results. */
        swap(pipeline.block_, innermost);
        pipeline(*op.parent());
        swap(pipeline.block_, innermost);
    }

    /*----- Compile code for attribute access. -----------------------------------------------------------------------*/
    auto compile_access = [&](const Linearization &L, std::size_t row_id) -> void {
        auto compile_access_impl = [&](const Linearization &L, std::size_t level, WasmTemporary base, uintptr_t offset,
                                       std::size_t row_id, auto compile_access_impl_ref) -> void
        {
            std::size_t idx = 0;
            for (const auto &e : L) {
                if (level == 0) {
                    if (root_offsets.empty()) {
                        /* Use offset provided in root node entry as base address. */
                        base = BinaryenConst(
                            /* module= */ pipeline.module(),
                            /* value=  */ BinaryenLiteralInt32(e.offset)
                        );
                    } else {
                        /* Use provided alternative offset as base address. */
                        M_insist(root_offsets.size() == L.num_sequences());
                        base = root_offsets[idx++].clone(pipeline.module());
                    }
                }

                if (e.is_null_bitmap()) {
                    /* nothing to be done */
                } else if (e.is_attribute()) {
                    auto &attr = e.as_attribute();

                    /* Locate the attribute in the operator schema. */
                    auto &S = op.schema();
                    if (auto it = S.find({attr.table.name, attr.name}); it != S.end()) {
                        const std::size_t ptr_byte_offset = (row_id * e.stride) / 8;
                        const std::size_t ptr_bit_offset = (row_id * e.stride) % 8;
                        const std::size_t attr_byte_offset = (e.offset + ptr_bit_offset) / 8;
                        const std::size_t attr_bit_offset = (e.offset + ptr_bit_offset) % 8;
                        M_insist(not (ptr_bit_offset or attr_bit_offset) or attr.type->is_boolean(),
                               "only booleans may not be byte aligned");

                        const std::size_t byte_stride = e.stride / 8;
                        const std::size_t bit_stride  = e.stride % 8;
                        M_insist(not bit_stride or attr.type->is_boolean(), "only booleans may not be byte aligned");
                        M_insist(bit_stride == 0 or byte_stride == 0,
                               "the stride must be a whole multiple of a byte or less than a byte");

                        /* Access NULL bit. */
                        if (null_bitmap_info) {
                            if (null_bitmap_info.has_varying_offset()) {
                                /* With bit stride. Use varying offset instead of fixed offset. */
                                WasmTemporary null_offset = BinaryenBinary(
                                    /* module= */ pipeline.module(),
                                    /* op=     */ BinaryenAddInt32(),
                                    /* lhs=    */ *null_bitmap_info.varying_bit_offset,
                                    /* rhs=    */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(attr.id))
                                ); // in bits
                                WasmTemporary null_byte_offset = BinaryenBinary(
                                    /* module= */ pipeline.module(),
                                    /* op=     */ BinaryenShrUInt32(),
                                    /* lhs=    */ null_offset.clone(pipeline.module()),
                                    /* rhs=    */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(3U))
                                ); // = null_offset / 8
                                WasmTemporary ptr = BinaryenBinary(
                                    /* module= */ pipeline.module(),
                                    /* op=     */ BinaryenAddInt32(),
                                    /* lhs=    */ *null_bitmap_info.ptr,
                                    /* rhs=    */ null_byte_offset
                                );
                                WasmTemporary null_bit_offset = BinaryenBinary(
                                    /* module= */ pipeline.module(),
                                    /* op=     */ BinaryenAndInt32(),
                                    /* lhs=    */ null_offset,
                                    /* rhs=    */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(0b111U))
                                ); // = null_offset % 8
                                WasmTemporary mask = BinaryenBinary(
                                    /* module= */ pipeline.module(),
                                    /* op=     */ BinaryenShlInt32(),
                                    /* lhs=    */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(1U)),
                                    /* rhs=    */ null_bit_offset
                                );

                                if constexpr (IsStore) {
                                    /* Load NULL bit to store. */
                                    WasmTemporary is_null = pipeline.context().get_null(it->id);

                                    /* Store NULL bit. */
                                    WasmTemporary byte = BinaryenLoad(
                                        /* module= */ pipeline.module(),
                                        /* bytes=  */ 1,
                                        /* signed= */ false,
                                        /* offset= */ 0,
                                        /* align=  */ 0,
                                        /* type=   */ BinaryenTypeInt32(),
                                        /* ptr=    */ ptr.clone(pipeline.module())
                                    );
#if 0
                                    WasmTemporary subed = BinaryenBinary(
                                        /* module= */ pipeline.module(),
                                        /* op=     */ BinaryenSubInt32(),
                                        /* lhs=    */ is_null,
                                        /* rhs=    */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(1U))
                                    );
                                    WasmTemporary xored = BinaryenBinary(
                                        /* module= */ pipeline.module(),
                                        /* op=     */ BinaryenXorInt32(),
                                        /* lhs=    */ subed,
                                        /* rhs=    */ byte.clone(pipeline.module())
                                    );
                                    WasmTemporary anded = BinaryenBinary(
                                        /* module= */ pipeline.module(),
                                        /* op=     */ BinaryenAndInt32(),
                                        /* lhs=    */ xored,
                                        /* rhs=    */ mask
                                    );
                                    WasmTemporary new_byte = BinaryenBinary(
                                        /* module= */ pipeline.module(),
                                        /* op=     */ BinaryenXorInt32(),
                                        /* lhs=    */ byte,
                                        /* rhs=    */ anded
                                    ); // = byte ^ (((is_null - 1) ^ byte) & mask) = byte ^ ((-(!is_null) ^ byte) & mask)
#else
                                    WasmTemporary set_bit = BinaryenBinary(
                                        /* module= */ pipeline.module(),
                                        /* op=     */ BinaryenOrInt32(),
                                        /* lhs=    */ byte.clone(pipeline.module()),
                                        /* rhs=    */ mask.clone(pipeline.module())
                                    );
                                    WasmTemporary inverted_mask = BinaryenBinary(
                                        /* module= */ pipeline.module(),
                                        /* op=     */ BinaryenXorInt32(),
                                        /* lhs=    */ mask,
                                        /* rhs=    */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(0xFF))
                                    );
                                    WasmTemporary unset_bit = BinaryenBinary(
                                        /* module= */ pipeline.module(),
                                        /* op=     */ BinaryenAndInt32(),
                                        /* lhs=    */ byte,
                                        /* rhs=    */ inverted_mask
                                    );
                                    WasmTemporary new_byte = BinaryenSelect(
                                        /* module=    */ pipeline.module(),
                                        /* condition= */ is_null,
                                        /* ifTrue=    */ unset_bit,
                                        /* ifFalse=   */ set_bit,
                                        /* type=      */ BinaryenTypeInt32()
                                    );
#endif
                                    innermost += BinaryenStore(
                                        /* module= */ pipeline.module(),
                                        /* bytes=  */ 1,
                                        /* offset= */ 0,
                                        /* align=  */ 0,
                                        /* ptr=    */ ptr,
                                        /* value=  */ new_byte,
                                        /* type=   */ BinaryenTypeInt32()
                                    );
                                } else {
                                    /* Load NULL bit. */
                                    WasmTemporary byte = BinaryenLoad(
                                        /* module= */ pipeline.module(),
                                        /* bytes=  */ 1,
                                        /* signed= */ false,
                                        /* offset= */ 0,
                                        /* align=  */ 0,
                                        /* type=   */ BinaryenTypeInt32(),
                                        /* ptr=    */ ptr
                                    );
                                    WasmTemporary isolated_null_bit = BinaryenBinary(
                                        /* module= */ pipeline.module(),
                                        /* op=     */ BinaryenAndInt32(),
                                        /* lhs=    */ byte,
                                        /* rhs=    */ mask
                                    );
                                    WasmTemporary is_null = BinaryenUnary(
                                        /* module= */ pipeline.module(),
                                        /* op=     */ BinaryenEqZInt32(),
                                        /* value=  */ isolated_null_bit
                                    );
                                    // TODO add to context
                                    (void) is_null;
                                }
                            } else {
                                /* No bit stride means the NULL bitmap only advances with parent sequence. */
                                const std::size_t null_offset = null_bitmap_info.bit_offset + attr.id; // in bits
                                const std::size_t null_byte_offset = null_offset / 8;
                                const std::size_t null_bit_offset = null_offset % 8;

                                if constexpr (IsStore) {
                                    /* Load NULL bit to store. */
                                    WasmTemporary is_null = pipeline.context().get_null(it->id);

                                    /* Store NULL bit. */
                                    WasmTemporary byte = BinaryenLoad(
                                        /* module= */ pipeline.module(),
                                        /* bytes=  */ 1,
                                        /* signed= */ false,
                                        /* offset= */ null_byte_offset,
                                        /* align=  */ 0,
                                        /* type=   */ BinaryenTypeInt32(),
                                        /* ptr=    */ *null_bitmap_info.ptr
                                    );
#if 0
                                    const unsigned clear_bit = ~(1UL << null_bit_offset);
                                    WasmTemporary anded = BinaryenBinary(
                                        /* module= */ pipeline.module(),
                                        /* op=     */ BinaryenAndInt32(),
                                        /* lhs=    */ byte,
                                        /* rhs=    */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(clear_bit))
                                    );
                                    WasmTemporary is_not_null = BinaryenBinary(
                                        /* module= */ pipeline.module(),
                                        /* op=     */ BinaryenNeInt32(),
                                        /* lhs=    */ is_null,
                                        /* rhs=    */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(1U))
                                    );
                                    WasmTemporary set_bit = BinaryenBinary(
                                        /* module= */ pipeline.module(),
                                        /* op=     */ BinaryenShlInt32(),
                                        /* lhs=    */ is_not_null,
                                        /* rhs=    */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(null_bit_offset))
                                    );
                                    WasmTemporary new_byte = BinaryenBinary(
                                        /* module= */ pipeline.module(),
                                        /* op=     */ BinaryenOrInt32(),
                                        /* lhs=    */ anded,
                                        /* rhs=    */ set_bit
                                    ); // = (byte & ~(1UL << null_bit_offset)) | (!is_null << null_bit_offset)
#else
                                    WasmTemporary set_bit = BinaryenBinary(
                                        /* module= */ pipeline.module(),
                                        /* op=     */ BinaryenOrInt32(),
                                        /* lhs=    */ byte.clone(pipeline.module()),
                                        /* rhs=    */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(1U << null_bit_offset))
                                    );
                                    WasmTemporary unset_bit = BinaryenBinary(
                                        /* module= */ pipeline.module(),
                                        /* op=     */ BinaryenAndInt32(),
                                        /* lhs=    */ byte,
                                        /* rhs=    */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(~(1U << null_bit_offset)))
                                    );
                                    WasmTemporary new_byte = BinaryenSelect(
                                        /* module=    */ pipeline.module(),
                                        /* condition= */ is_null,
                                        /* ifTrue=    */ unset_bit,
                                        /* ifFalse=   */ set_bit,
                                        /* type=      */ BinaryenTypeInt32()
                                    );
#endif
                                    innermost += BinaryenStore(
                                        /* module= */ pipeline.module(),
                                        /* bytes=  */ 1,
                                        /* offset= */ null_byte_offset,
                                        /* align=  */ 0,
                                        /* ptr=    */ *null_bitmap_info.ptr,
                                        /* value=  */ new_byte,
                                        /* type=   */ BinaryenTypeInt32()
                                    );
                                } else {
                                    /* Load NULL bit. */
                                    WasmTemporary byte = BinaryenLoad(
                                        /* module= */ pipeline.module(),
                                        /* bytes=  */ 1,
                                        /* signed= */ false,
                                        /* offset= */ null_byte_offset,
                                        /* align=  */ 0,
                                        /* type=   */ BinaryenTypeInt32(),
                                        /* ptr=    */ *null_bitmap_info.ptr
                                    );
                                    WasmTemporary isolated_null_bit = BinaryenBinary(
                                        /* module= */ pipeline.module(),
                                        /* op=     */ BinaryenAndInt32(),
                                        /* lhs=    */ byte,
                                        /* rhs=    */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(1U << null_bit_offset))
                                    );
                                    WasmTemporary is_null = BinaryenUnary(
                                        /* module= */ pipeline.module(),
                                        /* op=     */ BinaryenEqZInt32(),
                                        /* value=  */ isolated_null_bit
                                    );
                                    // TODO add to context
                                    (void) is_null;
                                }
                            }
                        }

                        /* Add pointer for this `Linearization`-`e.stride` combination if none exists.
                         * Note that individual pointers must be created for the first level since the root node's
                         * entries represent independent memory spaces. */
                        WasmVariable *ptr;
                        if (auto it = lin_stride2ptr.find({&L, e.stride}); level != 0 and it != lin_stride2ptr.end()) {
                            ptr = &it->second;
                        } else {
                            ptr = &lin_stride2ptr.emplace_hint(it, std::piecewise_construct,
                                                               std::forward_as_tuple(&L, e.stride),
                                                               std::forward_as_tuple(pipeline.module().main(), BinaryenTypeInt32())
                            )->second;
                            pipeline.block_ += ptr->set(BinaryenBinary(
                                /* module= */ pipeline.module(),
                                /* op=     */ BinaryenAddInt32(),
                                /* left=   */ base.clone(pipeline.module()),
                                /* right=  */ BinaryenConst(pipeline.module(),BinaryenLiteralInt32(offset + ptr_byte_offset))
                            ));
                        }

                        /* Access attribute. */
                        if (bit_stride) {
                            M_insist(e.stride == 1, "only booleans may not be byte aligned with one bit stride");

                            /* Add mask for this `Linearization`-`e.offset % 8` combination if none exists. */
                            WasmVariable *mask;
                            if (auto it = lin_offset2mask.find({&L, e.offset % 8}); it != lin_offset2mask.end()) {
                                mask = &it->second;
                            } else {
                                mask = &lin_offset2mask.emplace_hint(it, std::piecewise_construct,
                                                                     std::forward_as_tuple(&L, e.offset % 8),
                                                                     std::forward_as_tuple(pipeline.module().main(), BinaryenTypeInt32())
                                )->second;
                                pipeline.block_ += mask->set(BinaryenConst(
                                    /* module= */ pipeline.module(),
                                    /* value=  */ BinaryenLiteralInt32(1U << attr_bit_offset)
                                ));
                            }

                            if constexpr (IsStore) {
                                /* Load value to store. */
                                WasmTemporary value = pipeline.context().get_value(it->id);

                                /* Store value. */
                                WasmTemporary byte = BinaryenLoad(
                                    /* module= */ pipeline.module(),
                                    /* bytes=  */ 1,
                                    /* signed= */ false,
                                    /* offset= */ attr_byte_offset,
                                    /* align=  */ 0,
                                    /* type=   */ BinaryenTypeInt32(),
                                    /* ptr=    */ *ptr
                                );
                                WasmTemporary negated = BinaryenBinary(
                                    /* module= */ pipeline.module(),
                                    /* op=     */ BinaryenSubInt32(),
                                    /* lhs=    */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(0)),
                                    /* rhs=    */ value
                                );
                                WasmTemporary xored = BinaryenBinary(
                                    /* module= */ pipeline.module(),
                                    /* op=     */ BinaryenXorInt32(),
                                    /* lhs=    */ negated,
                                    /* rhs=    */ byte.clone(pipeline.module())
                                );
                                WasmTemporary anded = BinaryenBinary(
                                    /* module= */ pipeline.module(),
                                    /* op=     */ BinaryenAndInt32(),
                                    /* lhs=    */ xored,
                                    /* rhs=    */ *mask
                                );
                                WasmTemporary new_byte = BinaryenBinary(
                                    /* module= */ pipeline.module(),
                                    /* op=     */ BinaryenXorInt32(),
                                    /* lhs=    */ byte,
                                    /* rhs=    */ anded
                                ); // = (byte ^ ((-value ^ byte) & mask)
                                innermost += BinaryenStore(
                                    /* module= */ pipeline.module(),
                                    /* bytes=  */ 1,
                                    /* offset= */ attr_byte_offset,
                                    /* align=  */ 0,
                                    /* ptr=    */ *ptr,
                                    /* value=  */ new_byte,
                                    /* type=   */ BinaryenTypeInt32()
                                );
                            } else {
                                /* Load value. */
                                WasmTemporary byte = BinaryenLoad(
                                    /* module= */ pipeline.module(),
                                    /* bytes=  */ 1,
                                    /* signed= */ false,
                                    /* offset= */ attr_byte_offset,
                                    /* align=  */ 0,
                                    /* type=   */ BinaryenTypeInt32(),
                                    /* ptr=    */ *ptr
                                );
                                WasmTemporary isolated_bit = BinaryenBinary(
                                    /* module= */ pipeline.module(),
                                    /* op=     */ BinaryenAndInt32(),
                                    /* lhs=    */ byte,
                                    /* rhs=    */ *mask
                                );
                                WasmTemporary value = BinaryenBinary(
                                    /* module= */ pipeline.module(),
                                    /* op=     */ BinaryenNeInt32(),
                                    /* lhs=    */ isolated_bit,
                                    /* rhs=    */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(0))
                                );
                                pipeline.context().add(it->id, std::move(value));
                            }
                        } else {
                            if constexpr (IsStore) {
                                /* Load value to store. */
                                WasmTemporary value = pipeline.context().get_value(it->id);

                                /* Store value. */
                                if (attr.type->size() < 8) {
                                    M_insist(attr.type->size() <= 8 - attr_bit_offset,
                                           "attribute with size less than one byte must be contained in a single byte");
                                    WasmTemporary byte = BinaryenLoad(
                                        /* module= */ pipeline.module(),
                                        /* bytes=  */ 1,
                                        /* signed= */ false,
                                        /* offset= */ attr_byte_offset,
                                        /* align=  */ 0,
                                        /* type=   */ BinaryenTypeInt32(),
                                        /* ptr=    */ *ptr
                                    );
                                    const unsigned clear_bits = ~(((1UL << attr.type->size()) - 1UL) << attr_bit_offset);
                                    WasmTemporary anded = BinaryenBinary(
                                        /* module= */ pipeline.module(),
                                        /* op=     */ BinaryenAndInt32(),
                                        /* lhs=    */ byte,
                                        /* rhs=    */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(clear_bits))
                                    );
                                    WasmTemporary shifted = BinaryenBinary(
                                        /* module= */ pipeline.module(),
                                        /* op=     */ BinaryenShlInt32(),
                                        /* lhs=    */ value,
                                        /* rhs=    */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(attr.type->size()))
                                    );
                                    WasmTemporary extended_value = BinaryenBinary(
                                        /* module= */ pipeline.module(),
                                        /* op=     */ BinaryenSubInt32(),
                                        /* lhs=    */ shifted,
                                        /* rhs=    */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(1U))
                                    );
                                    WasmTemporary set_bits = BinaryenBinary(
                                        /* module= */ pipeline.module(),
                                        /* op=     */ BinaryenShlInt32(),
                                        /* lhs=    */ attr.type->is_boolean() ? value : extended_value,
                                        /* rhs=    */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(attr_bit_offset))
                                    );
                                    WasmTemporary ored = BinaryenBinary(
                                        /* module= */ pipeline.module(),
                                        /* op=     */ BinaryenOrInt32(),
                                        /* lhs=    */ anded,
                                        /* rhs=    */ set_bits
                                    ); // = (byte & ~(1UL << attr_bit_offset)) | (value << attr_bit_offset)
                                    innermost += BinaryenStore(
                                        /* module= */ pipeline.module(),
                                        /* bytes=  */ 1,
                                        /* offset= */ attr_byte_offset,
                                        /* align=  */ 0,
                                        /* ptr=    */ *ptr,
                                        /* value=  */ ored,
                                        /* type=   */ BinaryenTypeInt32()
                                    );
                                } else if (attr.type->is_character_sequence()) {
                                    WasmTemporary address = BinaryenBinary(
                                        /* module= */ pipeline.module(),
                                        /* op=     */ BinaryenAddInt32(),
                                        /* left=   */ *ptr,
                                        /* right=  */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(attr_byte_offset))
                                    );
                                    WasmStrncpy strncpy(pipeline.module().main());
                                    strncpy.emit(innermost, std::move(address), std::move(value),
                                                 as<const CharacterSequence>(attr.type)->length);
                                } else {
                                    innermost += BinaryenStore(
                                        /* module= */ pipeline.module(),
                                        /* bytes=  */ attr.type->size() / 8,
                                        /* offset= */ attr_byte_offset,
                                        /* align=  */ 0,
                                        /* ptr=    */ *ptr,
                                        /* value=  */ value,
                                        /* type=   */ get_binaryen_type(attr.type)
                                    );
                                }
                            } else {
                                /* Load value. */
                                if (attr.type->size() < 8) {
                                    M_insist(attr.type->size() <= 8 - attr_bit_offset,
                                           "attribute with size less than one byte must be contained in a single byte");
                                    WasmTemporary byte = BinaryenLoad(
                                        /* module= */ pipeline.module(),
                                        /* bytes=  */ 1,
                                        /* signed= */ false,
                                        /* offset= */ attr_byte_offset,
                                        /* align=  */ 0,
                                        /* type=   */ BinaryenTypeInt32(),
                                        /* ptr=    */ *ptr
                                    );
                                    WasmTemporary shifted = BinaryenBinary(
                                        /* module= */ pipeline.module(),
                                        /* op=     */ BinaryenShrUInt32(),
                                        /* lhs=    */ byte,
                                        /* rhs=    */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(attr_bit_offset))
                                    );
                                    WasmTemporary value = BinaryenBinary(
                                        /* module= */ pipeline.module(),
                                        /* op=     */ BinaryenAndInt32(),
                                        /* lhs=    */ shifted,
                                        /* rhs=    */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32((1U << attr.type->size()) - 1U))
                                    );
                                    pipeline.context().add(it->id, std::move(value));
                                } else if (attr.type->is_character_sequence()) {
                                    WasmTemporary address = BinaryenBinary(
                                        /* module= */ pipeline.module(),
                                        /* op=     */ BinaryenAddInt32(),
                                        /* left=   */ *ptr,
                                        /* right=  */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(attr_byte_offset))
                                    );
                                    pipeline.context().add(it->id, std::move(address));
                                } else {
                                    WasmTemporary value = BinaryenLoad(
                                        /* module= */ pipeline.module(),
                                        /* bytes=  */ attr.type->size() / 8,
                                        /* signed= */ true,
                                        /* offset= */ attr_byte_offset,
                                        /* align=  */ 0,
                                        /* type=   */ get_binaryen_type(attr.type),
                                        /* ptr=    */ *ptr
                                    );
                                    pipeline.context().add(it->id, std::move(value));
                                }
                            }
                        }
                    }
                } else {
                    M_insist(e.is_linearization());

                    auto &lin = e.as_linearization();
                    M_insist(lin.num_tuples() != 0);
                    const std::size_t lin_id = row_id / lin.num_tuples();
                    const std::size_t inner_row_id = row_id % lin.num_tuples();
                    const auto additional_offset = level == 0 ? e.stride * lin_id : e.offset + e.stride * lin_id;
                    compile_access_impl_ref(lin, level + 1, base.clone(pipeline.module()),
                                            offset + additional_offset, inner_row_id, compile_access_impl_ref);
                }
            }
        };
        compile_access_impl(L, 0, WasmTemporary(), 0, row_id, compile_access_impl);
    };
    compile_access(L, row_id);

    /*----- Compile entire pipeline for loading. ---------------------------------------------------------------------*/
    if constexpr (not IsStore) {
        /* Compilation after attribute access to be able to use the loaded values. */
        swap(pipeline.block_, innermost);
        pipeline(*op.parent());
        swap(pipeline.block_, innermost);
    }

    /*----- Compile code to update the varying NULL bitmap offset. ------------------------------------------------*/
    if (null_bitmap_info.has_varying_offset()) {
        innermost += null_bitmap_info.varying_bit_offset->set(BinaryenBinary(
            /* module= */ pipeline.module(),
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ *null_bitmap_info.varying_bit_offset,
            /* right=  */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(null_bitmap_info.bit_stride))
        ));
    }

    /*----- Compile code to update pointers and masks. ---------------------------------------------------------------*/
    auto compile_update = [&](const Linearization &L) -> void {
        auto compile_update_impl = [&](const Linearization &L, std::size_t level, auto &compile_update_impl_ref) -> void
        {
            /* Update pointers. */
            for (const auto &p : lin_stride2ptr) {
                if (p.first.first != &L)
                    continue; // skip all pointers that do not belong to this node

                const auto stride = p.first.second;
                const auto &ptr = p.second;

                /* Update pointer in innermost block if there is a stride of at least one byte. */
                if (const std::size_t byte_stride = stride / 8) {
                    innermost += ptr.set(BinaryenBinary(
                        /* module= */ pipeline.module(),
                        /* op=     */ BinaryenAddInt32(),
                        /* left=   */ ptr,
                        /* right=  */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(byte_stride))
                    ));
                }

                /* Compile code for stride jumps. */
                std::size_t prev_num_tuples = 1;
                std::size_t prev_stride = stride;
                std::size_t current_level = level;
                for (auto it = stride_info_stack.rbegin(), end = stride_info_stack.rend(); it != end; ++it) {
                    const auto &info = *it;

                    /* Compute the remaining stride in bits. */
                    const std::size_t stride_remaining = info.stride * 8 - (info.num_tuples / prev_num_tuples) * prev_stride;

                    /* Perform stride jump, if necessary. */
                    if (stride_remaining) {
                        std::size_t byte_stride = stride_remaining / 8;
                        const std::size_t bit_stride = stride_remaining % 8;

                        /* Find innermost level where to place the stride jump, i.e. where the gcd is greater than
                         * `info.num_tuples`. */
                        auto pred = [&info](const std::size_t &gcd) { return gcd > info.num_tuples; };
                        const auto gcd_it = std::find_if(gcd_stack.rbegin(), std::prev(gcd_stack.rend()), pred);
                        M_insist(gcd_stack.size() == 1 or gcd_it != gcd_stack.rbegin(),
                               "innermost level has at most gcd equal to sequence length");
                        M_insist(gcd_it != gcd_stack.rend(), "first level has unspecified (i.e. 0) gcd");
                        const std::size_t level = gcd_stack.size() - std::distance(gcd_stack.rbegin(), gcd_it) - 1;
                        auto &level_info = level_info_stack[level];

                        BlockBuilder stride_jump(pipeline.module(), "compile_linearization.stride_jump");

                        if (bit_stride) {
                            if (stride == 1UL) {
                                /* Masks may be introduced for this pointer. Reset them to their initial first bit. */
                                for (auto &p : lin_offset2mask) {
                                    if (p.first.first != &L)
                                        continue; // skip all masks that do not belong to this node

                                    stride_jump += p.second.set(BinaryenConst(
                                        /* module= */ pipeline.module(),
                                        /* value=  */ BinaryenLiteralInt32(1U << p.first.second)
                                    ));
                                }
                            }

                            if (null_bitmap_info.has_varying_offset() and null_bitmap_info.lin == &L) {
                                /* NULL bitmap is present in this `Linearization`. Reset its varying offset. */
                                stride_jump += null_bitmap_info.varying_bit_offset->set(BinaryenConst(
                                    /* module= */ pipeline.module(),
                                    /* value=  */ BinaryenLiteralInt32(null_bitmap_info.bit_offset)
                                ));
                            }

                            /* Ceil to next byte. */
                            ++byte_stride;
                        }

                        if (byte_stride) {
                            /* Advance pointer by `byte_stride` bytes. */
                            stride_jump += ptr.set(BinaryenBinary(
                                /* module= */ pipeline.module(),
                                /* op=     */ BinaryenAddInt32(),
                                /* lhs=    */ ptr,
                                /* rhs=    */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(byte_stride))
                            ));
                        }

                        if (gcd_it != gcd_stack.rbegin() and *std::prev(gcd_it) == info.num_tuples) {
                            /* No condition required since inner block processes exactly `info.num_tuples` tuples. */
                            level_info.block += stride_jump.finalize();
                        } else {
                            /* Check whether we are in an `info.num_tuples`-th iteration. If yes, perform stride jump. */
                            WasmTemporary cond_mod = BinaryenBinary(
                                /* module= */ pipeline.module(),
                                /* op=     */ BinaryenRemUInt32(),
                                /* lhs=    */ level_info.counter,
                                /* rhs=    */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(info.num_tuples))
                            );
                            WasmTemporary cond_and = BinaryenBinary(
                                /* module= */ pipeline.module(),
                                /* op=     */ BinaryenAndInt32(),
                                /* lhs=    */ level_info.counter,
                                /* rhs=    */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(info.num_tuples - 1U))
                            );
                            M_insist(info.num_tuples != 0);
                            const bool is_power_of_2 = (info.num_tuples bitand (info.num_tuples - 1)) == 0;
                            level_info.block += BinaryenIf(
                                /* module=    */ pipeline.module(),
                                /* condition= */ is_power_of_2 ? cond_and : cond_mod,
                                /* ifTrue=    */ stride_jump.finalize(),
                                /* ifFalse=   */ nullptr
                            );
                        }
                    }

                    /* Update variables for next iteration. */
                    prev_num_tuples = info.num_tuples;
                    prev_stride = info.stride * 8;
                    --current_level;
                }
            }

            /* Update masks. */
            bool has_masks = false;
            for (const auto &p : lin_offset2mask) {
                if (p.first.first != &L)
                    continue; // skip all masks that do not belong to this node

                const auto &mask = p.second;
                has_masks = true;

                /* Advance mask to the next bit in innermost block. */
                innermost += mask.set(BinaryenBinary(
                    /* module= */ pipeline.module(),
                    /* op=     */ BinaryenShlInt32(),
                    /* left=   */ mask,
                    /* right=  */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(1U))
                ));
            }

            /* Check whether any mask exceeds a single byte. If yes, reset all masks to their initial first bit and
             * advance the corresponding pointers to the next byte.
             * Since all masks are advanced by one bit simultaneously, only the mask with the highest initial bit may
             * exceed a single byte and has to be checked. */
            if (has_masks) {
                BlockBuilder reset_masks(pipeline.module(), "compile_linearization.reset_mask");

                uint32_t highest_offset = 0;
                for (const auto &p : lin_offset2mask) {
                    if (p.first.first != &L)
                        continue; // skip all masks that do not belong to this node

                    reset_masks += p.second.set(BinaryenConst(
                        /* module= */ pipeline.module(),
                        /* value=  */ BinaryenLiteralInt32(1U << p.first.second)
                    ));

                    highest_offset = std::max(highest_offset, p.first.second);
                }

                const auto &range = lin_stride2ptr.equal_range({&L, 1U}); // masks are only introduced for one bit strides
                M_insist(range.first != range.second);
                for (auto it = range.first; it != range.second; ++it) {
                    const auto &ptr = it->second;

                    reset_masks += ptr.set(BinaryenBinary(
                        /* module= */ pipeline.module(),
                        /* op=     */ BinaryenAddInt32(),
                        /* left=   */ ptr,
                        /* right=  */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(1U))
                    ));
                }

                WasmTemporary cond = BinaryenBinary(
                    /* module= */ pipeline.module(),
                    /* op=     */ BinaryenEqInt32(),
                    /* lhs=    */ lin_offset2mask.at({&L, highest_offset}),
                    /* rhs=    */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(1U << 8))
                );
                innermost += BinaryenIf(
                    /* module=    */ pipeline.module(),
                    /* condition= */ cond,
                    /* ifTrue=    */ reset_masks.finalize(),
                    /* ifFalse=   */ nullptr
                );
            }

            /* Perform recursive descent. */
            for (const auto &e : L) {
                if (e.is_linearization()) {
                    auto &lin = e.as_linearization();

                    /* Put context on stack. */
                    stride_info_stack.push_back(stride_info_t{
                        .num_tuples = lin.num_tuples(),
                        .stride = e.stride
                    });
                    compile_update_impl_ref(lin, level + 1, compile_update_impl_ref);
                    stride_info_stack.pop_back();
                }
            }
        };
        compile_update_impl(L, 0, compile_update_impl);
    };
    compile_update(L);

    /*----- Compile loops. -------------------------------------------------------------------------------------------*/
    for (auto level_it = level_info_stack.rbegin(); level_it != level_info_stack.rend(); ++level_it) {
        if (level_it != level_info_stack.rbegin()) {
            /* Add inner loop and reset its counter to 0. */
            auto inner_level_it = std::prev(level_it);
            level_it->loop += inner_level_it->loop.finalize();
            level_it->loop += inner_level_it->counter.set(BinaryenConst(
                /* module= */ pipeline.module(),
                /* value=  */ BinaryenLiteralInt32(0)
            ));
        }

        /* Add own block. */
        level_it->loop += level_it->block.finalize();
    }

    /* Add outermost loop to pipeline block. */
    pipeline.block_ += level_info_stack.front().loop.finalize();
}


/*======================================================================================================================
 * Codegen functions
 *====================================================================================================================*/

WasmTemporary m::wasm_emit_signum(FunctionBuilder &fn, BlockBuilder &block, WasmTemporary _val)
{
    const bool is_i32 = _val.type() == BinaryenTypeInt32();
    WasmVariable val(fn, BinaryenTypeInt32());
    block += val.set(std::move(_val));
    WasmTemporary zero = BinaryenConst(fn.module(), BinaryenLiteralInt32(0));
    WasmTemporary gtz = BinaryenBinary(fn.module(), BinaryenGtSInt32(), val, zero.clone(fn.module()));
    WasmTemporary ltz = BinaryenBinary(fn.module(), BinaryenLtSInt32(), val, zero);
    WasmTemporary sign = BinaryenBinary(fn.module(), BinaryenSubInt32(), gtz, ltz);
    return sign;
}

WasmTemporary m::wasm_emit_strhash(FunctionBuilder &fn, BlockBuilder &block,
                                   WasmTemporary ptr, const CharacterSequence &ty)
{
    if (ty.size() <= 64) {
        /* If the string is shorter than its max. length, it is padded with NUL bytes.  Hence, we can hash the string by
         * simply combinding all characters into a single i64 and bit mix. */
        WasmTemporary hash;
        for (std::size_t i = 0; i != ty.length; ++i) {
            WasmTemporary next_byte = BinaryenLoad(
                /* module= */ fn.module(),
                /* bytes=  */ 1,
                /* signed= */ false,
                /* offset= */ i,
                /* align=  */ 0,
                /* type=   */ BinaryenTypeInt64(),
                /* ptr=    */ ptr
            );
            if (hash.is()) {
                WasmTemporary shifted = BinaryenBinary(
                    /* module= */ fn.module(),
                    /* op=     */ BinaryenShlInt64(),
                    /* left=   */ hash,
                    /* right=  */ BinaryenConst(fn.module(), BinaryenLiteralInt64(8))
                );
                hash = BinaryenBinary(
                    /* module= */ fn.module(),
                    /* op=     */ BinaryenOrInt64(),
                    /* left=   */ shifted,
                    /* right=  */ next_byte
                );
            } else {
                hash = std::move(next_byte);
            }
        }
        return WasmBitMixMurmur3{}.emit(fn.module(), fn, block, std::move(hash));
    }

    /* Initialize hash. */
    WasmVariable hash(fn, BinaryenTypeInt64());
    block += hash.set(BinaryenConst(fn.module(), BinaryenLiteralInt64(0xcbf29ce484222325UL)));

    WasmVariable byte(fn, BinaryenTypeInt64());

#if 0
    if (ty.length <= 8) {
        /* Unroll FNV-1a loop to compute hash. */
        M_insist(ty.length >= 1);
        block += hash.set(BinaryenConst(fn.module(), BinaryenLiteralInt64(0xcbf29ce484222325UL)));
        for (std::size_t i = 0; i != ty.length; ++i) {
            WasmTemporary next_byte = BinaryenLoad(
                /* module= */ fn.module(),
                /* bytes=  */ 1,
                /* signed= */ false,
                /* offset= */ i,
                /* align=  */ 0,
                /* type=   */ BinaryenTypeInt64(),
                /* ptr=    */ ptr
            );
            block += byte.set(i == 0 ? std::move(next_byte) : WasmTemporary(BinaryenSelect(
                /* module= */ fn.module(),
                /* condition= */ byte,
                /* ifTrue=    */ next_byte,
                /* ifFalse=   */ byte,              // byte remains unmodified if byte is NUL
                /* type=      */ BinaryenTypeInt64()
            )));
            WasmTemporary Xor = BinaryenBinary(
                /* module= */ fn.module(),
                /* op=     */ BinaryenXorInt64(),
                /* left=   */ hash,
                /* right=  */ byte
            );
            WasmTemporary Mul = BinaryenBinary(
                /* module= */ fn.module(),
                /* op=     */ BinaryenMulInt64(),
                /* left=   */ Xor,
                /* right=  */ BinaryenConst(fn.module(), BinaryenLiteralInt64(0x100000001b3UL))
            );
            block += hash.set(BinaryenSelect(
                /* module=    */ fn.module(),
                /* condition= */ BinaryenUnary(fn.module(), BinaryenWrapInt64(), byte),
                /* ifTrue=    */ Mul,
                /* ifFalse=   */ hash,                  // hash remains unmodified if byte is NUL
                /* type=      */ BinaryenTypeInt64()
            ));
        }
        return hash;
    }
#endif

    /*----- Compute hash using FNV-1a. -------------------------------------------------------------------------------*/
    WasmVariable cursor(fn, BinaryenTypeInt32());
    block += cursor.set(std::move(ptr));

    WasmVariable end(fn, BinaryenTypeInt32());
    block += end.set(BinaryenBinary(
        /* module= */ fn.module(),
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ cursor,
        /* right=  */ BinaryenConst(fn.module(), BinaryenLiteralInt32(ty.length))
    ));

    /* Create loop with loop condition. */
    WasmTemporary is_in_bounds = BinaryenBinary(
        /* module= */ fn.module(),
        /* op=     */ BinaryenNeInt32(),
        /* left=   */ cursor,
        /* right=  */ end
    );
    WasmWhile loop(fn.module(), "strhash.foreach", std::move(is_in_bounds));

    /* Load next byte. */
    loop += byte.set(BinaryenLoad(
        /* module= */ fn.module(),
        /* bytes=  */ 1,
        /* signed= */ false,
        /* offset= */ 0,
        /* align=  */ 0,
        /* type=   */ BinaryenTypeInt64(),
        /* ptr=    */ cursor
    ));

    /* Abort if byte is NUL. */
    WasmTemporary byte_is_nul = BinaryenUnary(
        /* module= */ fn.module(),
        /* op=     */ BinaryenEqZInt32(),
        /* value=  */ byte
    );
    loop += BinaryenBreak(
        /* module=    */ fn.module(),
        /* name=      */ loop.body().name(),
        /* condition= */ byte_is_nul,
        /* value=     */ nullptr
    );

    /* Update hash. */
    WasmTemporary Xor = BinaryenBinary(
        /* module= */ fn.module(),
        /* op=     */ BinaryenXorInt64(),
        /* left=   */ hash,
        /* right=  */ byte
    );
    loop += hash.set(BinaryenBinary(
        /* module= */ fn.module(),
        /* op=     */ BinaryenMulInt64(),
        /* left=   */ Xor,
        /* right=  */ BinaryenConst(fn.module(), BinaryenLiteralInt64(0x100000001b3UL))
    ));

    /* Advance cursor. */
    loop += BinaryenBinary(
        /* module= */ fn.module(),
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ cursor,
        /* right=  */ BinaryenConst(fn.module(), BinaryenLiteralInt32(1))
    );

    return hash;
}

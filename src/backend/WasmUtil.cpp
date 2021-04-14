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

void WasmTemporary::dump() const { if (ref_) BinaryenExpressionPrint(ref_); }


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
        unreachable("this is a string and should've been handled earlier");
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
        unreachable("invalid type");

    set(BinaryenConst(module(), literal));
}

void WasmExprCompiler::operator()(const UnaryExpr &e)
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
                    unreachable("bitwise not is not supported for floating-point types");
            }
            break;
        }

        case TK_Not: {
            /* In WebAssembly, booleans are represented as 32 bit integers.  Hence, logical not is achieved by computing
             * 1 - value. */
            insist(e.type()->is_boolean());
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
    insist(lhs.is());
    (*this)(*e.rhs);
    WasmTemporary rhs = get();
    insist(rhs.is());

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
        insist(ty_rhs->is_character_sequence()); \
        auto cs_lhs = as<const CharacterSequence>(ty_lhs); \
        auto cs_rhs = as<const CharacterSequence>(ty_rhs); \
        set(WasmStrcmp::OP(fn(), *block_, *cs_lhs, *cs_rhs, std::move(lhs), std::move(rhs))); \
    } else if (ty_lhs->is_numeric()) { \
        /* Convert numeric operands, if necessary. */ \
        insist(ty_rhs->is_numeric()); \
        auto n_lhs = as<const Numeric>(ty_lhs); \
        auto n_rhs = as<const Numeric>(ty_rhs); \
        auto n = arithmetic_join(n_lhs, n_rhs); \
        lhs = convert(module(), std::move(lhs), n_lhs, n); \
        rhs = convert(module(), std::move(rhs), n_rhs, n); \
        set(WasmCompare::OP(fn(), *n, std::move(lhs), std::move(rhs))); \
    } else { \
        insist(ty_lhs->as_vectorial() == ty_rhs->as_vectorial()); \
        set(WasmCompare::OP(fn(), *ty_lhs, std::move(lhs), std::move(rhs))); \
    } \
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
        case TK_EQUAL:          CMP(Eq); break;
        case TK_BANG_EQUAL:     CMP(Ne); break;

        case TK_And:
            insist(e.type()->is_boolean());
            BINARY_OP(And, Int32); // booleans are represented as 32 bit integers
            break;

        case TK_Or:
            insist(e.type()->is_boolean());
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
    insist(wasm_cnf.is(), "empty CNF?");

    return wasm_cnf;
}

void WasmEnvironment::dump(std::ostream &out) const
{
    out << "WasmEnvironment in function " << fn().name() << "\n` entries: { ";
    for (auto it = values_.begin(), end = values_.end(); it != end; ++it) {
        if (it != values_.begin()) out << ", ";
        out << it->first;
    }
    out << " }" << std::endl;
}

void WasmEnvironment::operator()(const Designator &e)
{
    try {
        /* Search with fully qualified name. */
        set(get_value({e.table_name.text, e.attr_name.text}));
    } catch (std::out_of_range) { try {
        /* Search with unqualified name. */
        set(get_value({nullptr, e.attr_name.text}));
    } catch (std::out_of_range) {
        /* Search with target name. */
        auto t = e.target();
        if (auto pe = std::get_if<const Expr*>(&t)) {
            return (*this)(**pe);
        } else {
            auto pa = std::get_if<const Attribute*>(&t);
            insist(pa, "Target is neither an expression nor an attribute");
            auto &attr = **pa;
            try {
                set(get_value({attr.table.name, attr.name}));
            } catch (std::out_of_range) {
                unreachable("Designator could not be resolved");
            }
        }
    } }
}

void WasmEnvironment::operator()(const FnApplicationExpr &e)
{
    auto &C = Catalog::Get();
    auto &fn = e.get_function();

    /* Load the arguments for the function call. */
    switch (fn.fnid) {
        default:
            unreachable("function kind not implemented");

        case Function::FN_UDF:
            unreachable("UDFs not yet supported");

        case Function::FN_ISNULL:
            unreachable("not yet supported");

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
    WasmStruct::index_type idx;
    try {
        idx = index({e.table_name.text, e.attr_name.text});
    } catch (std::out_of_range) { try {
        idx = index({nullptr, e.attr_name.text});
    } catch (std::out_of_range) {
        auto t = e.target();
        if (auto pe = std::get_if<const Expr*>(&t)) {
            return (*this)(**pe);
        } else {
            auto pa = std::get_if<const Attribute*>(&t);
            insist(pa, "Target is neither an expression nor an attribute");
            auto &attr = **pa;
            try {
                idx = index({attr.table.name, attr.name});
            } catch (std::out_of_range) {
                unreachable("Designator could not be resolved");
            }
        }
    } }

    set(struc.load(fn(), base_ptr_.clone(module()), idx, struc_offset_));
}

void WasmStructCGContext::operator()(const FnApplicationExpr &e)
{
    auto &C = Catalog::Get();
    auto &fn = e.get_function();

    /* Load the arguments for the function call. */
    switch (fn.fnid) {
        default:
            unreachable("function kind not implemented");

        case Function::FN_UDF:
            unreachable("UDFs not yet supported");

        case Function::FN_ISNULL:
            unreachable("not yet supported");

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
        insist(offset(idx) % 8 == 0, "type must be byte-aligned");
        return BinaryenBinary(
            /* module= */ fn.module(),
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ ptr,
            /* right=  */ BinaryenConst(fn.module(), BinaryenLiteralInt32(offset(idx) / 8 + struc_offset))
        );
    } else {
        insist(offset(idx) % 8 == 0, "type must be byte-aligned");
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
        insist(offset(idx) % 8 == 0, "type must be byte-aligned");
        /* Compute destination address. */
        WasmTemporary dest = BinaryenBinary(
            /* module= */ fn.module(),
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ ptr,
            /* right=  */ BinaryenConst(fn.module(), BinaryenLiteralInt32(offset(idx) / 8))
        );
        /* Copy string to destination. */
        WasmStrncpy wasm_strncpy(fn);
        wasm_strncpy.emit(block, std::move(dest), std::move(val), as<const CharacterSequence>(type(idx)).length);
    } else {
        insist(offset(idx) % 8 == 0, "type must be byte-aligned");
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


/*======================================================================================================================
 * BlockBuilder
 *====================================================================================================================*/

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


/*======================================================================================================================
 * FunctionBuilder
 *====================================================================================================================*/

void FunctionBuilder::dump(std::ostream &out) const
{
    out << "function \"" << name() << "\"";
    block().dump(out);
}
void FunctionBuilder::dump() const { dump(std::cerr); }


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
        default: unreachable("unknown comparison operator"); \
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
        [](auto&) { unreachable("unsupported type"); }
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
                            unreachable("invalid integer type");
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
                    unreachable("not supported");

                case Numeric::N_Float:
                    if (n.size() <= 32)
                        LIMIT(Float32, float);
                    else
                        LIMIT(Float64, double);
                    break;
            }
        },
        [](auto&) { unreachable("invalid type"); },
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
            insist(not e.type->is_character_sequence());
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
            unreachable("not implemented");

        case JoinOperator::J_SimpleHashJoin: {
            insist(op.children().size() == 2, "SimpleHashJoin is a binary operation and expects exactly two children");
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
    const WasmStruct layout(schema); // create Wasm struct used as data layout
    const WasmVariable &out = module().head_of_heap();
    std::size_t idx = 0;
    for (auto &attr : schema)
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

    /*----- Get the number of rows in the scanned table. -------------------------------------------------------------*/
    auto & table = op.store().table();
    oss << table.name << "_num_rows";
    module().import(oss.str(), BinaryenTypeInt32());
    WasmTemporary num_rows = module().get_imported(oss.str(), BinaryenTypeInt32());

    WasmVariable induction(module().main(), BinaryenTypeInt32()); // initialized to 0

    oss.str("");
    oss << "scan_" << op.alias();
    WasmWhile loop(module(), oss.str().c_str(), BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenLtUInt32(),
        /* left=   */ induction,
        /* right=  */ num_rows
    ));

    /*----- Generate code to access attributes and emit code for the rest of the pipeline. ---------------------------*/
    swap(block_, loop);
    WasmStoreCG store(*this, op);
    store(op.store());
    swap(block_, loop);

    /*----- Increment induction variable. ----------------------------------------------------------------------------*/
    loop += induction.set(BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ induction,
        /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(1))
    ));

    block_ += loop.finalize();
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
        insist(it != intermediates_.end(), "unknown identifier");
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
            unreachable("not implemented");

        case JoinOperator::J_SimpleHashJoin: {
            WasmHashMumur3_64A hasher;
            auto data = as<SimpleHashJoinData>(op.data());

            /*----- Decompose the join predicate of the form `A.x = B.y` into parts `A.x` and `B.y`. -----------------*/
            auto &pred = op.predicate();
            insist(pred.size() == 1, "invalid predicate for simple hash join");
            auto &clause = pred[0];
            insist(clause.size() == 1, "invalid predicate for simple hash join");
            auto &literal = clause[0];
            insist(not literal.negative(), "invalid predicate for simple hash join");
            auto binary = as<const BinaryExpr>(literal.expr());
            insist(binary->tok == TK_EQUAL, "invalid predicate for simple hash join");
            auto first = as<const Designator>(binary->lhs);
            auto second = as<const Designator>(binary->rhs);
            auto [build, probe] = op.child(0)->schema().has({first->get_table_name(), first->attr_name.text}) ?
                                  std::make_pair(first, second) : std::make_pair(second, first);

            Schema::Identifier build_key_id(build->table_name.text, build->attr_name.text);
            Schema::Identifier probe_key_id(probe->table_name.text, probe->attr_name.text);

            if (data->is_build_phase) {
                data->struc = new WasmStruct(op.child(0)->schema());
                std::vector<WasmTemporary> key;
                key.emplace_back(context().get_value(build_key_id));

                /*----- Compute payload ids. -------------------------------------------------------------------------*/
                std::vector<Schema::Identifier> payload_ids;
                for (auto attr : op.child(0)->schema()) {
                    if (attr.id != build_key_id)
                        payload_ids.push_back(attr.id);
                }

                /* Get key field index. */
                auto [key_index, _] = op.child(0)->schema()[build_key_id];

                /*----- Allocate hash table. -------------------------------------------------------------------------*/
                uint32_t initial_capacity = 32;
                if (auto scan = cast<ScanOperator>(op.child(0))) /// XXX: hack for pre-allocation
                    initial_capacity = ceil_to_pow_2<decltype(initial_capacity)>(scan->store().num_rows() / .8);

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
                    /* numOperands= */ ARR_SIZE(rehash_args),
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
                std::size_t idx = 0;
                for (auto attr : op.child(0)->schema())
                    HT->store_value_to_slot(block_, slot_addr, idx++, context().get_value(attr.id));

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
                for (auto &attr : op.child(0)->schema())
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
    auto p = op.projections().begin();
    for (auto &e : op.schema()) {
        if (not context().has(e.id)) {
            insist(p != op.projections().end());
            context().add(e.id, context().compile(block_, *p->first));
        }
        ++p;
    }
    (*this)(*op.parent());
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
    std::size_t initial_capacity = 0;

    /*--- XXX: Crude hack to do pre-allocation in benchmarks. --------------------------------------------------------*/
    if (not op.group_by().empty()) {
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

        if (num_groups_est > 1e9)
            initial_capacity = 1e9 / .7;
        else
            initial_capacity = ceil_to_pow_2<decltype(initial_capacity)>(num_groups_est / .7);
estimation_failed:;
    }

    if (auto scan = cast<ScanOperator>(op.child(0))) { // XXX: hack for pre-allocation
        if (initial_capacity == 0)
            initial_capacity = ceil_to_pow_2<decltype(initial_capacity)>(scan->store().num_rows() / .8);
        else
            initial_capacity = std::min(initial_capacity, ceil_to_pow_2<decltype(initial_capacity)>(scan->store().num_rows() / .8));
    }

    if (initial_capacity == 0)
        initial_capacity = 32;

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
            /* numOperands= */ ARR_SIZE(rehash_args),
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
        insist(fn.kind == Function::FN_Aggregate, "not an aggregation function");

        /*----- Emit code to evaluate arguments. ---------------------------------------------------------------------*/
        insist(fn_expr->args.size() <= 1, "unsupported aggregate with more than one argument");
        std::vector<WasmTemporary> args;
        for (auto arg : fn_expr->args)
            args.emplace_back(context().compile(block_, *arg));

        switch (fn.fnid) {
            default:
                unreachable("unsupported aggregate function");

            case Function::FN_MIN: {
                insist(args.size() == 1, "aggregate function expects exactly one argument");
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
                        unreachable("not implemented");

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
                insist(args.size() == 1, "aggregate function expects exactly one argument");
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
                        unreachable("not implemented");

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
                insist(args.size() == 1, "aggregate function expects exactly one argument");
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
                insist(args.size() == 1, "aggregate function expects exactly one argument");
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
                    unreachable("not yet supported");
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
        insist(fn.kind == Function::FN_Aggregate, "not an aggregation function");

        auto &agg_ty = *fn_expr->type();
        auto &agg_val = data->aggregates[i];

        switch (fn.fnid) {
            default:
                unreachable("unsupported aggregate function");

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
                        unreachable("not implemented");

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
                        unreachable("not implemented");

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

void WasmStoreCG::operator()(const RowStore &store)
{
    std::ostringstream oss;
    auto &table = store.table();

    /*----- Import table address. ------------------------------------------------------------------------------------*/
    WasmVariable row_addr(pipeline.module().main(), BinaryenTypeInt32());
    pipeline.module().import(table.name, BinaryenTypeInt32());
    pipeline.module().main().block() += row_addr.set(pipeline.module().get_imported(table.name, BinaryenTypeInt32()));

    /*----- Generate code to access null bitmap and value of all required attributes. --------------------------------*/
    const auto null_bitmap_offset = store.offset(table.size());
    for (auto &e : op.schema()) {
        auto &attr = table[e.id.name];

        /*----- Generate code for null bit check. --------------------------------------------------------------------*/
        {
            const auto null_bit_offset = null_bitmap_offset + attr.id;
            const auto qwords = null_bit_offset / 32;
            const auto bits = null_bit_offset % 32;

            WasmTemporary null_bitmap_addr = BinaryenBinary(
                /* module= */ pipeline.module(),
                /* op=     */ BinaryenAddInt32(),
                /* left=   */ row_addr,
                /* right=  */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(qwords))
            );
            WasmTemporary qword = BinaryenLoad(
                /* module= */ pipeline.module(),
                /* bytes=  */ 4,
                /* signed= */ false,
                /* offset= */ 0,
                /* align=  */ 0,
                /* type=   */ BinaryenTypeInt32(),
                /* ptr=    */ null_bitmap_addr
            );
            WasmTemporary shr = BinaryenBinary(
                /* module= */ pipeline.module(),
                /* op=     */ BinaryenShrUInt32(),
                /* left=   */ qword,
                /* right=  */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(bits))
            );
            WasmTemporary is_null = BinaryenBinary(
                /* module= */ pipeline.module(),
                /* op=     */ BinaryenAndInt32(),
                /* left=   */ shr,
                /* right=  */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(1))
            );
            // TODO add to context nulls
            (void) is_null;
        }

        /*----- Generate code for value access. ----------------------------------------------------------------------*/
        if (attr.type->size() < 8) {
            WasmTemporary byte = BinaryenLoad(
                /* module= */ pipeline.module(),
                /* bytes=  */ 1,
                /* signed= */ false,
                /* offset= */ store.offset(attr.id) / 8,
                /* align=  */ 0,
                /* type=   */ BinaryenTypeInt32(),
                /* ptr=    */ row_addr
            );
            WasmTemporary shifted = BinaryenBinary(
                /* module= */ pipeline.module(),
                /* op=     */ BinaryenShrUInt32(),
                /* lhs=    */ byte,
                /* rhs=    */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(store.offset(attr.id) % 8))
            );
            WasmTemporary value = BinaryenBinary(
                /* module= */ pipeline.module(),
                /* op=     */ BinaryenAndInt32(),
                /* lhs=    */ shifted,
                /* rhs=    */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32((1 << attr.type->size()) - 1))
            );
            pipeline.context().add(e.id, std::move(value));
        } else if (attr.type->is_character_sequence()) {
            WasmTemporary address = BinaryenBinary(
                /* module= */ pipeline.module(),
                /* op=     */ BinaryenAddInt32(),
                /* left=   */ row_addr,
                /* right=  */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(store.offset(attr.id) / 8))
            );
            pipeline.context().add(e.id, std::move(address));
        } else {
            WasmTemporary value = BinaryenLoad(
                /* module= */ pipeline.module(),
                /* bytes=  */ attr.type->size() / 8,
                /* signed= */ true,
                /* offset= */ store.offset(attr.id) / 8,
                /* align=  */ 0,
                /* type=   */ get_binaryen_type(attr.type),
                /* ptr=    */ row_addr
            );
            pipeline.context().add(e.id, std::move(value));
        }
    }

    /*----- Generate code for rest of the pipeline. ------------------------------------------------------------------*/
    pipeline(*op.parent());

    /*----- Emit code to advance to next row. ------------------------------------------------------------------------*/
    pipeline.block_ += row_addr.set(BinaryenBinary(
        /* module= */ pipeline.module(),
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ row_addr,
        /* right=  */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(store.row_size() / 8))
    ));
}

void WasmStoreCG::operator()(const ColumnStore &store)
{
    std::ostringstream oss;
    auto &table = store.table();

    /* Import null bitmap column address. */
    // TODO

    /*----- Generate code to access null bitmap and value of all required attributes. --------------------------------*/
    std::vector<WasmVariable> col_addrs;
    for (auto &e : op.schema()) {
        auto &attr = table[e.id.name];

        oss.str("");
        oss << table.name << '.' << attr.name;
        auto name = oss.str();

        /*----- Import column address. -------------------------------------------------------------------------------*/
        auto &col_addr = col_addrs.emplace_back(pipeline.module().main(), BinaryenTypeInt32());
        pipeline.module().import(name, BinaryenTypeInt32());
        pipeline.module().main().block() += col_addr.set(pipeline.module().get_imported(name, BinaryenTypeInt32()));

        if (attr.type->size() < 8) {
            unreachable("not implemented");
        } else if (attr.type->is_character_sequence()) {
            pipeline.context().add(e.id, col_addr);
        } else {
            WasmTemporary value = BinaryenLoad(
                /* module=  */ pipeline.module(),
                /* bytes=   */ attr.type->size() / 8,
                /* signed=  */ true,
                /* offset=  */ 0,
                /* align=   */ 0,
                /* type=    */ get_binaryen_type(attr.type),
                /* ptr=     */ col_addr
            );
            pipeline.context().add(e.id, std::move(value));
        }
    }

    /*----- Generate code for rest of the pipeline. ------------------------------------------------------------------*/
    pipeline(*op.parent());

    /*----- Emit code to advance each column address to next row. ----------------------------------------------------*/
    auto col_addr_it = col_addrs.cbegin();
    for (auto &e : op.schema()) {
        auto &attr = table[e.id.name];
        auto &col_addr = *col_addr_it++;
        const auto attr_size = std::max<std::size_t>(1, attr.type->size() / 8);

        pipeline.block_ += col_addr.set(BinaryenBinary(
            /* module= */ pipeline.module(),
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ col_addr,
            /* right=  */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(attr_size))
        ));
    }
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
        std::cerr << "hashing CharacterSequence of length " << ty.length << " by combinding characters into single i64 "
                     "and bit mixing." << std::endl;
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
        insist(ty.length >= 1);
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

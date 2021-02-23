#include "backend/WasmUtil.hpp"

#include "backend/Interpreter.hpp"
#include <limits>


using namespace m;


/*======================================================================================================================
 * WasmCGContext
 *====================================================================================================================*/

WasmTemporary WasmCGContext::compile(const cnf::CNF &cnf) const
{
    WasmTemporary wasm_cnf;
    for (auto &clause : cnf) {
        WasmTemporary wasm_clause;
        for (auto &pred : clause) {
            /* Generate code for the literal of the predicate. */
            WasmTemporary wasm_pred = compile(*pred.expr());
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

void WasmCGContext::dump(std::ostream &out) const
{
    out << "WasmCGContext:\n";
    out << "  null values:\n";
    for (auto &e : nulls_) {
        out << "    " << e.first << ":\n";
        BinaryenExpressionPrint(e.second.clone(module()));
    }
    out << "  attribute values:\n";
    for (auto &e : values_) {
        out << "    " << e.first << ":\n";
        BinaryenExpressionPrint(e.second.clone(module()));
    }
    out << std::endl;
}
void WasmCGContext::dump() const { dump(std::cerr); }

void WasmCGContext::operator()(const ErrorExpr&) { unreachable("no errors at this stage"); }

void WasmCGContext::operator()(const Designator &e)
{
    /* Search with fully qualified name. */
    auto it = values_.find({e.table_name.text, e.attr_name.text});
    /* Search with unqualified name. */
    if (it == values_.end())
        it = values_.find({nullptr, e.attr_name.text});
    /* Search with target name. */
    if (it == values_.end()) {
        auto t = e.target();
        if (auto pe = std::get_if<const Expr*>(&t)) {
            return (*this)(**pe);
        } else {
            auto pa = std::get_if<const Attribute*>(&t);
            insist(pa, "Target is neither an expression nor an attribute");
            auto &attr = **pa;
            it = values_.find({attr.table.name, attr.name});
        }
    }
    insist(it != values_.end(), "no value for the given designator");
    expr_ = it->second.clone(module());
}

void WasmCGContext::operator()(const Constant &e)
{
    auto value = Interpreter::eval(e);
    struct BinaryenLiteral literal;

    auto ty = e.type();
    if (ty->is_boolean()) {
        literal = BinaryenLiteralInt32(value.as_b());
    } else if (ty->is_character_sequence()) {
        unreachable("not yet implemented");
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

    expr_ = BinaryenConst(module(), literal);
}

void WasmCGContext::operator()(const FnApplicationExpr &e)
{
    (void) e;
    unreachable("not implemented");
}

void WasmCGContext::operator()(const UnaryExpr &e)
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

void WasmCGContext::operator()(const BinaryExpr &e)
{
    (*this)(*e.lhs);
    auto lhs = std::move(expr_);
    insist(lhs.is());
    (*this)(*e.rhs);
    auto rhs = std::move(expr_);
    insist(rhs.is());

#define BINARY_OP(OP, TYPE) \
    expr_ = BinaryenBinary(/* module= */ module(), \
                           /* op=     */ Binaryen ## OP ## TYPE(), \
                           /* left=   */ lhs, \
                           /* right=  */ rhs) \

#define BINARY(OP) \
{ \
    auto n = as<const Numeric>(e.type()); \
    lhs = convert(module(), std::move(lhs), as<const Numeric>(e.lhs->type()), n); \
    rhs = convert(module(), std::move(rhs), as<const Numeric>(e.rhs->type()), n); \
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

#define CMP(OP, OPS) \
{ \
    if (e.lhs->type()->is_numeric()) { \
        insist(e.rhs->type()->is_numeric()); \
        auto n_lhs = as<const Numeric>(e.lhs->type()); \
        auto n_rhs = as<const Numeric>(e.rhs->type()); \
        auto n = arithmetic_join(n_lhs, n_rhs); \
        lhs = convert(module(), std::move(lhs), n_lhs, n); \
        rhs = convert(module(), std::move(rhs), n_rhs, n); \
        switch (n->kind) { \
            case Numeric::N_Int: \
            case Numeric::N_Decimal: { \
                if (n->size() <= 32) \
                    BINARY_OP(OPS, Int32); \
                else \
                    BINARY_OP(OPS, Int64); \
                break; \
            } \
            case Numeric::N_Float: \
                if (n->precision == 32) \
                    BINARY_OP(OP, Float32); \
                else \
                    BINARY_OP(OP, Float64); \
                break; \
        } \
        break; \
    } else if (e.lhs->type()->is_date()) { \
        insist(e.rhs->type()->is_date()); \
        BINARY_OP(OPS, Int32); \
    } else if (e.lhs->type()->is_date_time()) { \
        insist(e.rhs->type()->is_date_time()); \
        BINARY_OP(OPS, Int64); \
    } else { \
        unreachable("invalid type"); \
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

        case TK_LESS:           CMP(Lt, LtS); break;
        case TK_GREATER:        CMP(Gt, GtS); break;
        case TK_LESS_EQUAL:     CMP(Le, LeS); break;
        case TK_GREATER_EQUAL:  CMP(Ge, GeS); break;
        case TK_EQUAL:          CMP(Eq, Eq); break;
        case TK_BANG_EQUAL:     CMP(Ne, Ne); break;

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

void WasmCGContext::operator()(const QueryExpr &e)
{
    /* Search with fully qualified name. */
    Catalog &C = Catalog::Get();
    auto it = values_.find({e.alias(), C.pool("$res")});
    insist(it != values_.end(), "no value for the given designator");
    expr_ = it->second.clone(module());
}


/*======================================================================================================================
 * WasmStruct
 *====================================================================================================================*/

void WasmStruct::dump(std::ostream &out) const
{
    out << "WasmStruct of schema " << schema << " and size " << size() << " bytes";
    std::size_t idx = 0;
    for (auto &attr : schema) {
        out << "\n  " << idx << ": " << attr.id << " of type " << *attr.type << " at offset " << offset(idx);
        ++idx;
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

WasmTemporary WasmCompare::emit(FunctionBuilder &fn, BlockBuilder &block,
                                const WasmCGContext &left, const WasmCGContext &right)
{
    WasmTemporary val_cmp;

    for (auto &o : order) {
        WasmVariable val_left (fn, get_binaryen_type(o.first->type()));
        WasmVariable val_right(fn, get_binaryen_type(o.first->type()));
        block += val_left .set(left .compile(*o.first));
        block += val_right.set(right.compile(*o.first));

        WasmTemporary val_lt, val_gt;
        auto n = as<const Numeric>(o.first->type());
        switch (n->kind) {
            case Numeric::N_Int:
            case Numeric::N_Decimal: {
                val_lt = BinaryenBinary(
                    /* module= */ module_,
                    /* op=     */ n->size() <= 32 ? BinaryenLtSInt32() : BinaryenLtSInt64(),
                    /* left=   */ val_left,
                    /* right=  */ val_right
                );
                val_gt = BinaryenBinary(
                    /* module= */ module_,
                    /* op=     */ n->size() <= 32 ? BinaryenGtSInt32() : BinaryenGtSInt64(),
                    /* left=   */ val_left,
                    /* right=  */ val_right
                );
                break;
            }

            case Numeric::N_Float: {
                val_lt = BinaryenBinary(
                    /* module= */ module_,
                    /* op=     */ n->size() == 32 ? BinaryenLtFloat32() : BinaryenLtFloat64(),
                    /* left=   */ val_left,
                    /* right=  */ val_right
                );
                val_gt = BinaryenBinary(
                    /* module= */ module_,
                    /* op=     */ n->size() == 32 ? BinaryenGtFloat32() : BinaryenGtFloat64(),
                    /* left=   */ val_left,
                    /* right=  */ val_right
                );
                break;
            }
        }

        /* Ascending: val_gt - val_lt, Descending: val_lt - val_gt */
        WasmTemporary val_sub = BinaryenBinary(
            /* module= */ module_,
            /* op=     */ BinaryenSubInt32(),
            /* left=   */ o.second ? val_gt : val_lt,
            /* right=  */ o.second ? val_lt : val_gt
        );
        if (val_cmp.is()) {
            /*----- Update the comparison variable. ------------------------------------------------------------------*/
            WasmTemporary val_shifted = BinaryenBinary(
                /* module= */ module_,
                /* op=     */ BinaryenShlInt32(),
                /* left=   */ val_cmp,
                /* right=  */ BinaryenConst(module_, BinaryenLiteralInt32(1))
            );
            val_cmp = BinaryenBinary(
                /* module= */ module_,
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

WasmTemporary WasmCompare::Eq(BinaryenModuleRef module, const Type &ty, WasmTemporary left, WasmTemporary right)
{
    struct V : ConstTypeVisitor
    {
        BinaryenModuleRef module;
        WasmTemporary left, right, cmp;

        V(BinaryenModuleRef module, WasmTemporary left, WasmTemporary right)
            : module(module)
            , left(std::move(left))
            , right(std::move(right))
        { }

        WasmTemporary get() { return std::move(cmp); }

        using ConstTypeVisitor::operator();
        void operator()(Const<ErrorType>&) { unreachable("not allowed"); }
        void operator()(Const<Boolean>&) {
            cmp = BinaryenBinary(
                /* module= */ module,
                /* op=     */ BinaryenEqInt32(),
                /* left=   */ left,
                /* right=  */ right
            );
        }
        void operator()(Const<CharacterSequence>&) { unreachable("not supported"); }
        void operator()(Const<Date>&) {
            cmp = BinaryenBinary(
                    /* module= */ module,
                    /* op=     */ BinaryenEqInt32(),
                    /* left=   */ left,
                    /* right=  */ right
            );
        }
        void operator()(Const<DateTime>&) {
            cmp = BinaryenBinary(
                    /* module= */ module,
                    /* op=     */ BinaryenEqInt64(),
                    /* left=   */ left,
                    /* right=  */ right
            );
        }
        void operator()(Const<Numeric> &ty) {
            switch (ty.kind) {
                case Numeric::N_Int:
                    if (ty.size() <= 32) {
                        cmp = BinaryenBinary(
                            /* module= */ module,
                            /* op=     */ BinaryenEqInt32(),
                            /* left=   */ left,
                            /* right=  */ right
                        );
                    } else {
                        cmp = BinaryenBinary(
                            /* module= */ module,
                            /* op=     */ BinaryenEqInt64(),
                            /* left=   */ left,
                            /* right=  */ right
                        );
                    }
                    break;

                case Numeric::N_Decimal:
                    unreachable("not supported");

                case Numeric::N_Float:
                    if (ty.size() == 32) {
                        cmp = BinaryenBinary(
                            /* module= */ module,
                            /* op=     */ BinaryenEqFloat32(),
                            /* left=   */ left,
                            /* right=  */ right
                        );
                    } else {
                        cmp = BinaryenBinary(
                            /* module= */ module,
                            /* op=     */ BinaryenEqFloat64(),
                            /* left=   */ left,
                            /* right=  */ right
                        );
                    }
                    break;
            }
        }
        void operator()(Const<FnType>&) { unreachable("not allowed"); }
    };

    V v(module, std::move(left), std::move(right));
    v(ty);
    return v.get();
}

WasmTemporary WasmCompare::Ne(BinaryenModuleRef module, const Type &ty,
                                      WasmTemporary left, WasmTemporary right)
{
    struct V : ConstTypeVisitor
    {
        BinaryenModuleRef module;
        WasmTemporary left, right, cmp;

        V(BinaryenModuleRef module, WasmTemporary left, WasmTemporary right)
            : module(module)
            , left(std::move(left))
            , right(std::move(right))
        { }

        WasmTemporary get() { return std::move(cmp); }

        using ConstTypeVisitor::operator();
        void operator()(Const<ErrorType>&) { unreachable("not allowed"); }
        void operator()(Const<Boolean>&) {
            cmp = BinaryenBinary(
                /* module= */ module,
                /* op=     */ BinaryenNeInt32(),
                /* left=   */ left,
                /* right=  */ right
            );
        }
        void operator()(Const<CharacterSequence>&) { unreachable("not supported"); }
        void operator()(Const<Date>&) {
            cmp = BinaryenBinary(
                    /* module= */ module,
                    /* op=     */ BinaryenNeInt32(),
                    /* left=   */ left,
                    /* right=  */ right
            );
        }
        void operator()(Const<DateTime>&) {
            cmp = BinaryenBinary(
                    /* module= */ module,
                    /* op=     */ BinaryenNeInt64(),
                    /* left=   */ left,
                    /* right=  */ right
            );
        }
        void operator()(Const<Numeric> &ty) {
            switch (ty.kind) {
                case Numeric::N_Int:
                    if (ty.size() <= 32) {
                        cmp = BinaryenBinary(
                            /* module= */ module,
                            /* op=     */ BinaryenNeInt32(),
                            /* left=   */ left,
                            /* right=  */ right
                        );
                    } else {
                        cmp = BinaryenBinary(
                            /* module= */ module,
                            /* op=     */ BinaryenNeInt64(),
                            /* left=   */ left,
                            /* right=  */ right
                        );
                    }
                    break;

                case Numeric::N_Decimal:
                    unreachable("not supported");

                case Numeric::N_Float:
                    if (ty.size() == 32) {
                        cmp = BinaryenBinary(
                            /* module= */ module,
                            /* op=     */ BinaryenNeFloat32(),
                            /* left=   */ left,
                            /* right=  */ right
                        );
                    } else {
                        cmp = BinaryenBinary(
                            /* module= */ module,
                            /* op=     */ BinaryenNeFloat64(),
                            /* left=   */ left,
                            /* right=  */ right
                        );
                    }
                    break;
            }
        }
        void operator()(Const<FnType>&) { unreachable("not allowed"); }
    };

    V v(module, std::move(left), std::move(right));
    v(ty);
    return v.get();
}


/*======================================================================================================================
 * WasmSwap
 *====================================================================================================================*/

void WasmSwap::emit(BlockBuilder &block, const WasmStruct &struc, WasmTemporary ptr_first, WasmTemporary ptr_second)
{
    WasmVariable first (fn, BinaryenTypeInt32());
    WasmVariable second(fn, BinaryenTypeInt32());
    block += first .set(std::move(ptr_first ));
    block += second.set(std::move(ptr_second));

    auto context_first  = struc.create_load_context(first);
    auto context_second = struc.create_load_context(second);

    for (auto &attr : struc.schema) {
        BinaryenType b_attr_type = get_binaryen_type(attr.type);

        /*----- Swap left and right attribute. -----------------------------------------------------------------------*/
        /* Introduce temporary for the swap. */
        auto it = swap_temp.find(b_attr_type);
        if (it == swap_temp.end())
            it = swap_temp.emplace_hint(it, b_attr_type, fn.add_local(b_attr_type));
        auto idx_swap = it->second;

        /* tmp = *first */
        block += BinaryenLocalSet(
            /* module= */ module,
            /* index=  */ idx_swap,
            /* value=  */ context_first.get_value(attr.id)
        );
        /* *second = *first */
        block += struc.store(first, attr.id, context_second.get_value(attr.id));
        /* *first = tmp */
        block += struc.store(second, attr.id, BinaryenLocalGet(module, idx_swap, b_attr_type));
    }
}


/*======================================================================================================================
 * WasmLimits
 *====================================================================================================================*/

#define WASM_LIMIT(EXTERNAL_NAME, INTERNAL_NAME) \
BinaryenLiteral WasmLimits::EXTERNAL_NAME(const Type &type) \
{ \
    struct V : ConstTypeVisitor \
    { \
        BinaryenLiteral literal; \
        using ConstTypeVisitor::operator(); \
        void operator()(Const<ErrorType>&) { unreachable("not allowed"); } \
        void operator()(Const<Boolean>&) { literal = BinaryenLiteralInt32(std::numeric_limits<bool>::INTERNAL_NAME()); } \
        void operator()(Const<CharacterSequence>&) { unreachable("not supported"); } \
        void operator()(Const<Date>&) { literal = BinaryenLiteralInt32(std::numeric_limits<int32_t>::INTERNAL_NAME()); } \
        void operator()(Const<DateTime>&) { literal = BinaryenLiteralInt64(std::numeric_limits<int64_t>::INTERNAL_NAME()); } \
        void operator()(Const<Numeric> &ty) { \
            switch (ty.kind) { \
                case Numeric::N_Int: \
                    if (ty.size() <= 32) \
                        literal = BinaryenLiteralInt32(std::numeric_limits<int32_t>::INTERNAL_NAME()); \
                    else \
                        literal = BinaryenLiteralInt64(std::numeric_limits<int64_t>::INTERNAL_NAME()); \
                    break; \
\
                case Numeric::N_Decimal: \
                    unreachable("not supported"); \
\
                case Numeric::N_Float: \
                    if (ty.size() <= 32) \
                        literal = BinaryenLiteralFloat32(std::numeric_limits<float>::INTERNAL_NAME()); \
                    else \
                        literal = BinaryenLiteralFloat64(std::numeric_limits<double>::INTERNAL_NAME()); \
                    break; \
            } \
        } \
        void operator()(Const<FnType>&) { unreachable("not allowed"); } \
    }; \
\
    V v; \
    v(type); \
    return v.literal; \
}

WASM_LIMIT(min, min);
WASM_LIMIT(lowest, lowest);
WASM_LIMIT(max, max);
WASM_LIMIT(NaN, quiet_NaN);
WASM_LIMIT(infinity, infinity);

#undef WASM_LIMIT

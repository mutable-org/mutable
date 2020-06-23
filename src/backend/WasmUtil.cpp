#include "backend/WasmUtil.hpp"

#include "backend/Interpreter.hpp"


using namespace db;


/*======================================================================================================================
 * WasmCGContext
 *====================================================================================================================*/

BinaryenExpressionRef WasmCGContext::compile(const cnf::CNF &cnf) const
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

void WasmCGContext::dump(std::ostream &out) const
{
    out << "WasmCGContext:\n";
    out << "  null values:\n";
    for (auto &e : nulls_) {
        out << "    " << e.first << ":\n";
        BinaryenExpressionPrint(e.second);
    }
    out << "  attribute values:\n";
    for (auto &e : values_) {
        out << "    " << e.first << ":\n";
        BinaryenExpressionPrint(e.second);
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
            auto &attr = **pa;
            insist(pa, "Target is neither an expression nor an attribute");
            it = values_.find({attr.table.name, attr.name});
        }
    }
    insist(it != values_.end(), "no value for the given designator");
    expr_ = it->second;
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

#define CMP(OP, OPS) \
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
                BINARY_OP(OPS, Int32); \
            else \
                BINARY_OP(OPS, Int64); \
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
 * WasmCompare
 *====================================================================================================================*/

BinaryenExpressionRef WasmCompare::emit(FunctionBuilder &fn, BlockBuilder &block,
                                        const WasmCGContext &left, const WasmCGContext &right)
{
    BinaryenExpressionRef b_cmp = nullptr;

    for (auto &o : order) {
        auto b_left  = left.compile(*o.first);
        auto b_right = right.compile(*o.first);
        BinaryenExpressionRef b_lt, b_gt;

        auto n = as<const Numeric>(o.first->type());
        switch (n->kind) {
            case Numeric::N_Int:
            case Numeric::N_Decimal:
                if (n->size() <= 32) {
                    b_lt = BinaryenBinary(
                        /* module= */ module_,
                        /* op=     */ BinaryenLtSInt32(),
                        /* left=   */ b_left,
                        /* right=  */ b_right
                    );
                    b_gt = BinaryenBinary(
                        /* module= */ module_,
                        /* op=     */ BinaryenGtSInt32(),
                        /* left=   */ b_left,
                        /* right=  */ b_right
                    );
                } else {
                    b_lt = BinaryenBinary(
                        /* module= */ module_,
                        /* op=     */ BinaryenLtSInt64(),
                        /* left=   */ b_left,
                        /* right=  */ b_right
                    );
                    b_gt = BinaryenBinary(
                        /* module= */ module_,
                        /* op=     */ BinaryenGtSInt64(),
                        /* left=   */ b_left,
                        /* right=  */ b_right
                    );
                }
                break;

            case Numeric::N_Float:
                if (n->size() == 32) {
                    b_lt = BinaryenBinary(
                        /* module= */ module_,
                        /* op=     */ BinaryenLtFloat32(),
                        /* left=   */ b_left,
                        /* right=  */ b_right
                    );
                    b_gt = BinaryenBinary(
                        /* module= */ module_,
                        /* op=     */ BinaryenGtFloat32(),
                        /* left=   */ b_left,
                        /* right=  */ b_right
                    );
                } else {
                    b_lt = BinaryenBinary(
                        /* module= */ module_,
                        /* op=     */ BinaryenLtFloat64(),
                        /* left=   */ b_left,
                        /* right=  */ b_right
                    );
                    b_gt = BinaryenBinary(
                        /* module= */ module_,
                        /* op=     */ BinaryenGtFloat64(),
                        /* left=   */ b_left,
                        /* right=  */ b_right
                    );
                }
                break;
        }

        /* Ascending: b_gt - b_lt, Descending: b_lt - b_gt */
        auto b_sub = BinaryenBinary(
            /* module= */ module_,
            /* op=     */ BinaryenSubInt32(),
            /* left=   */ o.second ? b_gt : b_lt,
            /* right=  */ o.second ? b_lt : b_gt
        );
        if (b_cmp) {
            /*----- Update the comparison variable. ------------------------------------------------------------------*/
            auto b_shifted = BinaryenBinary(
                /* module= */ module_,
                /* op=     */ BinaryenShlInt32(),
                /* left=   */ b_cmp,
                /* right=  */ BinaryenConst(module_, BinaryenLiteralInt32(1))
            );
            auto b_upd = BinaryenBinary(
                /* module= */ module_,
                /* op=     */ BinaryenAddInt32(),
                /* left=   */ b_shifted,
                /* right=  */ b_sub
            );
            block += BinaryenLocalSet(
                /* module= */ module_,
                /* index=  */ BinaryenLocalGetGetIndex(b_cmp),
                /* value=  */ b_upd
            );
        } else {
            b_cmp = fn.add_local(BinaryenTypeInt32());
            block += BinaryenLocalSet(
                /* module= */ module_,
                /* index=  */ BinaryenLocalGetGetIndex(b_cmp),
                /* value=  */ b_sub
            );
        }
    }

    return b_cmp;
}


/*======================================================================================================================
 * WasmSwap
 *====================================================================================================================*/

void WasmSwap::emit(BlockBuilder &block, const WasmStruct &struc,
                    BinaryenExpressionRef b_first, BinaryenExpressionRef b_second)
{
    auto context_first  = struc.create_load_context(b_first);
    auto context_second = struc.create_load_context(b_second);

    for (auto &attr : struc.schema) {
        BinaryenType b_attr_type = get_binaryen_type(attr.type);

        /*----- Swap left and right attribute. -----------------------------------------------------------------------*/
        /* Introduce temporary for the swap. */
        auto it = swap_temp.find(b_attr_type);
        if (it == swap_temp.end())
            it = swap_temp.emplace_hint(it, b_attr_type, fn.add_local(b_attr_type));
        auto b_swap = it->second;

        /* tmp = *first */
        block += BinaryenLocalSet(
            /* module= */ module,
            /* index=  */ BinaryenLocalGetGetIndex(b_swap),
            /* value=  */ context_first.get_value(attr.id)
        );
        /* *second = *first */
        block += struc.store(b_first, attr.id, context_second.get_value(attr.id));
        /* *first = tmp */
        block += struc.store(b_second, attr.id, b_swap);
    }
}

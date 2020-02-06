#include "backend/StackMachine.hpp"

#include "backend/Interpreter.hpp"
#include <string_view>


using namespace db;


/*======================================================================================================================
 * StackMachineBuilder
 *====================================================================================================================*/

struct db::StackMachineBuilder : ConstASTVisitor
{
    private:
    StackMachine &stack_machine_;
    const OperatorSchema &schema_;

    public:
    StackMachineBuilder(StackMachine &stack_machine, const OperatorSchema &schema, const Expr &expr)
        : stack_machine_(stack_machine)
        , schema_(schema)
    {
        (*this)(expr); // compute the command sequence
    }

    private:
    using ConstASTVisitor::operator();

    /* Expressions */
    void operator()(Const<ErrorExpr>&) override { unreachable("invalid expression"); }
    void operator()(Const<Designator> &e) override;
    void operator()(Const<Constant> &e) override;
    void operator()(Const<FnApplicationExpr> &e) override;
    void operator()(Const<UnaryExpr> &e) override;
    void operator()(Const<BinaryExpr> &e) override;

    /* Clauses */
    void operator()(Const<ErrorClause>&) override { unreachable("not supported"); }
    void operator()(Const<SelectClause>&) override { unreachable("not supported"); }
    void operator()(Const<FromClause>&) override { unreachable("not supported"); }
    void operator()(Const<WhereClause>&) override { unreachable("not supported"); }
    void operator()(Const<GroupByClause>&) override { unreachable("not supported"); }
    void operator()(Const<HavingClause>&) override { unreachable("not supported"); }
    void operator()(Const<OrderByClause>&) override { unreachable("not supported"); }
    void operator()(Const<LimitClause>&) override { unreachable("not supported"); }

    /* Statements */
    void operator()(Const<ErrorStmt>&) override { unreachable("not supported"); }
    void operator()(Const<EmptyStmt>&) override { unreachable("not supported"); }
    void operator()(Const<CreateDatabaseStmt>&) override { unreachable("not supported"); }
    void operator()(Const<UseDatabaseStmt>&) override { unreachable("not supported"); }
    void operator()(Const<CreateTableStmt>&) override { unreachable("not supported"); }
    void operator()(Const<SelectStmt>&) override { unreachable("not supported"); }
    void operator()(Const<InsertStmt>&) override { unreachable("not supported"); }
    void operator()(Const<UpdateStmt>&) override { unreachable("not supported"); }
    void operator()(Const<DeleteStmt>&) override { unreachable("not supported"); }
    void operator()(Const<DSVImportStmt>&) override { unreachable("not supported"); }
};

void StackMachineBuilder::operator()(Const<Designator> &e)
{
    /* Given the designator, identify the position of its value in the tuple.  */
    std::size_t idx;
    if (e.has_explicit_table_name())
        idx = schema_[{e.table_name.text, e.attr_name.text}].first;
    else
        idx = schema_[{nullptr, e.attr_name.text}].first;
    insist(idx < schema_.size(), "index out of bounds");
    stack_machine_.emit_Ld_Tup(idx);
}

void StackMachineBuilder::operator()(Const<Constant> &e) { stack_machine_.add_and_emit_load(Interpreter::eval(e)); }

void StackMachineBuilder::operator()(Const<FnApplicationExpr> &e)
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
            insist(e.args.size() == 1);
            (*this)(*e.args[0]);
            stack_machine_.emit_Is_Null();
            break;

        /*----- Type casts -------------------------------------------------------------------------------------------*/
        case Function::FN_INT: {
            insist(e.args.size() == 1);
            (*this)(*e.args[0]);
            auto ty = e.args[0]->type();
            if (ty->is_float())
                stack_machine_.emit_Cast_i_f();
            else if (ty->is_double())
                stack_machine_.emit_Cast_i_d();
            else if (ty->is_decimal()) {
                unreachable("not implemented");
            } else if (ty->is_boolean()) {
                stack_machine_.emit_Cast_i_b();
            } else {
                /* nothing to be done */
            }
            break;
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
            auto idx = schema_[{name}].first;
            insist(idx < schema_.size(), "index out of bounds");
            stack_machine_.emit_Ld_Tup(idx);
            return;
        }
    }
}

void StackMachineBuilder::operator()(Const<UnaryExpr> &e)
{
    (*this)(*e.expr);
    auto ty = e.expr->type();

    switch (e.op.type) {
        default:
            unreachable("illegal token type");

        case TK_PLUS:
            /* nothing to be done */
            break;

        case TK_MINUS: {
            auto n = as<const Numeric>(ty);
            switch (n->kind) {
                case Numeric::N_Int:
                case Numeric::N_Decimal:
                    stack_machine_.emit_Minus_i();
                    break;

                case Numeric::N_Float:
                    if (n->precision == 32)
                        stack_machine_.emit_Minus_f();
                    else
                        stack_machine_.emit_Minus_d();
            }
            break;
        }

        case TK_TILDE:
            if (ty->is_integral())
                stack_machine_.emit_Neg_i();
            else if (ty->is_boolean())
                stack_machine_.emit_Not_b(); // negation of bool is always logical
            else
                unreachable("illegal type");
            break;

        case TK_Not:
            insist(ty->is_boolean(), "illegal type");
            stack_machine_.emit_Not_b();
            break;
    }
}

void StackMachineBuilder::operator()(Const<BinaryExpr> &e)
{
    auto tystr = [](const PrimitiveType *ty) -> std::string {
        if (ty->is_boolean())
            return "_b";
        if (ty->is_character_sequence())
            return "_s";
        auto n = as<const Numeric>(ty);
        switch (n->kind) {
            case Numeric::N_Int:
            case Numeric::N_Decimal:
                return "_i";
            case Numeric::N_Float:
                return n->precision == 32 ? "_f" : "_d";
        }
    };

    auto ty = as<const PrimitiveType>(e.type());
    auto ty_lhs = as<const PrimitiveType>(e.lhs->type());
    auto ty_rhs = as<const PrimitiveType>(e.rhs->type());
    auto tystr_to   = tystr(ty);

    auto emit_cast = [&](const PrimitiveType *from_ty, const PrimitiveType *to_ty) {
        auto tystr_to = tystr(to_ty);
        auto tystr_from = tystr(from_ty);
        /* Cast LHS if necessary. */
        if (tystr_from != tystr_to) {
            std::string opstr = "Cast" + tystr_to + tystr_from;
            auto opcode = StackMachine::STR_TO_OPCODE.at(opstr);
            stack_machine_.emit(opcode);
        }
    };

    auto scale = [&](const PrimitiveType *from_ty, const PrimitiveType *to_ty) {
        auto n_from = as<const Numeric>(from_ty);
        auto n_to = as<const Numeric>(to_ty);
        if (n_from->scale < n_to->scale) {
            insist(n_to->is_decimal(), "only decimals have a scale");
            /* Scale up. */
            auto delta = n_to->scale - n_from->scale;
            const int64_t factor = powi<int64_t>(10, delta);
            stack_machine_.add_and_emit_load(factor);
            stack_machine_.emit_Mul_i(); // multiply by scale factor
        } else if (n_from->scale > n_to->scale) {
            insist(n_from->is_decimal(), "only decimals have a scale");
            insist(n_to->is_floating_point(), "only floating points are more precise than decimals");
            /* Scale down. */
            auto delta = n_from->scale - n_to->scale;
            const int64_t factor = powi<int64_t>(10, delta);
            if (n_to->is_float()) {
                stack_machine_.add_and_emit_load(float(1.f / factor));
                stack_machine_.emit_Mul_f(); // multiply by inverse scale factor
            } else {
                stack_machine_.add_and_emit_load(double(1. / factor));
                stack_machine_.emit_Mul_d(); // multiply by inverse scale factor
            }
        }
    };

    auto load_numeric = [&](auto val, const Numeric *n) {
        switch (n->kind) {
            case Numeric::N_Int:
            case Numeric::N_Decimal:
                return stack_machine_.add_and_emit_load(int64_t(val));

            case Numeric::N_Float:
                if (n->precision == 32)
                    return stack_machine_.add_and_emit_load(float(val));
                else
                    return stack_machine_.add_and_emit_load(double(val));
                break;
        }
    };

    std::string opname;
    switch (e.op.type) {
        default: unreachable("illegal operator");

        /*----- Arithmetic operators ---------------------------------------------------------------------------------*/
        case TK_PLUS:           opname = "Add"; break;
        case TK_MINUS:          opname = "Sub"; break;
        case TK_ASTERISK:       opname = "Mul"; break;
        case TK_SLASH:          opname = "Div"; break;
        case TK_PERCENT:        opname = "Mod"; break;

        /*----- Concatenation operator -------------------------------------------------------------------------------*/
        case TK_DOTDOT:         opname = "Cat"; break;

        /*----- Comparison operators ---------------------------------------------------------------------------------*/
        case TK_LESS:           opname = "LT";  break;
        case TK_GREATER:        opname = "GT";  break;
        case TK_LESS_EQUAL:     opname = "LE";  break;
        case TK_GREATER_EQUAL:  opname = "GE";  break;
        case TK_EQUAL:          opname = "Eq";  break;
        case TK_BANG_EQUAL:     opname = "NE";  break;

        /*----- Logical operators ------------------------------------------------------------------------------------*/
        case TK_And:            opname = "And"; break;
        case TK_Or:             opname = "Or";  break;
    }

    switch (e.op.type) {
        default: unreachable("illegal operator");

        /*----- Arithmetic operators ---------------------------------------------------------------------------------*/
        case TK_PLUS:
        case TK_MINUS: {
            (*this)(*e.lhs);
            emit_cast(ty_lhs, ty);
            scale(ty_lhs, ty);

            (*this)(*e.rhs);
            emit_cast(ty_rhs, ty);
            scale(ty_rhs, ty);

            std::string opstr = e.op.type == TK_PLUS ? "Add" : "Sub";
            opstr += tystr_to;
            auto opcode = StackMachine::STR_TO_OPCODE.at(opstr);
            stack_machine_.emit(opcode);
            break;
        }

        case TK_ASTERISK: {
            auto n_lhs = as<const Numeric>(ty_lhs);
            auto n_rhs = as<const Numeric>(ty_rhs);
            auto n_res = as<const Numeric>(ty);

            (*this)(*e.lhs);
            emit_cast(ty_lhs, ty);

            (*this)(*e.rhs);
            emit_cast(ty_rhs, ty);

            std::string opstr = "Mul";
            opstr += tystr_to;
            auto opcode = StackMachine::STR_TO_OPCODE.at(opstr);
            stack_machine_.emit(opcode); // Mul_x

            const int64_t scale = n_lhs->scale + n_rhs->scale - n_res->scale;
            insist(scale >= 0);
            if (scale != 0) {
                const int64_t factor = powi<int64_t>(10, scale);
                load_numeric(factor, n_res);

                opstr = "Div";
                opstr += tystr_to;
                opcode = StackMachine::STR_TO_OPCODE.at(opstr);
                stack_machine_.emit(opcode); // Div_x
            }

            break;
        }

        case TK_SLASH: {
            /* Division with potentially different numeric types is a tricky thing.  Not only must we convert the values
             * from their original data type to the type of the result, but we also must scale them correctly.  Below is
             * a list with examples of the different cases.  From this list we derive the formula:
             *
             *      scale = scale_rhs + (scale_res - scale_lhs)
             *
             *
             *  INT / DECIMAL(2) -> DECIMAL(2)
             *
             *      42 / 2.37
             *  ->  42 / 237
             *  ->  42 * 10^2 * 10^2 // 237
             *   =  420,000 // 237
             *   =  1772
             *  ->  17.72
             *
             *  DECIMAL(2) / INT -> DECIMAL(2)
             *
             *      42.00 / 2
             *  ->  4200 / 2
             *  ->  4200 * 10^0 * 10^0 // 2
             *   =  4200 // 2
             *   =  2100
             *  ->  21.00
             *
             *  FLOAT / DECIMAL(2) -> FLOAT
             *
             *      42.f / 2.37
             *  ->  42.f / 237
             *  ->  42.f * 10^2 * 10^0 / 237
             *   =  4200.f / 237
             *   =  17.72f
             *  ->  17.72f
             *
             *  DECIMAL(2) / FLOAT -> FLOAT
             *
             *      42.00 / 2.37f
             *  ->  4200 / 2.37f
             *  ->  4200 * 10^0 * 10^-2 / 2.37f
             *   =  42.f / 2.37f
             *   =  17.72f
             *  ->  17.72f
             *
             *  DECIMAL(2) / DECIMAL(2) -> DECIMAL(2)
             *
             *      42.00 / 2.37
             *  ->  4200 / 237
             *  ->  4200 * 10^2 * 10^0 // 237
             *  ->  420,000 // 237
             *   =  1772
             *  ->  17.72
             *
             *  DECIMAL(3) / DECIMAL(2) -> DECIMAL(3)
             *
             *      42.000 / 2.37
             *  ->  42,000 / 237
             *  ->  42,000 * 10^2 * 10^0 // 237
             *   =  4,200,000 // 237
             *   =  17721
             *  ->  17.721
             *
             *  DECIMAL(2) / DECIMAL(3) -> DECIMAL(3)
             *
             *      42.00 / 2.370
             *  ->  4200 / 2370
             *  ->  4200 * 10^3 * 10^1 // 2370
             *  ->  4,200,000 // 2370
             *   =  17721
             *  ->  17.721
             */

            auto n_lhs = as<const Numeric>(ty_lhs);
            auto n_rhs = as<const Numeric>(ty_rhs);
            auto n_res = as<const Numeric>(ty);

            (*this)(*e.lhs);
            emit_cast(ty_lhs, ty);

            const auto scale = n_rhs->scale + (n_res->scale - n_lhs->scale);
            if (scale > 0) {
                const int64_t factor = powi<int64_t>(10, scale); // scale up by rhs
                load_numeric(factor, n_res);

                std::string opstr = "Mul";
                opstr += tystr_to;
                auto opcode = StackMachine::STR_TO_OPCODE.at(opstr);
                stack_machine_.emit(opcode); // Mul_x
            } else if (scale < 0) {
                const int64_t factor = powi<int64_t>(10, -scale); // scale up by rhs
                load_numeric(factor, n_res);

                std::string opstr = "Div";
                opstr += tystr_to;
                auto opcode = StackMachine::STR_TO_OPCODE.at(opstr);
                stack_machine_.emit(opcode); // Div_x
            }

            (*this)(*e.rhs);
            emit_cast(ty_rhs, ty);

            std::string opstr = "Div";
            opstr += tystr_to;
            auto opcode = StackMachine::STR_TO_OPCODE.at(opstr);
            stack_machine_.emit(opcode); // Div_x

            break;
        }

        case TK_PERCENT:
            (*this)(*e.lhs);
            (*this)(*e.rhs);
            stack_machine_.emit_Mod_i();
            break;

        /*----- Concatenation operator -------------------------------------------------------------------------------*/
        case TK_DOTDOT:
            (*this)(*e.lhs);
            (*this)(*e.rhs);
            stack_machine_.emit_Cat_s();
            break;

        /*----- Comparison operators ---------------------------------------------------------------------------------*/
        case TK_LESS:
        case TK_GREATER:
        case TK_LESS_EQUAL:
        case TK_GREATER_EQUAL:
        case TK_EQUAL:
        case TK_BANG_EQUAL:
            if (ty_lhs->is_numeric()) {
                insist(ty_rhs->is_numeric());
                auto n_lhs = as<const Numeric>(ty_lhs);
                auto n_rhs = as<const Numeric>(ty_rhs);
                auto n_res = arithmetic_join(n_lhs, n_rhs);

                (*this)(*e.lhs);
                emit_cast(n_lhs, n_res);
                scale(n_lhs, n_res);

                (*this)(*e.rhs);
                emit_cast(n_rhs, n_res);
                scale(n_rhs, n_res);

                std::string opstr = opname + tystr(n_res);
                auto opcode = StackMachine::STR_TO_OPCODE.at(opstr);
                stack_machine_.emit(opcode);
            } else {
                (*this)(*e.lhs);
                (*this)(*e.rhs);
                std::string opstr = opname + tystr(ty_lhs);
                auto opcode = StackMachine::STR_TO_OPCODE.at(opstr);
                stack_machine_.emit(opcode);
            }
            break;

        /*----- Logical operators ------------------------------------------------------------------------------------*/
        case TK_And:
            (*this)(*e.lhs);
            (*this)(*e.rhs);
            stack_machine_.emit_And_b();
            break;

        case TK_Or:
            (*this)(*e.lhs);
            (*this)(*e.rhs);
            stack_machine_.emit_Or_b();
            break;
    }
}

/*======================================================================================================================
 * Stack Machine
 *====================================================================================================================*/

const std::unordered_map<std::string, StackMachine::Opcode> StackMachine::STR_TO_OPCODE = {
#define DB_OPCODE(CODE, ...) { #CODE, StackMachine::Opcode:: CODE },
#include "tables/Opcodes.tbl"
#undef DB_OPCODE
};

StackMachine::StackMachine(const OperatorSchema &schema, const Expr &expr)
    : schema(schema)
{
    StackMachineBuilder Builder(*this, schema, expr); // compute the command sequence for this stack machine
}

StackMachine::StackMachine(const OperatorSchema &schema)
    : schema(schema)
{ }

void StackMachine::emit(const Expr &expr)
{
    StackMachineBuilder Builder(*this, schema, expr); // compute the command sequence for this stack machine
}

void StackMachine::emit(const cnf::CNF &cnf)
{
    /* Compile filter into stack machine.  TODO: short-circuit evaluation. */
    for (auto clause_it = cnf.cbegin(); clause_it != cnf.cend(); ++clause_it) {
        auto &C = *clause_it;
        for (auto pred_it = C.cbegin(); pred_it != C.cend(); ++pred_it) {
            auto &P = *pred_it;
            emit(*P.expr()); // emit code for predicate
            if (P.negative())
                ops.push_back(StackMachine::Opcode::Not_b); // negate if negative
            if (pred_it != C.cbegin())
                ops.push_back(StackMachine::Opcode::Or_b);
        }
        if (clause_it != std::prev(cnf.cend()))
            ops.push_back(StackMachine::Opcode::Stop_False); // a single false clause renders the CNF false
        if (clause_it != cnf.cbegin())
            ops.push_back(StackMachine::Opcode::And_b);
    }
}

void StackMachine::operator()(tuple_type *out, const tuple_type &in)
{
    static const void *labels[] = {
#define DB_OPCODE(CODE, ...) && CODE,
#include "tables/Opcodes.tbl"
#undef DB_OPCODE
    };

    emit_Stop();
    tuple_type &stack = *out;
    stack.clear();
    auto op = ops.cbegin();

#ifndef NDEBUG
    insist(stack.capacity() >= std::size_t(required_stack_size_), "insufficient memory provided");
    auto initial_stack_size = stack.capacity();
#define NEXT \
    insist(stack.capacity() == initial_stack_size, "failed to pre-allocate sufficient memory"); \
    goto *labels[std::size_t(*op++)]
#else
#define NEXT goto *labels[std::size_t(*op++)]
#endif

    NEXT;

/*======================================================================================================================
 * Control flow operations
 *====================================================================================================================*/

Stop_Z: {
    insist(stack.size() >= 1);
    auto pv = std::get_if<int64_t>(&stack.back());
    insist(pv, "invalid type of variant");
    if (*pv == 0) goto Stop; // stop evaluation on ZERO
}
NEXT;

Stop_NZ: {
    insist(stack.size() >= 1);
    auto pv = std::get_if<int64_t>(&stack.back());
    insist(pv, "invalid type of variant");
    if (*pv != 0) goto Stop; // stop evaluation on NOT ZERO
}
NEXT;

Stop_False: {
    insist(stack.size() >= 1);
    auto pv = std::get_if<bool>(&stack.back());
    insist(pv, "invalid type of variant");
    if (not *pv) goto Stop; // stop evaluation on NOT ZERO
}
NEXT;

Stop_True: {
    insist(stack.size() >= 1);
    auto pv = std::get_if<bool>(&stack.back());
    insist(pv, "invalid type of variant");
    if (*pv) goto Stop; // stop evaluation on NOT ZERO
}
NEXT;

/*======================================================================================================================
 * Stack manipulation operations
 *====================================================================================================================*/

Pop:
    stack.pop_back();
    NEXT;

/*======================================================================================================================
 * Load / Update operations
 *====================================================================================================================*/

/* Load a value from the tuple to the top of the stack. */
Ld_Tup: {
    std::size_t idx = static_cast<std::size_t>(*op++);
    insist(idx < in.size(), "index out of bounds");
    stack.emplace_back(in[idx]);
}
NEXT;

    /* Load a value from the context to the top of the stack. */
Ld_Ctx: {
    std::size_t idx = static_cast<std::size_t>(*op++);
    insist(idx < context_.size(), "index out of bounds");
    stack.emplace_back(context_[idx]);
}
NEXT;

Upd_Ctx: {
    std::size_t idx = static_cast<std::size_t>(*op++);
    insist(idx < context_.size(), "index out of bounds");
    insist(stack.back().index() == context_[idx].index());
    context_[idx] = stack.back();
}
NEXT;

/*----- Load from row store ------------------------------------------------------------------------------------------*/
#define PREPARE \
    insist(stack.size() >= 3); \
\
    /* Get value bit offset. */ \
    auto pv_value_off = std::get_if<int64_t>(&stack.back()); \
    insist(pv_value_off, "invalid type of variant"); \
    auto value_off = std::size_t(*pv_value_off); \
    const std::size_t bytes = value_off / 8; \
    stack.pop_back(); \
\
    /* Get null bit offset. */ \
    auto pv_null_off = std::get_if<int64_t>(&stack.back()); \
    insist(pv_null_off, "invalid type of variant"); \
    auto null_off = std::size_t(*pv_null_off); \
    stack.pop_back(); \
\
    /* Row address. */ \
    auto pv_addr = std::get_if<int64_t>(&stack.back()); \
    insist(pv_addr, "invalid type of variant"); \
    auto addr = reinterpret_cast<uint8_t*>(*pv_addr);

#define PREPARE_LOAD \
    PREPARE \
\
    /* Check if null. */ \
    { \
        const std::size_t bytes = null_off / 8; \
        const std::size_t bits = null_off % 8; \
        bool is_null = not bool((*(addr + bytes) >> bits) & 0x1); \
        if (is_null) { \
            stack.back() = value_type(null_type()); \
            NEXT; \
        } \
    } \

Ld_RS_i8: {
    PREPARE_LOAD;
    auto pv_res = std::get_if<int64_t>(&stack.back());
    insist(pv_res, "invalid type of variant");
    *pv_res = int64_t(*reinterpret_cast<int8_t*>(addr + bytes));
}
NEXT;

Ld_RS_i16: {
    PREPARE_LOAD;
    auto pv_res = std::get_if<int64_t>(&stack.back());
    insist(pv_res, "invalid type of variant");
    *pv_res = int64_t(*reinterpret_cast<int16_t*>(addr + bytes));
}
NEXT;

Ld_RS_i32: {
    PREPARE_LOAD;
    auto pv_res = std::get_if<int64_t>(&stack.back());
    insist(pv_res, "invalid type of variant");
    *pv_res = int64_t(*reinterpret_cast<int32_t*>(addr + bytes));
}
NEXT;

Ld_RS_i64: {
    PREPARE_LOAD;
    stack.back() = *reinterpret_cast<int64_t*>(addr + bytes);
}
NEXT;

Ld_RS_f: {
    PREPARE_LOAD;
    stack.back() = *reinterpret_cast<float*>(addr + bytes);
}
NEXT;

Ld_RS_d: {
    PREPARE_LOAD;
    stack.back() = *reinterpret_cast<double*>(addr + bytes);
}
NEXT;

Ld_RS_s: {
    /* Get string length. */
    auto pv_len = std::get_if<int64_t>(&stack.back());
    insist(pv_len, "invalid type of variant");
    auto len = std::size_t(*pv_len);
    stack.pop_back();

    PREPARE_LOAD;

    auto first = reinterpret_cast<char*>(addr + bytes);
    stack.back() = std::string_view(first, len);
}
NEXT;

Ld_RS_b: {
    PREPARE_LOAD;
    const std::size_t bits = value_off % 8;
    stack.back() = bool((*reinterpret_cast<uint8_t*>(addr + bytes) >> bits) & 0x1);
}
NEXT;

#undef PREPARE_LOAD

#define PREPARE_STORE(TYPE) \
    PREPARE \
    stack.pop_back(); \
\
    auto pv_value = std::get_if<TYPE>(&stack.back()); \
    insist(pv_value or std::holds_alternative<null_type>(stack.back()), "invalid type of variant"); \
\
    /* Set null bit. */ \
    { \
        const std::size_t bytes = null_off / 8; \
        const std::size_t bits = null_off % 8; \
        setbit(addr + bytes, bool(pv_value), bits); \
        if (not pv_value) { \
            stack.pop_back(); \
            NEXT; \
        } \
    } \
\
    auto value = *pv_value; \
    stack.pop_back();

St_RS_i8: {
    PREPARE_STORE(int64_t);
    auto p = reinterpret_cast<int8_t*>(addr + bytes);
    *p = value;
}
NEXT;

St_RS_i16: {
    PREPARE_STORE(int64_t);
    auto p = reinterpret_cast<int16_t*>(addr + bytes);
    *p = value;
}
NEXT;

St_RS_i32: {
    PREPARE_STORE(int64_t);
    auto p = reinterpret_cast<int32_t*>(addr + bytes);
    *p = value;
}
NEXT;

St_RS_i64: {
    PREPARE_STORE(int64_t);
    auto p = reinterpret_cast<int64_t*>(addr + bytes);
    *p = value;
}
NEXT;

St_RS_f: {
    PREPARE_STORE(float);
    auto p = reinterpret_cast<float*>(addr + bytes);
    *p = value;
}
NEXT;

St_RS_d: {
    PREPARE_STORE(double);
    auto p = reinterpret_cast<double*>(addr + bytes);
    *p = value;
}
NEXT;

St_RS_s: {
    auto pv_len = std::get_if<int64_t>(&stack.back());
    insist(pv_len, "invalid type of variant");
    auto len = *pv_len;
    stack.pop_back();

    PREPARE_STORE(std::string_view);

    auto p = reinterpret_cast<char*>(addr + bytes);
    strncpy(p, value.data(), len);
}
NEXT;

St_RS_b: {
    PREPARE_STORE(bool);
    const auto bits = value_off % 8;
    setbit(addr + bytes, value, bits);
}
NEXT;

#undef PREPARE_STORE

#undef PREPARE

/*----- Load from column store ---------------------------------------------------------------------------------------*/
#define PREPARE(TYPE) \
    insist(stack.size() >= 4); \
\
    /* Get attribute id. */ \
    auto pv_attr_id = std::get_if<int64_t>(&stack.back()); \
    insist(pv_attr_id, "invalid type of variant"); \
    auto attr_id = std::size_t(*pv_attr_id); \
    stack.pop_back(); \
\
    /* Get address of value column. */ \
    auto pv_value_col_addr = std::get_if<int64_t>(&stack.back()); \
    insist(pv_value_col_addr, "invalid type of variant"); \
    TYPE *value_col_addr = reinterpret_cast<TYPE*>(static_cast<uintptr_t>(*pv_value_col_addr)); \
    stack.pop_back(); \
\
    /* Get address of null bitmap column. */ \
    auto pv_null_bitmap_col_addr = std::get_if<int64_t>(&stack.back()); \
    insist(pv_null_bitmap_col_addr, "invalid type of variant"); \
    int64_t *null_bitmap_col_addr = reinterpret_cast<int64_t*>(static_cast<uintptr_t>(*pv_null_bitmap_col_addr)); \
    stack.pop_back(); \
\
    /* Get row id. */ \
    auto pv_row_id = std::get_if<int64_t>(&stack.back()); \
    insist(pv_row_id, "invalid type of variant"); \
    auto row_id = static_cast<std::size_t>(*pv_row_id);

#define PREPARE_LOAD(TYPE) \
    PREPARE(TYPE) \
\
    /* Check if null. */ \
    bool is_null = not ((null_bitmap_col_addr[row_id] >> attr_id) & 0x1); \
    if (is_null) { \
        stack.back() = value_type(null_type()); \
        NEXT; \
    }

Ld_CS_i8: {
    PREPARE_LOAD(int8_t);
    auto pv_res = std::get_if<int64_t>(&stack.back());
    insist(pv_res, "invalid type of variant");
    *pv_res = int64_t(value_col_addr[row_id]);
}
NEXT;

Ld_CS_i16: {
    PREPARE_LOAD(int16_t);
    auto pv_res = std::get_if<int64_t>(&stack.back());
    insist(pv_res, "invalid type of variant");
    *pv_res = int64_t(value_col_addr[row_id]);
}
NEXT;

Ld_CS_i32: {
    PREPARE_LOAD(int32_t);
    auto pv_res = std::get_if<int64_t>(&stack.back());
    insist(pv_res, "invalid type of variant");
    *pv_res = int64_t(value_col_addr[row_id]);
}
NEXT;

Ld_CS_i64: {
    PREPARE_LOAD(int64_t);
    auto pv_res = std::get_if<int64_t>(&stack.back());
    insist(pv_res, "invalid type of variant");
    *pv_res = int64_t(value_col_addr[row_id]);
}
NEXT;

Ld_CS_f: {
    PREPARE_LOAD(float);
    stack.back() = float(value_col_addr[row_id]);
}
NEXT;

Ld_CS_d: {
    PREPARE_LOAD(double);
    stack.back() = double(value_col_addr[row_id]);
}
NEXT;

Ld_CS_s: {
    /* Get string length. */
    auto pv_len = std::get_if<int64_t>(&stack.back());
    insist(pv_len, "invalid type of variant");
    auto len = std::size_t(*pv_len);
    stack.pop_back();

    PREPARE_LOAD(char);

    auto p = value_col_addr + row_id * len;
    stack.back() = std::string_view(p, len);
}
NEXT;

Ld_CS_b: {
    PREPARE_LOAD(uint8_t);
    const std::size_t bytes = row_id / 8;
    const std::size_t bits = row_id % 8;
    stack.back() = bool((value_col_addr[bytes] >> bits) & 0x1);
}
NEXT;

#undef PREPARE_LOAD

#define PREPARE_STORE(TO_TYPE, FROM_TYPE) \
    PREPARE(TO_TYPE); \
    stack.pop_back(); \
\
    auto pv_value = std::get_if<FROM_TYPE>(&stack.back()); \
    insist(pv_value or std::holds_alternative<null_type>(stack.back()), "invalid type of variant"); \
\
    /* Set null bit. */ \
    { \
        setbit(&null_bitmap_col_addr[row_id], bool(pv_value), attr_id); \
        if (not pv_value) { \
            stack.pop_back(); \
            NEXT; \
        } \
    } \
\
    auto value = *pv_value; \
    stack.pop_back();

St_CS_i8: {
    PREPARE_STORE(int8_t, int64_t);
    value_col_addr[row_id] = value;
}
NEXT;

St_CS_i16: {
    PREPARE_STORE(int16_t, int64_t);
    value_col_addr[row_id] = value;
}
NEXT;

St_CS_i32: {
    PREPARE_STORE(int32_t, int64_t);
    value_col_addr[row_id] = value;
}
NEXT;

St_CS_i64: {
    PREPARE_STORE(int64_t, int64_t);
    value_col_addr[row_id] = value;
}
NEXT;

St_CS_f: {
    PREPARE_STORE(float, float);
    value_col_addr[row_id] = value;
}
NEXT;

St_CS_d: {
    PREPARE_STORE(double, double);
    value_col_addr[row_id] = value;
}
NEXT;

St_CS_s: {
    PREPARE_STORE(std::string_view, std::string_view);
    // TODO
    unreachable("not implemented");
}
NEXT;

St_CS_b: {
    PREPARE_STORE(bool, bool);
    // TODO
    unreachable("not implemented");
}
NEXT;

#undef PREPARE_STORE

#undef PREPARE

#define UNARY(OP, TYPE) { \
    insist(stack.size() >= 1); \
    auto &v = stack.back(); \
    auto pv = std::get_if<TYPE>(&v); \
    insist(pv, "invalid type of variant"); \
    auto res = OP (*pv); \
    if constexpr (std::is_same_v<decltype(res), TYPE>) \
        *pv = res; \
    else \
        stack.back() = res; \
} \
NEXT;

#define BINARY(OP, TYPE) { \
    insist(stack.size() >= 2); \
    auto &v_rhs = stack.back(); \
    TYPE *pv_rhs = std::get_if<TYPE>(&v_rhs); \
    insist(pv_rhs, "invalid type of rhs"); \
    TYPE rhs = *pv_rhs; \
    stack.pop_back(); \
    auto &v_lhs = stack.back(); \
    TYPE *pv_lhs = std::get_if<TYPE>(&v_lhs); \
    insist(pv_lhs, "invalid type of lhs"); \
    auto res = *pv_lhs OP rhs; \
    stack.back() = res; \
} \
NEXT;

/*======================================================================================================================
 * Arithmetical operations
 *====================================================================================================================*/

/* Integral increment. */
Inc: UNARY(++, int64_t);

/* Integral decrement. */
Dec: UNARY(--, int64_t);

/* Bitwise negation */
Neg_i: UNARY(~, int64_t);

/* Arithmetic negation */
Minus_i: UNARY(-, int64_t);
Minus_f: UNARY(-, float);
Minus_d: UNARY(-, double);

/* Add two values. */
Add_i: BINARY(+, int64_t);
Add_f: BINARY(+, float);
Add_d: BINARY(+, double);

/* Subtract two values. */
Sub_i: BINARY(-, int64_t);
Sub_f: BINARY(-, float);
Sub_d: BINARY(-, double);

/* Multiply two values. */
Mul_i: BINARY(*, int64_t);
Mul_f: BINARY(*, float);
Mul_d: BINARY(*, double);

/* Divide two values. */
Div_i: BINARY(/, int64_t);
Div_f: BINARY(/, float);
Div_d: BINARY(/, double);

/* Modulo divide two values. */
Mod_i: BINARY(%, int64_t);

/* Concatenate two strings. */
Cat_s: stack.pop_back(); NEXT; /* TODO currently not supported */

/*======================================================================================================================
 * Logical operations
 *====================================================================================================================*/

/* Logical not */
Not_b: UNARY(not, bool);

/* Logical and. */
And_b: BINARY(and, bool);

/* Logical or. */
Or_b: BINARY(or, bool);

/*======================================================================================================================
 * Comparison operations
 *====================================================================================================================*/

Eq_i: BINARY(==, int64_t);
Eq_f: BINARY(==, float);
Eq_d: BINARY(==, double);
Eq_b: BINARY(==, bool);
Eq_s: BINARY(==, std::string_view);

NE_i: BINARY(!=, int64_t);
NE_f: BINARY(!=, float);
NE_d: BINARY(!=, double);
NE_b: BINARY(!=, bool);
NE_s: BINARY(!=, std::string_view);

LT_i: BINARY(<, int64_t);
LT_f: BINARY(<, float);
LT_d: BINARY(<, double);
LT_s: BINARY(<, std::string_view);

GT_i: BINARY(>, int64_t);
GT_f: BINARY(>, float);
GT_d: BINARY(>, double);
GT_s: BINARY(>, std::string_view);

LE_i: BINARY(<=, int64_t);
LE_f: BINARY(<=, float);
LE_d: BINARY(<=, double);
LE_s: BINARY(<=, std::string_view);

GE_i: BINARY(>=, int64_t);
GE_f: BINARY(>=, float);
GE_d: BINARY(>=, double);
GE_s: BINARY(>=, std::string_view);

#define CMP(TYPE) { \
    insist(stack.size() >= 2); \
    auto &v_rhs = stack.back(); \
    auto pv_rhs = std::get_if<TYPE>(&v_rhs); \
    insist(pv_rhs, "invalid type of rhs"); \
    auto rhs = *pv_rhs; \
    stack.pop_back(); \
    auto &v_lhs = stack.back(); \
    auto pv_lhs = std::get_if<TYPE>(&v_lhs); \
    insist(pv_lhs, "invalid type of lhs"); \
    stack.back() = *pv_lhs == rhs ? int64_t(0) : (*pv_lhs < rhs ? int64_t(-1) : int64_t(1)); \
} \
NEXT;

Cmp_i: CMP(int64_t);
Cmp_f: CMP(float);
Cmp_d: CMP(double);
Cmp_b: CMP(bool);
Cmp_s: {
    insist(stack.size() >= 2);
    auto &v_rhs = stack.back();
    auto pv_rhs = std::get_if<std::string_view>(&v_rhs);
    insist(pv_rhs, "invalid type of rhs");
    auto rhs = *pv_rhs;
    stack.pop_back();
    auto &v_lhs = stack.back();
    auto pv_lhs = std::get_if<std::string_view>(&v_lhs);
    insist(pv_lhs, "invalid type of lhs");
    stack.back() = int64_t(pv_lhs->compare(rhs));
}
NEXT;

#undef CMP

/*======================================================================================================================
 * Intrinsic functions
 *====================================================================================================================*/

Is_Null:
    stack.back() = std::holds_alternative<null_type>(stack.back());
    NEXT;

/* Cast to int. */
Cast_i_f: UNARY((int64_t), float);
Cast_i_d: UNARY((int64_t), double);
Cast_i_b: UNARY((int64_t), bool);

/* Cast to float. */
Cast_f_i: UNARY((float), int64_t);
Cast_f_d: UNARY((float), float);

/* Cast to double. */
Cast_d_i: UNARY((double), int64_t);
Cast_d_f: UNARY((double), float);

#undef BINARY
#undef UNARY

Stop:
    ops.pop_back(); // terminating Stop

#ifndef NDEBUG
    insist(initial_stack_size == stack.capacity(), "failed to pre-allocate sufficient memory");
#endif
}

void StackMachine::dump(std::ostream &out) const
{
    out << "StackMachine\n    Context: [";
    for (auto it = context_.cbegin(); it != context_.cend(); ++it) {
        if (it != context_.cbegin()) out << ", ";
        out << *it;
    }
    out << "]\n    Tuple Schema: " << schema
        << "\n    Opcode Sequence:\n";
    for (std::size_t i = 0; i != ops.size(); ++i) {
        auto opc = ops[i];
        out << "        [0x" << std::hex << std::setfill('0') << std::setw(4) << i << std::dec << "]: "
            << StackMachine::OPCODE_TO_STR[static_cast<std::size_t>(opc)];
        switch (opc) {
            case Opcode::Ld_Tup:
            case Opcode::Ld_Ctx:
            case Opcode::Upd_Ctx:
                ++i;
                out << ' ' << static_cast<int64_t>(ops[i]);
            default:;
        }
        out << '\n';
    }
    out.flush();
}

void StackMachine::dump() const { dump(std::cerr); }

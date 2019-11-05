#include "IR/Interpreter.hpp"

#include "parse/AST.hpp"
#include "parse/ASTVisitor.hpp"
#include "util/fn.hpp"
#include <cerrno>
#include <cstdlib>
#include <type_traits>


using namespace db;


/*======================================================================================================================
 * Helper methods to handle the value_type.
 *====================================================================================================================*/

namespace db {

value_type operator+(const value_type &value)
{
    value_type result;
    std::visit(overloaded {
        [&](auto value) { result = value; },
        [&](null_type value) { result = value; },
        [&](std::string) { unreachable("operator- not defined for std::string"); },
        [&](bool) { unreachable("operator- not defined for bool"); },
    }, value);
    return result;
}

value_type operator-(const value_type &value)
{
    value_type result;
    std::visit(overloaded {
        [&](auto value) { result = -value; },
        [&](null_type value) { result = value; },
        [&](std::string) { unreachable("operator- not defined for std::string"); },
        [&](bool) { unreachable("operator- not defined for bool"); },
    }, value);
    return result;
}

value_type operator~(const value_type &value)
{
    value_type result;
    std::visit(overloaded {
        [&](auto value) { result = ~value; },
        [&](null_type value) { result = value; },
        [&](std::string) { unreachable("operator- not defined for std::string"); },
        [&](bool) { unreachable("operator- not defined for bool"); },
        [&](float) { unreachable("operator- not defined for float"); },
        [&](double) { unreachable("operator- not defined for double"); },
    }, value);
    return result;
}

value_type operator!(const value_type &value)
{
    value_type result;
    std::visit(overloaded {
        [&](auto) { unreachable("operator! not defined"); },
        [&](bool value) { result = not value; },
    }, value);
    return result;
}

}

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

    static value_type eval(const Constant &c);

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

value_type StackMachineBuilder::eval(const Constant &c)
{
    errno = 0;
    switch (c.tok.type) {
        default: unreachable("illegal token");

        /* Integer */
        case TK_OCT_INT:
            return int64_t(strtoll(c.tok.text, nullptr, 8));

        case TK_DEC_INT:
            return int64_t(strtoll(c.tok.text, nullptr, 10));

        case TK_HEX_INT:
            return int64_t(strtoll(c.tok.text, nullptr, 16));

        /* Float */
        case TK_DEC_FLOAT:
            return strtod(c.tok.text, nullptr);

        case TK_HEX_FLOAT:
            unreachable("not implemented");

        /* String */
        case TK_STRING_LITERAL:
            return interpret(c.tok.text);

        /* Boolean */
        case TK_True:
            return true;

        case TK_False:
            return false;
    }
    if (errno)
        throw std::invalid_argument("constant could not be parsed");
}

void StackMachineBuilder::operator()(Const<Designator> &e)
{
    /* Given the designator, identify the position of its value in the tuple.  */
    auto target = e.target();
    if (auto p = std::get_if<const Expr*>(&target)) {
        /* This designator references another expression.  Evaluate it. */
        (*this)(**p);
    } else if (auto p = std::get_if<const Attribute*>(&target)) {
        insist(e.table_name.text, "must have been set by sema");
        /* Lookup tuple element type by attribute identifier. */
        std::size_t idx = schema_[{e.table_name.text, e.attr_name.text}].first;
        insist(idx < schema_.size(), "index out of bounds");
        stack_machine_.ops.push_back(StackMachine::Opcode::Ld_Idx);
        stack_machine_.ops.push_back(static_cast<StackMachine::Opcode>(idx));
    } else {
        unreachable("designator has no target");
    }
}

void StackMachineBuilder::operator()(Const<Constant> &e)
{
    auto value = StackMachineBuilder::eval(e);
    const auto idx = stack_machine_.constants.size();
    stack_machine_.constants.emplace_back(value); // put the constant into the vector of constants
    stack_machine_.ops.push_back(StackMachine::Opcode::Ld_Const); // load a constant
    stack_machine_.ops.push_back(static_cast<StackMachine::Opcode>(idx)); // from index `idx`
}

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
            stack_machine_.ops.push_back(StackMachine::Opcode::Is_Null);
            break;

        /*----- Type casts -------------------------------------------------------------------------------------------*/
        case Function::FN_INT: {
            insist(e.args.size() == 1);
            (*this)(*e.args[0]);
            auto ty = e.args[0]->type();
            if (ty->is_float())
                stack_machine_.ops.push_back(StackMachine::Opcode::Cast_i_f);
            else if (ty->is_double())
                stack_machine_.ops.push_back(StackMachine::Opcode::Cast_i_d);
            else if (ty->is_decimal()) {
                unreachable("not implemented");
            } else if (ty->is_boolean()) {
                stack_machine_.ops.push_back(StackMachine::Opcode::Cast_i_b);
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
            stack_machine_.ops.push_back(StackMachine::Opcode::Ld_Idx);
            stack_machine_.ops.push_back(static_cast<StackMachine::Opcode>(idx));
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
                    stack_machine_.ops.push_back(StackMachine::Opcode::Minus_i);
                    break;

                case Numeric::N_Float:
                    if (n->precision == 32)
                        stack_machine_.ops.push_back(StackMachine::Opcode::Minus_f);
                    else
                        stack_machine_.ops.push_back(StackMachine::Opcode::Minus_d);
            }
            break;
        }

        case TK_TILDE:
            if (ty->is_integral())
                stack_machine_.ops.push_back(StackMachine::Opcode::Neg_i);
            else if (ty->is_boolean())
                stack_machine_.ops.push_back(StackMachine::Opcode::Not_b); // negation of bool is always logical
            else
                unreachable("illegal type");
            break;

        case TK_Not:
            insist(ty->is_boolean(), "illegal type");
            stack_machine_.ops.push_back(StackMachine::Opcode::Not_b);
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
            stack_machine_.ops.push_back(opcode);
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
            auto cidx = stack_machine_.constants.size();
            stack_machine_.constants.push_back(factor);
            stack_machine_.ops.push_back(StackMachine::Opcode::Ld_Const);
            stack_machine_.ops.push_back(static_cast<StackMachine::Opcode>(cidx));
            stack_machine_.ops.push_back(StackMachine::Opcode::Mul_i); // multiply by scale factor
        } else if (n_from->scale > n_to->scale) {
            insist(n_from->is_decimal(), "only decimals have a scale");
            insist(n_to->is_floating_point(), "only floating points are more precise than decimals");
            /* Scale down. */
            auto delta = n_from->scale - n_to->scale;
            const int64_t factor = powi<int64_t>(10, delta);
            auto cidx = stack_machine_.constants.size();
            stack_machine_.ops.push_back(StackMachine::Opcode::Ld_Const);
            stack_machine_.ops.push_back(static_cast<StackMachine::Opcode>(cidx));
            if (n_to->is_float()) {
                stack_machine_.constants.push_back(float(1.f / factor));
                stack_machine_.ops.push_back(StackMachine::Opcode::Mul_f); // multiply by inverse scale factor
            } else {
                stack_machine_.constants.push_back(double(1. / factor));
                stack_machine_.ops.push_back(StackMachine::Opcode::Mul_d); // multiply by inverse scale factor
            }
        }
    };

    auto put_numeric = [&](auto val, const Numeric *n) {
        switch (n->kind) {
            case Numeric::N_Int:
            case Numeric::N_Decimal:
                stack_machine_.constants.push_back(int64_t(val));
                break;

            case Numeric::N_Float:
                if (n->precision == 32)
                    stack_machine_.constants.push_back(float(val));
                else
                    stack_machine_.constants.push_back(double(val));
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
            stack_machine_.ops.push_back(opcode);
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
            stack_machine_.ops.push_back(opcode); // Mul_x

            const int64_t scale = n_lhs->scale + n_rhs->scale - n_res->scale;
            insist(scale >= 0);
            if (scale != 0) {
                const int64_t factor = powi<int64_t>(10, scale);
                const auto cidx = stack_machine_.constants.size();
                put_numeric(factor, n_res);
                stack_machine_.ops.push_back(StackMachine::Opcode::Ld_Const);
                stack_machine_.ops.push_back(static_cast<StackMachine::Opcode>(cidx));

                opstr = "Div";
                opstr += tystr_to;
                opcode = StackMachine::STR_TO_OPCODE.at(opstr);
                stack_machine_.ops.push_back(opcode); // Div_x
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
                const auto cidx = stack_machine_.constants.size();
                put_numeric(factor, n_res);
                stack_machine_.ops.push_back(StackMachine::Opcode::Ld_Const);
                stack_machine_.ops.push_back(static_cast<StackMachine::Opcode>(cidx));

                std::string opstr = "Mul";
                opstr += tystr_to;
                auto opcode = StackMachine::STR_TO_OPCODE.at(opstr);
                stack_machine_.ops.push_back(opcode); // Mul_x
            } else if (scale < 0) {
                const int64_t factor = powi<int64_t>(10, -scale); // scale up by rhs
                const auto cidx = stack_machine_.constants.size();
                put_numeric(factor, n_res);
                stack_machine_.ops.push_back(StackMachine::Opcode::Ld_Const);
                stack_machine_.ops.push_back(static_cast<StackMachine::Opcode>(cidx));

                std::string opstr = "Div";
                opstr += tystr_to;
                auto opcode = StackMachine::STR_TO_OPCODE.at(opstr);
                stack_machine_.ops.push_back(opcode); // Div_x
            }

            (*this)(*e.rhs);
            emit_cast(ty_rhs, ty);

            std::string opstr = "Div";
            opstr += tystr_to;
            auto opcode = StackMachine::STR_TO_OPCODE.at(opstr);
            stack_machine_.ops.push_back(opcode); // Div_x

            break;
        }

        case TK_PERCENT:
            (*this)(*e.lhs);
            (*this)(*e.rhs);
            stack_machine_.ops.push_back(StackMachine::Opcode::Mod_i);
            break;

        /*----- Concatenation operator -------------------------------------------------------------------------------*/
        case TK_DOTDOT:
            (*this)(*e.lhs);
            (*this)(*e.rhs);
            stack_machine_.ops.push_back(StackMachine::Opcode::Cat_s);
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
                stack_machine_.ops.push_back(opcode);
            } else {
                (*this)(*e.lhs);
                (*this)(*e.rhs);
                std::string opstr = opname + tystr(ty_lhs);
                auto opcode = StackMachine::STR_TO_OPCODE.at(opstr);
                stack_machine_.ops.push_back(opcode);
            }
            break;

        /*----- Logical operators ------------------------------------------------------------------------------------*/
        case TK_And:
            (*this)(*e.lhs);
            (*this)(*e.rhs);
            stack_machine_.ops.push_back(StackMachine::Opcode::And_b);
            break;

        case TK_Or:
            (*this)(*e.lhs);
            (*this)(*e.rhs);
            stack_machine_.ops.push_back(StackMachine::Opcode::Or_b);
            break;
    }
}

/*======================================================================================================================
 * Stack Machine
 *====================================================================================================================*/

const std::unordered_map<std::string, StackMachine::Opcode> StackMachine::STR_TO_OPCODE = {
#define DB_OPCODE(CODE) { #CODE, StackMachine::Opcode:: CODE },
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

void StackMachine::add(const Expr &expr)
{
    StackMachineBuilder Builder(*this, schema, expr); // compute the command sequence for this stack machine
}

void StackMachine::add(const cnf::CNF &cnf)
{
    /* Compile filter into stack machine.  TODO: short-circuit evaluation. */
    for (auto clause_it = cnf.cbegin(); clause_it != cnf.cend(); ++clause_it) {
        auto &C = *clause_it;
        for (auto pred_it = C.cbegin(); pred_it != C.cend(); ++pred_it) {
            auto &P = *pred_it;
            add(*P.expr()); // emit code for predicate
            if (P.negative())
                ops.push_back(StackMachine::Opcode::Not_b); // negate if negative
            if (pred_it != C.cbegin())
                ops.push_back(StackMachine::Opcode::Or_b);
        }
        ops.push_back(StackMachine::Opcode::Stop_False); // a single false clause renders the CNF false
        if (clause_it != cnf.cbegin())
            ops.push_back(StackMachine::Opcode::And_b);
    }
}

tuple_type && StackMachine::operator()(const tuple_type &t)
{
#define UNARY(OP, TYPE) { \
    insist(stack_.size() >= 1); \
    auto &v = stack_.back(); \
    auto pv = std::get_if<TYPE>(&v); \
    insist(pv, "invalid type of variant"); \
    auto res = OP (*pv); \
    if constexpr (std::is_same_v<decltype(res), TYPE>) \
        *pv = res; \
    else \
        stack_.back() = res; \
    break; \
}

#define BINARY(OP, TYPE) { \
    insist(stack_.size() >= 2); \
    auto &v_rhs = stack_.back(); \
    auto pv_rhs = std::get_if<TYPE>(&v_rhs); \
    insist(pv_rhs, "invalid type of rhs"); \
    auto rhs = *pv_rhs; \
    stack_.pop_back(); \
    auto &v_lhs = stack_.back(); \
    auto pv_lhs = std::get_if<TYPE>(&v_lhs); \
    insist(pv_lhs, "invalid type of lhs"); \
    auto res = *pv_lhs OP rhs; \
    if constexpr (std::is_same_v<decltype(res), TYPE>) \
        *pv_lhs = res; \
    else \
        stack_.back() = res; \
    break; \
}

    stack_.clear();
    for (auto it = ops.cbegin(); it != ops.cend(); ++it) {
        auto opcode = *it;

        switch (opcode) {
            default:
                break;
                unreachable("illegal opcode");

            /*==========================================================================================================
             * Control flow operations
             *========================================================================================================*/

            case Opcode::Stop_Z: {
                insist(stack_.size() >= 1);
                auto pv = std::get_if<int64_t>(&stack_.back());
                insist(pv, "invalid type of variant");
                if (*pv == 0) goto exit; // stop evaluation on ZERO
                break;
            }

            case Opcode::Stop_NZ: {
                insist(stack_.size() >= 1);
                auto pv = std::get_if<int64_t>(&stack_.back());
                insist(pv, "invalid type of variant");
                if (*pv != 0) goto exit; // stop evaluation on NOT ZERO
                break;
            }

            case Opcode::Stop_False: {
                insist(stack_.size() >= 1);
                auto pv = std::get_if<bool>(&stack_.back());
                insist(pv, "invalid type of variant");
                if (not *pv) goto exit; // stop evaluation on NOT ZERO
                break;
            }

            case Opcode::Stop_True: {
                insist(stack_.size() >= 1);
                auto pv = std::get_if<bool>(&stack_.back());
                insist(pv, "invalid type of variant");
                if (*pv) goto exit; // stop evaluation on NOT ZERO
                break;
            }

            /*==========================================================================================================
             * Stack manipulation operations
             *========================================================================================================*/

            case Opcode::Pop:
                stack_.pop_back();
                break;

            /*==========================================================================================================
             * Load / Update operations
             *========================================================================================================*/

            /* Load a value from the tuple to the top of the stack. */
            case Opcode::Ld_Idx: {
                std::size_t idx = static_cast<std::size_t>(*++it);
                insist(idx < t.size(), "index out of bounds");
                stack_.emplace_back(t[idx]);
                break;
            }

            /* Load a value from the array of constants to the top of the stack. */
            case Opcode::Ld_Const: {
                std::size_t idx = static_cast<std::size_t>(*++it);
                insist(idx < constants.size(), "index out of bounds");
                stack_.emplace_back(constants[idx]);
                break;
            }

            case Opcode::Upd_Const: {
                std::size_t idx = static_cast<std::size_t>(*++it);
                insist(idx < constants.size(), "index out of bounds");
                constants[idx] = stack_.back();
                break;
            }

            /*----- Load from row store ------------------------------------------------------------------------------*/
#define PREPARE_LOAD \
    insist(stack_.size() >= 3); \
    /* Get value bit offset. */ \
    auto pv_value_off = std::get_if<int64_t>(&stack_.back()); \
    insist(pv_value_off, "invalid type of variant"); \
    auto value_off = std::size_t(*pv_value_off); \
    stack_.pop_back(); \
\
    /* Get null bit offset. */ \
    auto pv_null_off = std::get_if<int64_t>(&stack_.back()); \
    insist(pv_null_off, "invalid type of variant"); \
    auto null_off = std::size_t(*pv_null_off); \
    stack_.pop_back(); \
\
    /* Row address. */ \
    auto pv_addr = std::get_if<int64_t>(&stack_.back()); \
    insist(pv_addr, "invalid type of variant"); \
    auto addr = reinterpret_cast<uint8_t*>(*pv_addr); \
\
    /* Check if null. */ \
    { \
        const std::size_t bytes = null_off / 8; \
        const std::size_t bits = null_off % 8; \
        bool is_null = not bool((*(addr + bytes) >> bits) & 0x1); \
        if (is_null) { \
            stack_.back() = null_type(); \
            break; \
        } \
    } \
\
    const std::size_t bytes = value_off / 8;

            case Opcode::Ld_RS_i8: {
                PREPARE_LOAD;
                stack_.back() = int64_t(*reinterpret_cast<int8_t*>(addr + bytes));
                break;
            }

            case Opcode::Ld_RS_i16: {
                PREPARE_LOAD;
                stack_.back() = int64_t(*reinterpret_cast<int16_t*>(addr + bytes));
                break;
            }

            case Opcode::Ld_RS_i32: {
                PREPARE_LOAD;
                stack_.back() = int64_t(*reinterpret_cast<int32_t*>(addr + bytes));
                break;
            }

            case Opcode::Ld_RS_i64: {
                PREPARE_LOAD;
                stack_.back() = *reinterpret_cast<int64_t*>(addr + bytes);
                break;
            }

            case Opcode::Ld_RS_f: {
                PREPARE_LOAD;
                stack_.back() = *reinterpret_cast<float*>(addr + bytes);
                break;
            }

            case Opcode::Ld_RS_d: {
                PREPARE_LOAD;
                stack_.back() = *reinterpret_cast<double*>(addr + bytes);
                break;
            }

            case Opcode::Ld_RS_s: {
                /* Get string length. */
                auto pv_len = std::get_if<int64_t>(&stack_.back());
                insist(pv_len, "invalid type of variant");
                auto len = std::size_t(*pv_len);
                stack_.pop_back();

                PREPARE_LOAD;

                auto first = reinterpret_cast<char*>(addr + bytes);
                stack_.back() = std::string(first, first + len);
                break;
            }

            case Opcode::Ld_RS_b: {
                PREPARE_LOAD;
                const std::size_t bits = value_off % 8;
                stack_.back() = bool((*reinterpret_cast<uint8_t*>(addr + bytes) >> bits) & 0x1);
                break;
            }

#undef PREPARE_LOAD

            /*======================================================================================================================
             * Arithmetical operations
             *====================================================================================================================*/

            /* Bitwise negation */
            case Opcode::Neg_i: UNARY(~, int64_t);

            /* Arithmetic negation */
            case Opcode::Minus_i: UNARY(-, int64_t);
            case Opcode::Minus_f: UNARY(-, float);
            case Opcode::Minus_d: UNARY(-, double);

            /* Add two values. */
            case Opcode::Add_i: BINARY(+, int64_t);
            case Opcode::Add_f: BINARY(+, float);
            case Opcode::Add_d: BINARY(+, double);

            /* Subtract two values. */
            case Opcode::Sub_i: BINARY(-, int64_t);
            case Opcode::Sub_f: BINARY(-, float);
            case Opcode::Sub_d: BINARY(-, double);

            /* Multiply two values. */
            case Opcode::Mul_i: BINARY(*, int64_t);
            case Opcode::Mul_f: BINARY(*, float);
            case Opcode::Mul_d: BINARY(*, double);

            /* Divide two values. */
            case Opcode::Div_i: BINARY(/, int64_t);
            case Opcode::Div_f: BINARY(/, float);
            case Opcode::Div_d: BINARY(/, double);

            /* Modulo divide two values. */
            case Opcode::Mod_i: BINARY(%, int64_t);

            /* Concatenate two strings. */
            case Opcode::Cat_s: BINARY(+, std::string);

            /*======================================================================================================================
             * Logical operations
             *====================================================================================================================*/

            /* Logical not */
            case Opcode::Not_b: UNARY(not, bool);

            /* Logical and. */
            case Opcode::And_b: BINARY(and, bool);

            /* Logical or. */
            case Opcode::Or_b: BINARY(or, bool);

            /*======================================================================================================================
             * Comparison operations
             *====================================================================================================================*/

            case Opcode::Eq_i: BINARY(==, int64_t);
            case Opcode::Eq_f: BINARY(==, float);
            case Opcode::Eq_d: BINARY(==, double);
            case Opcode::Eq_b: BINARY(==, bool);
            case Opcode::Eq_s: BINARY(==, std::string);

            case Opcode::NE_i: BINARY(!=, int64_t);
            case Opcode::NE_f: BINARY(!=, float);
            case Opcode::NE_d: BINARY(!=, double);
            case Opcode::NE_b: BINARY(!=, bool);
            case Opcode::NE_s: BINARY(!=, std::string);

            case Opcode::LT_i: BINARY(<, int64_t);
            case Opcode::LT_f: BINARY(<, float);
            case Opcode::LT_d: BINARY(<, double);
            case Opcode::LT_s: BINARY(<, std::string);

            case Opcode::GT_i: BINARY(>, int64_t);
            case Opcode::GT_f: BINARY(>, float);
            case Opcode::GT_d: BINARY(>, double);
            case Opcode::GT_s: BINARY(>, std::string);

            case Opcode::LE_i: BINARY(<=, int64_t);
            case Opcode::LE_f: BINARY(<=, float);
            case Opcode::LE_d: BINARY(<=, double);
            case Opcode::LE_s: BINARY(<=, std::string);

            case Opcode::GE_i: BINARY(>=, int64_t);
            case Opcode::GE_f: BINARY(>=, float);
            case Opcode::GE_d: BINARY(>=, double);
            case Opcode::GE_s: BINARY(>=, std::string);

#define CMP(TYPE) { \
    insist(stack_.size() >= 2); \
    auto &v_rhs = stack_.back(); \
    auto pv_rhs = std::get_if<TYPE>(&v_rhs); \
    insist(pv_rhs, "invalid type of rhs"); \
    auto rhs = *pv_rhs; \
    stack_.pop_back(); \
    auto &v_lhs = stack_.back(); \
    auto pv_lhs = std::get_if<TYPE>(&v_lhs); \
    insist(pv_lhs, "invalid type of lhs"); \
    stack_.back() = *pv_lhs == rhs ? int64_t(0) : (*pv_lhs < rhs ? int64_t(-1) : int64_t(1)); \
    break; \
}
            case Opcode::Cmp_i: CMP(int64_t);
            case Opcode::Cmp_f: CMP(float);
            case Opcode::Cmp_d: CMP(double);
            case Opcode::Cmp_b: CMP(bool);
            case Opcode::Cmp_s: {
                insist(stack_.size() >= 2);
                auto &v_rhs = stack_.back();
                auto pv_rhs = std::get_if<std::string>(&v_rhs);
                insist(pv_rhs, "invalid type of rhs");
                auto rhs = *pv_rhs;
                stack_.pop_back();
                auto &v_lhs = stack_.back();
                auto pv_lhs = std::get_if<std::string>(&v_lhs);
                insist(pv_lhs, "invalid type of lhs");
                stack_.back() = int64_t(pv_lhs->compare(rhs));
                break;
            }
#undef CMP

            /*======================================================================================================================
             * Intrinsic functions
             *====================================================================================================================*/

            case Opcode::Is_Null:
                stack_.back() = std::holds_alternative<null_type>(stack_.back());
                break;

            /* Cast to int. */
            case Opcode::Cast_i_f: UNARY((int64_t), float);
            case Opcode::Cast_i_d: UNARY((int64_t), double);
            case Opcode::Cast_i_b: UNARY((int64_t), bool);

            /* Cast to float. */
            case Opcode::Cast_f_i: UNARY((float), int64_t);
            case Opcode::Cast_f_d: UNARY((float), float);

            /* Cast to double. */
            case Opcode::Cast_d_i: UNARY((double), int64_t);
            case Opcode::Cast_d_f: UNARY((double), float);
        }
    }
exit:

#undef BINARY
#undef UNARY

    return std::move(stack_);
}

void StackMachine::dump(std::ostream &out) const
{
    out << "StackMachine\n    Constants: [";
    for (auto it = constants.cbegin(); it != constants.cend(); ++it) {
        if (it != constants.cbegin()) out << ", ";
        out << *it;
    }
    out << "]\n    Tuple Schema: " << schema
        << "]\n    Opcode Sequence:\n";
    for (std::size_t i = 0; i != ops.size(); ++i) {
        auto opc = ops[i];
        out << "        [0x" << std::hex << std::setfill('0') << std::setw(4) << i << std::dec << "]: "
            << StackMachine::OPCODE_TO_STR[static_cast<std::size_t>(opc)];
        switch (opc) {
            case Opcode::Ld_Idx:
            case Opcode::Ld_Const:
            case Opcode::Upd_Const:
                ++i;
                out << ' ' << static_cast<int64_t>(ops[i]);
            default:;
        }
        out << '\n';
    }
    out.flush();
}

void StackMachine::dump() const { dump(std::cerr); }

/*======================================================================================================================
 * Declaration of operator data.
 *====================================================================================================================*/

struct ProjectionData : OperatorData
{
    StackMachine projections;

    ProjectionData(StackMachine &&P) : projections(std::move(P)) { }
};

struct NestedLoopsJoinData : OperatorData
{
    using buffer_type = std::vector<tuple_type>;

    StackMachine predicate;
    buffer_type *buffers;
    std::size_t active_child;

    NestedLoopsJoinData(StackMachine &&predicate, std::size_t num_children)
        : predicate(std::move(predicate))
        , buffers(new buffer_type[num_children - 1])
    { }

    ~NestedLoopsJoinData() { delete[] buffers; }
};

struct LimitData : OperatorData
{
    std::size_t num_tuples = 0;
};

struct GroupingData : OperatorData
{
    StackMachine keys;

    GroupingData(StackMachine &&keys) : keys(std::move(keys)) { }
};

struct HashBasedGroupingData : GroupingData
{
    std::unordered_map<tuple_type, tuple_type> groups;

    HashBasedGroupingData(StackMachine &&keys) : GroupingData(std::move(keys)) { }
};

struct SortingData : OperatorData
{
    using buffer_type = std::vector<tuple_type>;

    buffer_type buffer;
};

struct FilterData : OperatorData
{
    StackMachine filter;

    FilterData(StackMachine &&filter) : filter(std::move(filter)) { }
};

/*======================================================================================================================
 * Interpreter - Recursive descent
 *====================================================================================================================*/

void Interpreter::operator()(const CallbackOperator &op)
{
    op.child(0)->accept(*this);
}

void Interpreter::operator()(const ScanOperator &op)
{
    OperatorSchema empty_schema;
    auto loader = op.store().loader(empty_schema);

    for (auto i = op.store().num_rows(); i; --i) {
        auto &&tuple = loader(tuple_type());
        op.parent()->accept(*this, tuple);
    }
}

void Interpreter::operator()(const FilterOperator &op)
{
    auto data = new FilterData(StackMachine(op.child(0)->schema()));
    op.data(data);
    data->filter.add(op.filter());
    op.child(0)->accept(*this);
}

void Interpreter::operator()(const JoinOperator &op)
{
    switch (op.algo()) {
        default:
            unreachable("Undefined join algorithm.");

        case JoinOperator::J_Undefined:
        case JoinOperator::J_NestedLoops: {
            auto data = new NestedLoopsJoinData(StackMachine(op.schema()), op.children().size());
            op.data(data);
            data->predicate.add(op.predicate());
            for (std::size_t i = 0, end = op.children().size(); i != end; ++i) {
                data->active_child = i;
                auto c = op.child(i);
                c->accept(*this);
            }
            break;
        }

        case JoinOperator::J_SimpleHashJoin:
            // TODO
            unreachable("Simple hash join not implemented.");
    }
}

void Interpreter::operator()(const ProjectionOperator &op)
{
    bool has_child = op.children().size();
    OperatorSchema empty_schema;
    auto &S = has_child ? op.child(0)->schema() : empty_schema;
    auto data = new ProjectionData(StackMachine(S));
    op.data(data);
    for (auto &p : op.projections())
        data->projections.add(*p.first);

    /* Evaluate the projection. */
    if (has_child) {
        op.child(0)->accept(*this);
    } else {
        tuple_type t;
        (*this)(op, t); // evaluate the projection EXACTLY ONCE on an empty tuple
    }
}

void Interpreter::operator()(const LimitOperator &op)
{
    try {
        op.data(new LimitData());
        op.child(0)->accept(*this);
    } catch (LimitOperator::stack_unwind) {
        /* OK, we produced all tuples and unwinded the stack */
    }
}

void Interpreter::operator()(const GroupingOperator &op)
{
    auto &S = op.child(0)->schema();
    switch (op.algo()) {
        case GroupingOperator::G_Undefined:
        case GroupingOperator::G_Ordered:
            unreachable("not implemented");

        case GroupingOperator::G_Hashing: {
            auto data = new HashBasedGroupingData(StackMachine(S));
            op.data(data);
            for (auto e : op.group_by())
                data->keys.add(*e);
            op.child(0)->accept(*this);
            for (auto g : data->groups) {
                auto t = g.first + g.second;
                op.parent()->accept(*this, t); // pass groups on to parent
            }
            break;
        }
    }
}

void Interpreter::operator()(const SortingOperator &op)
{
    auto data = new SortingData();
    op.data(data);
    op.child(0)->accept(*this);

    const auto &orderings = op.order_by();
    auto &S = op.schema();

    StackMachine comparator(S);
    for (auto o : orderings) {
        comparator.add(*o.first); // LHS
        auto num_ops = comparator.ops.size();
        comparator.add(*o.first); // RHS
        /* Patch indices of RHS. */
        for (std::size_t i = num_ops; i != comparator.ops.size(); ++i) {
            auto opc = comparator.ops[i];
            switch (opc) {
                case StackMachine::Opcode::Ld_Const:
                    ++i;
                default:
                    break;

                case StackMachine::Opcode::Ld_Idx:
                    /* Add offset equal to size of LHS. */
                    ++i;
                    comparator.ops[i] =
                        static_cast<StackMachine::Opcode>(static_cast<uint8_t>(comparator.ops[i]) + S.size());
                    break;
            }
        }

        /* Emit comparison. */
        auto ty = o.first->type();
        if (ty->is_boolean())
            comparator.ops.push_back(StackMachine::Opcode::Cmp_b);
        else if (ty->is_character_sequence())
            comparator.ops.push_back(StackMachine::Opcode::Cmp_s);
        else if (ty->is_integral() or ty->is_decimal())
            comparator.ops.push_back(StackMachine::Opcode::Cmp_i);
        else if (ty->is_float())
            comparator.ops.push_back(StackMachine::Opcode::Cmp_f);
        else if (ty->is_double())
            comparator.ops.push_back(StackMachine::Opcode::Cmp_d);
        else
            unreachable("invalid type");

        if (not o.second)
            comparator.ops.push_back(StackMachine::Opcode::Minus_i); // sort descending

        comparator.ops.push_back(StackMachine::Opcode::Stop_NZ);
    }

    std::vector<StackMachine> stack_machines;
    for (auto &o : orderings)
        stack_machines.emplace_back(S, *o.first);

    std::sort(data->buffer.begin(), data->buffer.end(), [&](const tuple_type &first, const tuple_type &second) {
        auto res = comparator(first + second);
        auto pv = std::get_if<int64_t>(&res.back());
        insist(pv, "invalid type of variant");
        return *pv < 0;
    });
    for (auto t : data->buffer)
        op.parent()->accept(*this, t);
}

/*======================================================================================================================
 * Interpreter - Recursive ascent
 *====================================================================================================================*/

void Interpreter::operator()(const CallbackOperator &op, tuple_type &t)
{
    op.callback()(op.schema(), t);
}

void Interpreter::operator()(const FilterOperator &op, tuple_type &t)
{
    auto data = as<FilterData>(op.data());
    auto res = data->filter(t);
    auto pv = std::get_if<bool>(&res[0]);
    insist(pv, "invalid type of variant");
    if (*pv)
        op.parent()->accept(*this, t);
}

void Interpreter::operator()(const JoinOperator &op, tuple_type &t)
{
    switch (op.algo()) {
        default:
            unreachable("Illegal join algorithm.");

        case JoinOperator::J_Undefined:
            /* fall through */
        case JoinOperator::J_NestedLoops: {
            auto data = (NestedLoopsJoinData*)(op.data());
            auto size = op.children().size();

            if (data->active_child == size - 1) {
                /* This is the right-most child.  Combine its produced tuple with all combinations of the buffered
                 * tuples. */
                std::vector<std::size_t> positions(size - 1, std::size_t(-1L)); // positions within each buffer
                std::size_t child_id = 0; // cursor to the child that provides the next part of the joined tuple
                tuple_type joined;
                joined.reserve(op.schema().size());

                for (;;) {
                    if (child_id == size - 1) { // right-most child, which produced `t`
                        /* Combine the tuples.  One tuple from each buffer. */
                        joined.clear();
                        for (std::size_t i = 0; i != positions.size(); ++i) {
                            auto &buffer = data->buffers[i];
                            joined += buffer[positions[i]];
                        }
                        joined += t; // append the tuple just produced by the right-most child

                        /* Evaluate the join predicate on the joined tuple. */
                        auto res = data->predicate(joined);
                        insist(res.size() == 1);
                        auto pv = std::get_if<bool>(&res[0]);
                        insist(pv, "invalid type of variant");
                        if (*pv)
                            op.parent()->accept(*this, joined);

                        --child_id;
                    } else { // child whose tuples have been materialized in a buffer
                        ++positions[child_id];
                        auto &buffer = data->buffers[child_id];
                        if (positions[child_id] == buffer.size()) { // reached the end of this buffer; backtrack
                            if (child_id == 0)
                                break;
                            positions[child_id] = std::size_t(-1L);
                            --child_id;
                        } else {
                            insist(positions[child_id] < buffer.size(), "position out of bounds");
                            ++child_id;
                        }
                    }
                }
            } else {
                /* This is not the right-most child.  Collect its produced tuples in a buffer. */
                data->buffers[data->active_child].emplace_back(t);
            }
            break;
        }

        case JoinOperator::J_SimpleHashJoin:
            // TODO
            unreachable("Simple hash join not implemented.");
    }
}

void Interpreter::operator()(const ProjectionOperator &op, tuple_type &t)
{
    auto data = as<ProjectionData>(op.data());
    auto result = data->projections(t);

    if (op.is_anti())
        result.insert(result.end(), t.begin(), t.end());

    op.parent()->accept(*this, result);
}

void Interpreter::operator()(const LimitOperator &op, tuple_type &t)
{
    auto data = as<LimitData>(op.data());
    if (data->num_tuples < op.offset())
        /* discard this tuple */;
    else if (data->num_tuples < op.offset() + op.limit())
        op.parent()->accept(*this, t); // pass tuple on to parent
    else
        throw LimitOperator::stack_unwind(); // all tuples produced, now unwind the stack
    ++data->num_tuples;
}

void Interpreter::operator()(const GroupingOperator &op, tuple_type &t)
{
    tuple_type *aggregates = nullptr;

    /* Find the group. */
    switch (op.algo()) {
        case GroupingOperator::G_Undefined:
        case GroupingOperator::G_Ordered:
            unreachable("not implemented");

        case GroupingOperator::G_Hashing: {
            auto data = as<HashBasedGroupingData>(op.data());
            auto &groups = data->groups;
            tuple_type key = data->keys(t); // TODO do the same for aggregates
            auto it = groups.find(key);
            if (it == groups.end()) {
                /* Initialize the group's aggregate to NULL.  This will be overwritten by the neutral element w.r.t. the
                 * aggregation function. */
                it = groups.emplace_hint(it, key, tuple_type(op.aggregates().size(), null_type()));
            }
            aggregates = &it->second;
            break;
        }
    }
    insist(aggregates, "must have found the group's aggregate");

    /* Add this tuple to its group by computing the aggregates. */
    for (std::size_t i = 0, end = op.aggregates().size(); i != end; ++i) {
        auto fe = as<const FnApplicationExpr>(op.aggregates()[i]);
        auto ty = fe->type();
        auto &fn = fe->get_function();
        auto &agg = (*aggregates)[i];

        switch (fn.fnid) {
            default:
                unreachable("function kind not implemented");

            case Function::FN_UDF:
                unreachable("UDFs not yet supported");

            case Function::FN_COUNT:
                if (std::holds_alternative<null_type>(agg))
                    agg = int64_t(0); // initialize
                if (fe->args.size() == 0) {
                    agg = std::get<int64_t>(agg) + 1;
                } else {
                    StackMachine eval(op.child(0)->schema(), *fe->args[0]);
                    if (not std::holds_alternative<null_type>(eval(t)[0]))
                        agg = std::get<int64_t>(agg) + 1;
                }
                break;

            case Function::FN_SUM: {
                if (std::holds_alternative<null_type>(agg))
                    agg = int64_t(0); // initialize
                auto arg = fe->args[0];
                StackMachine eval(op.child(0)->schema(), *arg);
                auto res = eval(t)[0];
                if (std::holds_alternative<null_type>(res))
                    continue; // skip NULL
                auto n = as<const Numeric>(ty);
                if (n->kind == Numeric::N_Float) {
                    agg = to<double>(agg) + to<double>(res);
                } else {
                    agg = to<int64_t>(agg) + to<int64_t>(res);
                }
                break;
            }

            case Function::FN_MIN: {
                using std::min;
                auto arg = fe->args[0];
                StackMachine eval(op.child(0)->schema(), *arg);
                auto res = eval(t)[0];
                if (std::holds_alternative<null_type>(res))
                    continue; // skip NULL

                auto n = as<const Numeric>(ty);
                if (n->kind == Numeric::N_Float and n->precision == 32) {
                    if (std::holds_alternative<null_type>(agg))
                        agg = to<float>(res);
                    else
                        agg = min(to<float>(agg), to<float>(res));
                } else if (n->kind == Numeric::N_Float and n->precision == 64) {
                    if (std::holds_alternative<null_type>(agg))
                        agg = to<double>(res);
                    else
                        agg = min(to<double>(agg), to<double>(res));
                } else {
                    if (std::holds_alternative<null_type>(agg))
                        agg = to<int64_t>(res);
                    else
                        agg = min(to<int64_t>(agg), to<int64_t>(res));
                }
                break;
            }

            case Function::FN_MAX: {
                using std::max;
                auto arg = fe->args[0];
                StackMachine eval(op.child(0)->schema(), *arg);
                auto res = eval(t)[0];
                if (std::holds_alternative<null_type>(res))
                    continue; // skip NULL

                auto n = as<const Numeric>(ty);
                if (n->kind == Numeric::N_Float and n->precision == 32) {
                    if (std::holds_alternative<null_type>(agg))
                        agg = to<float>(res);
                    else
                        agg = max(to<float>(agg), to<float>(res));
                } else if (n->kind == Numeric::N_Float and n->precision == 64) {
                    if (std::holds_alternative<null_type>(agg))
                        agg = to<double>(res);
                    else
                        agg = max(to<double>(agg), to<double>(res));
                } else {
                    if (std::holds_alternative<null_type>(agg))
                        agg = to<int64_t>(res);
                    else
                        agg = max(to<int64_t>(agg), to<int64_t>(res));
                }
                break;
            }
        }
    }
}

void Interpreter::operator()(const SortingOperator &op, tuple_type &t)
{
    /* cache all tuples for sorting */
    auto data = as<SortingData>(op.data());
    data->buffer.emplace_back(t);
}


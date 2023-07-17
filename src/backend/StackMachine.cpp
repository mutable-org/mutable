#include "backend/StackMachine.hpp"

#include "backend/Interpreter.hpp"
#include <ctime>
#include <functional>
#include <mutable/util/fn.hpp>
#include <regex>


using namespace m;


/*======================================================================================================================
 * Helper functions
 *====================================================================================================================*/

/** Return the type suffix for the given `PrimitiveType` `ty`. */
const char * tystr(const PrimitiveType *ty) {
    if (ty->is_boolean())
        return "_b";
    if (ty->is_character_sequence())
        return "_s";
    if (ty->is_date())
        return "_i";
    if (ty->is_date_time())
        return "_i";
    auto n = as<const Numeric>(ty);
    switch (n->kind) {
        case Numeric::N_Int:
        case Numeric::N_Decimal:
            return "_i";
        case Numeric::N_Float:
            return n->precision == 32 ? "_f" : "_d";
    }
};


/*======================================================================================================================
 * StackMachineBuilder
 *====================================================================================================================*/

struct m::StackMachineBuilder : ast::ConstASTExprVisitor
{
    private:
    StackMachine &stack_machine_;
    const std::vector<Schema> &schemas_;
    const std::vector<std::size_t> &tuple_ids_;

    public:
    StackMachineBuilder(StackMachine &stack_machine, const ast::Expr &expr,
                        const std::vector<Schema> &schemas,
                        const std::vector<std::size_t> &tuple_ids)
        : stack_machine_(stack_machine)
        , schemas_(schemas)
        , tuple_ids_(tuple_ids)
    {
        (*this)(expr); // compute the command sequence
    }

    private:
    static std::unordered_map<std::string, std::regex> regexes_; ///< regexes built from patterns in LIKE expressions

    /** Returns a pair of (tuple_id, attr_id). */
    std::pair<std::size_t, std::size_t>
    find_id(Schema::Identifier id) const {
        for (std::size_t schema_idx = 0; schema_idx != schemas_.size(); ++schema_idx) {
            auto &S = schemas_[schema_idx];
            auto it = S.find(id);
            if (it != S.cend())
                return { tuple_ids_[schema_idx], std::distance(S.cbegin(), it) };
        }
        M_unreachable("identifier not found");
    }

    using ConstASTExprVisitor::operator();
    void operator()(Const<ast::ErrorExpr>&) override { M_unreachable("invalid expression"); }
    void operator()(Const<ast::Designator> &e) override;
    void operator()(Const<ast::Constant> &e) override;
    void operator()(Const<ast::FnApplicationExpr> &e) override;
    void operator()(Const<ast::UnaryExpr> &e) override;
    void operator()(Const<ast::BinaryExpr> &e) override;
    void operator()(Const<ast::QueryExpr> &e) override;
};

std::unordered_map<std::string, std::regex> StackMachineBuilder::regexes_;

void StackMachineBuilder::operator()(Const<ast::Designator> &e)
{
    auto [tuple_id, attr_id] = find_id({e.table_name.text, e.attr_name.text});
    stack_machine_.emit_Ld_Tup(tuple_id, attr_id);
}

void StackMachineBuilder::operator()(Const<ast::Constant> &e) {
    if (e.tok == TK_Null)
        stack_machine_.emit_Push_Null();
    else
        stack_machine_.add_and_emit_load(Interpreter::eval(e));
}

void StackMachineBuilder::operator()(Const<ast::FnApplicationExpr> &e)
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
            M_insist(e.args.size() == 1);
            (*this)(*e.args[0]);
            stack_machine_.emit_Is_Null();
            break;

        /*----- Type casts -------------------------------------------------------------------------------------------*/
        case Function::FN_INT: {
            M_insist(e.args.size() == 1);
            (*this)(*e.args[0]);
            auto ty = e.args[0]->type();
            if (ty->is_float())
                stack_machine_.emit_Cast_i_f();
            else if (ty->is_double())
                stack_machine_.emit_Cast_i_d();
            else if (ty->is_decimal()) {
                M_unreachable("not implemented");
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
            auto [tuple_id, attr_id] = find_id({name});
            stack_machine_.emit_Ld_Tup(tuple_id, attr_id);
            return;
        }
    }
}

void StackMachineBuilder::operator()(Const<ast::UnaryExpr> &e)
{
    (*this)(*e.expr);
    auto ty = e.expr->type();

    switch (e.op().type) {
        default:
            M_unreachable("illegal token type");

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
                M_unreachable("illegal type");
            break;

        case TK_Not:
            M_insist(ty->is_boolean(), "illegal type");
            stack_machine_.emit_Not_b();
            break;
    }
}

void StackMachineBuilder::operator()(Const<ast::BinaryExpr> &e)
{
    auto ty = as<const PrimitiveType>(e.type());
    auto ty_lhs = as<const PrimitiveType>(e.lhs->type());
    auto ty_rhs = as<const PrimitiveType>(e.rhs->type());
    auto tystr_to = tystr(ty);

    /* Emit instructions to convert the current top-of-stack of type `from_ty` to type `to_ty`. */
    auto emit_cast = [&](const PrimitiveType *from_ty, const PrimitiveType *to_ty) {
        std::string tystr_to = tystr(to_ty);
        std::string tystr_from = tystr(from_ty);
        /* Cast LHS if necessary. */
        if (tystr_from != tystr_to) {
            std::string opstr = "Cast" + tystr_to + tystr_from;
            auto opcode = StackMachine::STR_TO_OPCODE.at(opstr);
            stack_machine_.emit(opcode);
        }
    };

    /* Emit instructions to bring the current top-of-stack with precision of `from_ty` to precision of `to_ty`. */
    auto scale = [&](const PrimitiveType *from_ty, const PrimitiveType *to_ty) {
        auto n_from = as<const Numeric>(from_ty);
        auto n_to = as<const Numeric>(to_ty);
        if (n_from->scale < n_to->scale) {
            M_insist(n_to->is_decimal(), "only decimals have a scale");
            /* Scale up. */
            const uint32_t delta = n_to->scale - n_from->scale;
            const int64_t factor = powi<int64_t>(10, delta);
            switch (n_from->kind) {
                case Numeric::N_Float:
                    if (n_from->precision == 32) {
                        stack_machine_.add_and_emit_load(float(factor));
                        stack_machine_.emit_Mul_f(); // multiply by scale factor
                    } else {
                        stack_machine_.add_and_emit_load(double(factor));
                        stack_machine_.emit_Mul_d(); // multiply by scale factor
                    }
                    break;

                case Numeric::N_Decimal:
                case Numeric::N_Int:
                    stack_machine_.add_and_emit_load(factor);
                    stack_machine_.emit_Mul_i(); // multiply by scale factor
                    break;
            }
        } else if (n_from->scale > n_to->scale) {
            M_insist(n_from->is_decimal(), "only decimals have a scale");
            /* Scale down. */
            const uint32_t delta = n_from->scale - n_to->scale;
            const int64_t factor = powi<int64_t>(10, delta);
            switch (n_from->kind) {
                case Numeric::N_Float:
                    if (n_from->precision == 32) {
                        stack_machine_.add_and_emit_load(float(1.f / factor));
                        stack_machine_.emit_Mul_f(); // multiply by inverse scale factor
                    } else {
                        stack_machine_.add_and_emit_load(double(1. / factor));
                        stack_machine_.emit_Mul_d(); // multiply by inverse scale factor
                    }
                    break;

                case Numeric::N_Decimal:
                    stack_machine_.add_and_emit_load(factor);
                    stack_machine_.emit_Div_i(); // divide by scale factor
                    break;

                case Numeric::N_Int:
                    M_unreachable("int cannot be scaled down");
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
    switch (e.op().type) {
        default: M_unreachable("illegal operator");

        /*----- Arithmetic operators ---------------------------------------------------------------------------------*/
        case TK_PLUS:           opname = "Add"; break;
        case TK_MINUS:          opname = "Sub"; break;
        case TK_ASTERISK:       opname = "Mul"; break;
        case TK_SLASH:          opname = "Div"; break;
        case TK_PERCENT:        opname = "Mod"; break;

        /*----- Concatenation operator -------------------------------------------------------------------------------*/
        case TK_DOTDOT:         opname = "Cat"; break;

        /*----- Comparison operators ---------------------------------------------------------------------------------*/
        case TK_LESS:           opname = "LT";    break;
        case TK_GREATER:        opname = "GT";    break;
        case TK_LESS_EQUAL:     opname = "LE";    break;
        case TK_GREATER_EQUAL:  opname = "GE";    break;
        case TK_EQUAL:          opname = "Eq";    break;
        case TK_BANG_EQUAL:     opname = "NE";    break;
        case TK_Like:           opname = "Like";  break;

        /*----- Logical operators ------------------------------------------------------------------------------------*/
        case TK_And:            opname = "And"; break;
        case TK_Or:             opname = "Or";  break;
    }

    switch (e.op().type) {
        default: M_unreachable("illegal operator");

        /*----- Arithmetic operators ---------------------------------------------------------------------------------*/
        case TK_PLUS:
        case TK_MINUS: {
            (*this)(*e.lhs);
            scale(ty_lhs, ty);
            emit_cast(ty_lhs, ty);

            (*this)(*e.rhs);
            scale(ty_rhs, ty);
            emit_cast(ty_rhs, ty);

            std::string opstr = e.op().type == TK_PLUS ? "Add" : "Sub";
            opstr += tystr_to;
            auto opcode = StackMachine::STR_TO_OPCODE.at(opstr);
            stack_machine_.emit(opcode);
            break;
        }

        case TK_ASTERISK: {
            auto n_lhs = as<const Numeric>(ty_lhs);
            auto n_rhs = as<const Numeric>(ty_rhs);
            auto n_res = as<const Numeric>(ty);
            uint32_t the_scale = 0;

            (*this)(*e.lhs);
            if (n_lhs->is_floating_point()) {
                scale(n_lhs, n_res); // scale float up before cast to preserve decimal places
                the_scale += n_res->scale;
            } else {
                the_scale += n_lhs->scale;
            }
            emit_cast(n_lhs, n_res);

            (*this)(*e.rhs);
            if (n_rhs->is_floating_point()) {
                scale(n_rhs, n_res); // scale float up before cast to preserve decimal places
                the_scale += n_res->scale;
            } else {
                the_scale += n_rhs->scale;
            }
            emit_cast(n_rhs, n_res);

            std::string opstr = "Mul";
            opstr += tystr_to;
            auto opcode = StackMachine::STR_TO_OPCODE.at(opstr);
            stack_machine_.emit(opcode); // Mul_x

            /* Scale down again, if necessary. */
            the_scale -= n_res->scale;
            M_insist(the_scale >= 0);
            if (the_scale != 0) {
                M_insist(n_res->is_decimal());
                const int64_t factor = powi<int64_t>(10, the_scale);
                load_numeric(factor, n_res);
                stack_machine_.emit_Div_i();
            }

            break;
        }

        case TK_SLASH: {
            /* Division with potentially different numeric types is a tricky thing.  Not only must we convert the values
             * from their original data type to the type of the result, but we also must scale them correctly.
             *
             * The effective scale of the result is computed as `scale_lhs - scale_rhs`.
             *
             * (1) If the effective scale of the result is *less* than the expected scale of the result, the LHS must be
             *     scaled up.
             *
             * (2) If the effective scale of the result is *greater* than the expected scale of the result, the result
             *     must be scaled down.
             *
             * With these rules we can achieve maximum precision within the rules of the type system.
             */

            auto n_lhs = as<const Numeric>(ty_lhs);
            auto n_rhs = as<const Numeric>(ty_rhs);
            auto n_res = as<const Numeric>(ty);
            int32_t the_scale = 0;

            (*this)(*e.lhs);
            if (n_lhs->is_floating_point()) {
                scale(n_lhs, n_res); // scale float up before cast to preserve decimal places
                the_scale += n_res->scale;
            } else {
                the_scale += n_lhs->scale;
            }
            emit_cast(n_lhs, n_res);

            if (n_rhs->is_floating_point())
                the_scale -= n_res->scale;
            else
                the_scale -= n_rhs->scale;

            if (the_scale < int32_t(n_res->scale)) {
                const int64_t factor = powi(10L, n_res->scale - the_scale); // scale up
                load_numeric(factor, n_res);
                stack_machine_.emit_Mul_i();
            }

            (*this)(*e.rhs);
            if (n_rhs->is_floating_point())
                scale(n_rhs, n_res); // scale float up before cast to preserve decimal places
            emit_cast(n_rhs, n_res);

            std::string opstr = "Div";
            opstr += tystr_to;
            auto opcode = StackMachine::STR_TO_OPCODE.at(opstr);
            stack_machine_.emit(opcode); // Div_x

            if (the_scale > int32_t(n_res->scale)) {
                const int64_t factor = powi(10L, the_scale - n_res->scale); // scale down
                load_numeric(factor, n_res);
                stack_machine_.emit_Div_i();
            }

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
                M_insist(ty_rhs->is_numeric());
                auto n_lhs = as<const Numeric>(ty_lhs);
                auto n_rhs = as<const Numeric>(ty_rhs);
                auto n_res = arithmetic_join(n_lhs, n_rhs);

                (*this)(*e.lhs);
                scale(n_lhs, n_res);
                emit_cast(n_lhs, n_res);

                (*this)(*e.rhs);
                scale(n_rhs, n_res);
                emit_cast(n_rhs, n_res);

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

        case TK_Like: {
            if (auto rhs = cast<const ast::Constant>(e.rhs.get())) {
                (*this)(*e.lhs);
                auto pattern = interpret(rhs->tok.text);
                auto it = regexes_.find(pattern);
                if (it == regexes_.end())
                    it = StackMachineBuilder::regexes_.insert({pattern, pattern_to_regex(pattern.c_str(), true)}).first;
                stack_machine_.add_and_emit_load(&(it->second));
                stack_machine_.emit_Like_const();
            } else {
                (*this)(*e.lhs);
                (*this)(*e.rhs);
                stack_machine_.emit_Like_expr();
            }
            break;
        }

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

void StackMachineBuilder::operator()(Const<ast::QueryExpr> &e) {
    /* Given the query expression, identify the position of its value in the tuple.  */
    Catalog &C = Catalog::Get();
    auto [tuple_id, attr_id] = find_id({e.alias(), C.pool("$res")});
    stack_machine_.emit_Ld_Tup(tuple_id, attr_id);
}


/*======================================================================================================================
 * Stack Machine
 *====================================================================================================================*/

const std::unordered_map<std::string, StackMachine::Opcode> StackMachine::STR_TO_OPCODE = {
#define M_OPCODE(CODE, ...) { #CODE, StackMachine::Opcode:: CODE },
#include "tables/Opcodes.tbl"
#undef M_OPCODE
};

StackMachine::StackMachine(Schema in_schema, const ast::Expr &expr)
    : in_schema(in_schema)
{
    emit(expr, 1);
    // TODO emit St
}

StackMachine::StackMachine(Schema in_schema, const cnf::CNF &cnf)
    : in_schema(in_schema)
{
    emit(cnf);
    // TODO emit St
}

void StackMachine::emit(const ast::Expr &expr, std::size_t tuple_id)
{
    if (auto it = in_schema.find(Schema::Identifier(expr)); it != in_schema.end()) { // expression already computed
        /* Given the expression, identify the position of its value in the tuple.  */
        const unsigned idx = std::distance(in_schema.begin(), it);
        M_insist(idx < in_schema.num_entries(), "index out of bounds");
        emit_Ld_Tup(tuple_id, idx);
    } else { // expression has to be computed
        std::vector<Schema> svec({in_schema});
        std::vector<std::size_t> tuple_ids({tuple_id});
        StackMachineBuilder(*this, expr, svec, tuple_ids); // compute the command sequence for this stack machine
    }
}

void StackMachine::emit(const ast::Expr &expr, const Schema &schema, std::size_t tuple_id)
{
    if (auto it = in_schema.find(Schema::Identifier(expr)); it != in_schema.end()) { // expression already computed
        /* Given the expression, identify the position of its value in the tuple.  */
        const unsigned idx = std::distance(in_schema.begin(), it);
        M_insist(idx < in_schema.num_entries(), "index out of bounds");
        emit_Ld_Tup(tuple_id, idx);
    } else { // expression has to be computed
        std::vector<Schema> svec({schema});
        std::vector<std::size_t> tuple_ids({tuple_id});
        StackMachineBuilder(*this, expr, svec, tuple_ids); // compute the command sequence for this stack machine
    }
}

void StackMachine::emit(const ast::Expr &expr,
                        std::vector<Schema> &schemas,
                        std::vector<std::size_t> &tuple_ids)
{
    StackMachineBuilder(*this, expr, schemas, tuple_ids); // compute the command sequence for this stack machine
}

void StackMachine::emit(const cnf::CNF &cnf, std::size_t tuple_id)
{
    /* Compile filter into stack machine. */
    for (auto clause_it = cnf.cbegin(); clause_it != cnf.cend(); ++clause_it) {
        auto &C = *clause_it;
        for (auto pred_it = C.cbegin(); pred_it != C.cend(); ++pred_it) {
            auto &P = *pred_it;
            emit(*P, tuple_id); // emit code for predicate
            if (P.negative())
                ops.push_back(StackMachine::Opcode::Not_b); // negate if negative
            if (pred_it != C.cbegin())
                ops.push_back(StackMachine::Opcode::Or_b);
        }
        if (clause_it != cnf.cbegin())
            ops.push_back(StackMachine::Opcode::And_b);
    }
}

void StackMachine::emit(const cnf::CNF &cnf,
                        std::vector<Schema> &schemas,
                        std::vector<std::size_t> &tuple_ids)
{
    /* Compile filter into stack machine. */
    for (auto clause_it = cnf.cbegin(); clause_it != cnf.cend(); ++clause_it) {
        auto &C = *clause_it;
        for (auto pred_it = C.cbegin(); pred_it != C.cend(); ++pred_it) {
            auto &P = *pred_it;
            emit(*P, schemas, tuple_ids); // emit code for predicate
            if (P.negative())
                ops.push_back(StackMachine::Opcode::Not_b); // negate if negative
            if (pred_it != C.cbegin())
                ops.push_back(StackMachine::Opcode::Or_b);
        }
        if (clause_it != cnf.cbegin())
            ops.push_back(StackMachine::Opcode::And_b);
    }
}

void StackMachine::emit_Ld(const Type *ty)
{
    M_insist(not ty->is_boolean(), "to access a boolean use `emit_Ld_b(offset)`");

    if (auto n = cast<const Numeric>(ty)) {
        switch (n->kind) {
            case Numeric::N_Int:
            case Numeric::N_Decimal: {
                switch (n->size()) {
                    default: M_unreachable("illegal type");
                    case  8: emit_Ld_i8();  break;
                    case 16: emit_Ld_i16(); break;
                    case 32: emit_Ld_i32(); break;
                    case 64: emit_Ld_i64(); break;
                }
                break;
            }

            case Numeric::N_Float: {
                if (n->size() == 32)
                    emit_Ld_f();
                else
                    emit_Ld_d();
                break;
            }
        }
    } else if (auto cs = cast<const CharacterSequence>(ty)) {
        emit_Ld_s(cs->length);
    } else if (auto d = cast<const Date>(ty)) {
        emit_Ld_i32();
    } else if (auto dt = cast<const DateTime>(ty)) {
        emit_Ld_i64();
    } else {
        M_unreachable("illegal type");
    }
}

void StackMachine::emit_St(const Type *ty)
{
    M_insist(not ty->is_boolean(), "to access a boolean use `emit_St_b(offset)`");

    if (auto n = cast<const Numeric>(ty)) {
        switch (n->kind) {
            case Numeric::N_Int:
            case Numeric::N_Decimal: {
                switch (n->size()) {
                    default: M_unreachable("illegal type");
                    case  8: emit_St_i8();  break;
                    case 16: emit_St_i16(); break;
                    case 32: emit_St_i32(); break;
                    case 64: emit_St_i64(); break;
                }
                break;
            }

            case Numeric::N_Float: {
                if (n->size() == 32)
                    emit_St_f();
                else
                    emit_St_d();
                break;
            }
        }
    } else if (auto cs = cast<const CharacterSequence>(ty)) {
        emit_St_s(cs->length + cs->is_varying);
    } else if (auto d = cast<const Date>(ty)) {
        emit_St_i32();
    } else if (auto dt = cast<const DateTime>(ty)) {
        emit_St_i64();
    } else {
        M_unreachable("illegal type");
    }
}

void StackMachine::emit_St_Tup(std::size_t tuple_id, std::size_t index, const Type *ty)
{
    if (ty->is_none()) {
        emit_St_Tup_Null(tuple_id, index);
    } else if (auto cs = cast<const CharacterSequence>(ty)) {
        emit_St_Tup_s(tuple_id, index, cs->length);
    } else {
        std::ostringstream oss;
        oss << "St_Tup" << tystr(as<const PrimitiveType>(ty));
        auto opcode = StackMachine::STR_TO_OPCODE.at(oss.str());
        emit(opcode);
        emit(static_cast<Opcode>(tuple_id));
        emit(static_cast<Opcode>(index));
    }
}

void StackMachine::emit_Print(std::size_t ostream_index, const Type *ty)
{
    if (ty->is_none()) {
        emit_Push_Null();
        emit_Print_i(ostream_index);
    } else if (ty->is_date()) {
        emit(StackMachine::STR_TO_OPCODE.at("Print_date"));
        emit(static_cast<Opcode>(ostream_index));
    } else if (ty->is_date_time()) {
        emit(StackMachine::STR_TO_OPCODE.at("Print_datetime"));
        emit(static_cast<Opcode>(ostream_index));
    } else {
        std::ostringstream oss;
        oss << "Print" << tystr(as<const PrimitiveType>(ty));
        auto opcode = StackMachine::STR_TO_OPCODE.at(oss.str());
        emit(opcode);
        emit(static_cast<Opcode>(ostream_index));
    }
}

void StackMachine::emit_Cast(const Type *to_ty, const Type *from_ty)
{
    auto to   = as<const PrimitiveType>(to_ty);
    auto from = as<const PrimitiveType>(from_ty);
    if (from->as_vectorial() == to->as_vectorial()) return; // nothing to be done

    if (auto n_from = cast<const Numeric>(from)) {
        auto n_to = as<const Numeric>(to);

        switch (n_from->kind) {
            case Numeric::N_Int:
                switch (n_to->kind) {
                    case Numeric::N_Int: /* int -> int */
                        /* nothing to be done */
                        return;

                    case Numeric::N_Decimal: /* int -> decimal */
                        add_and_emit_load(powi(10L, n_to->scale));
                        emit_Mul_i();
                        return;

                    case Numeric::N_Float:
                        if (n_to->size() == 32) /* int -> float */
                            emit_Cast_f_i();
                        else                    /* int -> double */
                            emit_Cast_d_i();
                        return;
                }
                break;

            case Numeric::N_Float:
                if (n_from->size() == 32) {
                    switch (n_to->kind) {
                        case Numeric::N_Int: /* float -> int */
                            emit_Cast_i_f();
                            return;

                        case Numeric::N_Decimal: /* float -> decimal */
                            add_and_emit_load(float(powi(10L, n_to->scale)));
                            emit_Mul_f();
                            emit_Cast_i_f();
                            return;

                        case Numeric::N_Float: /* float -> double */
                            M_insist(n_to->size() == 64, "float to float");
                            emit_Cast_d_f();
                            return;
                    }
                } else {
                    switch (n_to->kind) {
                        case Numeric::N_Int: /* double -> int */
                            emit_Cast_i_d();
                            return;

                        case Numeric::N_Decimal: /* double -> decimal */
                            add_and_emit_load(double(powi(10L, n_to->scale)));
                            emit_Mul_d();
                            emit_Cast_i_d();
                            return;

                        case Numeric::N_Float:
                            M_insist(n_to->size() == 32, "double to double");
                            emit_Cast_f_d(); /* double -> float */
                            return;
                    }
                }
                break;

            case Numeric::N_Decimal:
                switch (n_to->kind) {
                    case Numeric::N_Int: /* decimal -> int */
                        add_and_emit_load(powi(10L, n_from->scale));
                        emit_Div_i();
                        return;

                    case Numeric::N_Decimal: { /* decimal -> decimal */
                        if (n_to->scale != n_from->scale) {
                            if (n_to->scale > n_from->scale) {
                                add_and_emit_load(powi(10L, n_to->scale - n_from->scale));
                                emit_Mul_i();
                            } else {
                                add_and_emit_load(powi(10L, n_from->scale - n_to->scale));
                                emit_Div_i();
                            }
                        }
                        return;
                    }

                    case Numeric::N_Float:
                        if (n_to->size() == 32) {   /* decimal -> float */
                            emit_Cast_f_i();
                            add_and_emit_load(float(powi(10L, n_from->scale)));
                            emit_Div_f();
                        } else {                    /* decimal -> double */
                            emit_Cast_d_i();
                            add_and_emit_load(double(powi(10L, n_from->scale)));
                            emit_Div_d();
                        }
                        return;
                }
                break;
        }
    } else if (auto cs_from = cast<const CharacterSequence>(from_ty)) {
        M_insist(to_ty->is_character_sequence()); // XXX any checks necessary?
        return; // nothing to be done
    } else if (auto b_from = cast<const Boolean>(from_ty)) {
        auto n_to = as<const Numeric>(to);

        M_insist(to_ty->is_numeric());
        switch (n_to->kind) {
            case Numeric::N_Int: /* bool -> int */
                emit_Cast_i_b();
                return;

            case Numeric::N_Float:
            case Numeric::N_Decimal:
                M_unreachable("unsupported conversion");
        }
    }

    M_unreachable("unsupported conversion");
}

void StackMachine::operator()(Tuple **tuples) const
{
    static const void *labels[] = {
#define M_OPCODE(CODE, ...) && CODE,
#include "tables/Opcodes.tbl"
#undef M_OPCODE
    };

    const_cast<StackMachine*>(this)->emit_Stop();
    if (not values_) {
        values_ = new Value[required_stack_size()];
        null_bits_ = new bool[required_stack_size()]();
    }
    top_ = 0; // points to the top of the stack, i.e. the top-most entry
    op_ = ops.cbegin();
    auto p_mem = memory_; // pointer to free memory; used like a linear allocator

#define NEXT goto *labels[std::size_t(*op_++)]

#define PUSH(VAL, NUL) { \
    M_insist(top_ < required_stack_size(), "index out of bounds"); \
    values_[top_] = (VAL); \
    null_bits_[top_] = (NUL); \
    ++top_; \
}
#define POP() --top_
#define TOP_IS_NULL (null_bits_[top_ - 1UL])
#define TOP (values_[top_ - 1UL])

    NEXT;


/*======================================================================================================================
 * Control flow operations
 *====================================================================================================================*/

Stop_Z: {
    M_insist(top_ >= 1);
    if (TOP.as_i() == 0) goto Stop; // stop evaluation on ZERO
}
NEXT;

Stop_NZ: {
    M_insist(top_ >= 1);
    if (TOP.as_i() != 0) goto Stop; // stop evaluation on NOT ZERO
}
NEXT;

Stop_False: {
    M_insist(top_ >= 1);
    if (not TOP.as_b()) goto Stop; // stop evaluation on FALSE
}
NEXT;

Stop_True: {
    M_insist(top_ >= 1);
    if (TOP.as_b()) goto Stop; // stop evaluation on TRUE
}
NEXT;


/*======================================================================================================================
 * Stack manipulation operations
 *====================================================================================================================*/

Pop:
    POP();
    NEXT;

Push_Null:
    PUSH(Value(), true);
    NEXT;

Dup:
    PUSH(TOP, TOP_IS_NULL);
    NEXT;


/*======================================================================================================================
 * Context Access Operations
 *====================================================================================================================*/

/* Load a value from the context to the top of the value_stack_. */
Ld_Ctx: {
    std::size_t idx = std::size_t(*op_++);
    M_insist(idx < context_.size(), "index out of bounds");
    PUSH(context_[idx], false);
}
NEXT;

Upd_Ctx: {
    M_insist(top_ >= 1);
    std::size_t idx = static_cast<std::size_t>(*op_++);
    M_insist(idx < context_.size(), "index out of bounds");
#ifdef M_ENABLE_SANITY_FIELDS
    M_insist(context_[idx].type == TOP.type, "update must not change the type of a context entry");
#endif
    const_cast<StackMachine*>(this)->context_[idx] = TOP;
}
NEXT;


/*======================================================================================================================
 * Tuple Access Operations
 *====================================================================================================================*/

Ld_Tup: {
    std::size_t tuple_id = std::size_t(*op_++);
    std::size_t index = std::size_t(*op_++);
    auto &t = *tuples[tuple_id];
    PUSH(t[index], t.is_null(index));
}
NEXT;

St_Tup_Null: {
    std::size_t tuple_id = std::size_t(*op_++);
    std::size_t index = std::size_t(*op_++);
    auto &t = *tuples[tuple_id];
    t.null(index);
}
NEXT;

St_Tup_b:
St_Tup_i:
St_Tup_f:
St_Tup_d: {
    std::size_t tuple_id = std::size_t(*op_++);
    std::size_t index = std::size_t(*op_++);
    auto &t = *tuples[tuple_id];
    t.set(index, TOP, TOP_IS_NULL);
}
NEXT;

St_Tup_s: {
    std::size_t tuple_id = std::size_t(*op_++);
    std::size_t index = std::size_t(*op_++);
    std::size_t length = std::size_t(*op_++);
    auto &t = *tuples[tuple_id];
    if (TOP_IS_NULL)
        t.null(index);
    else {
        t.not_null(index);
        char *dst = reinterpret_cast<char*>(t[index].as_p());
        char *src = reinterpret_cast<char*>(TOP.as_p());
        strncpy(dst, reinterpret_cast<char*>(src), length);
        dst[length] = 0; // always add terminating NUL byte, no matter whether this is a CHAR or VARCHAR
    }
}
NEXT;


/*======================================================================================================================
 * I/O Operations
 *====================================================================================================================*/

Putc: {
    std::size_t index = std::size_t(*op_++);
    unsigned char chr = (unsigned char)(*op_++);
    std::ostream &out = *reinterpret_cast<std::ostream*>(context_[index].as_p());
    out << chr;
}
NEXT;

Print_i: {
    M_insist(top_ >= 1);
    std::size_t index = std::size_t(*op_++);
    std::ostream &out = *reinterpret_cast<std::ostream*>(context_[index].as_p());
    if (TOP_IS_NULL)
        out << "NULL";
    else
        out << TOP.as_i();
}
NEXT;

Print_f: {
    M_insist(top_ >= 1);
    std::size_t index = std::size_t(*op_++);
    std::ostream &out = *reinterpret_cast<std::ostream*>(context_[index].as_p());
    if (TOP_IS_NULL) {
        out << "NULL";
    } else {
        const auto old_precision = out.precision(std::numeric_limits<float>::max_digits10 - 1);
        out << TOP.as_f();
        out.precision(old_precision);
    }
}
NEXT;

Print_d: {
    M_insist(top_ >= 1);
    std::size_t index = std::size_t(*op_++);
    std::ostream &out = *reinterpret_cast<std::ostream*>(context_[index].as_p());
    if (TOP_IS_NULL) {
        out << "NULL";
    } else {
        const auto old_precision = out.precision(std::numeric_limits<double>::max_digits10 - 1);
        out << TOP.as_d();
        out.precision(old_precision);
    }
}
NEXT;

Print_s: {
    M_insist(top_ >= 1);
    std::size_t index = std::size_t(*op_++);
    std::ostream &out = *reinterpret_cast<std::ostream*>(context_[index].as_p());
    if (TOP_IS_NULL) {
        out << "NULL";
    } else {
        const char *str = reinterpret_cast<char*>(TOP.as_p());
        out << '"' << str << '"';
    }
}
NEXT;

Print_b: {
    M_insist(top_ >= 1);
    std::size_t index = std::size_t(*op_++);
    std::ostream &out = *reinterpret_cast<std::ostream*>(context_[index].as_p());
    if (TOP_IS_NULL)
        out << "NULL";
    else
        out << (TOP.as_b() ? "TRUE" : "FALSE");
}
NEXT;

Print_date: {
    std::size_t index = std::size_t(*op_++);
    std::ostream &out = *reinterpret_cast<std::ostream*>(context_[index].as_p());
    if (TOP_IS_NULL) {
        out << "NULL";
    } else {
        const int32_t date = TOP.as_i(); // signed because year is signed
        const auto oldfill = out.fill('0');
        const auto oldfmt = out.flags();
        out << std::internal
            << std::setw(date >> 9 > 0 ? 4 : 5) << (date >> 9) << '-'
            << std::setw(2) << ((date >> 5) & 0xF) << '-'
            << std::setw(2) << (date & 0x1F);
        out.fill(oldfill);
        out.flags(oldfmt);
    }
}
NEXT;

Print_datetime: {
    std::size_t index = std::size_t(*op_++);
    std::ostream &out = *reinterpret_cast<std::ostream*>(context_[index].as_p());
    if (TOP_IS_NULL) {
        out << "NULL";
    } else {
        const time_t time = TOP.as_i();
        std::tm tm;
        gmtime_r(&time, &tm);
        out << put_tm(tm);
    }
}
NEXT;


/*======================================================================================================================
 * Storage Access Operations
 *====================================================================================================================*/

/*----- Load from memory ---------------------------------------------------------------------------------------------*/

#define LOAD(TO_TYPE, FROM_TYPE) { \
    M_insist(top_ >= 1); \
    const void *ptr = TOP.as_p(); \
    TOP = (TO_TYPE)(*reinterpret_cast<const FROM_TYPE*>(ptr)); \
} \
NEXT

Ld_i8:  LOAD(int64_t, int8_t);
Ld_i16: LOAD(int64_t, int16_t);
Ld_i32: LOAD(int64_t, int32_t);
Ld_i64: LOAD(int64_t, int64_t);
Ld_f:   LOAD(float,   float);
Ld_d:   LOAD(double,  double);

Ld_s: {
    M_insist(top_ >= 1);
    uint64_t length = uint64_t(*op_++);
    void *ptr = TOP.as_p();
    strncpy(reinterpret_cast<char*>(p_mem), reinterpret_cast<char*>(ptr), length);
    p_mem[length] = 0; // always add terminating NUL byte, no matter whether this is a CHAR or VARCHAR
    TOP = p_mem; // a pointer
    p_mem += length + 1;
}
NEXT;

Ld_b: {
    M_insist(top_ >= 1);
    uint64_t mask = uint64_t(*op_++);
    void *ptr = TOP.as_p();
    TOP = bool(*reinterpret_cast<uint8_t*>(ptr) & mask);
}
NEXT;

#undef LOAD

/*----- Store to memory ----------------------------------------------------------------------------------------------*/

#define STORE(TO_TYPE, FROM_TYPE) { \
    M_insist(top_ >= 2); \
    if (TOP_IS_NULL) { POP(); POP(); NEXT; } \
    TO_TYPE val = TOP.as<FROM_TYPE>(); \
    POP(); \
    void *ptr = TOP.as_p(); \
    *reinterpret_cast<TO_TYPE*>(ptr) = val; \
    POP(); \
} \
NEXT

St_i8:  STORE(int8_t,  int64_t);
St_i16: STORE(int16_t, int64_t);
St_i32: STORE(int32_t, int64_t);
St_i64: STORE(int64_t, int64_t);
St_f:   STORE(float,   float);
St_d:   STORE(double,  double);

St_s: {
    M_insist(top_ >= 2);
    uint64_t length = uint64_t(*op_++);
    if (TOP_IS_NULL) { POP(); POP(); NEXT; }

    char *str = reinterpret_cast<char*>(TOP.as_p());
    POP();
    char *dst = reinterpret_cast<char*>(TOP.as_p());
    POP();
    strncpy(dst, str, length);
}
NEXT;

St_b: {
    M_insist(top_ >= 2);
    uint64_t bit_offset = uint64_t(*op_++);
    if (TOP_IS_NULL) { POP(); POP(); NEXT; }

    bool val = TOP.as_b();
    POP();
    void *ptr = TOP.as_p();
    setbit(reinterpret_cast<uint8_t*>(ptr), val, bit_offset);
    POP();
}
NEXT;

#undef STORE


/*======================================================================================================================
 * Arithmetical operations
 *====================================================================================================================*/

#define UNARY(OP, TYPE) { \
    M_insist(top_ >= 1); \
    TYPE val = TOP.as<TYPE>(); \
    TOP = OP(val); \
} \
NEXT;

#define BINARY(OP, TYPE) { \
    M_insist(top_ >= 2); \
    TYPE rhs = TOP.as<TYPE>(); \
    bool is_rhs_null = TOP_IS_NULL; \
    POP(); \
    TYPE lhs = TOP.as<TYPE>(); \
    TOP = OP(lhs, rhs); \
    TOP_IS_NULL = TOP_IS_NULL or is_rhs_null; \
} \
NEXT;

/* Integral increment. */
Inc: UNARY(++, int64_t);

/* Integral decrement. */
Dec: UNARY(--, int64_t);

/* Arithmetic negation */
Minus_i: UNARY(-, int64_t);
Minus_f: UNARY(-, float);
Minus_d: UNARY(-, double);

/* Add two values. */
Add_i: BINARY(std::plus{}, int64_t);
Add_f: BINARY(std::plus{}, float);
Add_d: BINARY(std::plus{}, double);
Add_p: {
    M_insist(top_ >= 2);
    void *rhs = TOP.as_p();
    POP();
    const uint64_t lhs = TOP.as<int64_t>();
    bool is_lhs_null = TOP_IS_NULL;
    TOP = lhs + reinterpret_cast<uint8_t*>(rhs);
    TOP_IS_NULL = TOP_IS_NULL or is_lhs_null;
}
NEXT;

/* Subtract two values. */
Sub_i: BINARY(std::minus{}, int64_t);
Sub_f: BINARY(std::minus{}, float);
Sub_d: BINARY(std::minus{}, double);

/* Multiply two values. */
Mul_i: BINARY(std::multiplies{}, int64_t);
Mul_f: BINARY(std::multiplies{}, float);
Mul_d: BINARY(std::multiplies{}, double);

/* Divide two values. */
Div_i: BINARY(std::divides{}, int64_t);
Div_f: BINARY(std::divides{}, float);
Div_d: BINARY(std::divides{}, double);

/* Modulo divide two values. */
Mod_i: BINARY(std::modulus{}, int64_t);

/* Concatenate two strings. */
Cat_s: {
    M_insist(top_ >= 2);

    bool rhs_is_null = TOP_IS_NULL;
    Value rhs = TOP;
    POP();
    bool lhs_is_null = TOP_IS_NULL;
    Value lhs = TOP;

    if (rhs_is_null)
        NEXT; // nothing to be done
    if (lhs_is_null) {
        TOP = rhs;
        TOP_IS_NULL = rhs_is_null;
        NEXT;
    }

    M_insist(not rhs_is_null and not lhs_is_null);
    char *dest = reinterpret_cast<char*>(p_mem);
    TOP = dest;
    /* Append LHS. */
    for (auto src = lhs.as<char*>(); *src; ++src, ++dest)
        *dest = *src;
    /* Append RHS. */
    for (auto src = rhs.as<char*>(); *src; ++src, ++dest)
        *dest = *src;
    *dest++ = 0; // terminating NUL byte
    p_mem = reinterpret_cast<uint8_t*>(dest);
}
NEXT;


/*======================================================================================================================
 * Bitwise operations
 *====================================================================================================================*/

/* Bitwise negation */
Neg_i: UNARY(~, int64_t);

/* Bitwise And */
And_i: BINARY(std::bit_and{}, int64_t);

/* Bitwise Or */
Or_i: BINARY(std::bit_or{}, int64_t);

/* Bitwise Xor */
Xor_i: BINARY(std::bit_xor{}, int64_t);

/* Shift left - with operand on stack. */
ShL_i: {
    M_insist(top_ >= 2);
    uint64_t count = TOP.as<int64_t>();
    POP();
    uint64_t val = TOP.as<int64_t>();
    TOP = uint64_t(val << count);
}
NEXT;

/* Shift left immediate - with operand as argument. */
ShLi_i: {
    M_insist(top_ >= 1);
    std::size_t count = std::size_t(*op_++);
    uint64_t val = TOP.as<int64_t>();
    TOP = uint64_t(val << count);
}
NEXT;

/* Shift arithmetical right - with operand as argument. */
SARi_i: {
    M_insist(top_ >= 1);
    std::size_t count = std::size_t(*op_++);
    int64_t val = TOP.as<int64_t>(); // signed integer for arithmetical shift
    TOP = int64_t(val >> count);
}
NEXT;


/*======================================================================================================================
 * Logical operations
 *====================================================================================================================*/

/* Logical not */
Not_b: UNARY(not, bool);

/* Logical and with three-valued logic (https://en.wikipedia.org/wiki/Three-valued_logic#Kleene_and_Priest_logics). */
And_b: {
    M_insist(top_ >= 2);
    bool rhs = TOP.as<bool>();
    bool is_rhs_null = TOP_IS_NULL;
    POP();
    bool lhs = TOP.as<bool>();
    bool is_lhs_null = TOP_IS_NULL;
    TOP = lhs and rhs;
    TOP_IS_NULL = (lhs or is_lhs_null) and (rhs or is_rhs_null) and (is_lhs_null or is_rhs_null);
}
NEXT;

/* Logical or with three-valued logic (https://en.wikipedia.org/wiki/Three-valued_logic#Kleene_and_Priest_logics). */
Or_b: {
    M_insist(top_ >= 2);
    bool rhs = TOP.as<bool>();
    bool is_rhs_null = TOP_IS_NULL;
    POP();
    bool lhs = TOP.as<bool>();
    bool is_lhs_null = TOP_IS_NULL;
    TOP = lhs or rhs;
    TOP_IS_NULL = (not lhs or is_lhs_null) and (not rhs or is_rhs_null) and (is_lhs_null or is_rhs_null);
}
NEXT;


/*======================================================================================================================
 * Comparison operations
 *====================================================================================================================*/

EqZ_i: {
    M_insist(top_ >= 1);
    uint64_t val = TOP.as<int64_t>();
    TOP = val == 0;
}
NEXT;

NEZ_i: {
    M_insist(top_ >= 1);
    uint64_t val = TOP.as<int64_t>();
    TOP = val != 0;
}
NEXT;

Eq_i: BINARY(std::equal_to{}, int64_t);
Eq_f: BINARY(std::equal_to{}, float);
Eq_d: BINARY(std::equal_to{}, double);
Eq_b: BINARY(std::equal_to{}, bool);
Eq_s: BINARY(streq, char*);

NE_i: BINARY(std::not_equal_to{}, int64_t);
NE_f: BINARY(std::not_equal_to{}, float);
NE_d: BINARY(std::not_equal_to{}, double);
NE_b: BINARY(std::not_equal_to{}, bool);
NE_s: BINARY(not streq, char*);

LT_i: BINARY(std::less{}, int64_t);
LT_f: BINARY(std::less{}, float);
LT_d: BINARY(std::less{}, double);
LT_s: BINARY(0 > strcmp, char*)

GT_i: BINARY(std::greater{}, int64_t);
GT_f: BINARY(std::greater{}, float);
GT_d: BINARY(std::greater{}, double);
GT_s: BINARY(0 < strcmp, char*);

LE_i: BINARY(std::less_equal{}, int64_t);
LE_f: BINARY(std::less_equal{}, float);
LE_d: BINARY(std::less_equal{}, double);
LE_s: BINARY(0 >= strcmp, char*);

GE_i: BINARY(std::greater_equal{}, int64_t);
GE_f: BINARY(std::greater_equal{}, float);
GE_d: BINARY(std::greater_equal{}, double);
GE_s: BINARY(0 <= strcmp, char*);

#define CMP(TYPE) { \
    M_insist(top_ >= 2); \
    TYPE rhs = TOP.as<TYPE>(); \
    bool is_rhs_null = TOP_IS_NULL; \
    POP(); \
    TYPE lhs = TOP.as<TYPE>(); \
    bool is_lhs_null = TOP_IS_NULL; \
    TOP = int64_t(lhs >= rhs) - int64_t(lhs <= rhs); \
    TOP_IS_NULL = is_lhs_null or is_rhs_null; \
} \
NEXT;

Cmp_i: CMP(int64_t);
Cmp_f: CMP(float);
Cmp_d: CMP(double);
Cmp_b: CMP(bool);
Cmp_s: BINARY(strcmp, char*);

#undef CMP

Like_const: {
    M_insist(top_ >= 2);
    std::regex *re = TOP.as<std::regex*>();
    POP();
    if (not TOP_IS_NULL) {
        char *str = TOP.as<char*>();
        TOP = std::regex_match(str, *re);
    }
}
NEXT;

Like_expr: {
    M_insist(top_ >= 2);
    if (TOP_IS_NULL) {
        POP();
        TOP_IS_NULL = true;
    } else {
        char *pattern = TOP.as<char*>();
        POP();
        if (not TOP_IS_NULL) {
            char *str = TOP.as<char*>();
            TOP = like(str, pattern);
        }
    }
}
NEXT;


/*======================================================================================================================
 * Selection operation
 *====================================================================================================================*/

Sel: {
    M_insist(top_ >= 3);
    bool cond_is_null = null_bits_[top_ - 3UL];

    if (cond_is_null) {
        values_[top_ - 3UL] = values_[top_ - 2UL]; // pick any value
        null_bits_[top_ - 3UL] = true;
        POP();
        POP();
        NEXT;
    }

    bool cond = values_[top_ - 3UL].as_b();
    if (cond) {
        values_[top_ - 3UL] = values_[top_ - 2UL];
        null_bits_[top_ - 3UL] = null_bits_[top_ - 2UL];
    } else {
        values_[top_ - 3UL] = TOP;
        null_bits_[top_ - 3UL] = TOP_IS_NULL;
    }
    POP();
    POP();
}
NEXT;


/*======================================================================================================================
 * Intrinsic functions
 *====================================================================================================================*/

Is_Null:
    TOP = bool(TOP_IS_NULL);
    TOP_IS_NULL = false;
    NEXT;

/* Cast to int. */
Cast_i_f: UNARY((int64_t), float);
Cast_i_d: UNARY((int64_t), double);
Cast_i_b: UNARY((int64_t), bool);

/* Cast to float. */
Cast_f_i: UNARY((float), int64_t);
Cast_f_d: UNARY((float), double);

/* Cast to double. */
Cast_d_i: UNARY((double), int64_t);
Cast_d_f: UNARY((double), float);

#undef BINARY
#undef UNARY

Stop:
    const_cast<StackMachine*>(this)->ops.pop_back(); // terminating Stop

    op_ = ops.cbegin();
    top_ = 0;
}

M_LCOV_EXCL_START
void StackMachine::dump(std::ostream &out) const
{
    out << "StackMachine\n    Context: [";
    for (auto it = context_.cbegin(); it != context_.cend(); ++it) {
        if (it != context_.cbegin()) out << ", ";
        out << *it;
    }
    out << ']'
        << "\n    Input Schema:  " << in_schema
        << "\n    Output Schema: {[";
    for (auto it = out_schema.begin(), end = out_schema.end(); it != end; ++it) {
        if (it != out_schema.begin()) out << ',';
        out << ' ' << **it;
    }
    out << " ]}"
        << "\n    Opcode Sequence:\n";
    const std::size_t current_op = op_ - ops.begin();
    for (std::size_t i = 0; i != ops.size(); ++i) {
        auto opc = ops[i];
        if (i == current_op)
            out << "    --> ";
        else
            out << "        ";
        out << "[0x" << std::hex << std::setfill('0') << std::setw(4) << i << std::dec << "]: "
            << StackMachine::OPCODE_TO_STR[static_cast<std::size_t>(opc)];
        switch (opc) {
            /* Opcodes with *three* operands. */
            case Opcode::St_Tup_s:
                ++i;
                out << ' ' << static_cast<int64_t>(ops[i]);
                /* fall through */

            /* Opcodes with *two* operands. */
            case Opcode::Ld_Tup:
            case Opcode::St_Tup_Null:
            case Opcode::St_Tup_i:
            case Opcode::St_Tup_f:
            case Opcode::St_Tup_d:
            case Opcode::St_Tup_b:
            case Opcode::Putc:
                ++i;
                out << ' ' << static_cast<int64_t>(ops[i]);
                /* fall through */

            /* Opcodes with *one* operand. */
            case Opcode::Ld_Ctx:
            case Opcode::Upd_Ctx:
            case Opcode::Print_i:
            case Opcode::Print_f:
            case Opcode::Print_d:
            case Opcode::Print_s:
            case Opcode::Print_b:
            case Opcode::Ld_s:
            case Opcode::Ld_b:
            case Opcode::St_s:
            case Opcode::St_b:
                ++i;
                out << ' ' << static_cast<int64_t>(ops[i]);
                /* fall through */

            default:;
        }
        out << '\n';
    }
    out << "    Stack:\n";
    for (std::size_t i = top_; i --> 0; ) {
        if (null_bits_[i])
            out << "      NULL\n";
        else
            out << "      " << values_[i] << '\n';
    }
    out.flush();
}

void StackMachine::dump() const { dump(std::cerr); }
M_LCOV_EXCL_STOP

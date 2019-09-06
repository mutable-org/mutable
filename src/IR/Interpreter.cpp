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

template<typename To>
To to(const value_type &value)
{
    return std::visit(
        [](auto value) -> To {
            if constexpr (std::is_convertible_v<decltype(value), To>) {
                return value;
            } else {
                unreachable("value cannot be converted to target type");
            }
        }, value);
}

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
 * Expression Evaluator
 *====================================================================================================================*/

void ExpressionEvaluator::operator()(Const<Designator> &e)
{
    /* Given the designator, identify the position of its value in the tuple.  */
    auto target = e.target();
    if (auto p = std::get_if<const Expr*>(&target)) {
        /* This designator references another expression.  Evaluate it. */
        auto expr = *p;
        (*this)(*expr);
    } else if (auto p = std::get_if<const Attribute*>(&target)) {
        insist(e.table_name.text, "must have been set by sema");
        /* Lookup tuple element type by attribute identifier. */
        auto idx = schema_[{e.table_name.text, e.attr_name.text}].first;
        insist(idx < tuple_.size(), "index out of bounds");
        result_ = tuple_[idx];
    } else {
        unreachable("designator has no target");
    }
}

void ExpressionEvaluator::operator()(Const<Constant> &e)
{
    errno = 0;
    switch (e.tok.type) {
        default: unreachable("illegal token");

        /* Integer */
        case TK_OCT_INT:
            result_ = int64_t(strtoll(e.tok.text, nullptr, 8));
            break;

        case TK_DEC_INT:
            result_ = int64_t(strtoll(e.tok.text, nullptr, 10));
            break;

        case TK_HEX_INT:
            result_ = int64_t(strtoll(e.tok.text, nullptr, 16));
            break;

        /* Float */
        case TK_DEC_FLOAT:
            result_ = strtod(e.tok.text, nullptr);
            break;

        case TK_HEX_FLOAT:
            unreachable("not implemented");

        /* String */
        case TK_STRING_LITERAL:
            result_ = std::string(e.tok.text, 1, strlen(e.tok.text) - 2); // strip the surrounding quotes XXX: do we have to "un-escape" the string?
            break;

        /* Boolean */
        case TK_True:
            result_ = true;
            break;

        case TK_False:
            result_ = false;
            break;
    }
    if (errno)
        unreachable("constant could not be parsed");
}

void ExpressionEvaluator::operator()(Const<FnApplicationExpr> &e)
{
    auto &C = Catalog::Get();
    auto &fn = e.get_function();

    switch (fn.fnid) {
        default:
            unreachable("function kind not implemented");

        case Function::FN_UDF:
            unreachable("UDFs not yet supported");

        case Function::FN_ISNULL:
            insist(e.args.size() == 1);
            (*this)(*e.args[0]);
            result_ = std::holds_alternative<null_type>(result_);
            break;

        /*----- Type casts -------------------------------------------------------------------------------------------*/
        case Function::FN_INT: {
            insist(e.args.size() == 1);
            (*this)(*e.args[0]);

            if (std::holds_alternative<null_type>(result_))
                break; // nothing to do for NULL

            auto ty = as<const Numeric>(e.args[0]->type());
            switch (ty->kind) {
                case Numeric::N_Int:
                    break; // nothing to do

                case Numeric::N_Decimal: {
                    auto v = std::get<int64_t>(result_);
                    result_ = v / pow(10, ty->scale);
                    break;
                }

                case Numeric::N_Float: {
                    if (ty->precision == 32)
                        result_ = int64_t(std::get<float>(result_));
                    else
                        result_ = int64_t(std::get<double>(result_));
                    break;
                }
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
            insist(idx < tuple_.size(), "index out of bounds");
            result_ = tuple_[idx];
            break;
        }
    }
}

void ExpressionEvaluator::operator()(Const<UnaryExpr> &e)
{
    (*this)(*e.expr);
    auto res = result_;

    switch (e.op.type) {
        default: unreachable("illegal operator");

        case TK_PLUS:
            result_ = +result_;
            break;

        case TK_MINUS:
            result_ = -result_;
            break;

        case TK_TILDE:
            result_ = ~result_;
            break;

        case TK_Not:
            result_ = not result_;
            break;
    }
}

void ExpressionEvaluator::operator()(Const<BinaryExpr> &e)
{
    (*this)(*e.lhs);
    auto res_lhs = result_;
    (*this)(*e.rhs);
    auto res_rhs = result_;

    const Type *ty_lhs = e.lhs->type();
    const Type *ty_rhs = e.rhs->type();

    /* If either side is NULL, the result is NULL. */
    if (std::holds_alternative<null_type>(res_lhs)) {
        result_ = res_lhs;
        return;
    }
    if (std::holds_alternative<null_type>(res_rhs)) {
        result_ = res_rhs;
        return;
    }

#define EVAL_ARITHMETIC(OP) \
    if (e.type()->is_floating_point()) { \
        double v_lhs = to<double>(res_lhs); \
        double v_rhs = to<double>(res_rhs); \
        if (ty_lhs->is_decimal()) \
            v_lhs /= pow(10, as<const Numeric>(ty_lhs)->scale); /* scale decimal */ \
        if (ty_rhs->is_decimal()) \
            v_rhs /= pow(10, as<const Numeric>(ty_rhs)->scale); /* scale decimal */ \
        result_ = v_lhs OP v_rhs; \
    } else if (auto n = as<const Numeric>(e.type()); n->kind == Numeric::N_Decimal) { \
        int64_t v_lhs = to<int64_t>(res_lhs); \
        int64_t v_rhs = to<int64_t>(res_rhs); \
        int scale_lhs = 0; \
        int scale_rhs = 0; \
        if (ty_lhs->is_decimal()) \
            scale_lhs = as<const Numeric>(ty_lhs)->scale; \
        if (ty_rhs->is_decimal()) \
            scale_rhs = as<const Numeric>(ty_rhs)->scale; \
        if ((1 OP 1) == 1) { \
            /* multiplicative operation */ \
            result_ = (v_lhs OP v_rhs) / pow<int64_t>(10, scale_lhs + scale_rhs - n->scale); \
        } else { \
            /* additive operation */ \
            /* Scale values to the scale of the result. */ \
            v_lhs *= pow<int64_t>(10, n->scale - scale_lhs); \
            v_rhs *= pow<int64_t>(10, n->scale - scale_rhs); \
            result_ = v_lhs OP v_rhs; \
        } \
    } else { \
        int64_t v_lhs = to<int64_t>(res_lhs); \
        int64_t v_rhs = to<int64_t>(res_rhs); \
        result_ = v_lhs OP v_rhs; \
    }

#define EVAL_COMPARISON(OP) \
    if (ty_lhs->is_character_sequence()) { \
        std::string v_lhs = to<std::string>(res_lhs); \
        std::string v_rhs = to<std::string>(res_rhs); \
        result_ = bool(v_lhs OP v_rhs); \
    } else if (ty_lhs->is_floating_point()) { \
        double v_lhs = to<double>(res_lhs); \
        double v_rhs = to<double>(res_rhs); \
        result_ = bool(v_lhs OP v_rhs); \
    } else { \
        int64_t v_lhs = to<int64_t>(res_lhs); \
        int64_t v_rhs = to<int64_t>(res_rhs); \
        result_ = bool(v_lhs OP v_rhs); \
    }

    switch (e.op.type) {
        default: unreachable("illegal operator");

        /*----- Arithmetic operators ---------------------------------------------------------------------------------*/
        case TK_PLUS:           EVAL_ARITHMETIC(+);  break;
        case TK_MINUS:          EVAL_ARITHMETIC(-);  break;
        case TK_ASTERISK:       EVAL_ARITHMETIC(*);  break;
        case TK_SLASH:          EVAL_ARITHMETIC(/);  break;
        case TK_PERCENT: {
            int64_t v_lhs = to<int64_t>(res_lhs);
            int64_t v_rhs = to<int64_t>(res_rhs);
            result_ = v_lhs % v_rhs;
            break;
        }

        /*----- Comparison operators ---------------------------------------------------------------------------------*/
        case TK_LESS:           EVAL_COMPARISON(<);  break;
        case TK_GREATER:        EVAL_COMPARISON(>);  break;
        case TK_LESS_EQUAL:     EVAL_COMPARISON(<=); break;
        case TK_GREATER_EQUAL:  EVAL_COMPARISON(>=); break;
        case TK_EQUAL:          EVAL_COMPARISON(==);  break;
        case TK_BANG_EQUAL:     EVAL_COMPARISON(!=); break;

        /*----- Logical operators ------------------------------------------------------------------------------------*/
        case TK_And: {
            bool v_lhs = std::get<bool>(res_lhs);
            bool v_rhs = std::get<bool>(res_rhs);
            result_ = v_lhs and v_rhs;
            break;
        }
        case TK_Or: {
            bool v_lhs = std::get<bool>(res_lhs);
            bool v_rhs = std::get<bool>(res_rhs);
            result_ = v_lhs or v_rhs;
            break;
        }
    }

#undef EVAL_ARITHMETIC
#undef EVAL_COMPARISON
}

/*======================================================================================================================
 * Helper method to evaluate a CNF.
 *====================================================================================================================*/

bool db::eval(const OperatorSchema &schema, const cnf::CNF &cnf, const tuple_type &tuple)
{
    ExpressionEvaluator eval(schema, tuple);
    for (auto &clause : cnf) {
        for (auto pred : clause) {
            eval(*pred.expr());
            auto result = eval.result();
            if (std::holds_alternative<null_type>(result))
                break; // skip NULL; NULL is never TRUE
            insist(std::holds_alternative<bool>(result), "literal must evaluate to bool");
            bool value = std::get<bool>(result);
            bool satisfied = value != pred.negative();
            if (satisfied)
                goto clause_is_sat;
        }
        /* Clause is unsatisfied and hence the CNF is unsatisfied. */
        return false;

clause_is_sat:
        /* proceed with next clause in CNF */ ;
    }
    return true;
}

/*======================================================================================================================
 * Declaration of operator data.
 *====================================================================================================================*/

struct NestedLoopsJoinData : OperatorData
{
    using buffer_type = std::vector<tuple_type>;

    buffer_type *buffers;
    std::size_t active_child;

    NestedLoopsJoinData(std::size_t num_children)
        : buffers(new buffer_type[num_children - 1])
    { }

    ~NestedLoopsJoinData() { delete[] buffers; }
};

struct LimitData : OperatorData
{
    std::size_t num_tuples = 0;
};

struct HashBasedGroupingData : OperatorData
{
    std::unordered_map<tuple_type, tuple_type> groups;
};

/*======================================================================================================================
 * Recursive descent
 *====================================================================================================================*/

void Interpreter::operator()(const CallbackOperator &op)
{
    op.child(0)->accept(*this);
}

void Interpreter::operator()(const ScanOperator &op)
{
    tuple_type tuple;
    tuple.resize(op.store().table().size());
    op.store().for_each([&](const Store::Row &row) {
        tuple.clear();
        row.dispatch([&](const Attribute &, value_type value) {
            tuple.push_back(value);
        });
        op.parent()->accept(*this, tuple);
    });
}

void Interpreter::operator()(const FilterOperator &op)
{
    op.child(0)->accept(*this);
}

void Interpreter::operator()(const JoinOperator &op)
{
    switch (op.algo()) {
        default:
            unreachable("Undefined join algorithm.");

        case JoinOperator::J_Undefined:
        case JoinOperator::J_NestedLoops: {
            auto data = new NestedLoopsJoinData(op.children().size());
            op.data(data);
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
    op.child(0)->accept(*this);
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
    switch (op.algo()) {
        case GroupingOperator::G_Undefined:
        case GroupingOperator::G_Ordered:
            unreachable("not implemented");

        case GroupingOperator::G_Hashing: {
            auto data = new HashBasedGroupingData();
            op.data(data);
            op.child(0)->accept(*this);
            for (auto g : data->groups) {
                auto t = g.first + g.second;
                op.parent()->accept(*this, t); // pass groups on to parent
            }
            break;
        }
    }
}

/*======================================================================================================================
 * Recursive ascent
 *====================================================================================================================*/

void Interpreter::operator()(const CallbackOperator &op, tuple_type &t)
{
    op.callback()(op.schema(), t);
}

void Interpreter::operator()(const FilterOperator &op, tuple_type &t)
{
    if (eval(op.schema(), op.filter(), t))
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
                        if (eval(op.schema(), op.predicate(), joined))
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
    tuple_type result;
    result.reserve(op.projections().size());
    ExpressionEvaluator eval(op.child(0)->schema(), t);

    for (auto &P : op.projections()) {
        eval(*P.first);
        result.push_back(eval.result());
    }

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
            tuple_type key;
            ExpressionEvaluator eval(op.child(0)->schema(), t);
            for (auto expr : op.group_by()) {
                eval(*expr);
                key.push_back(eval.result());
            }
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
    ExpressionEvaluator eval(op.child(0)->schema(), t);
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
                    eval(*fe->args[0]);
                    if (not std::holds_alternative<null_type>(eval.result()))
                        agg = std::get<int64_t>(agg) + 1;
                }
                break;

            case Function::FN_SUM: {
                if (std::holds_alternative<null_type>(agg))
                    agg = int64_t(0); // initialize
                auto arg = fe->args[0];
                eval(*arg);
                if (std::holds_alternative<null_type>(eval.result()))
                    continue; // skip NULL
                auto n = as<const Numeric>(ty);
                if (n->kind == Numeric::N_Float) {
                    agg = to<double>(agg) + to<double>(eval.result());
                } else {
                    agg = to<int64_t>(agg) + to<int64_t>(eval.result());
                }
                break;
            }

            case Function::FN_MIN: {
                using std::min;
                auto arg = fe->args[0];
                eval(*arg);
                if (std::holds_alternative<null_type>(eval.result()))
                    continue; // skip NULL

                auto n = as<const Numeric>(ty);
                if (n->kind == Numeric::N_Float and n->precision == 32) {
                    if (std::holds_alternative<null_type>(agg))
                        agg = to<float>(eval.result());
                    else
                        agg = min(to<float>(agg), to<float>(eval.result()));
                } else if (n->kind == Numeric::N_Float and n->precision == 64) {
                    if (std::holds_alternative<null_type>(agg))
                        agg = to<double>(eval.result());
                    else
                        agg = min(to<double>(agg), to<double>(eval.result()));
                } else {
                    if (std::holds_alternative<null_type>(agg))
                        agg = to<int64_t>(eval.result());
                    else
                        agg = min(to<int64_t>(agg), to<int64_t>(eval.result()));
                }
                break;
            }

            case Function::FN_MAX: {
                using std::max;
                auto arg = fe->args[0];
                eval(*arg);
                if (std::holds_alternative<null_type>(eval.result()))
                    continue; // skip NULL

                auto n = as<const Numeric>(ty);
                if (n->kind == Numeric::N_Float and n->precision == 32) {
                    if (std::holds_alternative<null_type>(agg))
                        agg = to<float>(eval.result());
                    else
                        agg = max(to<float>(agg), to<float>(eval.result()));
                } else if (n->kind == Numeric::N_Float and n->precision == 64) {
                    if (std::holds_alternative<null_type>(agg))
                        agg = to<double>(eval.result());
                    else
                        agg = max(to<double>(agg), to<double>(eval.result()));
                } else {
                    if (std::holds_alternative<null_type>(agg))
                        agg = to<int64_t>(eval.result());
                    else
                        agg = max(to<int64_t>(agg), to<int64_t>(eval.result()));
                }
                break;
            }
        }
    }
}

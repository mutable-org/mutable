#pragma once

#include "backend/Backend.hpp"
#include "catalog/Schema.hpp"
#include "IR/Operator.hpp"
#include "IR/OperatorVisitor.hpp"
#include "util/macro.hpp"
#include <unordered_map>


namespace db {

/** Implements push-based evaluation of a pipeline in the plan. */
struct Pipeline : ConstOperatorVisitor
{
    private:
    tuple_type tuple_;

    public:
    Pipeline(std::size_t size) { tuple_.reserve(size); }
    Pipeline(tuple_type &&t) : tuple_(std::move(t)) { }

    static void Push(const Operator &pipeline_start, std::size_t size) {
        Pipeline P(size);
        P(pipeline_start);
    }

    static void Push(const Operator &pipeline_start, tuple_type &&t) {
        Pipeline P(std::move(t));
        P(pipeline_start);
    }

    void reserve(std::size_t tuple_size) {
        tuple_.reserve(tuple_size);
    }

    void push(const Operator &pipeline_start) { (*this)(pipeline_start); }

    using ConstOperatorVisitor::operator();
#define DECLARE(CLASS) void operator()(Const<CLASS> &op) override
    DECLARE(ScanOperator);
    DECLARE(CallbackOperator);
    DECLARE(FilterOperator);
    DECLARE(JoinOperator);
    DECLARE(ProjectionOperator);
    DECLARE(LimitOperator);
    DECLARE(GroupingOperator);
    DECLARE(SortingOperator);
#undef DECLARE
};

/** Evaluates SQL operator trees on the database. */
struct Interpreter : Backend, ConstOperatorVisitor
{
    public:
    Interpreter() = default;

    void execute(const Operator &plan) const override { (*const_cast<Interpreter*>(this))(plan); }

    using ConstOperatorVisitor::operator();
#define DECLARE(CLASS) void operator()(Const<CLASS> &op) override
    DECLARE(ScanOperator);
    DECLARE(CallbackOperator);
    DECLARE(FilterOperator);
    DECLARE(JoinOperator);
    DECLARE(ProjectionOperator);
    DECLARE(LimitOperator);
    DECLARE(GroupingOperator);
    DECLARE(SortingOperator);
#undef DECLARE

    static value_type eval(const Constant &c)
    {
        errno = 0;
        switch (c.tok.type) {
            default: unreachable("illegal token");

            /* Null */
            case TK_Null:
                return null_type();

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
        insist(errno == 0, "constant could not be parsed");
    }
};

}

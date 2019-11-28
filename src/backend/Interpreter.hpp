#pragma once

#include "backend/Backend.hpp"
#include "catalog/Schema.hpp"
#include "IR/Operator.hpp"
#include "IR/OperatorVisitor.hpp"
#include "util/macro.hpp"
#include <unordered_map>


namespace db {

bool eval(const OperatorSchema &schema, const cnf::CNF &cnf, const tuple_type &tuple);

/** Evaluates SQL operator trees on the database. */
struct Interpreter : Backend, ConstOperatorVisitor
{
    public:
    Interpreter() = default;

    void execute(const Operator &plan) const override { (*const_cast<Interpreter*>(this))(plan); }

    using ConstOperatorVisitor::operator();

#define DECLARE(CLASS) \
    void operator()(Const<CLASS> &op) override
#define DECLARE_CONSUMER(CLASS) \
    DECLARE(CLASS); \
    void operator()(Const<CLASS> &op, tuple_type &t) override

    DECLARE(ScanOperator);

    DECLARE_CONSUMER(CallbackOperator);
    DECLARE_CONSUMER(FilterOperator);
    DECLARE_CONSUMER(JoinOperator);
    DECLARE_CONSUMER(ProjectionOperator);
    DECLARE_CONSUMER(LimitOperator);
    DECLARE_CONSUMER(GroupingOperator);
    DECLARE_CONSUMER(SortingOperator);

#undef DECLARE_CONSUMER
#undef DECLARE

    static value_type eval(const Constant &c)
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
        insist(errno == 0, "constant could not be parsed");
    }
};

}

#pragma once

#include <mutable/IR/PhysicalOptimizer.hpp>


namespace m {

/** This macro declares a simple 1:1 mapping between logical and physical operator for the `Interpreter`. */
#define DECLARE(CLASS) \
/* forward declarations */ \
namespace interpreter { struct CLASS; } \
template<> struct Match<interpreter::CLASS>; \
\
namespace interpreter { \
\
struct CLASS : PhysicalOperator<CLASS, m::CLASS> \
{ \
    static void execute(const Match<CLASS>&, setup_t, pipeline_t, teardown_t) { M_unreachable("not implemented"); } \
    static double cost(const Match<CLASS>&) { return 1.0; } \
    static ConditionSet \
    adapt_post_conditions(const Match<CLASS>&, std::vector<std::reference_wrapper<const ConditionSet>>&&) { \
        return ConditionSet(); /* has to be overwritten for operators with multiple children, i.e. `JoinOperator` */ \
    } \
}; \
\
} \
\
template<> \
struct Match<interpreter::CLASS> : MatchBase \
{ \
    const CLASS &op; \
    std::vector<std::reference_wrapper<const MatchBase>> children; \
\
    Match(const CLASS *op, std::vector<std::reference_wrapper<const MatchBase>> &&children) \
        : op(*op) \
        , children(std::move(children)) \
    { } \
\
    void execute(setup_t, pipeline_t, teardown_t) const override { \
        M_unreachable("must not be called since `Interpreter` uses former visitor pattern on logical operators for " \
                      "execution"); \
    } \
\
    const Operator & get_matched_singleton() const override { return op; } \
\
    protected: \
    void print(std::ostream &out, unsigned level) const override { \
        indent(out, level) << "interpreter::" << #CLASS << " (cumulative cost " << cost() << ')'; \
        for (auto &child : children) \
            child.get().print(out, level + 1); \
    } \
};
M_OPERATOR_LIST(DECLARE)
#undef DECLARE

void register_interpreter_operators(PhysicalOptimizer &phys_opt);

}

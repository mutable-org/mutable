#pragma once

#include <mutable/IR/PhysicalOptimizer.hpp>


namespace m {

namespace interpreter {

// forward declarations
struct MatchBaseVisitor;
struct ConstMatchBaseVisitor;

/** An abstract `MatchBase` for the `Interpreter` backend.  Adds accept methods for respective visitor.  */
struct MatchBase : m::MatchBase
{
    virtual void accept(MatchBaseVisitor &v) = 0;
    virtual void accept(ConstMatchBaseVisitor &v) const = 0;
};

}

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
struct Match<interpreter::CLASS> : interpreter::MatchBase \
{ \
    const CLASS &op; \
    std::vector<unsharable_shared_ptr<const interpreter::MatchBase>> children; \
\
    Match(const CLASS *op, std::vector<unsharable_shared_ptr<const m::MatchBase>> &&children) \
        : op(*op) \
        , children([&children](){ \
            std::vector<unsharable_shared_ptr<const interpreter::MatchBase>> res; \
            for (auto &c : children) \
                res.push_back(as<const interpreter::MatchBase>(std::move(c))); \
            return res; \
        }()) \
    { } \
\
    void execute(setup_t, pipeline_t, teardown_t) const override { \
        M_unreachable("must not be called since `Interpreter` uses former visitor pattern on logical operators for " \
                      "execution"); \
    } \
\
    const Operator & get_matched_root() const override { return op; } \
\
    void accept(interpreter::MatchBaseVisitor &v) override; \
    void accept(interpreter::ConstMatchBaseVisitor &v) const override; \
\
    protected: \
    void print(std::ostream &out, unsigned level) const override { \
        indent(out, level) << "interpreter::" << #CLASS << " (cumulative cost " << cost() << ')'; \
        for (auto &child : children) \
            child->print(out, level + 1); \
    } \
};
M_OPERATOR_LIST(DECLARE)
#undef DECLARE

void register_interpreter_operators(PhysicalOptimizer &phys_opt);

namespace interpreter {

#define MAKE_INTERPRETER_MATCH_(OP) m::Match<m::interpreter::OP>
#define M_INTERPRETER_MATCH_LIST(X) M_TRANSFORM_X_MACRO(X, M_OPERATOR_LIST, MAKE_INTERPRETER_MATCH_)

M_DECLARE_VISITOR(MatchBaseVisitor, interpreter::MatchBase, M_INTERPRETER_MATCH_LIST)
M_DECLARE_VISITOR(ConstMatchBaseVisitor, const interpreter::MatchBase, M_INTERPRETER_MATCH_LIST)

/** A generic base class for implementing recursive `interpreter::MatchBase` visitors. */
template<bool C>
struct TheRecursiveMatchBaseVisitorBase : std::conditional_t<C, ConstMatchBaseVisitor, MatchBaseVisitor>
{
    using super = std::conditional_t<C, ConstMatchBaseVisitor, MatchBaseVisitor>;
    template<typename T> using Const = typename super::template Const<T>;

    virtual ~TheRecursiveMatchBaseVisitorBase() { }

    using super::operator();
#define DECLARE(CLASS) \
    void operator()(Const<CLASS> &M) override { for (auto &c : M.children) (*this)(*c); }
M_INTERPRETER_MATCH_LIST(DECLARE)
#undef DECLARE
};

using RecursiveConstMatchBaseVisitorBase = TheRecursiveMatchBaseVisitorBase<true>;

template<bool C>
struct ThePreOrderMatchBaseVisitor : std::conditional_t<C, ConstMatchBaseVisitor, MatchBaseVisitor>
{
    using super = std::conditional_t<C, ConstMatchBaseVisitor, MatchBaseVisitor>;
    template<typename T> using Const = typename super::template Const<T>;

    virtual ~ThePreOrderMatchBaseVisitor() { }

    void operator()(Const<MatchBase>&);
};

template<bool C>
struct ThePostOrderMatchBaseVisitor : std::conditional_t<C, ConstMatchBaseVisitor, MatchBaseVisitor>
{
    using super = std::conditional_t<C, ConstMatchBaseVisitor, MatchBaseVisitor>;
    template<typename T> using Const = typename super::template Const<T>;

    virtual ~ThePostOrderMatchBaseVisitor() { }

    void operator()(Const<MatchBase>&);
};

using ConstPreOrderMatchBaseVisitor = ThePreOrderMatchBaseVisitor<true>;
using ConstPostOrderMatchBaseVisitor = ThePostOrderMatchBaseVisitor<true>;

M_MAKE_STL_VISITABLE(ConstPreOrderMatchBaseVisitor, const interpreter::MatchBase, M_INTERPRETER_MATCH_LIST)
M_MAKE_STL_VISITABLE(ConstPostOrderMatchBaseVisitor, const interpreter::MatchBase, M_INTERPRETER_MATCH_LIST)

}

}

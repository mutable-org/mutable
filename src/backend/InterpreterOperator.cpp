#include "backend/InterpreterOperator.hpp"


using namespace m;
using namespace m::interpreter;


void m::register_interpreter_operators(PhysicalOptimizer &phys_opt)
{
#define REGISTER(CLASS) phys_opt.register_operator<interpreter::CLASS>();
M_OPERATOR_LIST(REGISTER)
#undef REGISTER
}

#define ACCEPT(CLASS) \
    void CLASS::accept(MatchBaseVisitor &v)            { v(*this); } \
    void CLASS::accept(ConstMatchBaseVisitor &v) const { v(*this); }
M_INTERPRETER_MATCH_LIST(ACCEPT)
#undef ACCEPT

namespace {

template<bool C, bool PreOrder>
struct recursive_matchbase_visitor : TheRecursiveMatchBaseVisitorBase<C>
{
    using super = TheRecursiveMatchBaseVisitorBase<C>;
    template<typename T> using Const = typename super::template Const<T>;
    using callback_t = std::conditional_t<C, ConstMatchBaseVisitor, MatchBaseVisitor>;

    private:
    callback_t &callback_;

    public:
    recursive_matchbase_visitor(callback_t &callback) : callback_(callback) { }

    using super::operator();
#define DECLARE(CLASS) \
    void operator()(Const<CLASS> &M) override { \
        if constexpr (PreOrder) try { callback_(M); } catch (visit_skip_subtree) { return; } \
        super::operator()(M); \
        if constexpr (not PreOrder) callback_(M); \
    }
M_INTERPRETER_MATCH_LIST(DECLARE)
#undef DECLARE
};

}

template<bool C>
void ThePreOrderMatchBaseVisitor<C>::operator()(Const<MatchBase> &M)
{
    recursive_matchbase_visitor<C, /* PreOrder= */ true>{*this}(M);
}

template<bool C>
void ThePostOrderMatchBaseVisitor<C>::operator()(Const<MatchBase> &M)
{
    recursive_matchbase_visitor<C, /* PreOrder= */ false>{*this}(M);
}

// explicit template instantiations
template struct m::interpreter::ThePreOrderMatchBaseVisitor<true>;
template struct m::interpreter::ThePostOrderMatchBaseVisitor<true>;

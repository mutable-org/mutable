#pragma once

#include <mutable/mutable-config.hpp>
#include <mutable/util/crtp.hpp>
#include <mutable/util/fn.hpp>
#include <mutable/util/macro.hpp>
#include <mutable/util/some.hpp>
#include <mutable/util/tag.hpp>
#include <optional>
#include <type_traits>


namespace m {

/** Exception class which can be thrown to stop entire recursion in visitors. */
struct visit_stop_recursion { };
/** Exception class which can be thrown to skip recursion of the subtree in pre-order visitors. */
struct visit_skip_subtree { };

namespace detail {

/**
 * Visitor base class, using CRTP.
 *
 * \tparam ConcreteVisitor is the actual type of the visitor, \tparam Base is the base type of the class hierarchy to
 * visit.
 */
template<typename ConcreteVisitor, typename Base>
struct Visitor : crtp<ConcreteVisitor, Visitor, Base>
{
    using crtp<ConcreteVisitor, Visitor, Base>::actual;

    /** Whether the visited objects are `const`-qualified. */
    static constexpr bool is_const = std::is_const_v<Base>;

    /** The base class of the class hierarchy to visit. */
    using base_type = Base;

    /** The concrete type of the visitor.  Uses the CRTP design. */
    using visitor_type = ConcreteVisitor;

    /** A helper type to apply the proper `const`-qualification to parameters. */
    template<typename T>
    using Const = std::conditional_t<is_const, const T, T>;

    /** Make `Visitor` inheritable from. */
    virtual ~Visitor() { }

    /** Visit the object `obj`. */
    virtual void operator()(base_type &obj) { obj.accept(actual()); }
};

/** This helper class creates a single override of `operator()` for one subtype in a class hierarchy, and then
 * recursively inherits from an instantiation of that same helper class for the next subtype in the hierarchy.  This
 * enables overriding all visit methods of \tparam Visitor. */
template<typename Callable, typename ResultType, typename Visitor, typename Class, typename... Classes>
struct stl_visit_helper : stl_visit_helper<Callable, ResultType, Visitor, Classes...>
{
    using super = stl_visit_helper<Callable, ResultType, Visitor, Classes...>;

    stl_visit_helper(Callable &callable, std::optional<some<ResultType>> &result) : super(callable, result) { }

    using super::operator();
    void operator()(typename Visitor::template Const<Class> &obj) override {
        if constexpr (std::is_same_v<void, ResultType>) { this->callable(obj); }
        else { super::result = m::some<ResultType>(this->callable(obj)); }
    }
};
/** This specialization marks the end of the class hierarchy.  It inherits from \tparam Visitor to override the visit
 * methods. */
template<typename Callable, typename ResultType, typename Visitor, typename Class>
struct stl_visit_helper<Callable, ResultType, Visitor, Class> : Visitor
{
    Callable &callable;
    std::optional<some<ResultType>> &result;

    stl_visit_helper(Callable &callable, std::optional<some<ResultType>> &result) : callable(callable), result(result) { }

    using Visitor::operator();
    void operator()(typename Visitor::template Const<Class> &obj) override {
        if constexpr (std::is_same_v<void, ResultType>) { this->callable(obj); }
        else { result = m::some<ResultType>(this->callable(obj)); }
    }
};

}

/**
 * Generic implementation to visit a class hierarchy, with similar syntax as `std::visit`.  This generic implementation
 * is meant to be *instantiated* once for each class hierarchy that should be `m::visit()`-able.  The class hierarchy
 * must provide a \tparam Visitor implementing our `m::Visitor` base class.
 *
 * The unnamed third parameter is used for [*tag
 * dispatching*](https://en.wikibooks.org/wiki/More_C%2B%2B_Idioms/Tag_Dispatching) to select a particular visitor to
 * apply.  This is very useful if, for example, you have a compositor pattern implementing a tree structure of objects
 * and you want to provide pre-order and post-order visitors.  These iterators inherit from `Visitor` base and implement
 * the pre- or post-order traversal.  You can then call to `m::visit()` and provide `m::tag<MyPreOrderVisitor>` to
 * select the desired traversal order.
 *
 * For an example of how to implement and use the tag dispatch, see `Expr::get_required()` and
 * `ConstPreOrderExprVisitor`.
 */
template<typename Callable, typename Visitor, typename Base, typename... Hierarchy>
auto visit(Callable &&callable, Base &obj, m::tag<Callable>&& = m::tag<Callable>())
{
    ///> compute the result type as the common type of the return types of all visitor applications
    using result_type = std::common_type_t< std::invoke_result_t<Callable, typename Visitor::template Const<Hierarchy>&>... >;

    std::optional<some<result_type>> result;

    /*----- Create a `visit_helper` and then invoke it on the `obj`. -----*/
    detail::stl_visit_helper<Callable, result_type, Visitor, Hierarchy...> V(callable, result);
    try { V(obj); } catch (visit_stop_recursion) { }
    if constexpr (not std::is_same_v<void, result_type>) {
        return std::move(V.result->value);
    } else {
        return; // result is `void`
    }
}

}


/*----- Generate a function similar to `std::visit` to easily implement a visitor for the given base class. ----------*/
#define M_MAKE_STL_VISITABLE(VISITOR, BASE_CLASS, CLASS_LIST) \
    template<typename Callable> \
    auto visit(Callable &&callable, BASE_CLASS &obj, m::tag<VISITOR>&& = m::tag<VISITOR>()) { \
        return m::visit<Callable, VISITOR, BASE_CLASS CLASS_LIST(M_COMMA_PRE)>(std::forward<Callable>(callable), obj); \
    }

/*----- Declare a visitor to visit the class hierarchy with the given base class and list of subclasses. -------------*/
#define M_DECLARE_VISIT_METHOD(CLASS) virtual void operator()(Const<CLASS>&) { };
#define M_DECLARE_VISITOR(VISITOR_NAME, BASE_CLASS, CLASS_LIST) \
    struct M_EXPORT VISITOR_NAME : m::detail::Visitor<VISITOR_NAME, BASE_CLASS> \
    { \
        using super = m::detail::Visitor<VISITOR_NAME, BASE_CLASS>; \
        template<typename T> using Const = typename super::Const<T>; \
        virtual ~VISITOR_NAME() {} \
        void operator()(BASE_CLASS &obj) { obj.accept(*this); } \
        CLASS_LIST(M_DECLARE_VISIT_METHOD) \
    }; \
    M_MAKE_STL_VISITABLE(VISITOR_NAME, BASE_CLASS, CLASS_LIST)

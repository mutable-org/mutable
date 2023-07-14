#pragma once

#include <mutable/mutable-config.hpp>
#include <mutable/util/macro.hpp>
#include <mutable/util/some.hpp>
#include <mutable/util/tag.hpp>
#include <optional>
#include <type_traits>


namespace m {

/** Exception class which may be throw to stop entire recursion in visitors. */
struct visit_stop_recursion { };
/** Exception class which may be throw to skip recursion of the subtree in pre-order visitors. */
struct visit_skip_subtree { };

/** Visitor base class. */
template<typename V, typename Base>
struct Visitor
{
    /** Whether the visited objects are `const`-qualified. */
    static constexpr bool is_const = std::is_const_v<Base>;

    /** The base class of the class hierarchy to visit. */
    using base_type = Base;

    /** The concrete type of the visitor.  Uses the CRTP design. */
    using visitor_type = V;

    /** A helper type to apply the proper `const`-qualification to parameters. */
    template<typename T>
    using Const = std::conditional_t<is_const, const T, T>;

    /** Visit the object `obj`. */
    void operator()(base_type &obj) { static_cast<V*>(this)->operator()(obj); }
};

}

/*----- Generate a function similar to `std::visit` to easily implement a visitor for the given base class. ----------*/
#define M_GET_INVOKE_RESULT(CLASS) std::invoke_result_t<Vis, Const<CLASS>&>,
#define M_MAKE_STL_VISIT_METHOD(CLASS) void operator()(Const<CLASS> &obj) override { \
    if constexpr (std::is_same_v<void, result_type>) { vis(obj); } \
    else { result = m::some<result_type>(vis(obj)); } \
}
#define M_MAKE_STL_VISITABLE(VISITOR, BASE_CLASS, CLASS_LIST) \
    template<typename Vis> \
    auto M_EXPORT visit(Vis &&vis, BASE_CLASS &obj, m::tag<VISITOR>&& = m::tag<VISITOR>()) { \
        struct V : VISITOR { \
            using result_type = std::common_type_t< CLASS_LIST(M_GET_INVOKE_RESULT) std::invoke_result_t<Vis, Const<M_EVAL(M_DEFER1(M_HEAD)(CLASS_LIST(M_COMMA)))>&> >; \
            Vis &&vis; \
            std::optional<m::some<result_type>> result; \
            V(Vis &&vis) : vis(std::forward<Vis>(vis)), result(std::nullopt) { } \
            V(const V&) = delete; \
            V(V&&) = default; \
            using VISITOR::operator(); \
            CLASS_LIST(M_MAKE_STL_VISIT_METHOD) \
        }; \
        V v(std::forward<Vis>(vis)); \
        try { v(obj); } catch (visit_stop_recursion) { } \
        if constexpr (not std::is_same_v<void, typename V::result_type>) { \
            return std::move(v.result->value); \
        } else { \
            return; \
        } \
    }

/*----- Declare a visitor to visit the class hierarchy with the given base class and list of subclasses. -------------*/
#define M_DECLARE_VISIT_METHOD(CLASS) virtual void operator()(Const<CLASS>&) { };
#define M_DECLARE_VISITOR(NAME, BASE_CLASS, CLASS_LIST) \
    struct M_EXPORT NAME : m::Visitor<NAME, BASE_CLASS> \
    { \
        using super = m::Visitor<NAME, BASE_CLASS>; \
        template<typename T> using Const = typename super::Const<T>; \
        virtual ~NAME() {} \
        void operator()(BASE_CLASS &obj) { obj.accept(*this); } \
        CLASS_LIST(M_DECLARE_VISIT_METHOD) \
    }; \
    M_MAKE_STL_VISITABLE(NAME, BASE_CLASS, CLASS_LIST)

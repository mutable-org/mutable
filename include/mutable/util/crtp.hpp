#pragma once

#include <utility>


namespace m {

/**
 * A helper class to define CRTP class hierarchies.
 *
 * Taken from [An Implementation Helper For The Curiously Recurring Template Pattern - Fluent
 * C++](https://www.fluentcpp.com/2017/05/19/crtp-helper/).
 *
 * This CRTP helper ensures that the \tparam ConcreteType actually inherits from \tparam CRTPBaseType.  The helper
 * defines a method `actual()` that allows for safely casting the CRTP base type \tparam CRTPBaseType to the \tparam
 * ConcreteType, properly `const`-quanified.
 *
 * A class that wants to make use of CRTP should inherit from this helper.  The following example demonstrates this:
 *
 * \code{.cpp}
 * template<typename ConcreteType>
 * struct CRTPBase : crtp<ConcreteType, CRTPBase>
 * {
 *     using crtp<ConcreteType, CRTPBase>::actual;
 * };
 * \endcode
 *
 * It is also possible for the class implementing CRTP to take additional template parameters:
 *
 * \code{.cpp}
 * template<typename ConcreteType, typename T1, typename T2>
 * struct CRTPBase : crtp<ConcreteType, CRTPBase, T1, T2>
 * {
 *     using crtp<ConcreteType, CRTPBase, T1, T2>::actual;
 * };
 * \endcode
 *
 * This should also work with variadic templates:
 *
 * \code{.cpp}
 * template<typename ConcreteType, typename... Args>
 * struct CRTPBase : crtp<ConcreteType, CRTPBase, Args...>
 * {
 *     using crtp<ConcreteType, CRTPBase, Args...>::actual;
 * };
 * \endcode
 */
template<typename ConcreteType, template<typename...> typename CRTPBaseType, typename... TParams>
struct crtp
{
    using actual_type = ConcreteType;
    actual_type & actual() { return *static_cast<actual_type*>(this); }
    const actual_type & actual() const { return *static_cast<const actual_type*>(this); }

    private:
    crtp() { }                                     // no one can construct this
    friend CRTPBaseType<actual_type, TParams...>;  // except classes that properly inherit from this class
};

/** A helper class to define CRTP class hierarchies with an additional boolean template parameter (this is often used
 * for iterators taking a boolean template parameter to decide on constness). */
template<typename ConcreteType, template<typename, bool, typename...> typename CRTPBaseType, bool B,
         typename... TParams>
struct crtp_boolean
{
    using actual_type = ConcreteType;
    actual_type & actual() { return *static_cast<actual_type*>(this); }
    const actual_type & actual() const { return *static_cast<const actual_type*>(this); }

    private:
    crtp_boolean() { }                  // no one can construct this
    friend CRTPBaseType<ConcreteType, B, TParams...>;   // except classes that properly inherit from this class
};

/** A helper class to define CRTP class hierarchies with an additional boolean template template parameter (this is
 * often used for iterators taking a boolean template parameter to decide on constness). */
template<typename ConcreteType, template<typename, template<bool> typename, typename...> typename CRTPBaseType,
         template<bool> typename It, typename... TParams>
struct crtp_boolean_templated
{
    using actual_type = ConcreteType;
    actual_type & actual() { return *static_cast<actual_type*>(this); }
    const actual_type & actual() const { return *static_cast<const actual_type*>(this); }

    private:
    crtp_boolean_templated() { }                   // no one can construct this
    friend CRTPBaseType<actual_type, It, TParams...>;   // except classes that properly inherit from this class
};

/** A helper class to introduce a virtual method overload per type to a class hierarchy. */
template<typename Tag, bool Const = false> // a type tag unique to the method
struct __virtual_crtp_helper
{
    template<typename ReturnType>       // return type
    struct returns
    {
        template<typename... CRTPArgs>  // types of CRTP parameters
        struct crtp_args
        {
            template<typename... Args>  // types of remaining parameters
            struct args
            {
                /*----- Virtual base ---------------------------------------------------------------------------------*/
                template<bool C, typename T, typename... Ts>
                struct base_type_helper : virtual base_type_helper<C, T>
                                        , virtual base_type_helper<C, Ts...>
                {
                    using base_type_helper<C, T>::operator();
                    using base_type_helper<C, Ts...>::operator();
                };
                template<typename T>
                struct base_type_helper<false, T>
                {
                    virtual ReturnType operator()(Tag, T, Args...) = 0;
                };
                template<typename T>
                struct base_type_helper<true, T>
                {
                    virtual ReturnType operator()(Tag, T, Args...) const = 0;
                };

                /*----- Overriding implementation --------------------------------------------------------------------*/
                template<bool C, typename Actual, typename T, typename... Ts>
                struct derived_type_helper : derived_type_helper<C, Actual, T>
                                           , derived_type_helper<C, Actual, Ts...>
                {
                    using derived_type_helper<C, Actual, T>::operator();
                    using derived_type_helper<C, Actual, Ts...>::operator();
                };
                template<typename Actual, typename T>
                struct derived_type_helper<false, Actual, T> : virtual base_type_helper<false, T>
                {
                    ReturnType operator()(Tag, T o, Args... args) override {
                        return static_cast<Actual*>(this)->template operator()<T>(
                            Tag{}, o, std::forward<Args>(args)...
                        );
                    }
                };
                template<typename Actual, typename T>
                struct derived_type_helper<true, Actual, T> : virtual base_type_helper<true, T>
                {
                    ReturnType operator()(Tag, T o, Args... args) const override {
                        return static_cast<const Actual*>(this)->template operator()<T>(
                            Tag{}, o, std::forward<Args>(args)...
                        );
                    }
                };

                /*----- The types to inherit from --------------------------------------------------------------------*/
                using base_type = base_type_helper<Const, CRTPArgs...>;
                template<typename Actual>
                using derived_type = derived_type_helper<Const, Actual, CRTPArgs...>;
            };
        };
    };
};

template<typename T>
using virtual_crtp_helper = __virtual_crtp_helper<T, false>;

template<typename T>
using const_virtual_crtp_helper = __virtual_crtp_helper<T, true>;

}

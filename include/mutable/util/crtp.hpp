#pragma once

#include <utility>


namespace m {

/** A helper class to define CRTP class hierarchies.
 *
 * Taken from [An Implementation Helper For The Curiously Recurring Template Pattern - Fluent
 * C++](https://www.fluentcpp.com/2017/05/19/crtp-helper/). */
template<typename T, template<typename...> typename crtpType, typename... Others>
struct crtp
{
    using actual_type = T;
    actual_type & actual() { return *static_cast<actual_type*>(this); }
    const actual_type & actual() const { return *static_cast<const actual_type*>(this); }

    private:
    crtp() { }                      // no one can construct this
    friend crtpType<actual_type, Others...>;   // except classes that properly inherit from this class
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

                /*----- Enforce template instantiation ---------------------------------------------------------------*/
                template<bool C, typename Actual, typename T, typename... Ts>
                struct template_instantiation_helper : template_instantiation_helper<C, Actual, T>
                                                     , template_instantiation_helper<C, Actual, Ts...>
                { };
                template<typename Actual, typename T>
                struct template_instantiation_helper<false, Actual, T>
                {
                    // ReturnType (Actual::template *operator()<T>)(Args...) instance;
                    // virtual ~template_instantiation_helper() { }
                    // template_instantiation_helper() {
                    //     std::declval<Actual>().template operator()<T>(
                    //         Tag{}, std::declval<T>(), std::declval<Args>()...
                    //     );
                    // }
                };
                template<typename Actual, typename T>
                struct template_instantiation_helper<true, Actual, T>
                {
                    // ReturnType (const Actual::template *operator()<T>)(Args...) instance;
                    // virtual ~template_instantiation_helper() { }
                    // template_instantiation_helper() {
                    //     std::declval<const Actual>().template operator()<T>(
                    //         Tag{}, std::declval<T>(), std::declval<Args>()...
                    //     );
                    // }
                };

                /*----- The types to inherit from --------------------------------------------------------------------*/
                using base_type = base_type_helper<Const, CRTPArgs...>;
                template<typename Actual>
                using derived_type = derived_type_helper<Const, Actual, CRTPArgs...>;
                template<typename Actual>
                struct enforce_template_instances : template_instantiation_helper<Const, Actual, CRTPArgs...> { };
            };
        };
    };
};

template<typename T>
using virtual_crtp_helper = __virtual_crtp_helper<T, false>;

template<typename T>
using const_virtual_crtp_helper = __virtual_crtp_helper<T, true>;

}

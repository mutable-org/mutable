#pragma once

#include <concepts>
#include <functional>
#include <memory>
#include <type_traits>


namespace m {

/** Check whether \tparam T is boolean. */
template<typename T>
concept boolean = std::same_as<T, bool>;

/** Check whether \tparam T is a character of a character sequence.  Note, that `char` is neither `signed char` nor
 * `unsigned char`. */
template<typename T>
concept character = std::same_as<T, char>;

/** Check whether \tparam T is an integral type (excluding boolean). */
template<typename T>
concept integral = (std::integral<T> and not boolean<T>);

/** Check whether \tparam T is a *signed* integral type (excluding boolean). */
template<typename T>
concept signed_integral = integral<T> and std::is_signed_v<T>;

/** Check whether \tparam T is an *unsigned* integral type (excluding boolean). */
template<typename T>
concept unsigned_integral = integral<T> and not std::is_signed_v<T>;

/** Check whether \tparam T is an arithmetic type, i.e. integral or floating point. */
template<typename T>
concept arithmetic = integral<T> or std::floating_point<T>;

/** Check whether \tparam T is a pointer type. */
template<typename T>
concept pointer = std::is_pointer_v<T>;

/** Check whether \tparam T is a *primitive* type, i.e. arithmetic, boolean, or character. */
template<typename T>
concept primitive = arithmetic<T> or boolean<T> or character<T>;

/** Check whether \tparam T is a pointer to primitive or void type. */
template<typename T>
concept pointer_to_primitive = (pointer<T> and
                                (primitive<std::remove_pointer_t<T>> or std::is_void_v<std::remove_pointer_t<T>>));

/** Check whether \tparam T is a decayable type. */
template<typename T>
concept decayable = not std::same_as<T, std::decay_t<T>>;

/** Check whether types \tparam T and \tparam U have equal signedness, i.e. both unsigned or both signed. */
template<typename T, typename U>
concept same_signedness = (arithmetic<T> and arithmetic<U> and std::is_signed_v<T> == std::is_signed_v<U>) or
                          (boolean<T> and boolean<U>);

/** Check whether types \tparam T and \tparam U have same floating point-ness, i.e. both floating or both integral. */
template<typename T, typename U>
concept equally_floating = arithmetic<T> and arithmetic<U> and
                           std::floating_point<T> == std::floating_point<U>;


/** Computes the *common type* of \tparam T and \tparam U.
 * Note that our implementation -- in contrast to `std::common_type` -- does not provide a common type for types of
 * different signedness and also differs considerably in the common type of integral types. */
template<typename T, typename U>
struct common_type;

/** Specialization for equal types. */
template<typename T>
struct common_type<T, T>
{ using type = T; };

/** If both \tparam T and \tparam U are arithmetic types of same signedness and same floating point-ness, the common
 * type is the larger of the types. */
template<typename T, typename U>
requires arithmetic<T> and arithmetic<U> and same_signedness<T, U> and equally_floating<T, U>
struct common_type<T, U>
{
    using type = std::conditional_t<sizeof(T) >= sizeof(U), T, U>;
};

/** If both \tparam T and \tparam U are arithmetic types of same signedness and *different* floating point-ness, the
 * common type is the floating-point type. */
template<typename T, typename U>
requires arithmetic<T> and arithmetic<U> and same_signedness<T, U> and (not equally_floating<T, U>)
struct common_type<T, U>
{
    using type = std::conditional_t<std::is_floating_point_v<T>, T, U>;
};

/** Convenience alias for `common_type<T, U>::type`. */
template<typename T, typename U>
using common_type_t = typename common_type<T, U>::type;

/** Check whether \tparam T and \tparam U have a common type. */
template<typename T, typename U>
concept have_common_type = requires { typename common_type_t<T, U>; };


/** Check whether \tparam T is a unique pointer type. */
template<typename T>
concept is_unique_ptr = std::same_as<std::decay_t<T>, std::unique_ptr<typename T::element_type>>;

/** Check whether \tparam T is a reference wrapper type. */
template<typename T>
concept is_reference_wrapper = std::same_as<std::decay_t<T>, std::reference_wrapper<typename T::type>>;


template<template<typename...> class Template, typename... Args>
concept is_template_instance = requires { typename Template<Args...>; };

namespace detail {

template<typename T, template<typename...> class Template>
struct is_specialization : std::false_type {};

template<template<typename...> class Template, typename... Args>
struct is_specialization<Template<Args...>, Template> : std::true_type {};

}

template<typename T, template <typename...> class Template>
concept is_specialization = detail::is_specialization<T, Template>::value;


/** Helper struct for parameter packs.  Enables use of multiple parameter packs after another. */
template<typename... Ts>
struct param_pack_t : std::tuple<Ts...> {};

namespace detail {

/** Checks whether \tparam T is a parameter pack type. */
template<typename T>
struct is_param_pack : std::false_type {};

/** Specialization for \tparam T being `param_pack_t`. */
template<typename... Ts>
struct is_param_pack<param_pack_t<Ts...>> : std::true_type {};

/** Convenience alias for `is_param_pack<T>::value`. */
template<typename T>
constexpr bool is_param_pack_v = is_param_pack<T>::value;

/** Static cast of \tparam T to `std::tuple<Ts...>`. */
template<typename T>
struct as_tuple;

/** Specialization for \tparam T being `param_pack_t`. */
template<typename... Ts>
struct as_tuple<param_pack_t<Ts...>>
{
    using type = std::tuple<Ts...>;
};

/** Convenience alias for `as_tuple<T>::type`. */
template<typename T>
using as_tuple_t = typename as_tuple<T>::type;

}

/** Check whether \tparam T is a parameter pack type. */
template<typename T>
concept param_pack = detail::is_param_pack_v<T>;

/** Returns the size of a parameter pack type \tparam P. */
template<param_pack P>
constexpr std::size_t param_pack_size_v = std::tuple_size_v<detail::as_tuple_t<P>>;

/** Returns the \tparam I -th element of a parameter pack type \tparam P. */
template<std::size_t I, param_pack P>
using param_pack_element_t = std::tuple_element_t<I, detail::as_tuple_t<P>>;

namespace detail {

/** Helper struct to check whether parameter pack type \tparam P contains only elements of type \tparam T. */
template<param_pack P, typename T>
struct typed_param_pack_helper
{
    template<std::size_t... Is>
    requires (sizeof...(Is) == param_pack_size_v<P>) and (std::same_as<param_pack_element_t<Is, P>, T> and ...)
    typed_param_pack_helper(std::index_sequence<Is...>) { }
};

}

/** Check whether \tparam P is a parameter pack type containing only types \tparam T. */
template<typename P, typename T>
concept typed_param_pack = requires {
    detail::typed_param_pack_helper<P, T>(std::make_index_sequence<param_pack_size_v<P>>{});
};

/** Check whether \tparam P is a parameter pack type containing only types `std::size_t`. */
template<typename P>
concept size_param_pack = typed_param_pack<P, std::size_t>;

}

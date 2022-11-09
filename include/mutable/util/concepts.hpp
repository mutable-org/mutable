#pragma once

#include <concepts>
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
                           std::is_floating_point_v<T> == std::is_floating_point_v<U>;


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
requires (arithmetic<T> and arithmetic<U> and same_signedness<T, U> and not equally_floating<T, U>)
struct common_type<T, U>
{
    using type = std::conditional_t<std::is_floating_point_v<T>, T, U>;
};

/** Specialization for \tparam T and \tparam U being `bool`. */
template<>
struct common_type<bool, bool>
{ using type = bool; };


/** Convenience alias for `common_type<T, U>::type`. */
template<typename T, typename U>
using common_type_t = typename common_type<T, U>::type;


template<typename T, typename U>
concept have_common_type = requires { typename common_type_t<T, U>; };

}

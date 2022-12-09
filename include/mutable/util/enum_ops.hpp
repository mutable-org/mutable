#pragma once

#include <concepts>
#include <type_traits>


namespace m {


template<typename T>
requires std::is_enum_v<T> and
         requires (std::underlying_type_t<T> x) {
             { x | x } -> std::same_as<std::underlying_type_t<T>>;
             T(x);
         }
T operator|(T left, T right)
{
    using U = std::underlying_type_t<T>;
    return T( U(left) | U(right) );
}

template<typename T>
requires std::is_enum_v<T> and
         requires (std::underlying_type_t<T> x) {
             { x | x } -> std::same_as<std::underlying_type_t<T>>;
             T(x);
         }
T operator&(T left, T right)
{
    using U = std::underlying_type_t<T>;
    return T( U(left) & U(right) );
}

template<typename T>
requires std::is_enum_v<T> and
         requires (std::underlying_type_t<T> x) {
             { ~x } -> std::same_as<std::underlying_type_t<T>>;
             { x & x } -> std::same_as<std::underlying_type_t<T>>;
             T(x);
         }
T operator-(T left, T right)
{
    using U = std::underlying_type_t<T>;
    return T( U(left) & ~U(right) );
}

template<typename T>
requires std::is_enum_v<T> and
         requires (std::underlying_type_t<T> x) {
             { ~x } -> std::same_as<std::underlying_type_t<T>>;
             T(x);
         }
T operator~(T t)
{
    using U = std::underlying_type_t<T>;
    return T( ~U(t) );
}

template<typename T>
requires std::is_enum_v<T> and requires (T x) { { x | x } -> std::same_as<T>; }
T & operator|=(T &left, T right)
{
    return left = left | right;
}

template<typename T>
requires std::is_enum_v<T> and requires (T x) { { x & x } -> std::same_as<T>; }
T & operator&=(T &left, T right)
{
    return left = left & right;
}

template<typename T>
requires std::is_enum_v<T> and requires (T x) { { x - x } -> std::same_as<T>; }
T & operator-=(T &left, T right)
{
    return left = left - right;
}

}

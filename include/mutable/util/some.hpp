#pragma once


#include <type_traits>
#include <utility>


namespace m {

/**
 * A helper type to wrap some template type parameter such that it can safely be used to declare a class field or local
 * variable.  In particular, handles `void`.
 */
template<typename T>
struct some
{
    using value_type = std::remove_reference_t<T>;
    value_type value;

    explicit some(value_type &&value)
        requires requires (value_type &&value) { { value_type(std::move(value)) } -> std::same_as<value_type>; }
    : value(std::move(value)) { }

    some() = delete;
    some(const some&) = delete;

    some(some &&other)
        requires requires (some &&other) { { value_type(std::move(other.value)) } -> std::same_as<value_type>; }
    : value(std::move(other.value)) { }

    some & operator=(some other)
        requires requires (value_type &this_value, some other) { this_value = std::move(other.value); }
    { this->value = std::move(other.value); return *this; }

    some & operator=(value_type &&value)
        requires requires (value_type &this_value, value_type &&other_value) { this_value = std::move(other_value); }
    { this->value = std::move(value); return *this; }
};

template<>
struct some<void> { };

}

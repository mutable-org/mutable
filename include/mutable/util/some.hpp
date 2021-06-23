#pragma once


#include <type_traits>
#include <utility>


namespace m {

template<typename T>
struct some
{
    using value_type = std::remove_reference_t<T>;
    value_type value;
    explicit some(value_type &&value) : value(std::move(value)) { }
    some() = delete;
    some(const some&) = delete;
    some(some &&other) : value(std::move(other.value)) { }
    some & operator=(some other) { this->value = std::move(other.value); return *this; }
    some & operator=(value_type &&value) { this->value = std::move(value); return *this; }
};

template<>
struct some<void> { };

}

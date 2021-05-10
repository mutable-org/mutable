#pragma once


template<typename T>
struct some
{
    T &&value;
    some(T &&value) : value(std::forward<T>(value)) { }
    operator T() const { return value; }
    some & operator=(T &&value) { this->value = std::forward<T>(value); return *this; }
};

template<>
struct some<void> { };

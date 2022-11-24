#pragma once

#include <concepts>
#include <cstddef>
#include <functional>


namespace std {

/** Conveniance specialization to provide hasing of `std::vector` by computing a rolling hash over the elements. */
template<typename Container>
requires requires (Container C) { {C.cbegin() < C.cend() } -> std::same_as<bool>; } and
         requires (Container C) { {C.cbegin() == C.cend() } -> std::same_as<bool>; } and
         requires (Container C) { {C.cbegin() != C.cend() } -> std::same_as<bool>; } and
         requires (Container C) { *C.cbegin(); }
struct hash<Container>
{
    std::size_t operator()(const Container &C) const {
        using value_type = decltype(*C.cbegin());
        std::hash<std::decay_t<value_type>> h;
        std::size_t value = 0xcbf29ce484222325UL;

        for (const auto &elem : C) {
            value ^= h(elem);
            value *= 0x100000001b3UL;
        }

        return value;
    }
};

/** Convenience specialization to provide hashing of `std::reference_wrapper` by hashing the referenced object. */
template<typename T>
struct hash<std::reference_wrapper<T>>
{
    std::size_t operator()(std::reference_wrapper<T> ref) const {
        std::hash<std::decay_t<T>> h;
        return h(ref.get());
    }
};

template<typename T>
bool operator==(std::reference_wrapper<T> left, std::reference_wrapper<T> right) { return left.get() == right.get(); }

}

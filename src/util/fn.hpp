#pragma once

#include "util/assert.hpp"
#include <cmath>
#include <cstring>
#include <type_traits>


inline bool streq(const char *first, const char *second) { return 0 == strcmp(first, second); }
inline bool strneq(const char *first, const char *second, std::size_t n) { return 0 == strncmp(first, second, n); }

struct StrHash
{
    std::size_t operator()(const char *c_str) const {
        /* FNV-1a 64 bit */
        std::size_t hash = 0xcbf29ce484222325;
        char c;
        while ((c = *c_str++)) {
            hash = hash ^ c;
            hash = hash * 1099511628211;
        }
        return hash;
    }
};

struct StrEqual
{
    bool operator()(const char *first, const char *second) const { return streq(first, second); }
};

template<typename T>
inline
typename std::enable_if<std::is_integral<T>::value and std::is_unsigned<T>::value and
                        sizeof(T) <= sizeof(unsigned long long), T>::type
ceil_to_pow_2(T n)
{
    /* Count leading zeros. */
    int lz;
    if (sizeof(T) <= sizeof(unsigned)) {
        lz = __builtin_clz(n - 1U);
    } else if (sizeof(T) <= sizeof(unsigned long)) {
        lz = __builtin_clzl(n - 1UL);
    } else if (sizeof(T) <= sizeof(unsigned long long)) {
        lz = __builtin_clzll(n - 1ULL);
    }

    T ceiled = T(1) << (8 * sizeof(T) - lz);
    assert(n <= ceiled);
    assert((n << 1) == 0 or ceiled < (n << 1));
    return ceiled;
}
template<typename T>
inline
typename std::enable_if<std::is_floating_point<T>::value, T>::type
ceil_to_pow_2(T f)
{
    return ceil_to_pow_2((unsigned long) std::ceil(f));
}

#pragma once

#include "util/macro.hpp"
#include <algorithm>
#include <cmath>
#include <cstring>
#include <type_traits>


inline bool streq(const char *first, const char *second) { return 0 == strcmp(first, second); }
inline bool strneq(const char *first, const char *second, std::size_t n) { return 0 == strncmp(first, second, n); }

inline std::string replace_all(std::string str, const std::string &from, const std::string &to)
{
    std::string::size_type pos = 0;
    while ((pos = str.find(from, pos)) != std::string::npos) {
        str.replace(pos, from.length(), to);
        pos += to.length();
    }
    return str;
}

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
    insist(n <= ceiled, "the ceiled value must be greater or equal to the original value");
    insist((n << 1) == 0 or ceiled < (n << 1), "the ceiled value must be smaller than twice the original value");
    return ceiled;
}
template<typename T>
inline
typename std::enable_if<std::is_floating_point<T>::value, T>::type
ceil_to_pow_2(T f)
{
    return ceil_to_pow_2((unsigned long) std::ceil(f));
}

/** Short version of dynamic_cast that works for pointers and references. */
template<typename T, typename U>
T * cast(U *u) { return dynamic_cast<T*>(u); }

/** Short version of static_cast that works for pointers and references.  In debug build, check that the cast is legit.
 */
template<typename T, typename U>
T * as(U *u) { insist(cast<T>(u)); return static_cast<T*>(u); }

/** Simple test whether expression u is of type T.  Works with pointers and references. */
template<typename T, typename U>
bool is(U *u) { return cast<T>(u) != nullptr; }

inline std::string escape_string(std::string str)
{
    str = replace_all(str, "\\", "\\\\"); // escape \.
    str = replace_all(str, "\"", "\\\""); // escape "
    str = replace_all(str, "\n", "\\n");  // escape newline
    return str;
}

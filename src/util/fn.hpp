#pragma once

#include "util/assert.hpp"
#include <cstring>


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

inline unsigned ceil_to_pow_2(unsigned n)
{
    unsigned ceiled = 1U << (32 - __builtin_clz(n));
    assert(n <= ceiled);
    assert((n << 1) == 0 or ceiled < (n << 1));
    return ceiled;
}

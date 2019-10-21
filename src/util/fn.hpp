#pragma once

#include "util/macro.hpp"
#include <algorithm>
#include <cmath>
#include <cstring>
#include <type_traits>
#include <variant>


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

/** Computes the FNV-1a 64-bit hash of a cstring. */
struct StrHash
{
    uint64_t operator()(const char *c_str) const {
        /* FNV-1a 64 bit */
        uint64_t hash = 0xcbf29ce484222325;
        char c;
        while ((c = *c_str++)) {
            hash = hash ^ c;
            hash = hash * 1099511628211;
        }
        return hash;
    }
};

/** Compares two cstrings for equality. */
struct StrEqual
{
    bool operator()(const char *first, const char *second) const { return streq(first, second); }
};

/** Compares two cstrings for equality.  Allows `nullptr`. */
struct StrEqualWithNull
{
    bool operator()(const char *first, const char *second) const {
        return first == second or (first != nullptr and second != nullptr and streq(first, second));
    }
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

inline std::string escape(char c)
{
    switch (c) {
        default: return std::string(1, c);
        case '\\': return "\\\\";
        case '\"': return "\\\"";
        case '\n': return "\\n";
    }
}

inline std::string escape_string(std::string str)
{
    str = replace_all(str, "\\", "\\\\"); // escape \.
    str = replace_all(str, "\"", "\\\""); // escape "
    str = replace_all(str, "\n", "\\n");  // escape newline
    return str;
}

inline std::string unescape(std::string str)
{
    str = replace_all(str, "\\\\", "\\"); // unescape \\.
    str = replace_all(str, "\\\"", "\""); // unescape "
    str = replace_all(str, "\\n", "\n"); // unescape newline
    return str;
}

/** Escapes special characters in a string to be printable in HTML documents.  Primarily used for DOT. */
std::string html_escape(std::string str);

/** Checks whether haystack contains needle. */
template<typename H, typename N>
bool contains(const H &haystack, const N &needle)
{
    using std::find, std::begin, std::end;
    return find(begin(haystack), end(haystack), needle) != end(haystack);
}

/** Checks whether `subset` is a subset of `set`. */
template<typename Container, typename Set>
bool subset(const Container &subset, const Set &set)
{
    for (auto t : subset) {
        if (set.count(t) == 0)
            return false;
    }
    return true;
}

/** Checks whether `first` and `second` intersect. */
template<typename Container, typename Set>
bool intersect(const Container &first, const Set &second)
{
    for (auto t : first) {
        if (second.count(t))
            return true;
    }
    return false;
}

/** Power function for integral types. */
template<typename T>
inline
typename std::enable_if<std::is_integral<T>::value, T>::type
powi(const T base, const T exp)
{
    if (exp == 0)
        return 1;
    else if (exp & 0x1)
        return base * powi(base, exp - 1);
    else {
        T tmp = powi(base, exp/2);
        return tmp * tmp;
    }
}

template<typename T>
void setbit(T *bytes, bool value, uint32_t n)
{
    *bytes ^= (-T(value) ^ *bytes) & (T(1) << n); // set n-th bit to `value`
}

template<typename T, typename... Args>
std::ostream & operator<<(std::ostream &out, const std::variant<T, Args...> value)
{
    std::visit([&](auto &&arg) { out << arg; }, value);
    return out;
}

/* Helper type to define visitors of std::variant */
template<class... Ts> struct overloaded : Ts... { using Ts::operator()...; };
template<class... Ts> overloaded(Ts...) -> overloaded<Ts...>;

inline uint64_t murmur3_64(uint64_t v)
{
    v ^= v >> 33;
    v *= 0xff51afd7ed558ccd;
    v ^= v >> 33;
    v *= 0xc4ceb9fe1a85ec53;
    v ^= v >> 33;
    return v;
}

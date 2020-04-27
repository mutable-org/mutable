#pragma once

#include "util/macro.hpp"
#include <algorithm>
#include <cctype>
#include <chrono>
#include <cmath>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <initializer_list>
#include <iomanip>
#include <iostream>
#include <regex>
#include <sstream>
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

    uint64_t operator()(const char *c_str, std::size_t len) const {
        /* FNV-1a 64 bit */
        uint64_t hash = 0xcbf29ce484222325;
        for (auto end = c_str + len; c_str != end and *c_str; ++c_str) {
            hash = hash ^ *c_str;
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
template<typename T, typename U>
T & as(U &u) { return *as<T>(&u); }

/** Simple test whether expression u is of type T.  Works with pointers and references. */
template<typename T, typename U>
bool is(U *u) { return cast<T>(u) != nullptr; }
template<typename T, typename U>
bool is(U &u) { return is<T>(&u); }

inline std::string escape(char c)
{
    switch (c) {
        default: return std::string(1, c);
        case '\\': return "\\\\";
        case '\"': return "\\\"";
        case '\n': return "\\n";
    }
}

std::string escape(const std::string &str, char esc = '\\', char quote = '"');

std::string unescape(const std::string &str, char esc = '\\', char quote = '"');

inline std::string quote(const std::string &str) { return std::string("\"") + str + '"'; }

inline std::string unquote(const std::string &str, char quote = '"')
{
    using std::next, std::prev;
    insist(str.length() >= 2); // two quotes
    if (str[0] != quote) return str; // nothing to do
    insist(*str.rbegin() == quote, "unmatched opening quote");
    return std::string(next(str.begin()), prev(str.end())); // return substring str[1:-1]
}

inline std::string interpret(const std::string &str, char esc = '\\', char quote = '"')
{
    return unescape(unquote(str, quote), esc, quote);
}

/** Escapes special characters in a string to be printable in HTML documents.  Primarily used for DOT. */
std::string html_escape(std::string str);

/** Transforms a SQL-style LIKE pattern into a std::regex. */
inline std::regex pattern_to_regex(const char *pattern, const bool optimize = false, const char escape_char = '\\')
{
    std::stringstream ss;
    for (const char *c = pattern; *c; ++c) {
        switch (*c) {
            default:
                if (*c == escape_char) {
                    ++c;
                    ss << "\\" << *c;
                } else
                    ss << *c;
                break;
            case '%':
                ss << "(.*)";
                break;
            case '_':
                ss << '.';
                break;
            case '[':
            case ']':
            case '(':
            case ')':
            case '\\':
            case '.':
            case '*':
            case '+':
            case '^':
            case '?':
            case '|':
            case '{':
            case '}':
            case '$':
                ss << "\\" << *c;
                break;
        }
    }
    return optimize ? std::regex(ss.str(), std::regex::optimize) : std::regex(ss.str());
}

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
template<typename T, typename U>
auto powi(const T base, const U exp) ->
std::enable_if_t< std::is_integral_v<T> and std::is_integral_v<U>, std::common_type_t<T, U> >
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

/** Print `std::chrono::time_point` in human-readable format. */
template<typename Clock, typename Duration>
std::ostream & operator<<(std::ostream &out, std::chrono::time_point<Clock, Duration> tp)
{
    using namespace std::chrono;
    time_t time;
    if constexpr (std::is_same_v<Clock, system_clock>) {
        time = system_clock::to_time_t(tp); // convert the clock's time_point to std::time_t
    } else {
        /* The time point cannot directly be converted to time_t.  To do so, we first must relate the time point in
         * Clock to a time point in the system_clock. */
        auto tpc_now = Clock::now();
        auto sys_now = system_clock::now();
        auto tp_sys = time_point_cast<system_clock::duration>(tp - tpc_now + sys_now);
        time = system_clock::to_time_t(tp_sys);
    }
    auto tm = std::localtime(&time); // convert the given time since epoch to local calendar time
    auto oldfill = out.fill('0');
    out << std::put_time(tm, "%T.") << std::setw(3)
        << duration_cast<milliseconds>(tp.time_since_epoch()).count() % 1000
        << std::put_time(tm, " (%Z)");
    out.fill(oldfill);
    return out;
}

/* Template class definition to concatenate more types to std::variant. */
template <typename T, typename... Args> struct Concat;

template <typename... Args0, typename... Args1>
struct Concat<std::variant<Args0...>, Args1...> {
    using type = std::variant<Args0..., Args1...>;
};

inline bool is_oct  (int c) { return '0' <= c && c <= '7'; }
inline bool is_dec  (int c) { return '0' <= c && c <= '9'; }
inline bool is_hex  (int c) { return is_dec(c) || ('a' <= c && c <= 'f') || ('A' <= c && c <= 'F'); }
inline bool is_lower(int c) { return ('a' <= c && c <= 'z'); }
inline bool is_upper(int c) { return ('A' <= c && c <= 'Z'); }
inline bool is_alpha(int c) { return is_lower(c) || is_upper(c); }
inline bool is_alnum(int c) { return is_dec(c) || is_alpha(c); }

/** Returns path of the user's home directory. */
inline std::filesystem::path get_home_path()
{
    std::filesystem::path path;
#if __linux
    auto home = getenv("HOME");
    if (home)
        path = home;
#elif __APPLE__
    auto home = getenv("HOME");
    if (home)
        path = home;
#elif _WIN32
    auto homedrive = getenv("HOMEDRIVE");
    auto homepath = getenv("HOMEPATH");
    if (homedrive and homepath) {
        path = homedrive;
        path /= homepath;
    }
#else
    path = ".";
#endif
    return path;
}

/** Returns true iff the character sequence only consists of white spaces. */
inline bool isspace(const char *s, std::size_t len)
{
    return std::all_of(s, s + len, static_cast<int(*)(int)>(std::isspace));
}

inline bool isspace(const char *s) { return isspace(s, strlen(s)); }

void exec(const char *executable, std::initializer_list<const char*> args);

/** Returns the least subset of a given `set`, i.e.\ the set represented by the lowest 1 bit. */
inline uint64_t least_subset(uint64_t set) { return set & -set; }
/** Returns the next subset of a given `subset` and `set. */
inline uint64_t next_subset(uint64_t subset, uint64_t set) { return (subset - set) & set; }

template<typename T, typename U>
auto add_wo_overflow(T left, U right)
{
    static_assert(std::is_integral_v<T>, "LHS must be an integral type");
    static_assert(std::is_integral_v<U>, "RHS must be an integral type");

    static_assert(not std::is_signed_v<T>, "LHS must be unsigned");
    static_assert(not std::is_signed_v<U>, "RHS must be unsigned");

    using CT = std::common_type_t<T, U>;
    CT res;
    if (__builtin_add_overflow(CT(left), CT(right), &res))
        return std::numeric_limits<CT>::max();
    return res;
}

template<typename N0, typename N1>
auto sum_wo_overflow(N0 n0, N1 n1)
{
    return add_wo_overflow(n0, n1);
}

/** Returns the sum of the given parameters. In case the addition overflows, the maximal numeric value for the common
 * type is returned.*/
template<typename N0, typename N1, typename... Numbers>
auto sum_wo_overflow(N0 n0, N1 n1, Numbers... numbers)
{
    return sum_wo_overflow(add_wo_overflow(n0, n1), numbers...);
}

/** A wrapper around `strdup()` that permits `nullptr`. */
inline const char * strdupn(const char *str) { return str ? strdup(str) : nullptr; }

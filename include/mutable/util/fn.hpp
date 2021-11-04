#pragma once

#include "mutable/util/exception.hpp"
#include "mutable/util/macro.hpp"
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
#include <limits>
#include <regex>
#include <sstream>
#include <type_traits>
#include <unistd.h>
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
        uint64_t hash = 0xcbf29ce484222325UL;
        char c;
        while ((c = *c_str++)) {
            hash = hash ^ c;
            hash = hash * 0x100000001b3UL;
        }
        return hash;
    }

    uint64_t operator()(const char *c_str, std::size_t len) const {
        /* FNV-1a 64 bit */
        uint64_t hash = 0xcbf29ce484222325;
        for (auto end = c_str + len; c_str != end and *c_str; ++c_str) {
            hash = hash ^ *c_str;
            hash = hash * 0x100000001b3UL;
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
typename std::enable_if_t<std::is_integral_v<T> and std::is_unsigned_v<T>, bool>
is_pow_2(T n)
{
    return n ? (n & (n - T(1))) == T(0) : false;
}

template<typename T>
typename std::enable_if_t<std::is_integral_v<T> and std::is_unsigned_v<T> and sizeof(T) <= sizeof(unsigned long long), T>
ceil_to_pow_2(T n)
{
    if (n <= 1) return 1;

    /* Count leading zeros. */
    int lz;
    if constexpr (sizeof(T) <= sizeof(unsigned)) {
        lz = __builtin_clz(n - 1U);
    } else if constexpr (sizeof(T) <= sizeof(unsigned long)) {
        lz = __builtin_clzl(n - 1UL);
    } else if constexpr (sizeof(T) <= sizeof(unsigned long long)) {
        lz = __builtin_clzll(n - 1ULL);
    } else {
        static_assert(sizeof(T) > sizeof(unsigned long long), "unsupported width of integral type");
    }

    T ceiled = T(1) << (8 * sizeof(T) - lz);
    insist(n <= ceiled, "the ceiled value must be greater or equal to the original value");
    insist((n << 1) == 0 or ceiled < (n << 1), "the ceiled value must be smaller than twice the original value");
    insist(is_pow_2(ceiled));
    return ceiled;
}
template<typename T>
typename std::enable_if<std::is_floating_point<T>::value, T>::type
ceil_to_pow_2(T f)
{
    return ceil_to_pow_2((unsigned long) std::ceil(f));
}

/** Ceils number `n` to the next whole multiple of `power_of_two`.  `power_of_two` must be a power of 2. */
template<typename T>
typename std::enable_if_t<std::is_integral_v<T> and std::is_unsigned_v<T>, T>
ceil_to_multiple_of_pow_2(T n, T power_of_two)
{
    insist(is_pow_2(power_of_two));
    T ceiled = (n + (power_of_two - T(1))) & ~(power_of_two - T(1));
    insist(ceiled % power_of_two == T(0));
    return ceiled;
}

template<typename T>
typename std::enable_if_t<std::is_integral_v<T> and std::is_unsigned_v<T>, T>
round_up_to_multiple(T val, T factor)
{
    if (val == 0) return val;
    if (factor == 0)
        throw m::invalid_argument("factor must not be 0");
    T d = val / factor;
    T differ = (d * factor) != val;
    return (d + differ) * factor;
}

template<typename T>
typename std::enable_if_t<std::is_integral_v<T> and std::is_unsigned_v<T> and sizeof(T) <= sizeof(unsigned long long), T>
log2_floor(T n)
{
    if constexpr (sizeof(T) <= sizeof(unsigned)) {
        return sizeof(T) * 8 - __builtin_clz(n) - 1;
    } else if constexpr (sizeof(T) <= sizeof(unsigned long)) {
        return sizeof(T) * 8 - __builtin_clzl(n) - 1;
    } else if constexpr (sizeof(T) <= sizeof(unsigned long long)) {
        return sizeof(T) * 8 - __builtin_clzll(n) - 1;
    } else {
        static_assert(sizeof(T) > sizeof(unsigned long long), "unsupported width of integral type");
    }
}

template<typename T>
typename std::enable_if_t<std::is_integral_v<T> and std::is_unsigned_v<T> and sizeof(T) <= sizeof(unsigned long long), T>
log2_ceil(T n)
{
    return n <= 1 ? 0 : log2_floor(n - T(1)) + T(1);
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
    if (str.length() < 2)
        throw m::invalid_argument("string must be at least two quotes long"); // two quotes
    if (str[0] != quote) return str; // nothing to do
    if (*str.rbegin() != quote)
        throw m::invalid_argument("unmatched opening quote");
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
    if ('_' == escape_char or '%' == escape_char)
        throw m::invalid_argument("illegal escape character");

    std::stringstream ss;
    for (const char *c = pattern; *c; ++c) {
        if (*c == escape_char) {
            ++c;
            if ('_' == *c or '%' == *c) {
                /* This is an escaped character of the input SQL pattern. */
                ss << *c;
                continue;
            } else if (escape_char == *c) {
                /* This is an escaped character of the input SQL pattern. Nothing to be done. Fallthrough. */
            } else {
                throw m::runtime_error("invalid escape sequence");
            }
        }
        switch (*c) {
            default:
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

/** Compares a SQL-style LIKE pattern with the given `std::string`. */
bool like(const std::string &str, const std::string &pattern, const char escape_char = '\\');

/** Checks whether haystack contains needle. */
template<typename H, typename N>
bool contains(const H &haystack, const N &needle)
{
    using std::find, std::begin, std::end;
    return find(begin(haystack), end(haystack), needle) != end(haystack);
}

/** Checks whether first and second are equal considering permutations. */
template<typename T, typename U>
bool equal(const T &first, const U &second)
{
    for (auto t : first) {
        if (not contains(second, t))
            return false;
    }
    for (auto t : second) {
        if (not contains(first, t))
            return false;
    }
    return true;
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

/** This function implements the 64-bit finalizer of Murmur3_x64 by Austin Appleby, available at
 * https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp .  We use the optimized constants found by David
 * Stafford, in particular the values for `Mix01`, as reported at
 * http://zimbry.blogspot.com/2011/09/better-bit-mixing-improving-on.html .
 *
 * @param v     the value to mix
 * @return      the mixed bits
 */
inline uint64_t murmur3_64(uint64_t v)
{
    v ^= v >> 31;
    v *= 0x7fb5d329728ea185ULL;
    v ^= v >> 27;
    v *= 0x81dadef4bc2dd44dULL;
    v ^= v >> 33;
    return v;
}

namespace m {

struct put_tm
{
    private:
    std::tm tm_;

    public:
    put_tm(std::tm tm) : tm_(tm) { }

    friend std::ostream & operator<<(std::ostream &out, const put_tm &pt) {
        using std::setw;
        const auto oldfill = out.fill('0');
        const auto oldflags = out.flags();
        out << std::internal;
        if (pt.tm_.tm_year < -1900)
            out << '-' << setw(4) << -(pt.tm_.tm_year + 1900);
        else
            out << setw(4) << pt.tm_.tm_year + 1900;
        out << '-'
            << setw(2) << pt.tm_.tm_mon + 1 << '-'
            << setw(2) << pt.tm_.tm_mday << ' '
            << setw(2) << pt.tm_.tm_hour << ':'
            << setw(2) << pt.tm_.tm_min << ':'
            << setw(2) << pt.tm_.tm_sec;
        out.flags(oldflags);
        out.fill(oldfill);
        return out;
    }
};

struct get_tm
{
    private:
    std::tm &tm_;

    public:
    get_tm(std::tm &tm) : tm_(tm) { }

    friend std::istream & operator>>(std::istream &in, get_tm gt) {
        in >> gt.tm_.tm_year;
        if (in and in.peek() == '-') in.get(); else return in;
        in >> gt.tm_.tm_mon;
        if (in and in.peek() == '-') in.get(); else return in;
        in >> gt.tm_.tm_mday;
        if (in and in.peek() == ' ') in.get(); else return in;
        in >> gt.tm_.tm_hour;
        if (in and in.peek() == ':') in.get(); else return in;
        in >> gt.tm_.tm_min;
        if (in and in.peek() == ':') in.get(); else return in;
        in >> gt.tm_.tm_sec;

        gt.tm_.tm_year -= 1900;
        gt.tm_.tm_mon -= 1;
        return in;
    }
};

template<typename Clock, typename Duration>
struct put_timepoint
{
    private:
    std::chrono::time_point<Clock, Duration> tp_;
    bool utc_;

    public:
    put_timepoint(std::chrono::time_point<Clock, Duration> tp, bool utc = false) : tp_(tp), utc_(utc) { }

    /** Print the given `std::chrono::time_point` in the given format. */
    friend std::ostream & operator<<(std::ostream &out, const put_timepoint &ptp) {
        using namespace std::chrono;
        time_t time;

        /* Convert `std::chrono::time_point` to `std::time_t`. */
        if constexpr (std::is_same_v<Clock, system_clock>) {
            time = system_clock::to_time_t(ptp.tp_); // convert the clock's time_point to std::time_t
        } else {
            /* The time point cannot directly be converted to time_t.  To do so, we first must relate the time point in
             * `Clock` to a time point in the `system_clock`. */
            auto tpc_now = Clock::now();
            auto sys_now = system_clock::now();
            auto tp_sys = time_point_cast<system_clock::duration>(ptp.tp_ - tpc_now + sys_now);
            time = system_clock::to_time_t(tp_sys);
        }

        /* Convert `std::time_t` to `std::tm`. */
        std::tm tm;
        std::tm *chk;
        if (ptp.utc_)
            chk = gmtime_r(&time, &tm); // convert the given `time_t` to UTC `std::tm`
        else
            chk = localtime_r(&time, &tm); // convert the given `time_t` to local `std::tm`
        insist(chk == &tm);

        /* Print `std::tm`. */
        return out << put_tm(tm);
    }
};

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

/*--- Add without overflow; clamp at max value. ----------------------------------------------------------------------*/
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

/*--- Multiply without overflow; clamp at max value. -----------------------------------------------------------------*/
template<typename T, typename U>
auto mul_wo_overflow(T left, U right)
{
    static_assert(std::is_integral_v<T>, "LHS must be an integral type");
    static_assert(std::is_integral_v<U>, "RHS must be an integral type");

    static_assert(not std::is_signed_v<T>, "LHS must be unsigned");
    static_assert(not std::is_signed_v<U>, "RHS must be unsigned");

    using CT = std::common_type_t<T, U>;
    CT res;
    if (__builtin_mul_overflow(CT(left), CT(right), &res))
        return std::numeric_limits<CT>::max();
    return res;
}

template<typename N0, typename N1>
auto prod_wo_overflow(N0 n0, N1 n1)
{
    return mul_wo_overflow(n0, n1);
}

/** Returns the product of the given parameters. In case the multiplication overflows, the maximal numeric value for the
 * common type is returned.*/
template<typename N0, typename N1, typename... Numbers>
auto prod_wo_overflow(N0 n0, N1 n1, Numbers... numbers)
{
    return prod_wo_overflow(prod_wo_overflow(n0, n1), numbers...);
}

/** A wrapper around `strdup()` that permits `nullptr`. */
inline const char * strdupn(const char *str) { return str ? strdup(str) : nullptr; }

/** Returns the page size of the system. */
std::size_t get_pagesize();

/** Returns `true` iff `n` is a integral multiple of the page size (in bytes). */
inline std::size_t Is_Page_Aligned(std::size_t n) { return (n & (get_pagesize() - 1UL)) == 0; }

/** Returns the smallest integral multiple of the page size (in bytes) greater than or equals to `n`. */
inline std::size_t Ceil_To_Next_Page(std::size_t n) { return ((n - 1UL) | (get_pagesize() - 1UL)) + 1UL; }

/** This function assigns an integral sequence number to each `double` that is not *NaN*, such that if
 *  `y = std::nextafter(x, INF)` then `sequence_number(y)` = `sequence_number(x) + 1`.
 *  Taken from https://stackoverflow.com/a/47184081/3029188 */
inline uint64_t sequence_number(double x)
{
    uint64_t u64;
    std::memcpy(&u64, &x, sizeof u64);
    if (u64 & 0x8000000000000000UL) {
        u64 ^= 0x8000000000000000UL;
        return 0x8000000000000000UL - u64;
    }
    return u64 + 0x8000000000000000UL;
}

/** This function assigns an integral sequence number to each `float` that is not *NaN*, such that if
 *  `y = std::nextafter(x, INF)` then `sequence_number(y)` = `sequence_number(x) + 1`.
 *  Inspired by https://stackoverflow.com/a/47184081/3029188 and adapted to `float`. */
inline uint32_t sequence_number(float x)
{
    uint32_t u32;
    std::memcpy(&u32, &x, sizeof u32);
    if (u32 & 0x80000000U) {
        u32 ^= 0x80000000U;
        return 0x80000000U - u32;
    }
    return u32 + 0x80000000U;
}

/** Checks whether the range `[a, b]` contains at least `n` distinct values. */
template<typename T>
constexpr bool is_range_wide_enough(T a, T b, std::size_t n)
{
    using std::swap;
    if (a > b) swap(a, b);

    if (n == 0) return true;

    insist(a <= b);
    insist(n > 0);
    if constexpr (std::is_integral_v<T>) {
        if constexpr (std::is_signed_v<T>) {
            using U = std::make_unsigned_t<T>;

            if ((a < 0) == (b < 0)) { // equal signs
                return U(b - a) >= n;
            } else { // different signs
                insist(a < 0);
                insist(b >= 0);
                U a_abs = U(~a) + 1U; // compute absolute without overflow
                return a_abs >= n or (U(b) >= n - a_abs);
            }
        } else { // unsigned
            return (b - a) >= n;
        }
    } else if constexpr (std::is_floating_point_v<T>) {
        return sequence_number(b) - sequence_number(a) >= n - 1;
    } else {
        static_assert(std::is_same_v<T, T>, "unsupported type");
    }
}

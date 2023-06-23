/*--- macros.hpp -------------------------------------------------------------------------------------------------------
 *
 * This file provides macros.
 *
 *--------------------------------------------------------------------------------------------------------------------*/

#pragma once

#include <cstdlib>
#include <initializer_list>
#include <iostream>
#include <memory>

namespace m {

/*===== Macro utilities ==============================================================================================*/
#define M_ID(X) X
#define M_CAT(X, Y) M_CAT_(X, Y)
#define M_CAT_(X, Y) X ## Y
#define M_EMPTY()
#define M_DEFER1(X) X M_EMPTY()
#define M_COMMA(X) X,
#define M_UNPACK(...) __VA_ARGS__

#define M_EVAL(...)  M_EVAL1(M_EVAL1(M_EVAL1(__VA_ARGS__)))
#define M_EVAL1(...) M_EVAL2(M_EVAL2(M_EVAL2(__VA_ARGS__)))
#define M_EVAL2(...) M_EVAL3(M_EVAL3(M_EVAL3(__VA_ARGS__)))
#define M_EVAL3(...) M_EVAL4(M_EVAL4(M_EVAL4(__VA_ARGS__)))
#define M_EVAL4(...) M_EVAL5(M_EVAL5(M_EVAL5(__VA_ARGS__)))
#define M_EVAL5(...) __VA_ARGS__

/*===== Stringify (useful when #X is too eager) ======================================================================*/
#define M_STR_(X) #X
#define M_STR(X) M_STR_(X)
#define M_STRCOMMA(X) M_STR(X),

#define M_PASTE_(X, Y) X ## Y
#define M_PASTE(X, Y) M_PASTE_(X, Y)

/*===== Head and tail of list ========================================================================================*/
#define M_HEAD(X, ...) X
#define M_TAIL(X, ...) __VA_ARGS__

/*===== Count elements in a list macro ===============================================================================*/
#define M_COUNT(LIST) (std::initializer_list<const char*>{ LIST(M_STRCOMMA) }.size())

/*===== constexpr conditional-operator ===============================================================================*/
#define M_CONSTEXPR_COND(COND, IF_TRUE, IF_FALSE) [&](){ \
    if constexpr (COND) { return (IF_TRUE); } else { return (IF_FALSE); } \
}()
#define M_CONSTEXPR_COND_UNCAPTURED(COND, IF_TRUE, IF_FALSE) [](){ \
    if constexpr (COND) { return (IF_TRUE); } else { return (IF_FALSE); } \
}()

/*===== Define enum ==================================================================================================*/
#define M_DECLARE_ENUM(LIST) \
    enum LIST { \
        LIST(M_COMMA) \
        LIST##_MAX = M_COUNT(LIST) - 1U /* MAX takes the same value as the last *real* enum value */ \
    }
#define M_ENUM_TO_STR(LIST) LIST(M_STRCOMMA)
#define M_DECL(NAME, TYPE) TYPE NAME;

/*===== Number of elements in an array ===============================================================================*/
#define M_ARR_SIZE(ARR) (sizeof(ARR) / sizeof(*(ARR)))

/*===== DEBUG(MSG): Print a message in debug build with location information. ========================================*/
#ifndef NDEBUG
#define M_DEBUG(MSG) \
    std::cout.flush(); \
    std::cerr << __FILE__ << ':' << __LINE__ << ": " << __FUNCTION__ << ' ' << MSG << std::endl
#else
#define M_DEBUG
#endif

/*===== Define Coverage Exclusion Block ==============================================================================*/
#define M_LCOV_EXCL_START /* Start exclusion block */
#define M_LCOV_EXCL_STOP  /* Stop exclusion block */
#define M_LCOV_EXCL_LINE  /* Exclude line */

/*======================================================================================================================
 * M_insist(COND [, MSG])
 *
 * Similarly to `assert()`, checks a condition in debug build and aborts if it evaluates to `false`.  Prints location
 * information.  Optionally, a message can be provided to describe the condition.
 *====================================================================================================================*/

#ifndef NDEBUG
inline void _insist(const bool cond, const char *filename, const unsigned line, const char *condstr, const char *msg)
{
    if (cond) return;

    std::cout.flush();
    std::cerr << filename << ':' << line << ": Condition '" << condstr << "' failed.";
    if (msg)
        std::cerr << "  " << msg << '.';
    std::cerr << std::endl;

    abort();
    __builtin_unreachable();
}
#define M_INSIST2_(COND, MSG) ::m::_insist((COND), __FILE__, __LINE__, #COND, MSG)
#define M_INSIST1_(COND) M_INSIST2_(COND, nullptr)

#else
#define M_INSIST2_(COND, MSG) while (0) { ((void) (COND), (void) (MSG)); }
#define M_INSIST1_(COND) while (0) { ((void) (COND)); }

#endif

#define M_GET_INSIST_(_1, _2, NAME, ...) NAME
#define M_insist(...) M_GET_INSIST_(__VA_ARGS__, M_INSIST2_, M_INSIST1_, XXX)(__VA_ARGS__)

/*======================================================================================================================
 * M_unreachable(MSG)
 *
 * Identifies unreachable code.  When executed in debug build, prints a error message and aborts execution.  In release
 * build, hints to the compiler that this code is unreachable.
 *====================================================================================================================*/

#ifndef NDEBUG
[[noreturn]] inline void _abort(const char *filename, const unsigned line, const char *msg)
{
    std::cout.flush();
    std::cerr << filename << ':' << line << ": " << msg << std::endl;
    abort();
    __builtin_unreachable();
}
#define M_unreachable(MSG) m::_abort(__FILE__, __LINE__, (MSG))

#else
#define M_unreachable(MSG) __builtin_unreachable()

#endif

/*======================================================================================================================
 * M_notnull(ARG)
 *
 * In debug build, checks whether ARG is NULL.  If this is the case, prints an error message and aborts execution.
 * Otherwise, the original value of ARG is returned.  In release build, evaluates to ARG.
 *====================================================================================================================*/

#ifndef NDEBUG
template<typename T>
T * _notnull(T *arg, const char *filename, const unsigned line, const char *argstr)
{
    if (not arg) {
        std::cout.flush();
        std::cerr << filename << ':' << line << ": " << argstr << " was NULL" << std::endl;
        abort();
    }
    return arg;
}

template<typename T>
std::unique_ptr<T> _notnull(std::unique_ptr<T> arg, const char *filename, const unsigned line, const char *argstr)
{
    if (not bool(arg)) {
        std::cout.flush();
        std::cerr << filename << ':' << line << ": " << argstr << " was NULL" << std::endl;
        abort();
    }
    return std::move(arg);
}
#define M_notnull(ARG) m::_notnull((ARG), __FILE__, __LINE__, #ARG)

#else
#define M_notnull(ARG) (ARG)

#endif

#define M_DISCARD (void)

}

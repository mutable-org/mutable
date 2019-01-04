/*--- macros.hpp -------------------------------------------------------------------------------------------------------
 *
 * This file provides macros.
 *
 *--------------------------------------------------------------------------------------------------------------------*/

#pragma once

#include <cstdlib>
#include <iostream>

namespace {

#define ID(X) X

/*===== Stringify (useful when #X is too eager) ======================================================================*/
#define STR_(X) #X
#define STR(X) STR_(X)

/*===== Define enum ==================================================================================================*/
#define DECLARE_ENUM(LIST) \
    enum LIST { \
        LIST(ID) \
    }
#define ENUM_TO_STR(LIST) LIST(STR)
#define DECL(NAME, TYPE) TYPE NAME;

/*===== Number of elements in an array ===============================================================================*/
#define ARR_SIZE(ARR) (sizeof(ARR) / sizeof(*(ARR)))

/*===== `dump`: prints a human-readable description of the object at hand; useful when debugging =====================*/
#define DECLARE_DUMP \
    void dump(std::ostream &out) const __attribute__((noinline)) { out << *this << std::endl; } \
    void dump() const __attribute__((noinline)) { dump(std::cerr); }
#define DECLARE_DUMP_VIRTUAL \
    virtual void dump(std::ostream &out) const __attribute__((noinline)) { out << *this << std::endl; } \
    void dump() const __attribute__((noinline)) { dump(std::cerr); }

/*===== DEBUG(MSG): Print a message in debug build with location information. ========================================*/
#ifndef NDEBUG
#define DEBUG(MSG) \
    std::cout.flush(); \
    std::cerr << __FILE__ << ':' << __LINE__ << ": " << __FUNCTION__ << ' ' << MSG << std::endl
#else
#define DEBUG
#endif

/*======================================================================================================================
 * insist(COND [, MSG])
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
        std::cout << "  " << msg << '.';
    std::cout << std::endl;

    abort();
    __builtin_unreachable();
}
#define _INSIST2(COND, MSG) _insist((COND), __FILE__, __LINE__, #COND, MSG)
#define _INSIST1(COND) _INSIST2(COND, nullptr)

#else
#define _INSIST2(COND, MSG) ((void) (COND), (void) (MSG))
#define _INSIST1(COND) ((void) (COND))

#endif

#define _GET_INSIST(_1, _2, NAME, ...) NAME
#define insist(...) _GET_INSIST(__VA_ARGS__, _INSIST2, _INSIST1, XXX)(__VA_ARGS__)

/*======================================================================================================================
 * unreachable(MSG)
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
#define unreachable(MSG) _abort(__FILE__, __LINE__, (MSG))

#else
#define unreachable(MSG) __builtin_unreachable()

#endif

/*======================================================================================================================
 * notnull(ARG)
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
#define notnull(ARG) _notnull((ARG), __FILE__, __LINE__, #ARG)

#else
#define notnull(ARG) (ARG)

#endif

}

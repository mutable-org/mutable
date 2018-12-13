#pragma once

#include <cstdlib>
#include <iostream>


/* This function implements `assert()` similar to 'assert.h', yet allows to provide an optional message. */
inline void _assert(const bool pred, const char *filename, const unsigned line, const char *predstr, const char *msg)
{
    if (pred) return;

    /* TODO: Use __FUNCTION__ ??? */
    std::cout.flush();
    std::cerr << filename << ':' << line << ": Assertion '" << predstr << "' failed.";
    if (msg)
        std::cout << "  " << msg << '.';
    std::cout << std::endl;

    abort();
    __builtin_unreachable();
}
#ifndef NDEBUG
#define _ASSERT2(PRED, MSG) _assert((PRED), __FILE__, __LINE__, #PRED, MSG)
#else
#define _ASSERT2(PRED, MSG)
#endif
#define _ASSERT1(PRED) _ASSERT2(PRED, nullptr)
#define _GET_ASSERT(_1, _2, NAME, ...) NAME
#ifdef assert
#undef assert
#endif
#define assert(...) _GET_ASSERT(__VA_ARGS__, _ASSERT2, _ASSERT1, XXX)(__VA_ARGS__)


[[noreturn]] inline void _abort(const char *filename, const unsigned line, const char *msg)
{
    std::cout.flush();
    std::cerr << filename << ':' << line << ": " << msg << std::endl;
    abort();
    __builtin_unreachable();
}

#define unreachable(MSG) _abort(__FILE__, __LINE__, (MSG))
/* TODO: What to do in release build? */


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
/* TODO: What to do in release build? */

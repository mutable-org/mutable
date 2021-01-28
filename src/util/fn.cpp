#include "mutable/util/fn.hpp"

#if __linux
#include <sys/types.h>
#include <unistd.h>
#elif __APPLE__
#include <string.h>
#include <unistd.h>
#endif


std::string escape(const std::string &str, char esc, char quote)
{
    std::string res;
    res.reserve(str.length());

    for (auto c : str) {
        if (c == esc or c == quote) {
            res += esc;
            res += c;
        } else if (c == '\n') {
            res += esc;
            res += 'n';
        } else {
            res += c;
        }
    }

    return res;
}

std::string unescape(const std::string &str, char esc, char quote)
{
    std::string res;
    res.reserve(str.length());

    for (auto it = str.begin(), end = str.end(); it != end; ++it) {
        if (*it == esc) {
            ++it;
            if (*it == esc or *it == quote) {
                res += *it;
            } else if (*it == 'n') {
                res += '\n';
            } else {
                /* invalid escape sequence; do not unescape */
                res += esc;
                --it;
            }
        } else {
            res += *it;
        }
    }

    return res;
}

std::string html_escape(std::string str)
{
    str = replace_all(str, "&", "&amp;");
    str = replace_all(str, "<", "&lt;");
    str = replace_all(str, ">", "&gt;");
    return str;
}

void exec(const char *executable, std::initializer_list<const char*> args)
{
#if __linux || __APPLE__
    if (fork()) {
        /* parent, nothing to be done */
    } else {
        /* child */
        char **c_args = new char*[args.size() + 2];
        char **p = c_args;
        *p++ = strdup(executable);
        for (auto arg : args)
            *p++ = strdup(arg);
        *p = nullptr;
        execv(executable, c_args);
    }
#endif
}

std::size_t get_pagesize()
{
    static std::size_t pagesize(0);
    if (0 == pagesize)
        pagesize = sysconf(_SC_PAGESIZE);
    return pagesize;
}

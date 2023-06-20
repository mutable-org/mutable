#include <mutable/util/fn.hpp>

#if __linux
#include <sys/types.h>
#include <unistd.h>
#elif __APPLE__
#include <string.h>
#include <unistd.h>
#endif


using namespace m;


std::string m::escape(const std::string &str, char esc, char quote)
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

std::string m::unescape(const std::string &str, char esc, char quote)
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

std::string m::html_escape(std::string str)
{
    str = replace_all(str, "&", "&amp;");
    str = replace_all(str, "<", "&lt;");
    str = replace_all(str, ">", "&gt;");
    return str;
} // M_LCOV_EXCL_LINE

bool m::like(const std::string &str, const std::string &pattern, const char escape_char)
{
    M_insist('_' != escape_char and '%' != escape_char, "illegal escape character");

    bool dp[pattern.length() + 1][str.length() + 1]; // dp[i][j] == true iff pattern[:i] contains str[:j]

    dp[0][0] = true; // empty pattern contains empty string
    for (std::size_t j = 1; j <= str.length(); ++j)
        dp[0][j] = false; // empty pattern does not contain non-empty string
    std::size_t escaped_row = 0;
    for (std::size_t i = 1; i <= pattern.length(); ++i) {
        const auto c = pattern[i - 1];
        const auto escaped = i == escaped_row;
        if (escaped and '_' != c and '%' != c and escape_char != c)
            throw m::runtime_error("invalid escape sequence");
        if (not escaped and escape_char == c)
            escaped_row = i + 1; // next row is escaped
        if (not escaped and '%' == c)
            dp[i][0] = dp[i - 1][0]; // pattern `X%` contains empty string iff `X` contains empty string
        else
            dp[i][0] = false; // pattern without `%`-wildcard does not contain empty string
    }
    if (pattern.length() + 1 == escaped_row)
        throw m::runtime_error("invalid escape sequence");

    escaped_row = 0;
    for (std::size_t i = 1; i <= pattern.length(); ++i) {
        const auto c = pattern[i - 1];
        const auto escaped = i == escaped_row;
        if (not escaped and escape_char == c) {
            /* Set `escaped_row` and copy entire above row. */
            escaped_row = i + 1;
            for (std::size_t j = 0; j <= str.length(); ++j)
                dp[i][j] = dp[i - 1][j];
            continue;
        }
        for (std::size_t j = 1; j <= str.length(); ++j) {
            if (not escaped and '%' == c) {
                /* pattern `X%` contains string `c_0...c_n` iff either `X%` contains `c_0...c_{n-1}
                 * or `X` contains `c_0...c_n` */
                dp[i][j] = dp[i][j - 1] or dp[i - 1][j];
            } else if ((not escaped and '_' == c) or str[j - 1] == c) {
                /* pattern `X_` contains string `c_0...c_n` iff `X` contains `c_0...c_{n-1}`,
                 * pattern `Xa` contains string `c_0...c_{n-1}a` iff `X` contains `c_0...c_{n-1}` */
                dp[i][j] = dp[i - 1][j - 1];
            } else {
                /* pattern `Xa` does not contain string `c_0...c_n` if a != c_n */
                dp[i][j] = false;
            }
        }
    }

    return dp[pattern.length()][str.length()];
}

void m::exec(const char *executable, std::initializer_list<const char*> args)
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
        M_unreachable("Invalid executable path or arguments"); // M_LCOV_EXCL_LINE
    }
#endif
}

std::size_t m::get_pagesize()
{
    static std::size_t pagesize(0);
    if (0 == pagesize)
        pagesize = sysconf(_SC_PAGESIZE);
    return pagesize;
}

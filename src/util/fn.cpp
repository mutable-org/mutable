#include "util/fn.hpp"


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
                res += *it;
            }
        } else {
            res += *it;
        }
    }

    return res;
}

/** Escapes special characters in a string to be printable in HTML documents.  Primarily used for DOT. */
std::string html_escape(std::string str)
{
    str = replace_all(str, "&", "&amp;");
    str = replace_all(str, "<", "&lt;");
    str = replace_all(str, ">", "&gt;");
    return str;
}

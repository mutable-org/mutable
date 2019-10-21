#include "util/fn.hpp"


std::string escape_string(const std::string &str)
{
    std::string res;
    res.reserve(str.length());

    for (auto c : str) {
        switch (c) {
            default:
                res += c;
                break;

            case '\\':
                res += "\\\\";
                break;

            case '\n':
                res += "\\n";
                break;

            case '\"':
                res += "\\";
                break;
        }
    }

    return res;
}

std::string unescape(const std::string &str)
{
    std::string res;
    res.reserve(str.length());

    for (auto it = str.begin(), end = str.end(); it != end; ++it) {
        switch (*it) {
            default:
                res += *it;
                break;

            case '\\':
                ++it;
                switch (*it) {
                    default:
                        /* invalid escape sequence; print as is */
                        res += '\\';
                        res += *it;
                        break;

                    case '\\':
                    case 'n':
                    case '\"':
                        res += *it;
                        break;
                }
                break;
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

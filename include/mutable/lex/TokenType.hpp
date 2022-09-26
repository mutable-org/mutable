#pragma once

#include <iostream>
#include <mutable/util/macro.hpp>


namespace m {

enum TokenType
{
#define M_TOKENTYPE(tok) TK_##tok,
#include <mutable/tables/TokenType.tbl>
#undef M_TOKENTYPE
    TokenType_MAX = TK_EOF
};

inline char const * get_name(const TokenType tt)
{
    switch (tt) {
        case TK_ERROR:          return "error";
        case TK_EOF:            return "eof";
        case TK_BACKSLASH:      return "\\";
        case TK_IDENTIFIER:     return "identifier";
        case TK_STRING_LITERAL: return "string-literal";
        case TK_DATE:           return "date";
        case TK_DATE_TIME:      return "datetime";
        case TK_INSTRUCTION:    return "instruction";

        case TK_OCT_INT:
        case TK_DEC_INT:
        case TK_HEX_INT:
        case TK_DEC_FLOAT:
        case TK_HEX_FLOAT:
            return "constant";

#define M_KEYWORD(tt, name) case TK_ ## tt:
#include <mutable/tables/Keywords.tbl>
#undef M_KEYWORD
            return "keyword";

#define M_OPERATOR(tt) case TK_ ## tt:
#include <mutable/tables/Operators.tbl>
#undef M_OPERATOR
            return "punctuator";
    }
}

M_LCOV_EXCL_START
inline std::ostream & operator<<(std::ostream &os, const TokenType tt)
{
    switch (tt) {
#define M_TOKENTYPE(tok) case TK_ ## tok: return os << "TK_"#tok;
#include <mutable/tables/TokenType.tbl>
#undef M_TOKENTYPE
    }
}

inline std::string to_string(const TokenType tt)
{
    switch (tt) {
#define M_TOKENTYPE(tok) case TK_ ## tok: return "TK_"#tok;
#include <mutable/tables/TokenType.tbl>
#undef M_TOKENTYPE
    }
}
M_LCOV_EXCL_STOP

}

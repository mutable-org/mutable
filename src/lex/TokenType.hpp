#pragma once


#include <iostream>


namespace db {

enum TokenType
{
#define DB_TOKENTYPE(tok) TK_##tok,
#include "tables/TokenType.tbl"
#undef DB_TOKENTYPE
};

inline char const * get_name(const TokenType tt)
{
    switch (tt) {
        case TK_ERROR:          return "error";
        case TK_EOF:            return "eof";
        case TK_BACKSLASH:      return "\\";
        case TK_IDENTIFIER:     return "identifier";
        case TK_STRING_LITERAL: return "string-literal";

        case TK_OCT_INT:
        case TK_DEC_INT:
        case TK_HEX_INT:
        case TK_DEC_FLOAT:
        case TK_HEX_FLOAT:
            return "constant";

#define DB_KEYWORD(tt, name) case TK_ ## tt:
#include "tables/Keywords.tbl"
#undef DB_KEYWORD
            return "keyword";

#define DB_OPERATOR(tt) case TK_ ## tt:
#include "tables/Operators.tbl"
#undef DB_OPERATOR
            return "punctuator";
    }
}

inline std::ostream & operator<<(std::ostream &os, const TokenType tt)
{
    switch (tt) {
#define DB_TOKENTYPE(tok) case TK_ ## tok: return os << "TK_"#tok;
#include "tables/TokenType.tbl"
#undef DB_TOKENTYPE
    }
}

inline std::string to_string(const TokenType tt)
{
    switch (tt) {
#define DB_TOKENTYPE(tok) case TK_ ## tok: return "TK_"#tok;
#include "tables/TokenType.tbl"
#undef DB_TOKENTYPE
    }
}

}

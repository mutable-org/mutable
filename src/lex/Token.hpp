#pragma once


#include "lex/TokenType.hpp"
#include "util/macro.hpp"
#include "util/Position.hpp"


namespace db {

struct Token
{
    Position pos;
    const char *text;
    TokenType type;

    explicit Token(Position pos, const char *text, TokenType type)
        : pos(pos)
        , text(text)
        , type(type)
    { }

    operator bool() const { return type != TK_EOF; }

    friend std::string to_string(const Token &tok) {
        std::ostringstream os;
        os << tok;
        return os.str();
    }

    friend std::ostream & operator<<(std::ostream &os, const Token &tok) {
        return os << tok.pos << ", '" << tok.text << "', " << tok.type;
    }

    DECLARE_DUMP
};

}

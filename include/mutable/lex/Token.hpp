#pragma once

#include <mutable/lex/TokenType.hpp>
#include <mutable/util/macro.hpp>
#include <mutable/util/Position.hpp>


namespace m {

namespace ast {

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

    Token() : pos(nullptr), text(nullptr), type(TK_EOF) { }

    operator bool() const { return type != TK_EOF; }
    operator TokenType() const { return type; }

M_LCOV_EXCL_START
    friend std::string to_string(const Token &tok) {
        std::ostringstream os;
        os << tok;
        return os.str();
    }

    friend std::ostream & operator<<(std::ostream &os, const Token &tok) {
        return os << tok.pos << ", '" << tok.text << "', " << tok.type;
    }

    void dump(std::ostream &out) const { out << *this << std::endl; }
    void dump() const;
M_LCOV_EXCL_STOP
};

}

}

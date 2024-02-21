#pragma once

#include <mutable/lex/Token.hpp>
#include <mutable/lex/TokenType.hpp>
#include <mutable/util/Diagnostic.hpp>
#include <mutable/util/Pool.hpp>
#include <istream>
#include <unordered_map>
#include <vector>


namespace m {

namespace ast {

struct M_EXPORT Lexer
{
    public:
    Diagnostic &diag;
    ThreadSafeStringPool &pool;
    const char *filename;
    std::istream &in;

    private:
    using Keywords_t = std::unordered_map<ThreadSafePooledString, TokenType>;
    using buf_t = std::vector<char>;
    Keywords_t keywords_;
    int c_;
    Position pos_, start_;
    buf_t buf_;

    public:
    explicit Lexer(Diagnostic &diag, ThreadSafeStringPool &pool, const char *filename, std::istream &in)
        : diag(diag)
        , pool(pool)
        , filename(filename)
        , in(in)
        , pos_(filename)
        , start_(pos_)
    {
        buf_.reserve(32);
        initialize_keywords();
        c_ = '\n';
    }

    /** Initializes the set of all keywords. */
    void initialize_keywords();

    /** Obtains the next token from the input stream. */
    Token next();

    private:
    /** Reads the next character from \p in to \p c_, and updates \p pos_ accordingly. */
    int step() {
        switch (c_) {
            case '\n':
                pos_.column = 1;
                pos_.line++;
                break;

            default:
                pos_.column++;
                break;
        }
        return c_ = in.get();
    }

    void push() {
        buf_.push_back(c_);
        step();
    }

    bool accept(const int c) {
        if (c == this->c_) {
            push();
            return true;
        }
        return false;
    }

    ThreadSafePooledString internalize() {
        buf_.push_back('\0');
        return pool(buf_.data());
    }

    /* Lexer routines. */
    Token read_keyword_or_identifier();
    Token read_number();
    Token read_string_literal();
    Token read_date_or_datetime();
    Token read_instruction();
};

}

}

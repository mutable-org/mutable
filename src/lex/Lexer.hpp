#pragma once

#include <istream>
#include <unordered_map>
#include <vector>
#include "lex/Token.hpp"
#include "lex/TokenType.hpp"
#include "util/Diagnostic.hpp"
#include "util/StringPool.hpp"


namespace db {

struct Lexer
{
    public:
    Diagnostic &diag;
    const char *filename;
    std::istream &in;

    private:
    using Keywords_t = std::unordered_map<const char*, TokenType>;
    using buf_t = std::vector<char>;
    Keywords_t keywords_;
    StringPool pool_;
    int c_;
    Position pos_, start_;
    buf_t buf_;

    public:
    explicit Lexer(Diagnostic &diag, const char *filename, std::istream &in)
        : diag(diag)
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

    const char * internalize() {
        buf_.push_back('\0');
        return pool_(buf_.data());
    }

    /* Lexer routines. */
    Token read_keyword_or_identifier();
    Token read_number(bool has_dot = false);
};

}

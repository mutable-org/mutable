#pragma once

#include <istream>
#include <unordered_map>
#include <vector>
#include "lex/Token.hpp"
#include "lex/TokenType.hpp"
#include "util/Diagnostic.hpp"
#include "util/StringPool.hpp"



static inline bool isOct  (int c) { return '0' <= c && c <= '7'; }
static inline bool isDec  (int c) { return '0' <= c && c <= '9'; }
static inline bool isHex  (int c) { return isDec(c) || ('a' <= c && c <= 'f') || ('A' <= c && c <= 'F'); }
static inline bool isLower(int c) { return ('a' <= c && c <= 'z'); }
static inline bool isUpper(int c) { return ('A' <= c && c <= 'Z'); }
static inline bool isAlpha(int c) { return isLower(c) || isUpper(c); }
static inline bool isAlNum(int c) { return isDec(c) || isAlpha(c); }

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
        return pool_(&buf_[-1]);
        /* TODO: after internalizing the string, clear the buffer? */
    }
};

}

#pragma once

#include <mutable/lex/Token.hpp>
#include <mutable/lex/TokenType.hpp>
#include <mutable/util/Diagnostic.hpp>
#include <mutable/util/StringPool.hpp>
#include <istream>
#include <unordered_map>
#include <vector>


namespace m {

namespace ast {

struct M_EXPORT Lexer
{
    public:
    Diagnostic &diag;
    StringPool &pool;
    const char *filename;
    std::istream &in;

    private:
    using Keywords_t = std::unordered_map<const char*, TokenType>;
    using buf_t = std::vector<char>;
    Keywords_t keywords_;
    int c_;
    Position pos_, start_;
    buf_t buf_;

    public:
    explicit Lexer(Diagnostic &diag, StringPool &pool, const char *filename, std::istream &in)
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

    /** Returns true iff the input stream provides a token. */
    bool has_next() {
        /* Use std::basic_istream::readsome() tp extract the next character if available.  Note that the behavior of
         * this function is highly implementation-specific.  However, we believe that it is safe to use here.
         * The reasoning is as follows:
         * (*) If characters are available in the buffer, readsome() returns 1 and this functions returns true.
         * (*) If characters are available in the file, yet the buffer is empty, readsome() returns 0 and this function
         *     returns false.  This is ok, as the next call to Lexer::next() or Parser::parse() will force reading the
         *     next character.
         * (*) If no more characers are in the buffer and the file was read entirely, readsome() returns 0 and this
         *     function returns false.  This is correct, since there is no next token available.
         *
         * see https://en.cppreference.com/w/cpp/io/basic_istream/readsome
         */
        char buf;
        if (in.readsome(&buf, 1)) {
            in.unget();
            return true;
        }
        return false;
    }

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

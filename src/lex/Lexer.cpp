#include "lex/Lexer.hpp"

#include <cctype>


#define UNDO(CHR) { in.putback(c_); c_ = CHR; pos_.column--; }


using namespace db;


void Lexer::initialize_keywords()
{
#define DB_KEYWORD(tok, text) keywords_.emplace(pool(#text), TK_##tok);
#include "tables/Keywords.tbl"
#undef DB_KEYWORD
}

Token Lexer::next()
{
    /* skip whitespaces and comments */
    for (;;) {
        switch (c_) {
            case EOF: return Token(pos_, "EOF", TK_EOF);
            case ' ': case '\t': case '\v': case '\f': case '\n': case '\r': step(); continue;

            case '-': {
                step();
                if (c_ == '-') {
                    /* read comment */
                    do step(); while (c_ != EOF and c_ != '\n');
                    continue;
                } else {
                    /* TK_MINUS */
                    UNDO('-');
                    goto after;
                }
            }

            default: goto after;
        }
    }
after:

    start_ = pos_;
    buf_.clear();

    switch (c_) {
        case '0': case '1': case '2': case '3': case '4': case '5': case '6': case '7': case '8': case '9':
            return read_number();

        case '"':
            return read_string_literal();

        /* Punctuators */
#define LEX(chr, text, tt, SUB) case chr: step(); switch (c_) { SUB } return Token(start_, text, tt);
#define GUESS(first, SUB) case first: step(); switch (c_) { SUB } UNDO(first); break;
        LEX('(', "(", TK_LPAR, );
        LEX(')', ")", TK_RPAR, );
        LEX('~', "~", TK_TILDE, );
        LEX('+', "+", TK_PLUS, );
        LEX('-', "-", TK_MINUS, );
        LEX('*', "*", TK_ASTERISK, );
        LEX('/', "/", TK_SLASH, );
        LEX('%', "%", TK_PERCENT, );
        LEX('=', "=", TK_EQUAL, );
        GUESS('!',
            LEX('=', "!=", TK_BANG_EQUAL, ) );
        LEX('<', "<", TK_LESS,
            LEX('=', "<=", TK_LESS_EQUAL, ) );
        LEX('>', ">", TK_GREATER,
            LEX('=', ">=", TK_GREATER_EQUAL, ) );
        LEX(',', ",", TK_COMMA, );
        LEX(';', ";", TK_SEMICOL, );
        LEX('.', ".", TK_DOT,
            LEX('.', "..", TK_DOTDOT, )
            case '0': case '1': case '2': case '3': case '4': case '5': case '6': case '7': case '8': case '9':
                UNDO('.');
                return read_number(););

#undef LEX
#undef GUESS

        default: /* fallthrough */;
    }

    if ('_' == c_ or is_alpha(c_)) return read_keyword_or_identifier();

    push();
    const char *str = internalize();
    diag.e(start_) << "illegal character '" << str << "'\n";
    return Token(start_, str, TK_ERROR);
}


/*====================================================================================================================*/
//
//  Lexer functions
//
/*====================================================================================================================*/

Token Lexer::read_keyword_or_identifier()
{
    while ('_' == c_ or is_alnum(c_))
        push();
    const auto str = internalize();
    auto it = keywords_.find(str);
    if (it == keywords_.end()) return Token(start_, str, TK_IDENTIFIER);
    else return Token(start_, str, it->second);
}

Token Lexer::read_number()
{
    bool is_float = false;
    bool empty = true;
    enum { Oct, Dec, Hex, Err } is, has;

    /*-- Prefix ----------------------*/
    is = Dec;
    if ('0' == c_) { is = Oct; empty = false; push(); }
    if ('x' == c_ || 'X' == c_) { is = Hex; empty = true; push(); }
    has = is;

    /*-- sequence before dot ---------*/
    for (;;) {
        if      (is == Oct && is_oct(c_)) /* OK */;
        else if (is == Oct && is_dec(c_)) has = Dec;
        else if (is == Dec && is_dec(c_)) /* OK */;
        else if (is == Hex && is_hex(c_)) /* OK */;
        else    break;
        empty = false;
        push();
    }

    /*-- the dot ---------------------*/
    if ('.' == c_) {
        push();
        is_float = true;
        if (is  == Oct) is  = Dec; // there are no octal floating point constants
        if (has == Oct) has = Dec;

        /*-- sequence after dot ------*/
        if      (is == Dec) { if (is_dec(c_)) empty = false; while (is_dec(c_)) push(); }
        else if (is == Hex) { if (is_hex(c_)) empty = false; while (is_hex(c_)) push(); }
    }

    /*-- exponent part ---------------*/
    if ((is == Oct && ('e' == c_ || 'E' == c_)) ||
        (is == Dec && ('e' == c_ || 'E' == c_)) ||
        (is == Hex && ('p' == c_ || 'P' == c_))) {
        push();
        is_float = true;
        if (is  == Oct) is  = Dec; // there are no octal floating point constants
        if (has == Oct) has = Dec;
        if ('-' == c_ || '+' == c_) push();
        empty = true;
        while (is_dec(c_)) { empty = false; push(); }
    }

    if (empty or is != has) {
        const auto str = internalize();
        diag.e(start_) << "invalid number '" << str << "'\n";
        return Token(start_, str, TK_ERROR);
    }
    TokenType tt;
    switch (is) {
        case Oct: tt = TK_OCT_INT; break;
        case Dec: tt = is_float ? TK_DEC_FLOAT : TK_DEC_INT; break;
        case Hex: tt = is_float ? TK_HEX_FLOAT : TK_HEX_INT; break;
        default: unreachable("invalid numeric type");
    }
    return Token(start_, internalize(), tt);
}

Token Lexer::read_string_literal()
{
    push(); // initial '"'
    while (EOF != c_ and '"' != c_) {
        if (c_ == '\\') { // escape character
            push();
            if (c_ == '"') // valid escape sequence
                push();
        } else
            push();
    }
    push(); // terminal '"'
    return Token(start_, internalize(), TK_STRING_LITERAL);
}

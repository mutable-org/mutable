#include "catch2/catch.hpp"

#include "lex/Lexer.hpp"
#include "testutil.hpp"

using namespace m;

TEST_CASE("Lexer::has_next()", "[core][lex][unit]")
{
    {
        const char *query = "SELECT";
        LEXER(query);

        REQUIRE(lexer.has_next());
    }

    {
        const char *query = "";
        LEXER(query);

        REQUIRE_FALSE(lexer.has_next());
    }
}

TEST_CASE("Lexer::next()", "[core][lex][unit]")
{
    SECTION("valid tokens")
    {
        std::tuple<const char*, const TokenType, const char*, const TokenType> expr[] = {
            /* { expression, Token, text, nextToken } */

            /* single token */
            { "(", TK_LPAR, "(", TK_EOF },
            { ")", TK_RPAR, ")", TK_EOF },
            { "~", TK_TILDE, "~", TK_EOF },
            { "+", TK_PLUS, "+", TK_EOF },
            { "-", TK_MINUS, "-", TK_EOF },
            { "*", TK_ASTERISK, "*", TK_EOF },
            { "/", TK_SLASH, "/", TK_EOF },
            { "%", TK_PERCENT, "%", TK_EOF },
            { "=", TK_EQUAL, "=", TK_EOF },
            { "!=", TK_BANG_EQUAL, "!=", TK_EOF },
            { "<", TK_LESS, "<", TK_EOF },
            { "<=", TK_LESS_EQUAL, "<=", TK_EOF },
            { ">", TK_GREATER, ">", TK_EOF },
            { ">=", TK_GREATER_EQUAL, ">=", TK_EOF },
            { ",", TK_COMMA, ",", TK_EOF },
            { ";", TK_SEMICOL, ";", TK_EOF },
            { ".", TK_DOT, ".", TK_EOF },
            { "..", TK_DOTDOT, "..", TK_EOF },

            /* whitespaces and comments */
            { "SELECT  \t   \v\f\n \
               \r -- this is a comment\n *", TK_Select, "SELECT", TK_ASTERISK },
            { "SELECT  -- this is the end of the document", TK_Select, "SELECT", TK_EOF },

            /* integer constants */
            { "123456789", TK_DEC_INT, "123456789", TK_EOF },
            { "01234567", TK_OCT_INT, "01234567", TK_EOF },
            { "1234567", TK_DEC_INT, "1234567", TK_EOF },
            { "0x0123456789abcdef", TK_HEX_INT, "0x0123456789abcdef", TK_EOF },
            { "0X0123456789ABCDEF", TK_HEX_INT, "0X0123456789ABCDEF", TK_EOF },
            { "0xFe1", TK_HEX_INT, "0xFe1", TK_EOF },

            /* floating constants */
            { "3012456789.", TK_DEC_FLOAT, "3012456789.", TK_EOF },
            { "4012356789.0123456789", TK_DEC_FLOAT, "4012356789.0123456789", TK_EOF },
            { ".0123456789", TK_DEC_FLOAT, ".0123456789", TK_EOF },
            { "0.0", TK_DEC_FLOAT, "0.0", TK_EOF },
            { "01234567.", TK_DEC_FLOAT, "01234567.", TK_EOF },
            { "5012346789e0123456789", TK_DEC_FLOAT, "5012346789e0123456789", TK_EOF },
            { "6012345789E0123456789", TK_DEC_FLOAT, "6012345789E0123456789", TK_EOF },
            { "7e+9", TK_DEC_FLOAT, "7e+9", TK_EOF },
            { "8e-5", TK_DEC_FLOAT, "8e-5", TK_EOF },
            { "01234567e1", TK_DEC_FLOAT, "01234567e1", TK_EOF },
            { "01234567E1", TK_DEC_FLOAT, "01234567E1", TK_EOF },
            { "9012345678.0123456789e+9876543210", TK_DEC_FLOAT, "9012345678.0123456789e+9876543210", TK_EOF },
            { "0xa0123456789bcdef.", TK_HEX_FLOAT, "0xa0123456789bcdef.", TK_EOF },
            { "0xb0123456789acdef.0123456789abcdef", TK_HEX_FLOAT, "0xb0123456789acdef.0123456789abcdef", TK_EOF },
            { "0XC0123456789ABDEF.0123456789ABCDEF", TK_HEX_FLOAT, "0XC0123456789ABDEF.0123456789ABCDEF", TK_EOF },
            { "0x.d0123456789abcef", TK_HEX_FLOAT, "0x.d0123456789abcef", TK_EOF },
            { "0xe0123456789abcdfp0123456789", TK_HEX_FLOAT, "0xe0123456789abcdfp0123456789", TK_EOF },
            { "0xf0123456789abcdeP0123456789", TK_HEX_FLOAT, "0xf0123456789abcdeP0123456789", TK_EOF },
            { "0x0p+9", TK_HEX_FLOAT, "0x0p+9", TK_EOF },
            { "0xfp-0", TK_HEX_FLOAT, "0xfp-0", TK_EOF },
            { "0x0123456789abcdef.fedcba9876543210p-0123456789", TK_HEX_FLOAT,
              "0x0123456789abcdef.fedcba9876543210p-0123456789", TK_EOF },

            /* incorrect numbers */
            { "2A", TK_DEC_INT, "2", TK_IDENTIFIER },
            { "0x1G", TK_HEX_INT, "0x1", TK_IDENTIFIER },
            { "1a.", TK_DEC_INT, "1", TK_IDENTIFIER },
            { ".1a", TK_DEC_FLOAT, ".1", TK_IDENTIFIER },
            { "1.a", TK_DEC_FLOAT, "1.", TK_IDENTIFIER },
            { "0xa.g", TK_HEX_FLOAT, "0xa.", TK_IDENTIFIER },
            { "0x.ag", TK_HEX_FLOAT, "0x.a", TK_IDENTIFIER },
            { "1234567890abcdef", TK_DEC_INT, "1234567890", TK_IDENTIFIER },
            { "Fe1", TK_IDENTIFIER, "Fe1", TK_EOF },
            { "b0123456789acdef.0123456789abcdef", TK_IDENTIFIER, "b0123456789acdef", TK_DEC_FLOAT },
            { ".d0123456789abcef", TK_DOT, ".", TK_IDENTIFIER },
            { "0p+9", TK_OCT_INT, "0", TK_IDENTIFIER },

            /* string literals */
            { "\"this is a string literal\"", TK_STRING_LITERAL, "\"this is a string literal\"", TK_EOF },
            { "\"this is valid \\\"too\\\"!\"", TK_STRING_LITERAL, "\"this is valid \\\"too\\\"!\"", TK_EOF },
            { "\"this is valid \\n too!\"", TK_STRING_LITERAL, "\"this is valid \\n too!\"", TK_EOF },
            { "\"this is valid \\t too!\"", TK_STRING_LITERAL, "\"this is valid \\t too!\"", TK_EOF },
            { "\"this is valid \\\\ too!\"", TK_STRING_LITERAL, "\"this is valid \\\\ too!\"", TK_EOF },
            { "\"this is \n not a string literal\"", TK_STRING_LITERAL, "\"this is \n not a string literal\"", TK_EOF },

            /* keywords and identifiers */
            { "SELECT attr", TK_Select, "SELECT", TK_IDENTIFIER },
            { "attr FROM", TK_IDENTIFIER, "attr", TK_From },
            { "FROM A", TK_From, "FROM", TK_IDENTIFIER },
            { "A", TK_IDENTIFIER, "A", TK_EOF },
            { "abcdefghijklmnopqrstuvwxyz_ABCDEFGHIJKLMNOPQRSTUVWXYZ_0123456789", TK_IDENTIFIER,
              "abcdefghijklmnopqrstuvwxyz_ABCDEFGHIJKLMNOPQRSTUVWXYZ_0123456789", TK_EOF },
            { "_____", TK_IDENTIFIER, "_____", TK_EOF },
            { "_0", TK_IDENTIFIER, "_0", TK_EOF },
            { "_l", TK_IDENTIFIER, "_l", TK_EOF },
            { "d", TK_IDENTIFIER, "d", TK_EOF },

            /* date constants */
            { "d'2021-01-29'", TK_DATE, "d'2021-01-29'", TK_EOF },
            { "d'-2021-01-29'", TK_DATE, "d'-2021-01-29'", TK_EOF },
            { "d'0099-17-49'", TK_DATE, "d'0099-17-49'", TK_EOF },
            { "date'2021-01-29'", TK_IDENTIFIER, "date", TK_ERROR },
            { "d2021-01-29'", TK_IDENTIFIER, "d2021", TK_MINUS },

            /* datetime constants */
            { "d'2021-01-29 12:17:49'", TK_DATE_TIME, "d'2021-01-29 12:17:49'", TK_EOF },
            { "d'-2021-01-29 12:17:49'", TK_DATE_TIME, "d'-2021-01-29 12:17:49'", TK_EOF },
            { "d'0099-17-49 29:93:67'", TK_DATE_TIME, "d'0099-17-49 29:93:67'", TK_EOF },
            { "date'2021-01-29 12:17:49'", TK_IDENTIFIER, "date", TK_ERROR },
            { "d2021-01-29 12:17:49'", TK_IDENTIFIER, "d2021", TK_MINUS },

            /* instructions */
            { "\\instr", TK_INSTRUCTION, "\\instr", TK_EOF },
            { "\\instr arg1\narg2", TK_INSTRUCTION, "\\instr arg1\narg2", TK_EOF },
            { "\\instr arg1 arg2 \n  arg3 ", TK_INSTRUCTION, "\\instr arg1 arg2 \n  arg3 ", TK_EOF },
        };

        for (auto e : expr) {
            LEXER(std::get<0>(e));

            auto tok = lexer.next();
            if (tok.type != std::get<1>(e))
                std::cerr << "expected " << std::get<1>(e) << ", got " << tok.type << " for expression "
                          << std::get<0>(e) << std::endl;
            REQUIRE(tok.type == std::get<1>(e));

            if (!streq(tok.text, std::get<2>(e)))
                std::cerr << "expected " << std::get<2>(e) << ", got " << tok.text << " for expression "
                          << std::get<0>(e) << std::endl;
            REQUIRE(streq(tok.text, std::get<2>(e)));

            tok = lexer.next();
            if (tok.type != std::get<3>(e))
                std::cerr << "expected " << std::get<3>(e) << ", got " << tok.type << " for expression "
                          << std::get<0>(e) << std::endl;
            REQUIRE(tok.type == std::get<3>(e));
        }
    }

    SECTION("invalid tokens")
    {
        SECTION("invalid characters")
        {
            const char *chars[] = {
                ":", "!", "§", "$", "&", "{", "}", "[", "]", "?", "#", "|", "ä", "ö", "ü", "Ä", "Ö", "Ü",
                "\u0080", "\u00FF", "\u00BF", "\u00C0", "\u0001", "\u0006", "\u0007", "\u007F"
            };

            for (auto c : chars) {
                LEXER(c);

                auto tok = lexer.next();
                if (tok.type != TK_ERROR)
                    std::cerr << "expected TK_ERROR, got " << tok.type << " for character " << c << std::endl;
                REQUIRE(tok.type == TK_ERROR);
            }
        }

        SECTION("valid characters") {
            std::tuple<const char *, const char *, const TokenType> expr[] = {
                /* { expression, text, nextToken } */

                /* invalid numbers */
                { "08", "08", TK_EOF },
                { "9e", "9e", TK_EOF },
                { "9ef", "9e", TK_IDENTIFIER },
                { "0xfpa", "0xfp", TK_IDENTIFIER },
                { "0xfp", "0xfp", TK_EOF },
                { "0xg", "0x", TK_IDENTIFIER },

                /* invalid string literals */
                { "\"this is not a string literal", "\"this is not a string literal", TK_EOF },
                { "\"this is \\x not a string literal\"", "\"this is \\x not a string literal\"", TK_EOF },

                /* invalid date constants */
                { "'2021-01-29'", "'", TK_DEC_INT },
                { "d'2021-01-29", "d'2021-01-29", TK_EOF },
                { "d'2021-1-29'", "d'2021-1-29'", TK_EOF },
                { "d'2021-01-1'", "d'2021-01-1'", TK_EOF },
                { "d'221-01-29'", "d'221-01-29'", TK_EOF },
                { "d'02021-01-29'", "d'02021-01", TK_MINUS },
                { "d'2021-01-029'", "d'2021-01-02", TK_DEC_INT },
                { "d'2021:01-29'", "d'2021", TK_ERROR },
                { "d'2021-01:29'", "d'2021-01", TK_ERROR },

                /* invalid datetime constants */
                { "'2021-01-29 12:17:49'", "'", TK_DEC_INT },
                { "d'2021-01-29 12:17:49", "d'2021-01-29 12:17:49", TK_EOF },
                { "d'2021-01-29 1:17:49'", "d'2021-01-29 1:17:49'", TK_EOF },
                { "d'2021-01-29 12:1:49'", "d'2021-01-29 12:1:49'", TK_EOF },
                { "d'2021-01-29 12:17:9'", "d'2021-01-29 12:17:9'", TK_EOF },
                { "d'2021-01-29 012:17:49'", "d'2021-01-29 012:17", TK_ERROR },
                { "d'2021-01-29 12:17:049'", "d'2021-01-29 12:17:04", TK_DEC_INT },
                { "d'2021-01-29 12-17:49'", "d'2021-01-29 12", TK_MINUS },
                { "d'2021-01-29 12:17-49'", "d'2021-01-29 12:17", TK_MINUS },
                { "d'2021-01-29,12:17:49'", "d'2021-01-29", TK_COMMA },
            };

            for (auto e : expr) {
                LEXER(std::get<0>(e));

                auto tok = lexer.next();
                if (tok.type != TK_ERROR)
                    std::cerr << "expected TK_ERROR, got " << tok.type << " for expression "
                              << std::get<0>(e) << std::endl;
                REQUIRE(tok.type == TK_ERROR);

                if (!streq(tok.text, std::get<1>(e)))
                    std::cerr << "expected " << std::get<1>(e) << ", got " << tok.text << " for expression "
                              << std::get<0>(e) << std::endl;
                REQUIRE(streq(tok.text, std::get<1>(e)));

                tok = lexer.next();
                if (tok.type != std::get<2>(e))
                    std::cerr << "expected " << std::get<2>(e) << ", got " << tok.type << " for expression "
                              << std::get<0>(e) << std::endl;
                REQUIRE(tok.type == std::get<2>(e));
            }
        }
    }
}

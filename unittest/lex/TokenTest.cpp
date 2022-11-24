#include "catch2/catch.hpp"

#include <mutable/lex/Token.hpp>


using namespace m;
using namespace m::ast;


TEST_CASE("Token c'tor", "[core][lex][unit]")
{
    Position pos("the_file");
    Token tok(pos, "the_text", TK_ERROR);

    REQUIRE(pos == tok.pos);
    REQUIRE(streq("the_text", tok.text));
    REQUIRE(TK_ERROR == tok.type);
}

TEST_CASE("Token::bool()", "[core][lex][unit]")
{
    Position pos("the_file");

    {
        Token tok(pos, "the_text", TK_ERROR);
        REQUIRE(bool(tok));
    }

    {
        Token tok(pos, "the_text", TK_EOF);
        REQUIRE(not bool(tok));
    }
}

TEST_CASE("Token::TokenType()", "[core][lex][unit]")
{
    Position pos("the_file");
    Token tok(pos, "the_text", TK_ERROR);

    REQUIRE(TK_ERROR == TokenType(tok));
}

#include "catch.hpp"

#include "lex/Token.hpp"


using namespace db;


TEST_CASE("Token c'tor", "[unit][util]")
{
    Position pos("the_file");
    Token tok(pos, "the_text", TK_ERROR);

    REQUIRE(pos == tok.pos);
    REQUIRE(streq("the_text", tok.text));
    REQUIRE(TK_ERROR == tok.type);
}

TEST_CASE("Token::bool()", "[unit][util]")
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

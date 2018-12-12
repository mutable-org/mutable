#include "catch.hpp"

#include "parse/Parser.hpp"
#include "testutil.hpp"
#include "util/fn.hpp"


using namespace db;


TEST_CASE("Parser c'tor", "[unit][util]")
{
    LEXER("SELECT * FROM Tbl WHERE x=42;");
    Parser parser(lexer);
    auto &tok = parser.token();
    REQUIRE(tok == TK_Select);
    REQUIRE(streq(tok.text, "SELECT"));
}

TEST_CASE("Parser::no()", "[unit][util]")
{
    LEXER("SELECT * FROM Tbl WHERE x=42;");
    Parser parser(lexer);
    REQUIRE(parser.no(TK_And));
    REQUIRE(not parser.no(TK_Select));
}

TEST_CASE("Parser::consume()", "[unit][util]")
{
    LEXER("SELECT * FROM Tbl WHERE x=42;");
    Parser parser(lexer);
    REQUIRE(parser.token() == TK_Select);
    parser.consume();
    REQUIRE(parser.token() == TK_ASTERISK);
}

TEST_CASE("Parser::accept()", "[unit][util]")
{
    LEXER("SELECT * FROM Tbl WHERE x=42;");
    Parser parser(lexer);
    REQUIRE(not parser.accept(TK_And));
    REQUIRE(parser.accept(TK_Select));
    REQUIRE(parser.token() == TK_ASTERISK);
}

TEST_CASE("Parser::expect()", "[unit][util]")
{
    LEXER("SELECT * FROM Tbl WHERE x=42;");
    Parser parser(lexer);

    /* Check no errors occured so far. */
    REQUIRE(diag.num_errors() == 0);
    REQUIRE(err.str().empty());

    /* Trigger an error by expecting the wrong token. */
    parser.expect(TK_And);
    REQUIRE(diag.num_errors() > 0);
    REQUIRE(not err.str().empty());

    /* Clear the errors for the next test case. */
    err.str("");
    REQUIRE(diag.num_errors() == 0);
    REQUIRE(err.str().empty());

    /* Expect the correct token. */
    parser.expect(TK_Select);
    REQUIRE(diag.num_errors() == 0);
    REQUIRE(err.str().empty());
}

TEST_CASE("Parser::expect_consume()", "[unit][util]")
{
    LEXER("SELECT * FROM Tbl WHERE x=42;");
    Parser parser(lexer);

    /* Check no errors occured so far. */
    REQUIRE(diag.num_errors() == 0);
    REQUIRE(err.str().empty());

    /* Trigger an error by expecting the wrong token. */
    parser.expect_consume(TK_And);
    REQUIRE(diag.num_errors() > 0);
    REQUIRE(not err.str().empty());

    /* Verify that the token has been consumed. */
    REQUIRE(parser.token() == TK_ASTERISK);

    /* Clear the errors for the next test case. */
    err.str("");
    REQUIRE(diag.num_errors() == 0);
    REQUIRE(err.str().empty());

    /* Expect the correct token. */
    parser.expect_consume(TK_ASTERISK);
    REQUIRE(diag.num_errors() == 0);
    REQUIRE(err.str().empty());
}

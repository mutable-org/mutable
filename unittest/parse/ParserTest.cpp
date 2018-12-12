#include "catch.hpp"

#include "parse/Parser.hpp"
#include "testutil.hpp"
#include "util/fn.hpp"


using namespace db;


/*======================================================================================================================
 * Test parser utility methods.
 *====================================================================================================================*/

TEST_CASE("Parser c'tor", "[unit][util]")
{
    LEXER("SELECT * FROM Tbl WHERE x=42;");
    Parser parser(lexer);

    /* Check no errors occured so far. */
    REQUIRE(diag.num_errors() == 0);
    REQUIRE(not in.eof());
    REQUIRE(out.str().empty());
    REQUIRE(err.str().empty());

    /* Verify the initial state. */
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

/*======================================================================================================================
 * Test miscellaneous parser routines.
 *====================================================================================================================*/

TEST_CASE("Parser::parse_designator()", "[unit][util]")
{
    LEXER("a.b");
    Parser parser(lexer);
    parser.parse_designator();
    REQUIRE(diag.num_errors() == 0);
    REQUIRE(err.str().empty());
    REQUIRE(parser.token() == TK_EOF);
}

TEST_CASE("Parser::expect_integer()", "[unit][util]")
{
    LEXER("07 19 0xC0d3 abc");
    Parser parser(lexer);

    /* 07 TK_OCT_INT */
    parser.expect_integer();
    REQUIRE(diag.num_errors() == 0);
    REQUIRE(err.str().empty());
    REQUIRE(parser.token() != TK_EOF);

    /* 19 TK_DEC_INT */
    parser.expect_integer();
    REQUIRE(diag.num_errors() == 0);
    REQUIRE(err.str().empty());
    REQUIRE(parser.token() != TK_EOF);

    /* 0xC0d3 TK_HEX_INT */
    parser.expect_integer();
    REQUIRE(diag.num_errors() == 0);
    REQUIRE(err.str().empty());
    REQUIRE(parser.token() != TK_EOF);

    /* abc - unexpected token */
    parser.expect_integer();
    REQUIRE(diag.num_errors() > 0);
    REQUIRE(not err.str().empty());
    REQUIRE(parser.token() == TK_IDENTIFIER);
}

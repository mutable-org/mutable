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
    diag.clear();
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
    diag.clear();
    err.str("");

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

TEST_CASE("Parser::parse_Expr()", "[unit][util]")
{
    const char *exprs[] = {
        /* primary expression */
        "id", "42", "(id)",
        /* postfix expression */
        "id()", "id()", "id(42)", "id(42, 13, 1337)", "id(x)(y)(z)",
        /* unary expression */
        "+id", "-id", "~id", "+-~-+id", "+id()",
        /* multiplicative expression */
        "a*b", "a/b", "a%b", "a*b*c", "a*(b*c)", "-a*+b",
        /* additive expression */
        "a+b", "a-b", "a+b+c", "a+(b+c)", "a++b", "a- -b",
        /* comparative expression */
        "a=b", "a!=b", "a>b", "a<b", "a<=b", "a>=b", "a<b<c", "a<(b<c)",
        /* logical NOT expression */
        "NOT a", "NOT NOT a", "NOT NOT a=b",
        /* logical AND expression */
        "a AND b", "a AND b AND c", "a AND (b AND c)", "NOT a AND NOT NOT b AND NOT c",
        /* logical OR expression */
        "a OR b", "a OR b OR c", "a OR (b OR c)", "a AND b OR c AND d OR e AND f",
    };

    for (auto expr : exprs) {
        LEXER(expr);
        Parser parser(lexer);
        parser.parse_Expr();
        if (diag.num_errors())
            std::cerr << "ERROR for input \"" << expr << "\": " << err.str() << std::endl;
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
    }
}

TEST_CASE("Parser::parse_Expr() sanity tests", "[unit][util]")
{
    const char *exprs[] = {
        /* primary expression */
        "(", "(id", "()",
        /* postfix expression */
        "id(", "id(42,", "id(42 13)", "id(42,,13)",
        /* unary expression */
        "+", "+(", "+(id",
        /* multiplicative expression */
        "*", "a*", "*a", "a**b", "a*(",
        /* additive expression */
        "+", "a+", "a+(",
        /* comparative expression */
        "=", "a=", "a=(", "a==b", "a!", "a=<b",
        /* logical NOT expression */
        "NOT", "NOT NOT", "NOT NOT (", "NOT +",
        /* logical AND expression */
        "AND", "a AND", "AND a", "a AND AND b",
        /* logical OR expression */
        "OR", "a OR", "OR a", "a OR OR b",
    };

    for (auto expr : exprs) {
        LEXER(expr);
        Parser parser(lexer);
        parser.parse_Expr();
        if (not diag.num_errors())
            std::cerr << "UNEXPECTED PASS for input \"" << expr << '"' << std::endl;
        CHECK(diag.num_errors() > 0);
        CHECK(not err.str().empty());
    }
}

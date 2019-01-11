#include "catch.hpp"

#include "catalog/Schema.hpp"
#include "parse/ASTPrinter.hpp"
#include "parse/Parser.hpp"
#include "testutil.hpp"
#include "util/fn.hpp"


using namespace db;


/*======================================================================================================================
 * Test parser utility methods.
 *====================================================================================================================*/

TEST_CASE("Parser c'tor", "[unit]")
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

TEST_CASE("Parser::no()", "[unit]")
{
    LEXER("SELECT * FROM Tbl WHERE x=42;");
    Parser parser(lexer);
    REQUIRE(parser.no(TK_And));
    REQUIRE(not parser.no(TK_Select));
}

TEST_CASE("Parser::consume()", "[unit]")
{
    LEXER("SELECT * FROM Tbl WHERE x=42;");
    Parser parser(lexer);
    REQUIRE(parser.token() == TK_Select);
    parser.consume();
    REQUIRE(parser.token() == TK_ASTERISK);
}

TEST_CASE("Parser::accept()", "[unit]")
{
    LEXER("SELECT * FROM Tbl WHERE x=42;");
    Parser parser(lexer);
    REQUIRE(not parser.accept(TK_And));
    REQUIRE(parser.accept(TK_Select));
    REQUIRE(parser.token() == TK_ASTERISK);
}

TEST_CASE("Parser::expect()", "[unit]")
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

/*======================================================================================================================
 * Test miscellaneous parser routines.
 *====================================================================================================================*/

TEST_CASE("Parser::parse_designator()", "[unit]")
{
    LEXER("a.b");
    Parser parser(lexer);
    auto ast = parser.parse_designator();
    REQUIRE(diag.num_errors() == 0);
    REQUIRE(err.str().empty());
    REQUIRE(parser.token() == TK_EOF);
    delete ast;
}

TEST_CASE("Parser::expect_integer()", "[unit]")
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

TEST_CASE("Parser::parse_Expr()", "[unit]")
{
    std::pair<const char*, const char*> exprs[] = {
        /* { expression , fully-parenthesized-expression } */
        /* primary expression */
        { "id", "id" },
        { "42", "42" },
        { "(id)", "id" },
        { "(((id)))", "id" },
        /* postfix expression */
        { "id()", "id()" },
        { "id()", "id()" },
        { "id(42)", "id(42)" },
        { "id(42, 13, 1337)", "id(42, 13, 1337)" },
        { "id(x)(y)(z)", "id(x)(y)(z)" },
        /* unary expression */
        { "+id", "(+id)" },
        { "-id", "(-id)" },
        { "~id", "(~id)" },
        { "+-~-+id", "(+(-(~(-(+id)))))" },
        { "+id()", "(+id())" },
        /* multiplicative expression */
        { "a*b", "(a * b)" },
        { "a/b", "(a / b)" },
        { "a%b", "(a % b)" },
        { "a*b*c", "((a * b) * c)" },
        { "a*(b*c)", "(a * (b * c))" },
        { "-a*+b", "((-a) * (+b))" },
        /* additive expression */
        { "a+b", "(a + b)" },
        { "a-b", "(a - b)" },
        { "a+b+c", "((a + b) + c)" },
        { "a+(b+c)", "(a + (b + c))" },
        { "a++b", "(a + (+b))" },
        { "a- -b", "(a - (-b))" },
        /* comparative expression */
        { "a=b", "(a = b)" },
        { "a!=b", "(a != b)" },
        { "a>b", "(a > b)" },
        { "a<b", "(a < b)" },
        { "a<=b", "(a <= b)" },
        { "a>=b", "(a >= b)" },
        { "a<b<c", "((a < b) < c)" },
        { "a<(b<c)", "(a < (b < c))" },
        /* logical NOT expression */
        { "NOT a", "(NOT a)" },
        { "NOT NOT a", "(NOT (NOT a))" },
        { "NOT NOT a=b", "(NOT (NOT (a = b)))" },
        /* logical AND expression */
        { "a AND b", "(a AND b)" },
        { "a AND b AND c", "((a AND b) AND c)" },
        { "a AND (b AND c)", "(a AND (b AND c))" },
        { "NOT a AND NOT NOT b AND NOT c", "(((NOT a) AND (NOT (NOT b))) AND (NOT c))" },
        /* logical OR expression */ /* logical OR expression */
        { "a OR b", "(a OR b)" },
        { "a OR b OR c", "((a OR b) OR c)" },
        { "a OR (b OR c)", "(a OR (b OR c))" },
        { "a AND b OR c AND d OR e AND f", "(((a AND b) OR (c AND d)) OR (e AND f))" },
    };

    for (auto e : exprs) {
        LEXER(e.first);
        Parser parser(lexer);
        auto ast = parser.parse_Expr();
        if (diag.num_errors())
            std::cerr << "ERROR for input \"" << e.first << "\": " << err.str() << std::endl;
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());

        std::ostringstream actual;
        ASTPrinter p(actual);
        p(*ast);
        CHECK(actual.str() == e.second);
        delete ast;
    }
}

TEST_CASE("Parser::parse_Expr() sanity tests", "[unit]")
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
        auto ast = parser.parse_Expr();
        if (not diag.num_errors())
            std::cerr << "UNEXPECTED PASS for input \"" << expr << '"' << std::endl;
        CHECK(diag.num_errors() > 0);
        CHECK(not err.str().empty());
        delete ast;
    }
}

TEST_CASE("Parser::parse_data_type()", "[unit]")
{
    SECTION("Boolean")
    {
        LEXER("BOOL");
        Parser parser(lexer);
        const Type *type = parser.parse_data_type();
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        REQUIRE(type == Type::Get_Boolean());
    }

    SECTION("Char(N)")
    {
        LEXER("CHAR(42)");
        Parser parser(lexer);
        const Type *type = parser.parse_data_type();
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        REQUIRE(type == Type::Get_Char(42));
    }

    SECTION("Varchar(N)")
    {
        LEXER("VARCHAR(42)");
        Parser parser(lexer);
        const Type *type = parser.parse_data_type();
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        REQUIRE(type == Type::Get_Varchar(42));
    }

    SECTION("Int(N)")
    {
        LEXER("INT(4)");
        Parser parser(lexer);
        const Type *type = parser.parse_data_type();
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        REQUIRE(type == Type::Get_Integer(4));
    }

    SECTION("Float")
    {
        LEXER("FLOAT");
        Parser parser(lexer);
        const Type *type = parser.parse_data_type();
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        REQUIRE(type == Type::Get_Float());
    }

    SECTION("Double")
    {
        LEXER("DOUBLE");
        Parser parser(lexer);
        const Type *type = parser.parse_data_type();
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        REQUIRE(type == Type::Get_Double());
    }

    SECTION("Decimal(p,s)")
    {
        LEXER("DECIMAL(10, 2)");
        Parser parser(lexer);
        const Type *type = parser.parse_data_type();
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        REQUIRE(type == Type::Get_Decimal(10, 2));
    }
}

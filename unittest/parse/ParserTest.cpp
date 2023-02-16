#include "catch2/catch.hpp"

#include "parse/ASTPrinter.hpp"
#include "parse/Parser.hpp"
#include "testutil.hpp"
#include <mutable/catalog/Schema.hpp>
#include <mutable/util/fn.hpp>


using namespace m;
using namespace m::ast;


/*======================================================================================================================
 * Test helper
 *====================================================================================================================*/

using test_triple_t = std::tuple<const char*, const char*, TokenType>;

/** Given a test triple of input, expected output, and the next token after parsing, and a callback function to invoke
 * the respective parser method, check that parsing succeeds. */
template<typename Expected, typename Base>
void test_parse_positive(test_triple_t triple, std::function<std::unique_ptr<Base>(ast::Parser&)> parse)
{
    auto [input, expected, tok_next] = triple;

    LEXER(input);
    ast::Parser parser(lexer);
    auto ast = parse(parser);

    CHECK(diag.num_errors() == 0);
    CHECK(err.str().empty());
    if (diag.num_errors()) {
        std::cerr << "ERROR for input \"" << input << "\": " << err.str() << std::endl;
        return;
    }

    CHECK(is<Expected>(ast.get()));
    if (not is<Expected>(ast.get())) {
        std::cerr << "Input \"" << input << "\" is not parsed as the expected type" << std::endl;
        return;
    }

    std::ostringstream actual;
    ast::ASTPrinter p(actual);
    p(*ast);

    CHECK(expected == actual.str());

    auto tok = parser.token();
    CHECK(tok == tok_next);
    if (tok != tok_next) {
        std::cerr << "Expected next token " << tok_next << ", got " << tok << " for input \"" << input << "\""
                  << std::endl;
    }
}


/*======================================================================================================================
 * Test parser utility methods.
 *====================================================================================================================*/

TEST_CASE("Parser c'tor", "[core][parse][unit]")
{
    LEXER("SELECT * FROM Tbl WHERE x=42;");
    ast::Parser parser(lexer);

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

TEST_CASE("Parser::is()", "[core][parse][unit]")
{
    LEXER("SELECT * FROM Tbl WHERE x=42;");
    ast::Parser parser(lexer);
    REQUIRE(not parser.is(TK_And));
    REQUIRE(parser.is(TK_Select));
}

TEST_CASE("Parser::no()", "[core][parse][unit]")
{
    LEXER("SELECT * FROM Tbl WHERE x=42;");
    ast::Parser parser(lexer);
    REQUIRE(parser.no(TK_And));
    REQUIRE(not parser.no(TK_Select));
}

TEST_CASE("Parser::consume()", "[core][parse][unit]")
{
    LEXER("SELECT * FROM Tbl WHERE x=42;");
    ast::Parser parser(lexer);
    REQUIRE(parser.token() == TK_Select);
    parser.consume();
    REQUIRE(parser.token() == TK_ASTERISK);
}

TEST_CASE("Parser::accept()", "[core][parse][unit]")
{
    {
        LEXER("SELECT * FROM Tbl WHERE x=42;");
        ast::Parser parser(lexer);
        REQUIRE(not parser.accept(TK_And));
        REQUIRE(parser.accept(TK_Select));
        REQUIRE(parser.token() == TK_ASTERISK);
    }

    {
        LEXER(":");
        ast::Parser parser(lexer);
        REQUIRE(parser.accept(TK_Select));
        REQUIRE(parser.token() == TK_EOF);
    }
}

TEST_CASE("Parser::expect()", "[core][parse][unit]")
{
    LEXER("SELECT * FROM Tbl WHERE x=42;");
    ast::Parser parser(lexer);

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
 * Test expressions.
 *====================================================================================================================*/

TEST_CASE("Parser::parse_designator()", "[core][parse][unit]")
{
    test_triple_t triples[] = {
        /* { designator , text } */
        { "a.b", "a.b", TK_EOF },
        { "ab", "ab", TK_EOF },
        { "_0a.b_42", "_0a.b_42", TK_EOF },
        { "a.42", "a", TK_DEC_FLOAT },
    };

    auto parse = [](ast::Parser &p) { return p.parse_designator(); };
    for (auto triple : triples)
        test_parse_positive<ast::Designator, ast::Expr>(triple, parse);
}

TEST_CASE("Parser::parse_designator() sanity tests", "[core][parse][unit]")
{
    const char* designators[] = {"0.b", "a."};

    for (auto d : designators) {
        LEXER(d);
        ast::Parser parser(lexer);
        auto ast = parser.parse_designator();
        if (not diag.num_errors())
            std::cerr << "UNEXPECTED PASS for input \"" << d << '"' << std::endl;
        CHECK(diag.num_errors() > 0);
        CHECK(not err.str().empty());
        if (not is<ast::ErrorExpr>(ast))
            std::cerr << "Input \"" << d << "\" is not parsed as ErrorExpr" << std::endl;
        CHECK(is<ast::ErrorExpr>(ast));
    }
}

TEST_CASE("Parser::expect_integer()", "[core][parse][unit]")
{
    LEXER("07 19 0xC0d3 abc");
    ast::Parser parser(lexer);

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

TEST_CASE("Parser::parse_Expr()", "[core][parse][unit]")
{
    auto parse = [](ast::Parser &p) { return p.parse_Expr(); };

    SECTION("Designator")
    {
        test_triple_t triples[] = {
            /* { expression , fully-parenthesized-expression, next token } */

            { "id", "id", TK_EOF },
            { "(id)", "id", TK_EOF },
            { "(((id)))", "id", TK_EOF }
        };

        for (auto triple : triples)
            test_parse_positive<ast::Designator, ast::Expr>(triple, parse);
    }

    SECTION("Constant")
    {
        test_triple_t triples[] = {
            /* { expression , fully-parenthesized-expression, next token } */

            { "NULL", "NULL", TK_EOF },
            { "TRUE", "TRUE", TK_EOF },
            { "FALSE", "FALSE", TK_EOF },
            { "\"string\"", "\"string\"", TK_EOF },
            { "01", "01", TK_EOF },
            { "42", "42", TK_EOF },
            { "0x1", "0x1", TK_EOF },
            { "0.", "0.", TK_EOF },
            { "0xA.", "0xA.", TK_EOF },
            { "d'2021-01-29'", "d'2021-01-29'", TK_EOF },
            { "d'2021-01-29 12:17:49'", "d'2021-01-29 12:17:49'", TK_EOF },
        };

        for (auto triple : triples)
            test_parse_positive<ast::Constant, ast::Expr>(triple, parse);
    }

    SECTION("FnApllicationExpr")
    {
        test_triple_t triples[] = {
            /* { expression , fully-parenthesized-expression, next token } */

            { "id()", "id()", TK_EOF },
            { "id(*)", "id()", TK_EOF },
            { "id(42)", "id(42)", TK_EOF },
            { "id(42, 13, 1337)", "id(42, 13, 1337)", TK_EOF },
            { "id(x)(y)(z)", "id(x)(y)(z)", TK_EOF },
            { "id(x)(0)(*)", "id(x)(0)()", TK_EOF }
        };

        for (auto triple : triples)
            test_parse_positive<ast::FnApplicationExpr, ast::Expr>(triple, parse);
    }

    SECTION("UnaryExpr")
    {
        test_triple_t triples[] = {
            /* { expression , fully-parenthesized-expression, next token } */

            /* unary expression */
            { "+id", "(+id)", TK_EOF },
            { "-id", "(-id)", TK_EOF },
            { "~id", "(~id)", TK_EOF },
            { "NOT id", "(NOT id)", TK_EOF },
            { "+-~-+id", "(+(-(~(-(+id)))))", TK_EOF },
            { "+id()", "(+id())", TK_EOF },
            /* logical NOT expression */
            { "NOT a", "(NOT a)", TK_EOF },
            { "NOT NOT a", "(NOT (NOT a))", TK_EOF },
            { "NOT NOT a=b", "(NOT (NOT (a = b)))", TK_EOF }
        };

        for (auto triple : triples)
            test_parse_positive<ast::UnaryExpr, Expr>(triple, parse);
    }

    SECTION("BinaryExpr")
    {
        test_triple_t triples[] = {
            /* { expression , fully-parenthesized-expression, next token } */

            /* multiplicative expression */
            { "a*b", "(a * b)", TK_EOF },
            { "a/b", "(a / b)", TK_EOF },
            { "a%b", "(a % b)", TK_EOF },
            { "a*b*c", "((a * b) * c)", TK_EOF },
            { "a*(b*c)", "(a * (b * c))", TK_EOF },
            { "-a*+b", "((-a) * (+b))", TK_EOF },
            /* additive expression */
            { "a+b", "(a + b)", TK_EOF },
            { "a-b", "(a - b)", TK_EOF },
            { "a+b+c", "((a + b) + c)", TK_EOF },
            { "a+(b+c)", "(a + (b + c))", TK_EOF },
            { "a++b", "(a + (+b))", TK_EOF },
            { "a- -b", "(a - (-b))", TK_EOF },
            /* comparative expression */
            { "a=b", "(a = b)", TK_EOF },
            { "a!=b", "(a != b)", TK_EOF },
            { "a>b", "(a > b)", TK_EOF },
            { "a<b", "(a < b)", TK_EOF },
            { "a<=b", "(a <= b)", TK_EOF },
            { "a>=b", "(a >= b)", TK_EOF },
            { "a<b<c", "((a < b) < c)", TK_EOF },
            { "a<(b<c)", "(a < (b < c))", TK_EOF },
            /* logical AND expression */
            { "a AND b", "(a AND b)", TK_EOF },
            { "a AND b AND c", "((a AND b) AND c)", TK_EOF },
            { "a AND (b AND c)", "(a AND (b AND c))", TK_EOF },
            { "NOT a AND NOT NOT b AND NOT c", "(((NOT a) AND (NOT (NOT b))) AND (NOT c))", TK_EOF },
            /* logical OR expression */ /* logical OR expression */
            { "a OR b", "(a OR b)", TK_EOF },
            { "a OR b OR c", "((a OR b) OR c)", TK_EOF },
            { "a OR (b OR c)", "(a OR (b OR c))", TK_EOF },
            { "a AND b OR c AND d OR e AND f", "(((a AND b) OR (c AND d)) OR (e AND f))", TK_EOF }
        };

        for (auto triple : triples)
            test_parse_positive<ast::BinaryExpr, ast::Expr>(triple, parse);
    }
}

TEST_CASE("Parser::parse_Expr() sanity tests", "[core][parse][unit]")
{
    const char *exprs[] = {
        /* primary expression */
        "(", "(id", "()",
        /* postfix expression */
        "id(", "id(42,", "id(42", "id(42 13)", "id(42,,13)", "id(42, *)",
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

    for (auto e : exprs) {
        LEXER(e);
        ast::Parser parser(lexer);
        auto ast = parser.parse_Expr();
        if (not diag.num_errors())
            std::cerr << "UNEXPECTED PASS for input \"" << e << '"' << std::endl;
        CHECK(diag.num_errors() > 0);
        CHECK_FALSE(err.str().empty());
    }
}

/*======================================================================================================================
 * Test data types.
 *====================================================================================================================*/

TEST_CASE("Parser::parse_data_type()", "[core][parse][unit]")
{
    SECTION("Boolean")
    {
        LEXER("BOOL");
        ast::Parser parser(lexer);
        const Type *type = parser.parse_data_type();
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        REQUIRE(type == Type::Get_Boolean(Type::TY_Scalar));
    }

    SECTION("Char(N)")
    {
        LEXER("CHAR(42)");
        ast::Parser parser(lexer);
        const Type *type = parser.parse_data_type();
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        REQUIRE(type == Type::Get_Char(Type::TY_Scalar, 42));
    }

    SECTION("Varchar(N)")
    {
        LEXER("VARCHAR(42)");
        ast::Parser parser(lexer);
        const Type *type = parser.parse_data_type();
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        REQUIRE(type == Type::Get_Varchar(Type::TY_Scalar, 42));
    }

    SECTION("Date")
    {
        LEXER("DATE");
        ast::Parser parser(lexer);
        const Type *type = parser.parse_data_type();
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        REQUIRE(type == Type::Get_Date(Type::TY_Scalar));
    }

    SECTION("Datetime")
    {
        LEXER("DATETIME");
        ast::Parser parser(lexer);
        const Type *type = parser.parse_data_type();
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        REQUIRE(type == Type::Get_Datetime(Type::TY_Scalar));
    }

    SECTION("Int(N)")
    {
        LEXER("INT(4)");
        ast::Parser parser(lexer);
        const Type *type = parser.parse_data_type();
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        REQUIRE(type == Type::Get_Integer(Type::TY_Scalar, 4));
    }

    SECTION("Float")
    {
        LEXER("FLOAT");
        ast::Parser parser(lexer);
        const Type *type = parser.parse_data_type();
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        REQUIRE(type == Type::Get_Float(Type::TY_Scalar));
    }

    SECTION("Double")
    {
        LEXER("DOUBLE");
        ast::Parser parser(lexer);
        const Type *type = parser.parse_data_type();
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        REQUIRE(type == Type::Get_Double(Type::TY_Scalar));
    }

    SECTION("DECIMAL(p,s)")
    {
        LEXER("DECIMAL(10, 2)");
        ast::Parser parser(lexer);
        const Type *type = parser.parse_data_type();
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        REQUIRE(type == Type::Get_Decimal(Type::TY_Scalar, 10, 2));
    }

    SECTION("DECIMAL(p)")
    {
        LEXER("DECIMAL(10)");
        ast::Parser parser(lexer);
        const Type *type = parser.parse_data_type();
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        REQUIRE(type == Type::Get_Decimal(Type::TY_Scalar, 10, 0));
    }
}

TEST_CASE("Parser::parse_data_type() sanity tests", "[core][parse][unit]")
{
    SECTION("underlying errors")
    {
        const char *types[] = {
            "INT(42",
            "INT(42,2)",
            "DECIMAL(10, 2, 42)"
        };

        for (auto t : types) {
            LEXER(t);
            ast::Parser parser(lexer);
            parser.parse_data_type();
            if (diag.num_errors() == 0)
                std::cerr << "UNEXPECTED PASS for input \"" << t << '"' << std::endl;
            CHECK(diag.num_errors() > 0);
            CHECK_FALSE(err.str().empty());
        }
    }

    SECTION("newly generated errors") {
        const char *types[] = {
            /* no data type */
            "default",
            /* invalid syntax */
            "INT(", "INT()", "INT(string)", "DECIMAL(42, string)", "DECIMAL(42,)", "DECIMAL(,42)",
            /* invalid length */
            "CHAR(0x42)", "CHAR(99999999999999999999)", "INT(0x42)", "INT(99999999999999999999)",
            "DECIMAL(0x10, 2)",
            "DECIMAL(10, 0x2)", "DECIMAL(99999999999999999999, 2)", "DECIMAL(10, 99999999999999999999)",
        };

        for (auto t : types) {
            LEXER(t);
            ast::Parser parser(lexer);
            const Type *type = parser.parse_data_type();
            if (diag.num_errors() == 0)
                std::cerr << "UNEXPECTED PASS for input \"" << t << '"' << std::endl;
            CHECK(diag.num_errors() > 0);
            CHECK_FALSE(err.str().empty());
            if (type != Type::Get_Error())
                std::cerr << "Input \"" << t << "\" is not parsed as ErrorType" << std::endl;
            CHECK(type == Type::Get_Error());
        }
    }
}

/*======================================================================================================================
 * Test clauses.
 *====================================================================================================================*/

TEST_CASE("Parser::parse_SelectClause()", "[core][parse][unit]")
{
    test_triple_t triples[] = {
        /* { select clause, fully-parenthesized select clause, next token } */
        { "SELECT *", "SELECT *", TK_EOF },
        { "SELECT id", "SELECT id", TK_EOF },
        { "SELECT T.id", "SELECT T.id", TK_EOF },
        { "SELECT *, id", "SELECT *, id", TK_EOF },
        { "SELECT key, val", "SELECT key, val", TK_EOF },
        { "SELECT 42", "SELECT 42", TK_EOF },
        { "SELECT key * val", "SELECT (key * val)", TK_EOF },
        { "SELECT (key * val)", "SELECT (key * val)", TK_EOF },
        { "SELECT id, (key * val)", "SELECT id, (key * val)", TK_EOF },
        { "SELECT id i", "SELECT id AS i", TK_EOF },
        { "SELECT id AS i", "SELECT id AS i", TK_EOF },
        { "SELECT *, id AS i", "SELECT *, id AS i", TK_EOF },
        { "SELECT key k, val v", "SELECT key AS k, val AS v", TK_EOF },
        { "SELECT key AS k, val AS v", "SELECT key AS k, val AS v", TK_EOF },
        { "SELECT key AS k, val", "SELECT key AS k, val", TK_EOF },
        { "SELECT 42 const", "SELECT 42 AS const", TK_EOF },
        { "SELECT 42 AS const", "SELECT 42 AS const", TK_EOF },
        { "SELECT key * val mul", "SELECT (key * val) AS mul", TK_EOF },
        { "SELECT key * val AS mul", "SELECT (key * val) AS mul", TK_EOF },
        { "SELECT (key * val) AS mul", "SELECT (key * val) AS mul", TK_EOF },
        { "SELECT id AS i, (key * val) AS mul", "SELECT id AS i, (key * val) AS mul", TK_EOF },
        { "SELECT * AS star", "SELECT *", TK_As },
        { "SELECT id as i", "SELECT id AS as", TK_IDENTIFIER },
        { "SELECT T.42", "SELECT T", TK_DEC_FLOAT },
    };

    auto parse = [](ast::Parser &p) { return p.parse_SelectClause(); };
    for (auto triple : triples)
        test_parse_positive<ast::SelectClause, ast::Clause>(triple, parse);
}

TEST_CASE("Parser::parse_SelectClause() sanity tests", "[core][parse][unit]")
{
    SECTION("underlying errors")
    {
        const char * clauses[] = {
            "FROM id",
            "select id",
            "SELECT id,",
            "SELECT id, *",
            "SELECT AS i"
        };

        for (auto c : clauses) {
            LEXER(c);
            ast::Parser parser(lexer);
            auto ast = parser.parse_SelectClause();
            if (diag.num_errors() == 0)
                std::cerr << "UNEXPECTED PASS for input \"" << c << '"' << std::endl;
            CHECK(diag.num_errors() > 0);
            CHECK_FALSE(err.str().empty());
        }
    }

    SECTION("newly generated errors")
    {
        const char * clauses[] = {
            "SELECT id AS 42",
            "SELECT id AS , val AS k"
        };

        for (auto c : clauses) {
            LEXER(c);
            ast::Parser parser(lexer);
            auto ast = parser.parse_SelectClause();
            if (diag.num_errors() == 0)
                std::cerr << "UNEXPECTED PASS for input \"" << c << '"' << std::endl;
            CHECK(diag.num_errors() > 0);
            CHECK_FALSE(err.str().empty());
            if (not is<ast::ErrorClause>(ast))
                std::cerr << "Input \"" << c << "\" is not parsed as ErrorClause" << std::endl;
            CHECK(is<ast::ErrorClause>(ast));
        }
    }
}

TEST_CASE("Parser::parse_FromClause()", "[core][parse][unit]")
{
    test_triple_t triples[] = {
        /* { from clause, fully-parenthesized from clause, next token } */

        /* non-nested queries */
        { "FROM A", "FROM A", TK_EOF },
        { "FROM A, B", "FROM A, B", TK_EOF },
        { "FROM A a", "FROM A AS a", TK_EOF },
        { "FROM A AS a", "FROM A AS a", TK_EOF },
        { "FROM A a, B b", "FROM A AS a, B AS b", TK_EOF },
        { "FROM A, B AS b", "FROM A, B AS b", TK_EOF },
        { "FROM A as a", "FROM A AS as", TK_IDENTIFIER },

        /* nested queries */
        { "FROM (SELECT * FROM A) sq", "FROM (SELECT *\nFROM A;) AS sq", TK_EOF },
        { "FROM (SELECT * FROM A) AS sq", "FROM (SELECT *\nFROM A;) AS sq", TK_EOF },
        { "FROM A, (SELECT * FROM A) AS sq", "FROM A, (SELECT *\nFROM A;) AS sq", TK_EOF },
        { "FROM A a, (SELECT * FROM A) AS sq", "FROM A AS a, (SELECT *\nFROM A;) AS sq", TK_EOF },
        { "FROM (SELECT * FROM A) AS sq, A", "FROM (SELECT *\nFROM A;) AS sq, A", TK_EOF },
        { "FROM (SELECT * FROM A) AS sq, A a", "FROM (SELECT *\nFROM A;) AS sq, A AS a", TK_EOF },
        { "FROM (SELECT id FROM a WHERE key < 10) AS sq_1, (SELECT * FROM A) sq_2",
          "FROM (SELECT id\nFROM a\nWHERE (key < 10);) AS sq_1, (SELECT *\nFROM A;) AS sq_2" , TK_EOF },
        { "FROM (SELECT * FROM A) as sq", "FROM (SELECT *\nFROM A;) AS as", TK_IDENTIFIER }
    };

    auto parse = [](ast::Parser &p) { return p.parse_FromClause(); };
    for (auto triple : triples)
        test_parse_positive<ast::FromClause, ast::Clause>(triple, parse);
}

TEST_CASE("Parser::parse_FromClause() sanity tests", "[core][parse][unit]")
{
    SECTION("underlying errors")
    {
        const char * clauses[] = {
            "SELECT A",
            "from A",
            "FROM A,",
            "FROM AS a"
        };

        for (auto c : clauses) {
            LEXER(c);
            ast::Parser parser(lexer);
            auto ast = parser.parse_FromClause();
            if (diag.num_errors() == 0)
                std::cerr << "UNEXPECTED PASS for input \"" << c << '"' << std::endl;
            CHECK(diag.num_errors() > 0);
            CHECK_FALSE(err.str().empty());
        }
    }

    SECTION("newly generated errors")
    {
        const char * clauses[] = {
            "FROM A AS 42",
            "FROM A AS 42, B AS b",
            "FROM 42",
            "FROM (SELECT * FROM R)"
        };

        for (auto c : clauses) {
            LEXER(c);
            ast::Parser parser(lexer);
            auto ast = parser.parse_FromClause();
            if (diag.num_errors() == 0)
                std::cerr << "UNEXPECTED PASS for input \"" << c << '"' << std::endl;
            CHECK(diag.num_errors() > 0);
            CHECK_FALSE(err.str().empty());
            if (not is<ast::ErrorClause>(ast))
                std::cerr << "Input \"" << c << "\" is not parsed as ErrorClause" << std::endl;
            CHECK(is<ast::ErrorClause>(ast));
        }
    }
}

TEST_CASE("Parser::parse_WhereClause()", "[core][parse][unit]")
{
    test_triple_t triples[] = {
            /* { where clause, fully-parenthesized where clause, next token } */

            { "WHERE TRUE", "WHERE TRUE", TK_EOF },
            { "WHERE id = 42", "WHERE (id = 42)", TK_EOF },
            { "WHERE A.val >= B.val AND FALSE != 10 * 5", "WHERE ((A.val >= B.val) AND (FALSE != (10 * 5)))", TK_EOF }
    };

    auto parse = [](ast::Parser &p) { return p.parse_WhereClause(); };
    for (auto triple : triples)
        test_parse_positive<ast::WhereClause, ast::Clause>(triple, parse);
}

TEST_CASE("Parser::parse_WhereClause() sanity tests", "[core][parse][unit]")
{
    SECTION("underlying errors")
    {
        const char * clauses[] = {
            "HAVING FALSE",
            "where FALSE",
            "WHERE FROM",
            "WHERE 10 : 5 = 2",
        };

        for (auto c : clauses) {
            LEXER(c);
            ast::Parser parser(lexer);
            auto ast = parser.parse_WhereClause();
            if (diag.num_errors() == 0)
                std::cerr << "UNEXPECTED PASS for input \"" << c << '"' << std::endl;
            CHECK(diag.num_errors() > 0);
            CHECK_FALSE(err.str().empty());
        }
    }
}

TEST_CASE("Parser::parse_GroupByClause()", "[core][parse][unit]")
{
    test_triple_t triples[] = {
            /* { group by clause, fully-parenthesized group by clause, next token } */

            { "GROUP BY TRUE", "GROUP BY TRUE", TK_EOF },
            { "GROUP BY A.id", "GROUP BY A.id", TK_EOF },
            { "GROUP BY A.val >= B.val AND FALSE != 10 * 5", "GROUP BY ((A.val >= B.val) AND (FALSE != (10 * 5)))",
              TK_EOF },
            { "GROUP BY key, B.val", "GROUP BY key, B.val", TK_EOF },
            { "GROUP BY key alias", "GROUP BY key AS alias", TK_EOF },
            { "GROUP BY key AS alias", "GROUP BY key AS alias", TK_EOF },
            { "GROUP BY a + b", "GROUP BY (a + b)", TK_EOF },
            { "GROUP BY a + b alias", "GROUP BY (a + b) AS alias", TK_EOF },
            { "GROUP BY a + b AS alias", "GROUP BY (a + b) AS alias", TK_EOF },
            { "GROUP BY a + b, c + d", "GROUP BY (a + b), (c + d)", TK_EOF },
            { "GROUP BY a + b x, c + d", "GROUP BY (a + b) AS x, (c + d)", TK_EOF },

            /*----- not fully parsed -----*/
            { "GROUP BY a + b c + d", "GROUP BY (a + b) AS c", TK_PLUS },
            { "GROUP BY a + b HAVING", "GROUP BY (a + b)", TK_Having },
            { "GROUP BY a + b x HAVING", "GROUP BY (a + b) AS x", TK_Having },
    };

    auto parse = [](ast::Parser &p) { return p.parse_GroupByClause(); };
    for (auto triple : triples)
        test_parse_positive<ast::GroupByClause, ast::Clause>(triple, parse);
}

TEST_CASE("Parser::parse_GroupByClause() sanity tests", "[core][parse][unit]")
{
    SECTION("underlying errors")
    {
        const char * clauses[] = {
            "HAVING BY id",
            "group BY id",
            "GROUP by id",
            "GROUPBY id",
            "GROUP BY id,",
            "GROUP BY FROM",
            "GROUP BY 10 : 5 = 2",
            "GROUP BY a + b AS",
        };

        for (auto c : clauses) {
            LEXER(c);
            ast::Parser parser(lexer);
            auto ast = parser.parse_GroupByClause();
            if (diag.num_errors() == 0)
                std::cerr << "UNEXPECTED PASS for input \"" << c << '"' << std::endl;
            CHECK(diag.num_errors() > 0);
            CHECK_FALSE(err.str().empty());
        }
    }
}

TEST_CASE("Parser::parse_HavingClause()", "[core][parse][unit]")
{
    test_triple_t triples[] = {
        /* { having clause, fully-parenthesized having clause, next token } */

        { "HAVING FALSE", "HAVING FALSE", TK_EOF },
        { "HAVING id = 42", "HAVING (id = 42)", TK_EOF },
        { "HAVING A.val >= B.val AND TRUE != 10 * 5", "HAVING ((A.val >= B.val) AND (TRUE != (10 * 5)))", TK_EOF }
    };

    auto parse = [](ast::Parser &p) { return p.parse_HavingClause(); };
    for (auto triple : triples)
        test_parse_positive<ast::HavingClause, ast::Clause>(triple, parse);
}

TEST_CASE("Parser::parse_HavingClause() sanity tests", "[core][parse][unit]")
{
    SECTION("underlying errors")
    {
        const char * clauses[] = {
            "WHERE TRUE",
            "having TRUE",
            "HAVING SELECT",
            "HAVING 10 : 5 = 2",
        };

        for (auto c : clauses) {
            LEXER(c);
            ast::Parser parser(lexer);
            auto ast = parser.parse_HavingClause();
            if (diag.num_errors() == 0)
                std::cerr << "UNEXPECTED PASS for input \"" << c << '"' << std::endl;
            CHECK(diag.num_errors() > 0);
            CHECK_FALSE(err.str().empty());
        }
    }
}

TEST_CASE("Parser::parse_OrderByClause()", "[core][parse][unit]")
{
    test_triple_t triples[] = {
        /* { order by clause, fully-parenthesized order by part, next token } */

        { "ORDER BY val", "ORDER BY val ASC", TK_EOF },
        { "ORDER BY val ASC", "ORDER BY val ASC", TK_EOF },
        { "ORDER BY val DESC", "ORDER BY val DESC", TK_EOF },
        { "ORDER BY A.val, B.val DESC", "ORDER BY A.val ASC, B.val DESC", TK_EOF },
        { "ORDER BY A.val DESC, B.val", "ORDER BY A.val DESC, B.val ASC", TK_EOF },
        { "ORDER BY A.val >= B.val AND FALSE != 10 * 5", "ORDER BY ((A.val >= B.val) AND (FALSE != (10 * 5))) ASC",
          TK_EOF },
        { "ORDER BY A.val B.val", "ORDER BY A.val ASC", TK_IDENTIFIER }
    };

    auto parse = [](ast::Parser &p) { return p.parse_OrderByClause(); };
    for (auto triple : triples)
        test_parse_positive<ast::OrderByClause, ast::Clause>(triple, parse);
}

TEST_CASE("Parser::parse_OrderByClause() sanity tests", "[core][parse][unit]")
{
    SECTION("underlying errors")
    {
        const char * clauses[] = {
            "GROUP BY val",
            "order BY val",
            "ORDER by val",
            "ORDERBY val",
            "ORDER BY val,",
            "ORDER BY FROM",
            "ORDER BY 10 : 5 = 2"
        };

        for (auto c : clauses) {
            LEXER(c);
            ast::Parser parser(lexer);
            auto ast = parser.parse_OrderByClause();
            if (diag.num_errors() == 0)
                std::cerr << "UNEXPECTED PASS for input \"" << c << '"' << std::endl;
            CHECK(diag.num_errors() > 0);
            CHECK_FALSE(err.str().empty());
        }
    }
}

TEST_CASE("Parser::parse_LimitClause()", "[core][parse][unit]")
{
    test_triple_t triples[] = {
        /* { limit clause, fully-parenthesized limit clause, next token } */

        { "LIMIT 1", "LIMIT 1", TK_EOF },
        { "LIMIT 1 OFFSET 2", "LIMIT 1 OFFSET 2", TK_EOF },
        { "LIMIT 01 OFFSET 0xA", "LIMIT 01 OFFSET 0xA", TK_EOF },
        { "LIMIT 0xA OFFSET 01", "LIMIT 0xA OFFSET 01", TK_EOF },
        { "LIMIT 1 offset 2", "LIMIT 1", TK_IDENTIFIER },
        { "LIMIT 1 2", "LIMIT 1", TK_DEC_INT },
        { "LIMIT 1, OFFSET 2", "LIMIT 1", TK_COMMA }
    };

    auto parse = [](ast::Parser &p) { return p.parse_LimitClause(); };
    for (auto triple : triples)
        test_parse_positive<ast::LimitClause, ast::Clause>(triple, parse);
}

TEST_CASE("Parser::parse_LimitClause() sanity tests", "[core][parse][unit]")
{
    SECTION("underlying errors")
    {
        const char * clauses[] = {
            "WHERE TRUE",
            "OFFSET 1",
            "1 OFFSET 2",
            "limit 1"
        };

        for (auto c : clauses) {
            LEXER(c);
            ast::Parser parser(lexer);
            auto ast = parser.parse_LimitClause();
            if (diag.num_errors() == 0)
                std::cerr << "UNEXPECTED PASS for input \"" << c << '"' << std::endl;
            CHECK(diag.num_errors() > 0);
            CHECK_FALSE(err.str().empty());
        }
    }

    SECTION("newly generated errors")
    {
        const char * clauses[] = {
            "LIMIT string",
            "LIMIT TRUE",
            "LIMIT 1.5",
            "LIMIT 1 OFFSET 2.5",
            "LIMIT",
            "LIMIT 1 OFFSET"
        };

        for (auto c : clauses) {
            LEXER(c);
            ast::Parser parser(lexer);
            auto ast = parser.parse_LimitClause();
            if (diag.num_errors() == 0)
                std::cerr << "UNEXPECTED PASS for input \"" << c << '"' << std::endl;
            CHECK(diag.num_errors() > 0);
            CHECK_FALSE(err.str().empty());
            if (not is<ast::ErrorClause>(ast))
                std::cerr << "Input \"" << c << "\" is not parsed as ErrorClause" << std::endl;
            CHECK(is<ast::ErrorClause>(ast));
        }
    }
}

/*======================================================================================================================
 * Test statements.
 *====================================================================================================================*/

TEST_CASE("Parser::parse_SelectStmt()", "[core][parse][unit]")
{
    test_triple_t triples[] = {
        /* { select statement, fully-parenthesized select statement, next token } */

        { "SELECT 42", "SELECT 42;", TK_EOF },
        { "SELECT * FROM A", "SELECT *\nFROM A;", TK_EOF },
        { "SELECT * WHERE TRUE", "SELECT *\nWHERE TRUE;", TK_EOF },
        { "SELECT * GROUP BY a", "SELECT *\nGROUP BY a;", TK_EOF },
        { "SELECT * HAVING FALSE", "SELECT *\nHAVING FALSE;", TK_EOF },
        { "SELECT * ORDER BY a", "SELECT *\nORDER BY a ASC;", TK_EOF },
        { "SELECT * LIMIT 1", "SELECT *\nLIMIT 1;", TK_EOF },
        { "SELECT * FROM A WHERE TRUE GROUP BY a HAVING FALSE ORDER BY a LIMIT 1",
          "SELECT *\nFROM A\nWHERE TRUE\nGROUP BY a\nHAVING FALSE\nORDER BY a ASC\nLIMIT 1;", TK_EOF },
        { "SELECT 42;", "SELECT 42;", TK_SEMICOL },
        { "SELECT * WHERE TRUE, GROUP BY a", "SELECT *\nWHERE TRUE;", TK_COMMA },
    };

    auto parse = [](ast::Parser &p) { return p.parse_SelectStmt(); };
    for (auto triple : triples)
        test_parse_positive<ast::SelectStmt, ast::Stmt>(triple, parse);
}

TEST_CASE("Parser::parse_SelectStmt() sanity tests", "[core][parse][unit]")
{
    SECTION("underlying errors")
    {
        const char * statements[] = {
            "",
            "FROM A",
            "SELECT FROM",
            "SELECT * FROM WHERE",
            "SELECT * WHERE GROUP BY",
            "SELECT * GROUP BY HAVING",
            "SELECT * HAVING ORDER BY",
            "SELECT * ORDER BY LIMIT",
            "SELECT * LIMIT OFFSET",
            "select 42"
        };

        for (auto s : statements) {
            LEXER(s);
            ast::Parser parser(lexer);
            auto ast = parser.parse_SelectStmt();
            if (diag.num_errors() == 0)
                std::cerr << "UNEXPECTED PASS for input \"" << s << '"' << std::endl;
            CHECK(diag.num_errors() > 0);
            CHECK_FALSE(err.str().empty());
        }
    }
}

TEST_CASE("Parser::parse_CreateDatabaseStmt()", "[core][parse][unit]")
{
    test_triple_t triples[] = {
        /* { create database statement, fully-parenthesized create database statement, next token } */

        { "DATABASE d", "CREATE DATABASE d;", TK_EOF },
        { "DATABASE d, second", "CREATE DATABASE d;", TK_COMMA }
    };

    auto parse = [](ast::Parser &p) { return p.parse_CreateDatabaseStmt(); };
    for (auto triple : triples)
        test_parse_positive<ast::CreateDatabaseStmt, ast::Stmt>(triple, parse);
}

TEST_CASE("Parser::parse_CreateDatabaseStmt() sanity tests", "[core][parse][unit]")
{
    SECTION("underlying errors")
    {
        const char * statements[] = {
            "CREATE DATABASE d",
            "database d",
            "TABLE d"
        };

        for (auto s : statements) {
            LEXER(s);
            ast::Parser parser(lexer);
            auto ast = parser.parse_CreateDatabaseStmt();
            if (diag.num_errors() == 0)
                std::cerr << "UNEXPECTED PASS for input \"" << s << '"' << std::endl;
            CHECK(diag.num_errors() > 0);
            CHECK_FALSE(err.str().empty());
        }
    }

    SECTION("newly generated errors")
    {
        const char * statements[] = {
            "",
            "DATABASE ",
            "DATABASE 0"
        };

        for (auto s : statements) {
            LEXER(s);
            ast::Parser parser(lexer);
            auto ast = parser.parse_CreateDatabaseStmt();
            if (diag.num_errors() == 0)
                std::cerr << "UNEXPECTED PASS for input \"" << s << '"' << std::endl;
            CHECK(diag.num_errors() > 0);
            CHECK_FALSE(err.str().empty());
            if (not is<ast::ErrorStmt>(ast))
                std::cerr << "Input \"" << s << "\" is not parsed as ErrorStmt" << std::endl;
            CHECK(is<ast::ErrorStmt>(ast));
        }
    }
}

TEST_CASE("Parser::parse_UseDatabaseStmt()", "[core][parse][unit]")
{
    test_triple_t triples[] = {
        /* { use database statement, fully-parenthesized use database statement, next token } */

        { "USE d", "USE d;", TK_EOF },
        { "USE d, second", "USE d;", TK_COMMA }
    };

    auto parse = [](ast::Parser &p) { return p.parse_UseDatabaseStmt(); };
    for (auto triple : triples)
        test_parse_positive<ast::UseDatabaseStmt, ast::Stmt>(triple, parse);
}

TEST_CASE("Parser::parse_UseDatabaseStmt() sanity tests", "[core][parse][unit]")
{
    SECTION("underlying errors")
    {
        const char * statements[] = {
            "USE DATABASE d",
            "use d"
        };

        for (auto s : statements) {
            LEXER(s);
            ast::Parser parser(lexer);
            auto ast = parser.parse_UseDatabaseStmt();
            if (diag.num_errors() == 0)
                std::cerr << "UNEXPECTED PASS for input \"" << s << '"' << std::endl;
            CHECK(diag.num_errors() > 0);
            CHECK_FALSE(err.str().empty());
        }
    }

    SECTION("newly generated errors")
    {
        const char * statements[] = {
            "",
            "USE ",
            "USE 0"
        };

        for (auto s : statements) {
            LEXER(s);
            ast::Parser parser(lexer);
            auto ast = parser.parse_UseDatabaseStmt();
            if (diag.num_errors() == 0)
                std::cerr << "UNEXPECTED PASS for input \"" << s << '"' << std::endl;
            CHECK(diag.num_errors() > 0);
            CHECK_FALSE(err.str().empty());
            if (not is<ast::ErrorStmt>(ast))
                std::cerr << "Input \"" << s << "\" is not parsed as ErrorStmt" << std::endl;
            CHECK(is<ast::ErrorStmt>(ast));
        }
    }
}

TEST_CASE("Parser::parse_CreateTableStmt()", "[core][parse][unit]")
{
    test_triple_t triples[] = {
        /* { create table statement, fully-parenthesized create table statement, next token } */

        { "TABLE t ( a BOOL )", "CREATE TABLE t\n(\n    a BOOL\n);", TK_EOF },
        { "TABLE t ( a BOOL PRIMARY KEY )", "CREATE TABLE t\n(\n    a BOOL PRIMARY KEY\n);", TK_EOF },
        { "TABLE t ( a BOOL NOT NULL )", "CREATE TABLE t\n(\n    a BOOL NOT NULL\n);", TK_EOF },
        { "TABLE t ( a BOOL UNIQUE )", "CREATE TABLE t\n(\n    a BOOL UNIQUE\n);", TK_EOF },
        { "TABLE t ( a BOOL CHECK ( 42 * b ) )", "CREATE TABLE t\n(\n    a BOOL CHECK ((42 * b))\n);", TK_EOF },
        { "TABLE t ( a BOOL REFERENCES B ( a ) )", "CREATE TABLE t\n(\n    a BOOL REFERENCES B(a)\n);", TK_EOF },
        { "TABLE t ( a BOOL PRIMARY KEY NOT NULL UNIQUE CHECK ( 42 * b ) REFERENCES B ( a ) )",
          "CREATE TABLE t\n(\n    a BOOL PRIMARY KEY NOT NULL UNIQUE CHECK ((42 * b)) REFERENCES B(a)\n);", TK_EOF },
        { "TABLE t ( a BOOL, b DOUBLE )", "CREATE TABLE t\n(\n    a BOOL,\n    b DOUBLE\n);", TK_EOF },
    };

    auto parse = [](ast::Parser &p) { return p.parse_CreateTableStmt(); };
    for (auto triple : triples)
        test_parse_positive<ast::CreateTableStmt, ast::Stmt>(triple, parse);
}

TEST_CASE("Parser::parse_CreateTableStmt() sanity tests", "[core][parse][unit]")
{
    SECTION("underlying errors")
    {
        const char * statements[] = {
            "CREATE TABLE t ( a BOOL )",
            "table t ( a BOOL )",
            "DATABASE t ( a BOOL )",
            "TABLE t ( a )",
            "TABLE t ( a INT )",
            "TABLE t ( a BOOL PRIMARY )",
            "TABLE t ( a BOOL NOT )",
            "TABLE t ( a BOOL CHECK TRUE) )",
            "TABLE t ( a BOOL CHECK (TRUE )",
            "TABLE t ( a BOOL CHECK () )",
            "TABLE t ( a BOOL REFERENCES a )",
            "TABLE t ( a BOOL REFERENCES () )",
            "TABLE t ( a BOOL REFERENCES ( 0 ) )",
            "TABLE t ( a BOOL REFERENCES a b) )",
            "TABLE t ( a BOOL REFERENCES a (b )",
            "TABLE t a BOOL )",
            "TABLE t ( a BOOL "
        };

        for (auto s : statements) {
            LEXER(s);
            ast::Parser parser(lexer);
            auto ast = parser.parse_CreateTableStmt();
            if (diag.num_errors() == 0)
                std::cerr << "UNEXPECTED PASS for input \"" << s << '"' << std::endl;
            CHECK(diag.num_errors() > 0);
            CHECK_FALSE(err.str().empty());
        }
    }

    SECTION("newly generated errors")
    {
        const char * statements[] = {
            "",
            "TABLE ( a BOOL )",
            "TABLE 0 ( a BOOL )",
            "TABLE t ( 0 BOOL )",
            "TABLE t ( BOOL )",
            "TABLE t ( a BOOL, )"
        };

        for (auto s : statements) {
            LEXER(s);
            ast::Parser parser(lexer);
            auto ast = parser.parse_CreateTableStmt();
            if (diag.num_errors() == 0)
                std::cerr << "UNEXPECTED PASS for input \"" << s << '"' << std::endl;
            CHECK(diag.num_errors() > 0);
            CHECK_FALSE(err.str().empty());
            if (not is<ast::ErrorStmt>(ast))
                std::cerr << "Input \"" << s << "\" is not parsed as ErrorStmt" << std::endl;
            CHECK(is<ast::ErrorStmt>(ast));
        }
    }
}

TEST_CASE("Parser::parse_InsertStmt()", "[core][parse][unit]")
{
    test_triple_t triples[] = {
        /* { insert statement, fully-parenthesized insert statement, next token } */

        { "INSERT INTO a VALUES (DEFAULT)", "INSERT INTO a\nVALUES\n    (DEFAULT);", TK_EOF },
        { "INSERT INTO a VALUES (NULL)", "INSERT INTO a\nVALUES\n    (NULL);", TK_EOF },
        { "INSERT INTO a VALUES (42 * b)", "INSERT INTO a\nVALUES\n    ((42 * b));", TK_EOF },
        { "INSERT INTO a VALUES (42, NULL, DEFAULT)", "INSERT INTO a\nVALUES\n    (42, NULL, DEFAULT);", TK_EOF },
        { "INSERT INTO a VALUES (42), (DEFAULT, 17), (NULL)",
          "INSERT INTO a\nVALUES\n    (42),\n    (DEFAULT, 17),\n    (NULL);", TK_EOF },
        { "INSERT INTO a VALUES (42) (17)", "INSERT INTO a\nVALUES\n    (42);", TK_LPAR }
    };

    auto parse = [](ast::Parser &p) { return p.parse_InsertStmt(); };
    for (auto triple : triples)
        test_parse_positive<ast::InsertStmt, ast::Stmt>(triple, parse);
}

TEST_CASE("Parser::parse_InsertStmt() sanity tests", "[core][parse][unit]")
{
    SECTION("underlying errors")
    {
        const char * statements[] = {
            "INSERT INTO a (42)",
            "INSERT a VALUES (42)",
            "insert INTO a VALUES (42)",
            "INSERT into a VALUES (42)",
            "INSERT INTO a values (42)",
            "INSERT INTO a VALUES (10 : 5)",
            "INSERT INTO a VALUES ",
            "INSERT INTO a VALUES ()",
            "INSERT INTO a VALUES 42)",
            "INSERT INTO a VALUES (42"
        };

        for (auto s : statements) {
            LEXER(s);
            ast::Parser parser(lexer);
            auto ast = parser.parse_InsertStmt();
            if (diag.num_errors() == 0)
                std::cerr << "UNEXPECTED PASS for input \"" << s << '"' << std::endl;
            CHECK(diag.num_errors() > 0);
            CHECK_FALSE(err.str().empty());
        }
    }

    SECTION("newly generated errors")
    {
        const char * statements[] = {
            "INSERT INTO VALUES (NULL)",
            "INSERT INTO VALUES (42)"
        };

        for (auto s : statements) {
            LEXER(s);
            ast::Parser parser(lexer);
            auto ast = parser.parse_InsertStmt();
            if (diag.num_errors() == 0)
                std::cerr << "UNEXPECTED PASS for input \"" << s << '"' << std::endl;
            CHECK(diag.num_errors() > 0);
            CHECK_FALSE(err.str().empty());
            if (not is<ErrorStmt>(ast))
                std::cerr << "Input \"" << s << "\" is not parsed as ErrorStmt" << std::endl;
            CHECK(is<ErrorStmt>(ast));
        }
    }
}

TEST_CASE("Parser::parse_UpdateStmt()", "[core][parse][unit]")
{
    test_triple_t triples[] = {
        /* { update statement, fully-parenthesized update statement, next token } */

        { "UPDATE A SET a = 42 * b", "UPDATE A\nSET\n    a = (42 * b);", TK_EOF },
        { "UPDATE A SET a = 42 * b, b = TRUE, c = NOT a",
          "UPDATE A\nSET\n    a = (42 * b),\n    b = TRUE,\n    c = (NOT a);", TK_EOF },
        { "UPDATE A SET a = 42 * b WHERE a > 10", "UPDATE A\nSET\n    a = (42 * b)\nWHERE (a > 10);", TK_EOF },
        { "UPDATE A SET a = 42 * b b = TRUE", "UPDATE A\nSET\n    a = (42 * b);", TK_IDENTIFIER },
        { "UPDATE A SET a = 42 where TRUE", "UPDATE A\nSET\n    a = 42;", TK_IDENTIFIER }
    };

    auto parse = [](Parser &p) { return p.parse_UpdateStmt(); };
    for (auto triple : triples)
        test_parse_positive<UpdateStmt, Stmt>(triple, parse);
}

TEST_CASE("Parser::parse_UpdateStmt() sanity tests", "[core][parse][unit]")
{
    SECTION("underlying errors")
    {
        const char * statements[] = {
            "UPDATE A SET a < 42",
            "UPDATE A SET a",
            "update A SET a = 42",
            "UPDATE A set a = 42"
        };

        for (auto s : statements) {
            LEXER(s);
            Parser parser(lexer);
            auto ast = parser.parse_UpdateStmt();
            if (diag.num_errors() == 0)
                std::cerr << "UNEXPECTED PASS for input \"" << s << '"' << std::endl;
            CHECK(diag.num_errors() > 0);
            CHECK_FALSE(err.str().empty());
        }
    }

    SECTION("newly generated errors")
    {
        const char * statements[] = {
                "UPDATE SET fkey = 42",
                "UPDATE R WHERE TRUE"
        };

        for (auto s : statements) {
            LEXER(s);
            Parser parser(lexer);
            auto ast = parser.parse_UpdateStmt();
            if (diag.num_errors() == 0)
                std::cerr << "UNEXPECTED PASS for input \"" << s << '"' << std::endl;
            CHECK(diag.num_errors() > 0);
            CHECK_FALSE(err.str().empty());
            if (not is<ErrorStmt>(ast))
                std::cerr << "Input \"" << s << "\" is not parsed as ErrorStmt" << std::endl;
            CHECK(is<ErrorStmt>(ast));
        }
    }
}

TEST_CASE("Parser::parse_DeleteStmt()", "[core][parse][unit]")
{
    test_triple_t triples[] = {
        /* { delete statement, fully-parenthesized delete statement, next token } */

        { "DELETE FROM A", "DELETE FROM A;", TK_EOF },
        { "DELETE FROM A WHERE 42 * b", "DELETE FROM A\nWHERE (42 * b);", TK_EOF },
        { "DELETE FROM A, B", "DELETE FROM A;", TK_COMMA },
        { "DELETE FROM A where TRUE", "DELETE FROM A;", TK_IDENTIFIER }
    };

    auto parse = [](Parser &p) { return p.parse_DeleteStmt(); };
    for (auto triple : triples)
        test_parse_positive<DeleteStmt, Stmt>(triple, parse);
}

TEST_CASE("Parser::parse_DeleteStmt() sanity tests", "[core][parse][unit]")
{
    SECTION("underlying errors")
    {
        const char * statements[] = {
            "DELETE FROM A WHERE 10 : 5",
            "DELETE A",
            "delete FROM A",
            "DELETE from A"
        };

        for (auto s : statements) {
            LEXER(s);
            Parser parser(lexer);
            auto ast = parser.parse_DeleteStmt();
            if (diag.num_errors() == 0)
                std::cerr << "UNEXPECTED PASS for input \"" << s << '"' << std::endl;
            CHECK(diag.num_errors() > 0);
            CHECK_FALSE(err.str().empty());
        }
    }

    SECTION("newly generated errors")
    {
        const char * statements[] = {
            "",
            "DELETE FROM ",
            "DELETE FROM 0",
            "DELETE FROM WHERE TRUE"
        };

        for (auto s : statements) {
            LEXER(s);
            Parser parser(lexer);
            auto ast = parser.parse_DeleteStmt();
            if (diag.num_errors() == 0)
                std::cerr << "UNEXPECTED PASS for input \"" << s << '"' << std::endl;
            CHECK(diag.num_errors() > 0);
            CHECK_FALSE(err.str().empty());
            if (not is<ErrorStmt>(ast))
                std::cerr << "Input \"" << s << "\" is not parsed as ErrorStmt" << std::endl;
            CHECK(is<ErrorStmt>(ast));
        }
    }
}

TEST_CASE("Parser::parse_ImportStmt() DSV", "[core][parse][unit]")
{
    test_triple_t triples[] = {
        /* { import statement, fully-parenthesized import statement, next token } */

        { "IMPORT INTO A DSV \"dsv\"", "IMPORT INTO A DSV \"dsv\";", TK_EOF },
        { "IMPORT INTO A DSV \"dsv\" DELIMITER \"del\"", "IMPORT INTO A DSV \"dsv\" DELIMITER \"del\";",
          TK_EOF },
          /* TODO Issue incomplete ASTPrinter */
        { "IMPORT INTO A DSV \"dsv\" ESCAPE \"esc\"", "IMPORT INTO A DSV \"dsv\";",
          TK_EOF },
        { "IMPORT INTO A DSV \"dsv\" QUOTE \"quo\"", "IMPORT INTO A DSV \"dsv\";",
          TK_EOF },
        { "IMPORT INTO A DSV \"dsv\" HAS HEADER", "IMPORT INTO A DSV \"dsv\" HAS HEADER;", TK_EOF },
        { "IMPORT INTO A DSV \"dsv\" SKIP HEADER", "IMPORT INTO A DSV \"dsv\" SKIP HEADER;", TK_EOF },
        { "IMPORT INTO A DSV \"dsv\" DELIMITER \"del\" ESCAPE \"esc\" QUOTE \"quo\" HAS HEADER SKIP HEADER",
          "IMPORT INTO A DSV \"dsv\" DELIMITER \"del\" HAS HEADER SKIP HEADER;", TK_EOF },
        { "IMPORT INTO A DSV \"dsv\" \"dsvPrime\"", "IMPORT INTO A DSV \"dsv\";", TK_STRING_LITERAL },
        { "IMPORT INTO A DSV \"dsv\" delimiter \"del\"", "IMPORT INTO A DSV \"dsv\";", TK_IDENTIFIER },
        { "IMPORT INTO A DSV \"dsv\" escpae \"esc\"", "IMPORT INTO A DSV \"dsv\";", TK_IDENTIFIER },
        { "IMPORT INTO A DSV \"dsv\" quote \"quo\"", "IMPORT INTO A DSV \"dsv\";", TK_IDENTIFIER },
        { "IMPORT INTO A DSV \"dsv\" has header", "IMPORT INTO A DSV \"dsv\";", TK_IDENTIFIER },
        { "IMPORT INTO A DSV \"dsv\" skip header", "IMPORT INTO A DSV \"dsv\";", TK_IDENTIFIER }
    };

    auto parse = [](Parser &p) { return p.parse_ImportStmt(); };
    for (auto triple : triples)
        test_parse_positive<ImportStmt, Stmt>(triple, parse);
}

TEST_CASE("Parser::parse_ImportStmt() sanity tests", "[core][parse][unit]")
{
    SECTION("underlying errors")
    {
        const char * statements[] = {
            "INTO A DSV \"dsv\"",
            "IMPORT A DSV \"dsv\"",
            "IMPORT INTO A DSV \"dsv\" HAS header",
            "IMPORT INTO A DSV \"dsv\" SKIP header"
        };

        for (auto s : statements) {
            LEXER(s);
            Parser parser(lexer);
            auto ast = parser.parse_ImportStmt();
            if (diag.num_errors() == 0)
                std::cerr << "UNEXPECTED PASS for input \"" << s << '"' << std::endl;
            CHECK(diag.num_errors() > 0);
            CHECK_FALSE(err.str().empty());
        }
    }

    SECTION("newly generated errors")
    {
        const char * statements[] = {
            "",
            "import INTO A DSV \"dsv\"",
            "IMPORT into A DSV \"dsv\"",
            "IMPORT INTO A dsv \"dsv\"",
            "IMPORT INTO DSV \"dsv\" ",
            "IMPORT INTO 0 DSV \"dsv\"",
            "IMPORT INTO A DSV dsv",
            "IMPORT INTO A DSV \"dsv\" DELIMITER del",
            "IMPORT INTO A DSV \"dsv\" ESCAPE esc",
            "IMPORT INTO A DSV \"dsv\" QUOTE quo",
            "IMPORT INTO A DSV DELIMITER \"del\"",
            "IMPORT INTO A DSV ESCAPE \"esc\"",
            "IMPORT INTO A DSV QUOTE \"quo\""
        };

        for (auto s : statements) {
            LEXER(s);
            Parser parser(lexer);
            auto ast = parser.parse_ImportStmt();
            if (diag.num_errors() == 0)
                std::cerr << "UNEXPECTED PASS for input \"" << s << '"' << std::endl;
            CHECK(diag.num_errors() > 0);
            CHECK_FALSE(err.str().empty());
            if (not is<ErrorStmt>(ast))
                std::cerr << "Input \"" << s << "\" is not parsed as ErrorStmt" << std::endl;
            CHECK(is<ErrorStmt>(ast));
        }
    }
}

/*======================================================================================================================
 * Test Parser::parse_Stmt().
 *====================================================================================================================*/

TEST_CASE("Parser::parse_Stmt()", "[core][parse][unit]")
{
    auto parse = [](Parser &p) { return p.parse_Stmt(); };

    {
        test_triple_t triple = { ";", ";", TK_EOF };
        test_parse_positive<EmptyStmt, Stmt>(triple, parse);
    }

    {
        test_triple_t triple = { "CREATE DATABASE d;", "CREATE DATABASE d;", TK_EOF };
        test_parse_positive<CreateDatabaseStmt, Stmt>(triple, parse);
    }

    {
        test_triple_t triple = { "USE d;", "USE d;", TK_EOF };
        test_parse_positive<UseDatabaseStmt, Stmt>(triple, parse);
    }

    {
        test_triple_t triple = { "CREATE TABLE A ( key INT(32) );", "CREATE TABLE A\n(\n    key INT(32)\n);", TK_EOF };
        test_parse_positive<CreateTableStmt, Stmt>(triple, parse);
    }

    {
        test_triple_t triple = { "SELECT * FROM A;", "SELECT *\nFROM A;", TK_EOF };
        test_parse_positive<SelectStmt, Stmt>(triple, parse);
    }

    {
        test_triple_t triple = { "INSERT INTO A VALUES (42);", "INSERT INTO A\nVALUES\n    (42);", TK_EOF };
        test_parse_positive<InsertStmt, Stmt>(triple, parse);
    }

    {
        test_triple_t triple = { "UPDATE A SET key = 17;", "UPDATE A\nSET\n    key = 17;", TK_EOF };
        test_parse_positive<UpdateStmt, Stmt>(triple, parse);
    }

    {
        test_triple_t triple = { "DELETE FROM A;", "DELETE FROM A;", TK_EOF };
        test_parse_positive<DeleteStmt, Stmt>(triple, parse);
    }

    {
        test_triple_t triple = { "IMPORT INTO A DSV \"A\";", "IMPORT INTO A DSV \"A\";", TK_EOF };
        test_parse_positive<ImportStmt, Stmt>(triple, parse);
    }
}

TEST_CASE("Parser::parse_Stmt() sanity tests", "[core][parse][unit]")
{
    SECTION("underlying errors")
    {
        const char * statements[] = {
            "CREATE DATABASE d",
            "USE d", "USE d",
            "CREATE TABLE A ( key INT(32) )",
            "SELECT * FROM A",
            "INSERT INTO A VALUES (42)",
            "UPDATE A SET key = 17",
            "DELETE FROM A",
            "IMPORT INTO A DSV \"A\""
        };

        for (auto s : statements) {
            LEXER(s);
            Parser parser(lexer);
            auto ast = parser.parse_Stmt();
            if (diag.num_errors() == 0)
                std::cerr << "UNEXPECTED PASS for input \"" << s << '"' << std::endl;
            CHECK(diag.num_errors() > 0);
            CHECK_FALSE(err.str().empty());
        }
    }

    SECTION("newly generated errors")
    {
        const char * statements[] = {
            "",
            "STATEMENT SELECT 42;",
            "CREATE STATEMENT s;"
        };

        for (auto s : statements) {
            LEXER(s);
            Parser parser(lexer);
            auto ast = parser.parse_Stmt();
            if (diag.num_errors() == 0)
                std::cerr << "UNEXPECTED PASS for input \"" << s << '"' << std::endl;
            CHECK(diag.num_errors() > 0);
            CHECK_FALSE(err.str().empty());
            if (not is<ErrorStmt>(ast))
                std::cerr << "Input \"" << s << "\" is not parsed as ErrorStmt" << std::endl;
            CHECK(is<ErrorStmt>(ast));
        }
    }
}

/*======================================================================================================================
 * Test Parser::parse_Instruction().
 *====================================================================================================================*/

TEST_CASE("Parser::parse_Instruction()", "[core][parse][unit]")
{
    std::pair<const char*, Instruction> instruction_pairs[] = {
            /* { instruction, expected resulting instruction } */

            { "\\instr;", Instruction(Token(), "instr", {}) },
            { "\\instr\n ;", Instruction(Token(), "instr", {}) },
            { "\\instr arg1;", Instruction(Token(), "instr", {"arg1"}) },
            { "\\instr arg1\n;", Instruction(Token(), "instr", {"arg1"}) },
            { "\\instr arg1 ;", Instruction(Token(), "instr", {"arg1"}) },
            { "\\instr\narg1;", Instruction(Token(), "instr", {"arg1"}) },
            { "\\instr arg1 arg2;", Instruction(Token(), "instr", {"arg1", "arg2"}) },
            { "\\instr arg1\narg2;", Instruction(Token(), "instr", {"arg1", "arg2"}) },
            { "\\instr\narg1\n  arg2;", Instruction(Token(), "instr", {"arg1", "arg2"}) },
            { "\\instr arg1 arg2 arg3;", Instruction(Token(), "instr", {"arg1", "arg2", "arg3"}) },
            { "\\instr \narg1 arg2\n   arg3  ;", Instruction(Token(), "instr", {"arg1", "arg2", "arg3"}) }
    };

    for (const auto &test_pair : instruction_pairs) {
        LEXER(test_pair.first);
        Parser parser(lexer);
        auto instruction = parser.parse_Instruction();
        CHECK(instruction->name == C.pool(test_pair.second.name));
        CHECK(instruction->args.size() == test_pair.second.args.size());
        for (std::size_t i = 0; i < instruction->args.size(); ++i) {
            CHECK(instruction->args[i] == C.pool(test_pair.second.args[i]));
        }

        CHECK(diag.num_errors() == 0);
        CHECK(err.str().empty());
        if (diag.num_errors()) {
            std::cerr << "ERROR for input \"" << test_pair.first << "\": " << err.str() << std::endl;
            return;
        }

        auto tok = parser.token();
        CHECK(tok == TK_EOF);
        if (tok != TK_EOF) {
            std::cerr << "Expected next token " << TK_EOF << ", got " << tok << " for input \"" << test_pair.first
                      << "\"" << std::endl;
        }
    }
}

TEST_CASE("Parser::parse_Instruction() sanity tests", "[core][parse][unit]")
{
    SECTION("errors")
    {
        const char * instructions[] = {
            "\\instr",
            "\\instr\n ",
            "\\instr arg1\n",
            "\\instr arg1 arg2",
            "\\instr \narg1 arg2\n   arg3  ",
        };

        for (auto instruction_text : instructions) {
            LEXER(instruction_text);
            Parser parser(lexer);
            auto instruction = parser.parse_Instruction();
            if (diag.num_errors() == 0)
                std::cerr << "UNEXPECTED PASS for input \"" << instruction_text << '"' << std::endl;
            CHECK(diag.num_errors() > 0);
            CHECK_FALSE(err.str().empty());
        }
    }
}

/*======================================================================================================================
 * Test Parser::parse().
 *====================================================================================================================*/

TEST_CASE("Parser::parse()", "[core][parse][unit]")
{
    SECTION("instructions")
    {
        const char * instructions[] = {
            "\\instr;",
            "\\instr arg1\n;",
            "\\instr \narg1 arg2\n   arg3  ;"
        };

        for (auto instruction_text : instructions) {
            LEXER(instruction_text);
            Parser parser(lexer);
            auto command = parser.parse();
            CHECK(diag.num_errors() == 0);
            CHECK(err.str().empty());
            CHECK(is<Instruction>(command));
        }
    }

    SECTION("statements")
    {
        const char * statements[] = {
            "SELECT * FROM A;",
            "UPDATE A SET key = 17;",
            "CREATE TABLE A ( key INT(32) );"
        };

        for (auto stmt : statements) {
            LEXER(stmt);
            Parser parser(lexer);
            auto command = parser.parse();
            CHECK(diag.num_errors() == 0);
            CHECK(err.str().empty());
            CHECK(is<Stmt>(command));
        }
    }
}

#include "catch2/catch.hpp"

#include "parse/Parser.hpp"
#include "parse/Sema.hpp"
#include "testutil.hpp"
#include <iostream>
#include <mutable/catalog/Catalog.hpp>
#include <mutable/util/Diagnostic.hpp>
#include <mutable/util/fn.hpp>


using namespace m;
using namespace m::ast;


TEST_CASE("Sema c'tor", "[core][parse][sema]")
{
    LEXER("SELECT * FROM test;");
    Sema sema(diag);
    REQUIRE(diag.num_errors() == 0);
    REQUIRE(out.str().empty());
    REQUIRE(err.str().empty());
}

TEST_CASE("Sema/Expressions", "[core][parse][sema]")
{
    std::pair<const char*, const Type*> exprs[] = {
        /* { expression , type } */

        /* NULL expressions */
        { "NULL", Type::Get_None() },

        /* boolean constants */
        { "TRUE", Type::Get_Boolean(Type::TY_Scalar) },
        { "FALSE", Type::Get_Boolean(Type::TY_Scalar) },

        /* string literals */
        { "\"Hello, World\"", Type::Get_Char(Type::TY_Scalar, 12) }, // strlen without quotes

        /* numeric constants */
        { "42", Type::Get_Integer(Type::TY_Scalar, 4) },
        { "017", Type::Get_Integer(Type::TY_Scalar, 4) },
        { "0xC0FF33", Type::Get_Integer(Type::TY_Scalar, 4) },
        { "0xc0ff33", Type::Get_Integer(Type::TY_Scalar, 4) },

        { "017777777777", Type::Get_Integer(Type::TY_Scalar, 4) }, // 2^31 - 1, octal
        { "2147483647", Type::Get_Integer(Type::TY_Scalar, 4) }, // 2^31 - 1, decimal
        { "0x7fffffff", Type::Get_Integer(Type::TY_Scalar, 4) }, // 2^31 - 1, hexadecimal

        { "020000000000", Type::Get_Integer(Type::TY_Scalar, 8) }, // 2^31, octal
        { "2147483648", Type::Get_Integer(Type::TY_Scalar, 8) }, // 2^31, decimal
        { "0x80000000", Type::Get_Integer(Type::TY_Scalar, 8) }, // 2^31, hexadecimal

        { ".1", Type::Get_Double(Type::TY_Scalar) },
        { "0xC0F.F33", Type::Get_Double(Type::TY_Scalar) },

        /* date constants */
        { "d'2000-03-14'", Type::Get_Date(m::Type::TY_Scalar) },
        { "d'-2000-03-14'", Type::Get_Date(m::Type::TY_Scalar) },
        { "d'0001-01-01'", Type::Get_Date(m::Type::TY_Scalar) },
        { "d'9999-12-31'", Type::Get_Date(m::Type::TY_Scalar) },
        { "d'9999-02-29'", Type::Get_Date(m::Type::TY_Scalar) },
        { "d'9999-04-30'", Type::Get_Date(m::Type::TY_Scalar) },
        { "d'0000-01-01'", Type::Get_Error() },
        { "d'0001-00-01'", Type::Get_Error() },
        { "d'0001-13-01'", Type::Get_Error() },
        { "d'0001-01-00'", Type::Get_Error() },
        { "d'9999-12-32'", Type::Get_Error() },
        { "d'9999-02-30'", Type::Get_Error() },
        { "d'9999-04-31'", Type::Get_Error() },

        /* datetime constants */
        { "d'2000-03-14 17:54:38'", Type::Get_Datetime(m::Type::TY_Scalar) },
        { "d'-2000-03-14 17:54:38'", Type::Get_Datetime(m::Type::TY_Scalar) },
        { "d'2000-03-14 00:00:00'", Type::Get_Datetime(m::Type::TY_Scalar) },
        { "d'2000-03-14 23:59:59'", Type::Get_Datetime(m::Type::TY_Scalar) },
        { "d'0000-01-01 00:00:00'", Type::Get_Error() },
        { "d'0001-00-01 00:00:00'", Type::Get_Error() },
        { "d'0001-13-01 00:00:00'", Type::Get_Error() },
        { "d'0001-01-00 00:00:00'", Type::Get_Error() },
        { "d'9999-12-32 00:00:00'", Type::Get_Error() },
        { "d'9999-02-30 00:00:00'", Type::Get_Error() },
        { "d'9999-04-31 00:00:00'", Type::Get_Error() },
        { "d'2000-03-14 24:00:00'", Type::Get_Error() },
        { "d'2000-03-14 23:60:00'", Type::Get_Error() },
        { "d'2000-03-14 23:59:60'", Type::Get_Error() },

        /* unary expressions */
        { "~42", Type::Get_Integer(Type::TY_Scalar, 4) },
        { "+42", Type::Get_Integer(Type::TY_Scalar, 4) },
        { "-42", Type::Get_Integer(Type::TY_Scalar, 4) },

        { "~42.", Type::Get_Double(Type::TY_Scalar) },
        { "+42.", Type::Get_Double(Type::TY_Scalar) },
        { "-42.", Type::Get_Double(Type::TY_Scalar) },

        { "~ TRUE", Type::Get_Error() },
        { "+ TRUE", Type::Get_Error() },
        { "- TRUE", Type::Get_Error() },

        { "~ \"Hello, World\"", Type::Get_Error() },
        { "+ \"Hello, World\"", Type::Get_Error() },
        { "- \"Hello, World\"", Type::Get_Error() },

        { "- (42 + NULL)", Type::Get_Error() },

#if 0
        /* fuse unary operator with number */
        { "+017777777777", Type::Get_Integer(Type::TY_Scalar, 4) }, // 2^31 - 1, octal
        { "+2147483647", Type::Get_Integer(Type::TY_Scalar, 4) }, // 2^31 - 1, decimal
        { "+0x7fffffff", Type::Get_Integer(Type::TY_Scalar, 4) }, // 2^31 - 1, hexadecimal

        { "+020000000000", Type::Get_Integer(Type::TY_Scalar, 8) }, // 2^31, octal
        { "+2147483648", Type::Get_Integer(Type::TY_Scalar, 8) }, // 2^31, decimal
        { "+0x80000000", Type::Get_Integer(Type::TY_Scalar, 8) }, // 2^31, hexadecimal

        { "-020000000000", Type::Get_Integer(Type::TY_Scalar, 4) }, // -2^31, octal
        { "-2147483648", Type::Get_Integer(Type::TY_Scalar, 4) }, // -2^31, decimal
        { "-0x80000000", Type::Get_Integer(Type::TY_Scalar, 4) }, // -2^31, hexadecimal

        { "-020000000001", Type::Get_Integer(Type::TY_Scalar, 8) }, // -2^31 - 1, octal
        { "-2147483649", Type::Get_Integer(Type::TY_Scalar, 8) }, // -2^31 - 1, decimal
        { "-0x80000001", Type::Get_Integer(Type::TY_Scalar, 8) }, // -2^31 - 1, hexadecimal
#endif

        /* arithmetic binary expressions */
        { "1 + 2", Type::Get_Integer(Type::TY_Scalar, 4) },
        { "1 - 2", Type::Get_Integer(Type::TY_Scalar, 4) },
        { "1 * 2", Type::Get_Integer(Type::TY_Scalar, 4) },
        { "1 / 2", Type::Get_Integer(Type::TY_Scalar, 4) },
        { "1 % 2", Type::Get_Integer(Type::TY_Scalar, 4) },
        { "0x80000000 + 42", Type::Get_Integer(Type::TY_Scalar, 8) },

        { "0x80000000 + NULL", Type::Get_Error() },
        { "NULL - 0x80000000", Type::Get_Error() },
        { "0x80000000 - NULL", Type::Get_Error() },
        { "0x80000000 * NULL", Type::Get_Error() },
        { "0x80000000 / NULL", Type::Get_Error() },
        { "NULL / 0x80000000", Type::Get_Error() },
        { "0x80000000 % NULL", Type::Get_Error() },
        { "NULL % 0x80000000", Type::Get_Error() },

        { "2.718 + 3.14", Type::Get_Double(Type::TY_Scalar) },
        { "42 + 3.14", Type::Get_Double(Type::TY_Scalar) },
        { "3.14 + 42", Type::Get_Double(Type::TY_Scalar) },

        { "TRUE + FALSE", Type::Get_Error() },
        { "TRUE + 42", Type::Get_Error() },
        { "42 + TRUE", Type::Get_Error() },
        { "\"Hello, World\" + 42", Type::Get_Error() },
        { "42 + \"Hello, World\"", Type::Get_Error() },
        { "42 + d'1993-02-24'", Type::Get_Error() },
        { "d'1993-02-24' + d'3421-11-26'", Type::Get_Error() },
        { "42 + d'3942-12-11 23:45:36'", Type::Get_Error() },
        { "d'3942-12-11 23:45:36' + d'1134-01-30 13:45:51'", Type::Get_Error() },

        { "\"Hello, World\" + NULL", Type::Get_Error() },
        { "NULL + \"Hello, World\"", Type::Get_Error() },

        /* comparative expressions */
        { "42 < 1337", Type::Get_Boolean(Type::TY_Scalar) },
        { "42 <= 1337", Type::Get_Boolean(Type::TY_Scalar) },
        { "42 > 1337", Type::Get_Boolean(Type::TY_Scalar) },
        { "42 >= 1337", Type::Get_Boolean(Type::TY_Scalar) },
        { "42 = 1337", Type::Get_Boolean(Type::TY_Scalar) },
        { "42 != 1337", Type::Get_Boolean(Type::TY_Scalar) },
        { "d'1993-02-24' < d'3421-11-26'", Type::Get_Boolean(m::Type::TY_Scalar) },
        { "d'3942-12-11 23:45:36' = d'1134-01-30 13:45:51'", Type::Get_Boolean(m::Type::TY_Scalar) },
        { "3.14 < 0x80000000", Type::Get_Boolean(Type::TY_Scalar) },
        { "0x80000000 = NULL", Type::Get_Error() },
        { "0x80000000 < NULL", Type::Get_Error() },
        { "0x80000000 > NULL", Type::Get_Error() },
        { "0x80000000 <= NULL", Type::Get_Error() },
        { "0x80000000 >= NULL", Type::Get_Error() },
        { "0x80000000 != NULL", Type::Get_Error() },

        { "TRUE < FALSE", Type::Get_Error() },
        { "TRUE < 42", Type::Get_Error() },
        { "42 < TRUE", Type::Get_Error() },
        { "42 < \"Hello, World\"", Type::Get_Error() },
        { "\"Hello, World\" < 42", Type::Get_Error() },
        { "NULL < \"Hello, World\"", Type::Get_Error() },
        { "\"Hello, World\" < NULL", Type::Get_Error() },

        { "TRUE = FALSE", Type::Get_Boolean(Type::TY_Scalar) },
        { "TRUE != FALSE", Type::Get_Boolean(Type::TY_Scalar) },
        { "NULL = NULL", Type::Get_Error() },
        { "NULL != NULL", Type::Get_Error() },
        { "\"verylongtext\" = \"shorty\"", Type::Get_Boolean(Type::TY_Scalar) },

        { "TRUE = 42", Type::Get_Error() },
        { "42 = TRUE", Type::Get_Error() },
        { "TRUE = \"text\"", Type::Get_Error() },
        { "\"text\" = TRUE", Type::Get_Error() },
        { "42 = \"text\"", Type::Get_Error() },
        { "\"text\" = 42", Type::Get_Error() },
        { "d'1993-02-24' < 42", Type::Get_Error() },
        { "d'3942-12-11 23:45:36' = 17", Type::Get_Error() },

        { "\"text\" LIKE \"pattern\"", Type::Get_Boolean(Type::TY_Scalar) },
        { "\"text\" LIKE \"pattern\" .. \"other\"", Type::Get_Boolean(Type::TY_Scalar) },

        { "42 LIKE \"pattern\"", Type::Get_Error() },
        { "3.14 LIKE \"pattern\"", Type::Get_Error() },
        { "TRUE LIKE \"pattern\"", Type::Get_Error() },
        { "\"text\" LIKE 42", Type::Get_Error() },
        { "\"text\" LIKE 3.14", Type::Get_Error() },
        { "\"text\" LIKE TRUE", Type::Get_Error() },
        { "42 LIKE 1337", Type::Get_Error() },
        { "TRUE LIKE FALSE", Type::Get_Error() },
        { "3.14 LIKE 3.14", Type::Get_Error() },

        /* string concatenation */
        { "\"text\" .. \"text\"", Type::Get_Char(Type::TY_Scalar, 8) },

        { "42 .. 42", Type::Get_Error() },
        { "3.14 .. 3.14", Type::Get_Error() },
        { "TRUE .. FALSE", Type::Get_Error() },
        { "\"text\" .. 42", Type::Get_Error() },
        { "\"text\" .. 3.14", Type::Get_Error() },
        { "\"text\" .. TRUE", Type::Get_Error() },
        { "42 .. \"text\"", Type::Get_Error() },
        { "3.14 .. \"text\"", Type::Get_Error() },
        { "TRUE .. \"text\"", Type::Get_Error() },
        { "\"text\" .. NULL", Type::Get_Error() },
        { "NULL .. \"text\"", Type::Get_Error() },

        /* Three Valued Logic */
        { "FALSE AND FALSE", Type::Get_Boolean(Type::TY_Scalar) },
        { "FALSE AND NULL", Type::Get_Error() },
        { "FALSE AND TRUE", Type::Get_Boolean(Type::TY_Scalar) },
        { "NULL AND FALSE", Type::Get_Error() },
        { "NULL AND NULL", Type::Get_Error() },
        { "NULL AND TRUE", Type::Get_Error() },
        { "TRUE AND FALSE", Type::Get_Boolean(Type::TY_Scalar) },
        { "TRUE AND NULL", Type::Get_Error() },
        { "TRUE AND TRUE", Type::Get_Boolean(Type::TY_Scalar) },

        { "FALSE OR FALSE", Type::Get_Boolean(Type::TY_Scalar) },
        { "FALSE OR NULL", Type::Get_Error() },
        { "FALSE OR TRUE", Type::Get_Boolean(Type::TY_Scalar) },
        { "NULL OR FALSE", Type::Get_Error() },
        { "NULL OR NULL", Type::Get_Error() },
        { "NULL OR TRUE", Type::Get_Error() },
        { "TRUE OR FALSE", Type::Get_Boolean(Type::TY_Scalar) },
        { "TRUE OR NULL", Type::Get_Error() },
        { "TRUE OR TRUE", Type::Get_Boolean(Type::TY_Scalar) },

        /* Tk_Not */
        { "NOT 42", Type::Get_Error() },
        { "NOT NOT TRUE", Type::Get_Boolean(Type::TY_Scalar) },
    };

    for (auto e : exprs) {
        LEXER(e.first);
        Parser parser(lexer);
        Sema sema(diag);
        auto ast = parser.parse_Expr();
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(out.str().empty());
        REQUIRE(err.str().empty());
        sema(*ast);

        if (ast->type() != e.second)
            std::cerr << "expected " << *e.second << ", got " << *ast->type() << " for expression " << e.first
                      << std::endl;
        REQUIRE(ast->type() == e.second);
        if (e.second != Type::Get_Error()) {
            /* We do not expect an error for this input. */
            CHECK(diag.num_errors() == 0);
            CHECK(err.str().empty());
        }
    }
}

TEST_CASE("Sema/Empty & Error")
{
    Catalog::Clear();

    /* Create a dummy DB and a dummy table with a scalar and a vector attribute. */
    Catalog &C = Catalog::Get();
    const char *db_name = "mydb";
    auto &DB = C.add_database(db_name);
    C.set_database_in_use(DB);
    auto &table = DB.add_table(C.pool("mytable"));
    table.push_back(C.pool("v"), Type::Get_Integer(Type::TY_Vector, 4));

    SECTION("Error Expression")
    {
        LEXER(";");
        Token tok;
        ErrorExpr expr(tok);
        Sema sema(diag);
        sema(expr);

        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
    }

    SECTION("Error clause")
    {
        LEXER(";");
        Parser parser(lexer);
        Token tok;
        ErrorClause clause(tok);
        Sema sema(diag);
        sema(clause);

        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
    }

    SECTION("Error Statement")
    {
        LEXER("CREATE DATABASE;");
        Parser parser(lexer);
        auto stmt = as<ErrorStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 1);  // parser error expected
        REQUIRE(not err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 1);
        REQUIRE(not err.str().empty());
    }

    SECTION("Empty statement")
    {
        LEXER(";");
        Parser parser(lexer);
        auto stmt = as<EmptyStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
    }
}

TEST_CASE("Sema/Expressions scalar-vector inference", "[core][parse][sema]")
{
    Catalog::Clear();

    /* Create a dummy DB and a dummy table with a scalar and a vector attribute. */
    Catalog &C = Catalog::Get();
    const char *db_name = "mydb";
    auto &DB = C.add_database(db_name);
    C.set_database_in_use(DB);
    auto &table = DB.add_table(C.pool("mytable"));
    table.push_back(C.pool("v"), Type::Get_Integer(Type::TY_Vector, 4));

    SECTION("Vector compared to scalar yields vector")
    {
        LEXER("SELECT * FROM mytable WHERE v > 42;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        WhereClause *where = as<WhereClause>(stmt->where.get());
        const Boolean *ty = cast<const Boolean>(where->where->type());
        REQUIRE(ty);
        CHECK(ty->is_vectorial());
    }

    SECTION( "Vector compared to vector yields vector")
    {
        LEXER("SELECT * FROM mytable WHERE v > v;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        WhereClause *where = as<WhereClause>(stmt->where.get());
        const Boolean *ty = cast<const Boolean>(where->where->type());
        REQUIRE(ty);
        CHECK(ty->is_vectorial());
    }

    SECTION("Scalar and scalar yields scalar")
    {
        LEXER("SELECT * FROM mytable WHERE 13 < 42;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        WhereClause *where = as<WhereClause>(stmt->where.get());
        const Boolean *ty = cast<const Boolean>(where->where->type());
        REQUIRE(ty);
        CHECK(ty->is_scalar());
    }

}

TEST_CASE("Sema/Expressions/Functions", "[core][parse][sema]")
{
    Catalog::Clear();

    /* Create a dummy DB and a dummy table with a scalar and a vector attribute. */
    Catalog &C = Catalog::Get();
    const char *db_name = "mydb";
    auto &DB = C.add_database(db_name);
    C.set_database_in_use(DB);
    auto &table = DB.add_table(C.pool("mytable"));
    table.push_back(C.pool("v"), Type::Get_Integer(Type::TY_Vector, 4));
    table.push_back(C.pool("b"), Type::Get_Boolean(Type::TY_Vector));
    table.push_back(C.pool("f"), Type::Get_Float(Type::TY_Vector));
    table.push_back(C.pool("d"), Type::Get_Decimal(Type::TY_Vector, 4, 4));

    SECTION("Vectorial WHERE condition is ok.")
    {
        LEXER("SELECT * FROM mytable WHERE v = v;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());

        WhereClause *where = as<WhereClause>(stmt->where.get());
        const PrimitiveType *pt = as<const PrimitiveType>(where->where->type());
        CHECK(pt->is_vectorial());
    }

    SECTION("Vectorial WHERE condition is ok.")
    {
        LEXER("SELECT * FROM mytable WHERE v > 42;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 0); // no error
        REQUIRE(err.str().empty());

        WhereClause *where = as<WhereClause>(stmt->where.get());
        const PrimitiveType *pt = as<const PrimitiveType>(where->where->type());
        CHECK(pt->is_vectorial());
    }

    SECTION("Scalar WHERE condition is ok.")
    {
        LEXER("SELECT * FROM mytable WHERE 13 < 42;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());

        WhereClause *where = as<WhereClause>(stmt->where.get());
        const PrimitiveType *pt = as<const PrimitiveType>(where->where->type());
        CHECK(not pt->is_vectorial());
    }

    SECTION("Function name does not exist.")
    {
        LEXER("SELECT MINMAX(v) FROM mytable;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 1);
        REQUIRE(not err.str().empty());
    }

    SECTION("MIN with argument of error type")
    {
        LEXER("SELECT * FROM mytable GROUP BY v HAVING MIN(TRUE<FALSE);");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 1);
        REQUIRE(not err.str().empty());
    }

    SECTION("ISNULL with argument of error type")
    {
        LEXER("SELECT * FROM mytable GROUP BY v HAVING ISNULL(TRUE<FALSE);");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 1);
        REQUIRE(not err.str().empty());
    }

    SECTION("AVG without argument")
    {
        LEXER("SELECT * FROM mytable GROUP BY v HAVING AVG()>0;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 1);
        REQUIRE(not err.str().empty());
    }

    SECTION("AVG with too many arguments")
    {
        LEXER("SELECT * FROM mytable GROUP BY v HAVING AVG(v,v)>0;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 1);
        REQUIRE(not err.str().empty());
    }

    SECTION("AVG with non-numeric argument")
    {
        LEXER("SELECT * FROM mytable GROUP BY v HAVING AVG(b)>0;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 1);
        REQUIRE(not err.str().empty());
    }

    SECTION("COUNT without argument")
    {
        LEXER("SELECT COUNT() FROM mytable;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
    }

    SECTION("COUNT with too many arguments")
    {
        LEXER("SELECT * FROM mytable GROUP BY v HAVING COUNT(v,v)>0;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 1);
        REQUIRE(not err.str().empty());
    }

    SECTION("ISNULL without argument")
    {
        LEXER("SELECT * FROM mytable WHERE ISNULL();");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 1);
        REQUIRE(not err.str().empty());
    }

    SECTION("ISNULL with too many arguments")
    {
        LEXER("SELECT * FROM mytable WHERE ISNULL(v,v);");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 1);
        REQUIRE(not err.str().empty());
    }

    SECTION("ISNULL with argument NULL")
    {
        LEXER("SELECT * FROM mytable WHERE ISNULL(NULL);");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 1);
        REQUIRE(not err.str().empty());
    }

    SECTION("Aggregate function in GROUP BY clause")
    {
        LEXER("SELECT * FROM mytable GROUP BY MIN(v);");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 3);
        REQUIRE(not err.str().empty());
    }

    SECTION("SUM with float argument")
    {
        LEXER("SELECT SUM(f) FROM mytable;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
    }

    SECTION("SUM with decimal argument")
    {
        LEXER("SELECT SUM(d) FROM mytable;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
    }

    SECTION("AVG with int argument")
    {
        LEXER("SELECT AVG(v) FROM mytable;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        auto &selects = as<SelectClause>(stmt->select.get())->select;
        REQUIRE(selects.size() == 1);
        auto &avg = selects[0];
        REQUIRE(avg.first->type()->is_double());

        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
    }

    SECTION("AVG with float argument")
    {
        LEXER("SELECT AVG(f) FROM mytable;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        auto &selects = as<SelectClause>(stmt->select.get())->select;
        REQUIRE(selects.size() == 1);
        auto &avg = selects[0];
        REQUIRE(avg.first->type()->is_double());

        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
    }
}

TEST_CASE("Sema/Expressions/Designator", "[core][parse][sema]")
{
    Catalog::Clear();

    /* Create a dummy DB and dummy tables with attributes. */
    Catalog &C = Catalog::Get();
    const char *db_name = "mydb";
    auto &DB = C.add_database(db_name);
    C.set_database_in_use(DB);
    auto &table1 = DB.add_table(C.pool("mytable1"));
    table1.push_back(C.pool("v"), Type::Get_Integer(Type::TY_Vector, 4));
    table1.push_back(C.pool("b"), Type::Get_Boolean(Type::TY_Vector));
    auto &table2 = DB.add_table(C.pool("mytable2"));
    table2.push_back(C.pool("v"), Type::Get_Float(Type::TY_Vector));

    SECTION("Select attribute without specifying table in FROM")
    {
        LEXER("SELECT b;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 1);
        REQUIRE(not err.str().empty());
    }

    SECTION("Select table.attribute without FROM clause")
    {
        LEXER("SELECT mytable1.b;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 1);
        REQUIRE(not err.str().empty());
    }

    SECTION("Select non-existent attribute without specifying table in FROM")
    {
        LEXER("SELECT x;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 1);
        REQUIRE(not err.str().empty());
    }

    SECTION("Ambiguous attribute without specifying table")
        {
            LEXER("SELECT v FROM mytable1, mytable2;");
            Parser parser(lexer);
            auto stmt = as<SelectStmt>(parser.parse());
            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            REQUIRE(diag.num_errors() == 1);
            REQUIRE(not err.str().empty());
        }

    SECTION("Named Expressions")
    {
        SECTION("Named in select, used in order by")
        {
            LEXER("SELECT v AS newname FROM mytable1 ORDER BY newname;");
            Parser parser(lexer);
            auto stmt = as<SelectStmt>(parser.parse());
            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
        }

        SECTION("Ambiguous attribute in ORDER BY")
        {
            LEXER("SELECT mytable1.v AS V, mytable2.v AS V FROM mytable1, mytable2 ORDER BY V;");
            Parser parser(lexer);
            auto stmt = as<SelectStmt>(parser.parse());
            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            REQUIRE(diag.num_errors() == 1);
            REQUIRE(not err.str().empty());
        }

        SECTION("Ambiguous attribute in ORDER BY due to repeatedly selecting")
        {
            LEXER("SELECT v, v FROM mytable1 ORDER BY v;");
            Parser parser(lexer);
            auto stmt = as<SelectStmt>(parser.parse());
            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            REQUIRE(diag.num_errors() == 1);
            REQUIRE(not err.str().empty());
        }

        SECTION("Ambiguous attribute in ORDER BY due to anti projection")
        {
            LEXER("SELECT *, v FROM mytable1 ORDER BY v;");
            Parser parser(lexer);
            auto stmt = as<SelectStmt>(parser.parse());
            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            REQUIRE(diag.num_errors() == 1);
            REQUIRE(not err.str().empty());
        }

        SECTION("Ambiguous attribute in ORDER BY due to renaming")
        {
            LEXER("SELECT v, b AS v FROM mytable1 ORDER BY v;");
            Parser parser(lexer);
            auto stmt = as<SelectStmt>(parser.parse());
            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            REQUIRE(diag.num_errors() == 1);
            REQUIRE(not err.str().empty());
        }

        SECTION("Two named expressions used to order")
        {
            LEXER("SELECT v AS V, b AS B FROM mytable1 ORDER BY V ASC, B DESC;");
            Parser parser(lexer);
            auto stmt = as<SelectStmt>(parser.parse());
            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
        }

        SECTION("Alias on every attribute, some used in order by")
        {
            LEXER("SELECT mytable1.v AS V1, b AS B, mytable2.v AS V2 FROM mytable1, mytable2 ORDER BY V2 ASC, B DESC;");
            Parser parser(lexer);
            auto stmt = as<SelectStmt>(parser.parse());
            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
        }

        SECTION("Alias on every attribute with ambiguous v")
        {
            LEXER("SELECT v AS V1, b AS B, mytable2.v AS V2 FROM mytable1, mytable2 ORDER BY V2 ASC, B DESC;");
            Parser parser(lexer);
            auto stmt = as<SelectStmt>(parser.parse());
            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            REQUIRE(diag.num_errors() == 1);
            REQUIRE(not err.str().empty());
        }

        SECTION("Table alias + attribute alias used")
        {
            LEXER("SELECT T.v AS TV, T.b AS TB, R.v AS RV FROM mytable1 AS T, mytable2 AS R ORDER BY TV, TB, RV;");
            Parser parser(lexer);
            auto stmt = as<SelectStmt>(parser.parse());
            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
        }

        SECTION("Same table alias used twice on different tables")
        {
            LEXER("SELECT T.v, T.v FROM mytable1 AS T, mytable2 AS T;");
            Parser parser(lexer);
            auto stmt = as<SelectStmt>(parser.parse());
            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            REQUIRE(diag.num_errors() == 1);
            REQUIRE(not err.str().empty());
        }

        SECTION("Table alias used in ORDER BY")
        {
            LEXER("SELECT T.v AS R FROM mytable1 AS T ORDER BY T;");
            Parser parser(lexer);
            auto stmt = as<SelectStmt>(parser.parse());
            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            REQUIRE(diag.num_errors() == 1);
            REQUIRE(not err.str().empty());
        }

        SECTION("Named Expressions in nested statements")
        {
            LEXER("SELECT * FROM \
                (SELECT M1.v AS V1, M2.v AS V2 FROM mytable1 AS M1, mytable2 AS M2) AS T \
                ORDER BY T.V1, V2;");
            Parser parser(lexer);
            auto stmt = as<SelectStmt>(parser.parse());
            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
        }

        SECTION("Nested named expression with attribute that does not exist")
        {
            LEXER("SELECT T.X FROM \
                (SELECT M1.v AS V1, M2.v AS V2 FROM mytable1 AS M1, mytable2 AS M2) AS T;");
            Parser parser(lexer);
            auto stmt = as<SelectStmt>(parser.parse());
            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            REQUIRE(diag.num_errors() == 1);
            REQUIRE(not err.str().empty());
        }

        SECTION("Nested named expression with attributes that have same alias")
        {
            LEXER("SELECT T.V FROM \
                (SELECT M1.v AS V, M2.v AS V FROM mytable1 AS M1, mytable2 AS M2) AS T;");
            Parser parser(lexer);
            auto stmt = as<SelectStmt>(parser.parse());
            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            REQUIRE(diag.num_errors() == 1);
            REQUIRE(not err.str().empty());
        }

        SECTION("Nested named expression with attributes that have same alias without specifying table name")
        {
            LEXER("SELECT V FROM \
                (SELECT M1.v AS V, M2.v AS V FROM mytable1 AS M1, mytable2 AS M2) AS T;");
            Parser parser(lexer);
            auto stmt = as<SelectStmt>(parser.parse());
            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            REQUIRE(diag.num_errors() == 1);
            REQUIRE(not err.str().empty());
        }

        SECTION("Nested named expression without specifying table in outer query")
        {
            LEXER("SELECT V1, V2 FROM \
                (SELECT M1.v AS V1, M2.v AS V2 FROM mytable1 AS M1, mytable2 AS M2) AS T;");
            Parser parser(lexer);
            auto stmt = as<SelectStmt>(parser.parse());
            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
        }

        SECTION("Nested named expression, two attributes have same alias, select all, \
            ORDER BY is ambiguous")
        {
            LEXER("SELECT * FROM \
                (SELECT M1.v AS V, M2.v AS V FROM mytable1 AS M1, mytable2 AS M2) AS T \
                ORDER BY T.V;");
            Parser parser(lexer);
            auto stmt = as<SelectStmt>(parser.parse());
            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            REQUIRE(diag.num_errors() == 3);
            REQUIRE(not err.str().empty());
        }

        SECTION("Nested named expression, two attributes have same alias, select all, \
            ORDER BY is ambiguous, no table specified in ORDER BY")
        {
            LEXER("SELECT * FROM \
                (SELECT M1.v AS V, M2.v AS V FROM mytable1 AS M1, mytable2 AS M2) AS T \
                ORDER BY V;");
            Parser parser(lexer);
            auto stmt = as<SelectStmt>(parser.parse());
            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            REQUIRE(diag.num_errors() == 3);
            REQUIRE(not err.str().empty());
        }

        SECTION("Nested named expression, two attributes have same alias, SELECT with alias, \
            SELECT is ambiguous")
        {
            LEXER("SELECT T.V AS X FROM \
                (SELECT M1.v AS V, M2.v AS V FROM mytable1 AS M1, mytable2 AS M2) AS T;");
            Parser parser(lexer);
            auto stmt = as<SelectStmt>(parser.parse());
            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            REQUIRE(diag.num_errors() == 1);
            REQUIRE(not err.str().empty());
        }

        SECTION("Nested named expression, SELECT with alias and table specified, \
            ORDER BY is ambiguous")
        {
            LEXER("SELECT T.V1 AS X, T.V2 AS X FROM \
                (SELECT M1.v AS V1, M2.v AS V2 FROM mytable1 AS M1, mytable2 AS M2) AS T \
                ORDER BY X;");
            Parser parser(lexer);
            auto stmt = as<SelectStmt>(parser.parse());
            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            REQUIRE(diag.num_errors() == 1);
            REQUIRE(not err.str().empty());
        }

        SECTION("Nested named expression, SELECT with alias and table specified, \
            ORDER BY is not ambiguous")
        {
            LEXER("SELECT T.V1 AS X, T.V2 AS Y FROM \
                (SELECT M1.v AS V1, M2.v AS V2 FROM mytable1 AS M1, mytable2 AS M2) AS T \
                ORDER BY X ASC, Y DESC;");
            Parser parser(lexer);
            auto stmt = as<SelectStmt>(parser.parse());
            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
        }

        SECTION("Nested named expression, SELECT with alias and no table specified, \
            ORDER BY is ambiguous")
        {
            LEXER("SELECT V1 AS X, V2 AS X FROM \
                (SELECT M1.v AS V1, M2.v AS V2 FROM mytable1 AS M1, mytable2 AS M2) AS T \
                ORDER BY X;");
            Parser parser(lexer);
            auto stmt = as<SelectStmt>(parser.parse());
            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            REQUIRE(diag.num_errors() == 1);
            REQUIRE(not err.str().empty());
        }

        SECTION("Nested named expression, SELECT with alias and no table specified, \
            ORDER BY is not ambiguous")
        {
            LEXER("SELECT V1 AS X, V2 AS Y FROM \
                (SELECT M1.v AS V1, M2.v AS V2 FROM mytable1 AS M1, mytable2 AS M2) AS T \
                ORDER BY X ASC, Y DESC;");
            Parser parser(lexer);
            auto stmt = as<SelectStmt>(parser.parse());
            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
        }

        SECTION("SELECT is ambiguous from nested statement")
        {
            LEXER("SELECT v FROM (SELECT v FROM mytable1) AS X, (SELECT v FROM mytable2) AS Y;");
            Parser parser(lexer);
            auto stmt = as<SelectStmt>(parser.parse());
            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            REQUIRE(diag.num_errors() == 1);
            REQUIRE(not err.str().empty());
        }

        SECTION("SELECT ambiguous attribute from two tables")
        {
            LEXER("SELECT v FROM mytable1, mytable2;");
            Parser parser(lexer);
            auto stmt = as<SelectStmt>(parser.parse());
            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            REQUIRE(diag.num_errors() == 1);
            REQUIRE(not err.str().empty());
        }
    }
}

/*========================================================================================
  Clause Tests
  ========================================================================================*/

TEST_CASE("Sema/Clauses/Select", "[core][parse][sema]")
{
    Catalog::Clear();

    /* Create a dummy DB and a dummy table with 2 vector attributes. */
    Catalog &C = Catalog::Get();
    const char *db_name = "mydb";
    auto &DB = C.add_database(db_name);
    C.set_database_in_use(DB);
    auto &table = DB.add_table(C.pool("mytable"));
    table.push_back(C.pool("v"), Type::Get_Integer(Type::TY_Vector, 4));
    table.push_back(C.pool("w"), Type::Get_Integer(Type::TY_Vector, 4));

    SECTION("SELECT all.")
    {
        LEXER("SELECT * FROM mytable;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
    }

    SECTION("SELECT specific value.")
    {
        LEXER("SELECT v FROM mytable;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
    }

    SECTION("SELECT specific attribute with table specified.")
    {
        LEXER("SELECT mytable.v FROM mytable;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
    }

    SECTION("SELECT attribute does not exist.")
    {
        LEXER("SELECT mytable.x FROM mytable;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 1);
        REQUIRE(not err.str().empty());
    }

    SECTION("SELECT table does not exist.")
    {
        LEXER("SELECT myothertable.v FROM mytable;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 1);
        REQUIRE(not err.str().empty());
    }

    SECTION("SELECT with vectorial and scalar mixed is not allowed")
    {
        LEXER("SELECT v, w FROM mytable GROUP BY w;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 1);
        REQUIRE(not err.str().empty());
    }

    SECTION("SELECT 2 scalars where one is aggregated.")
    {
        LEXER("SELECT 42,v FROM mytable GROUP BY v;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
    }

    SECTION("SELECT MIN of a grouping attribute.")
    {
        LEXER("SELECT MIN(v) FROM mytable GROUP BY v;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 0); // This is not an error,
        REQUIRE(not err.str().empty());  // but we expect a warning
    }

    SECTION("SELECT MAX(42).")
    {
        LEXER("SELECT MAX(42);");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 0);
        REQUIRE(not err.str().empty());
    }
}

TEST_CASE("Sema/Clauses/From", "[core][parse][sema]")
{
    Catalog::Clear();

    /* Create a dummy DB and 2 dummy tables with 2 vector attributes. */
    Catalog &C = Catalog::Get();
    const char *db_name = "mydb";
    auto &DB = C.add_database(db_name);
    C.set_database_in_use(DB);
    auto &table = DB.add_table(C.pool("mytable"));
    table.push_back(C.pool("v"), Type::Get_Integer(Type::TY_Vector, 4));
    auto &table2 = DB.add_table(C.pool("mytable2"));
    table2.push_back(C.pool("w"), Type::Get_Integer(Type::TY_Vector, 4));

    SECTION("FROM is ok.")
    {
        LEXER("SELECT * FROM mytable;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
    }

    SECTION("FROM table does not exist.")
    {
        LEXER("SELECT * FROM myothertable;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 1);
        REQUIRE(not err.str().empty());
    }

    SECTION("FROM with alias is ok.")
    {
        LEXER("SELECT mt.v FROM mytable AS mt;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
    }

    SECTION("FROM with 2 aliases which are the same.")
    {
        LEXER("SELECT * FROM mytable AS M, mytable2 AS M;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 1);
        REQUIRE(not err.str().empty());
    }

    SECTION("FROM with alias that is equal to another table name.")
    {
        LEXER("SELECT mytable2.w,mytable2.v FROM mytable AS mytable2;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 1);
        REQUIRE(not err.str().empty());
    }

    SECTION("Nested FROM statement is ok.")
    {
        LEXER("SELECT * FROM (SELECT * FROM mytable) AS sub;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
    }

    SECTION("Nested FROM statement with alias is ok.")
    {
        LEXER("SELECT * FROM (SELECT * FROM mytable AS mt) AS sub;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
    }

    SECTION("Nested FROM statement with alias that is equal to another table name.")
    {
        LEXER("SELECT * FROM (SELECT * FROM mytable) AS mytable2;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
    }

    SECTION("Same alias occurs twice")
    {
        LEXER("SELECT * FROM mytable2 AS table, mytable1 AS table;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 1);
        REQUIRE(not err.str().empty());
    }

    SECTION("Same alias occurs twice in nested statement")
    {
        LEXER("SELECT * FROM (SELECT * FROM mytable2 AS table, mytable1 AS table) AS T;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 1);
        REQUIRE(not err.str().empty());
    }

    SECTION("Alias is equal to another table name")
    {
        LEXER("SELECT * FROM mytable, (SELECT 42) AS mytable;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 1);
        REQUIRE(not err.str().empty());
    }
}

TEST_CASE("Sema/Clauses/Where", "[core][parse][sema]")
{
    Catalog::Clear();

    /* Create a dummy DB and a dummy table with a vector attribute. */
    Catalog &C = Catalog::Get();
    const char *db_name = "mydb";
    auto &DB = C.add_database(db_name);
    C.set_database_in_use(DB);
    auto &table = DB.add_table(C.pool("mytable"));
    table.push_back(C.pool("v"), Type::Get_Integer(Type::TY_Vector, 4));

    SECTION("WHERE condition is ok.")
    {
        LEXER("SELECT * FROM mytable WHERE v > (2*21);");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());

        WhereClause *where = as<WhereClause>(stmt->where.get());
        const PrimitiveType *pt = as<const PrimitiveType>(where->where->type());
        CHECK(pt->is_vectorial());
    }

    SECTION("WHERE condition is not boolean.")
    {
        LEXER("SELECT * FROM mytable WHERE 42;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 1);
        REQUIRE(not err.str().empty());
    }

    SECTION("Scalar function in WHERE clause. (test/ours/sema-pos-select-scalar_function_in_where.yml)")
    {
        LEXER("SELECT * FROM mytable WHERE ISNULL(1 = 1);");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
    }

    SECTION("Scalar function in WHERE clause. (test/ours/sema-pos-select-scalar_function_in_where.yml)")
    {
        LEXER("SELECT * FROM mytable WHERE ISNULL(v);");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
    }

    SECTION("Illegal scalar in WHERE condition")
    {
        LEXER("SELECT * FROM mytable WHERE AVG(v)>42;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 1);
        REQUIRE(not err.str().empty());
    }

    SECTION("WHERE condition has erroneous expressoin")
    {
        LEXER("SELECT * FROM mytable WHERE TRUE>42;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 1);
        REQUIRE(not err.str().empty());
    }

}

TEST_CASE("Sema/Clauses/GroupBy", "[core][parse][sema]")
{
    Catalog::Clear();

    /* Create a dummy DB and a dummy table with a vector attribute. */
    Catalog &C = Catalog::Get();
    const char *db_name = "mydb";
    auto &DB = C.add_database(db_name);
    C.set_database_in_use(DB);
    auto &table = DB.add_table(C.pool("mytable"));
    table.push_back(C.pool("v"), Type::Get_Integer(Type::TY_Vector, 4));

    SECTION("GROUP BY is ok.")
    {
        LEXER("SELECT * FROM mytable GROUP BY v;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
    }

    SECTION("GROUP BY clause is scalar instead of vector.")
    {
        LEXER("SELECT * FROM mytable GROUP BY 42;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 2);
        REQUIRE(not err.str().empty());
    }

    // This test should throw an error?
    SECTION("Scalar function in GROUP BY clause. (test/ours/sema-pos-select-scalar_function_in_group_by.yml)")
    {
        LEXER("SELECT * FROM mytable GROUP BY ISNULL(v);");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
    }

    SECTION("GROUP BY SUM() is illegal")
    {
        LEXER("SELECT * FROM mytable GROUP BY SUM(v);");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 3);
        REQUIRE(not err.str().empty());
    }

    SECTION("GROUP BY error expression.")
    {
        LEXER("SELECT * FROM mytable GROUP BY 42+NULL;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 2);
        REQUIRE(not err.str().empty());
    }

}

TEST_CASE("Sema/Clauses/Having", "[core][parse][sema]")
{
    Catalog::Clear();

    /* Create a dummy DB and a dummy table with a vector attribute. */
    Catalog &C = Catalog::Get();
    const char *db_name = "mydb";
    auto &DB = C.add_database(db_name);
    C.set_database_in_use(DB);
    auto &table = DB.add_table(C.pool("mytable"));
    table.push_back(C.pool("v"), Type::Get_Integer(Type::TY_Vector, 4));

    SECTION("HAVING without GROUP BY should be ok.")
    {
        LEXER("SELECT * FROM mytable HAVING AVG(v)>0;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        // Just warning expected
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(not err.str().empty());
    }

    SECTION("HAVING expression is integer instead of boolean")
    {
        LEXER("SELECT * FROM mytable HAVING 0;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 1);
        REQUIRE(not err.str().empty());
    }

    SECTION("HAVING expression is vector instead of scalar")
    {
        LEXER("SELECT * FROM mytable HAVING v=42;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 1);
        REQUIRE(not err.str().empty());
    }

    SECTION("HAVING condition has erroneous expressoin")
    {
        LEXER("SELECT * FROM mytable HAVING TRUE>42;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 1);
        REQUIRE(not err.str().empty());
    }

}

TEST_CASE("Sema/Clauses/OrderBy", "[core][parse][sema]")
{
    Catalog::Clear();

    /* Create a dummy DB and a dummy table with 2 vector attributes. */
    Catalog &C = Catalog::Get();
    const char *db_name = "mydb";
    auto &DB = C.add_database(db_name);
    C.set_database_in_use(DB);
    auto &table = DB.add_table(C.pool("mytable"));
    table.push_back(C.pool("v"), Type::Get_Integer(Type::TY_Vector, 4));
    table.push_back(C.pool("b"), Type::Get_Boolean(Type::TY_Vector));


    SECTION("ORDER BY expression is ok.")
    {
        LEXER("SELECT * FROM mytable ORDER BY v;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
    }

    SECTION("ORDER BY expression is invalid.")
    {
        LEXER("SELECT * FROM mytable ORDER BY 42;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 1);
        REQUIRE(not err.str().empty());
    }

    SECTION("ORDER BY with GROUP BY is ok.")
    {
        LEXER("SELECT * FROM mytable GROUP BY v ORDER BY v DESC;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
    }

    SECTION("ORDER BY is not scalar and not grouping key of GROUP BY.")
    {
        LEXER("SELECT * FROM mytable GROUP BY v ORDER BY b;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 1);
        REQUIRE(not err.str().empty());
    }

    SECTION("ORDER BY is not scalar and implicit grouping because of HAVING clause.")
    {
        LEXER("SELECT 1 FROM mytable HAVING SUM(v) > 42 ORDER BY b;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 1);
        REQUIRE(not err.str().empty());
    }
}

TEST_CASE("Sema/Clauses/Limit", "[core][parse][sema]")
{
    Catalog::Clear();

    /* Create a dummy DB and a dummy table with a vector attribute. */
    Catalog &C = Catalog::Get();
    const char *db_name = "mydb";
    auto &DB = C.add_database(db_name);
    C.set_database_in_use(DB);
    auto &table = DB.add_table(C.pool("mytable"));
    table.push_back(C.pool("v"), Type::Get_Integer(Type::TY_Vector, 4));

    SECTION("LIMIT is ok.")
    {
        LEXER("SELECT * FROM mytable LIMIT 42;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
    }

    SECTION("LIMIT with OFFSET is ok.")
    {
        LEXER("SELECT * FROM mytable LIMIT 3 OFFSET 5;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
    }
}

/*========================================================================================
  Statement Tests
  ========================================================================================*/

TEST_CASE("Sema/Statements/CreateDatabase", "[core][parse][sema]")
{
    Catalog::Clear();

    /* Create a dummy DB to test on. */
    Catalog &C = Catalog::Get();
    const char *db_name = C.pool("mydb");
    auto &DB = C.add_database(db_name);
    C.set_database_in_use(DB);

    SECTION("Create Database Statement is ok.")
    {
        LEXER("CREATE DATABASE foo;");
        Parser parser(lexer);
        auto stmt = as<CreateDatabaseStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
    }

    SECTION("Create Database which already exists.")
    {
        LEXER("CREATE DATABASE mydb;");
        Parser parser(lexer);
        auto stmt = as<CreateDatabaseStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 1);
        REQUIRE(not err.str().empty());
    }

}

TEST_CASE("Sema/Statements/UseDatabase", "[core][parse][sema]")
{
    Catalog::Clear();

    /* Create 2 dummy DBs to test on and set mydb in use. */
    Catalog &C = Catalog::Get();
    const char *db_name1 = C.pool("mydb1");
    auto &DB1 = C.add_database(db_name1);
    const char *db_name2 = C.pool("mydb2");
    C.add_database(db_name2);
    C.set_database_in_use(DB1);

    SECTION("Use Database Statement is ok")
    {
        LEXER("USE mydb2;");
        Parser parser(lexer);
        auto stmt = as<UseDatabaseStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
    }

    SECTION("Use Database which does not exist.")
    {
        LEXER("USE nonexistent;");
        Parser parser(lexer);
        auto stmt = as<UseDatabaseStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 1);
        REQUIRE(not err.str().empty());
    }

    // Error or no error?
    SECTION("Use Database which is already used.")
    {
        LEXER("USE mydb;");
        Parser parser(lexer);
        auto stmt = as<UseDatabaseStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 1);
        REQUIRE(not err.str().empty());
    }

}

TEST_CASE("Sema/Statements/CreateTable", "[core][parse][sema]")
{
    Catalog::Clear();

    /* Create a dummy DB to test on. */
    Catalog &C = Catalog::Get();
    const char *db_name = "mydb";
    auto &DB = C.add_database(db_name);

    SECTION("Create table without database selected")
    {
        LEXER("CREATE TABLE my_table(x INT(4));");
        Parser parser(lexer);
        auto stmt = as<CreateTableStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 1);
        REQUIRE(not err.str().empty());
    }

    SECTION("With database selected")
    {
        C.set_database_in_use(DB);

        SECTION("Create table statement which is ok. (test/ours/sema-pos-create-all_datatype.yml)")
        {
            LEXER("CREATE TABLE my_table ( \
                x INT(4), \
                y FLOAT, \
                z DECIMAL(10, 2), \
                da DATE, \
                dt DATETIME, \
                vc VARCHAR(42), \
                c CHAR(13), \
                b BOOL, \
                d DOUBLE \
                );");
            Parser parser(lexer);
            auto stmt = as<CreateTableStmt>(parser.parse());
            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
        }

        SECTION("Create table with duplicate attribute.")
        {
            LEXER("CREATE TABLE my_table ( \
                x FLOAT, \
                y FLOAT, \
                x DECIMAL(10, 2) \
                );");
            Parser parser(lexer);
            auto stmt = as<CreateTableStmt>(parser.parse());
            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            REQUIRE(diag.num_errors() == 1);
            REQUIRE(not err.str().empty());
        }

        SECTION("Create table that already exists.")
        {
            auto &table = DB.add_table(C.pool("exists"));
            table.push_back(C.pool("v"), Type::Get_Integer(Type::TY_Vector, 4));

            LEXER("CREATE TABLE exists (x FLOAT);");
            Parser parser(lexer);
            auto stmt = as<CreateTableStmt>(parser.parse());
            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            REQUIRE(diag.num_errors() == 1);
            REQUIRE(not err.str().empty());
        }

        SECTION("Create table with constraints.")
        {
            SECTION("Create table with constraints is ok.")
            {
                LEXER("CREATE TABLE my_table ( \
                x INT(4) PRIMARY KEY, \
                y FLOAT NOT NULL, \
                b BOOL CHECK (b=TRUE), \
                d DOUBLE UNIQUE \
                );");
                Parser parser(lexer);
                auto stmt = as<CreateTableStmt>(parser.parse());
                REQUIRE(diag.num_errors() == 0);
                REQUIRE(err.str().empty());
                Sema sema(diag);
                sema(*stmt);

                REQUIRE(diag.num_errors() == 0);
                REQUIRE(err.str().empty());
            }

            SECTION("Create table with attribute which has double primary key.")
            {
                LEXER("CREATE TABLE my_table ( \
                x INT(4) PRIMARY KEY PRIMARY KEY \
                );");
                Parser parser(lexer);
                auto stmt = as<CreateTableStmt>(parser.parse());
                REQUIRE(diag.num_errors() == 0);
                REQUIRE(err.str().empty());
                Sema sema(diag);
                sema(*stmt);

                REQUIRE(diag.num_errors() == 1);
                REQUIRE(not err.str().empty());
            }

            // Only warning, not error expected
            SECTION("Create table with attribute which has double UNIQUE.")
            {
                LEXER("CREATE TABLE my_table ( \
                x INT(4) UNIQUE UNIQUE \
                );");
                Parser parser(lexer);
                auto stmt = as<CreateTableStmt>(parser.parse());
                REQUIRE(diag.num_errors() == 0);
                REQUIRE(err.str().empty());
                Sema sema(diag);
                sema(*stmt);

                REQUIRE(diag.num_errors() == 0);
                REQUIRE(not err.str().empty());
            }

            // Only warning, not error expected
            SECTION("Create table with attribute which has double NOT NULL.")
            {
                LEXER("CREATE TABLE my_table ( \
                x INT(4) NOT NULL NOT NULL \
                );");
                Parser parser(lexer);
                auto stmt = as<CreateTableStmt>(parser.parse());
                REQUIRE(diag.num_errors() == 0);
                REQUIRE(err.str().empty());
                Sema sema(diag);
                sema(*stmt);

                REQUIRE(diag.num_errors() == 0);
                REQUIRE(not err.str().empty());
            }

            SECTION("Create table with attribute which has non-boolean CHECK-condition.")
            {
                LEXER("CREATE TABLE my_table ( \
                x INT(4) CHECK (x) \
                );");
                Parser parser(lexer);
                auto stmt = as<CreateTableStmt>(parser.parse());
                REQUIRE(diag.num_errors() == 0);
                REQUIRE(err.str().empty());
                Sema sema(diag);
                sema(*stmt);

                REQUIRE(diag.num_errors() == 1);
                REQUIRE(not err.str().empty());
            }

            SECTION("Create table with attribute which has multiple constraints is ok.")
            {
                LEXER("CREATE TABLE my_table ( \
                x INT(4) PRIMARY KEY UNIQUE NOT NULL CHECK (x<10) \
                );");
                Parser parser(lexer);
                auto stmt = as<CreateTableStmt>(parser.parse());
                REQUIRE(diag.num_errors() == 0);
                REQUIRE(err.str().empty());
                Sema sema(diag);
                sema(*stmt);

                REQUIRE(diag.num_errors() == 0);
                REQUIRE(err.str().empty());
            }

            SECTION("REFERENCES")
            {
                // Add a table to the database for referencing
                auto &table = DB.add_table(C.pool("mytable"));
                table.push_back(C.pool("v"), Type::Get_Integer(Type::TY_Vector, 4));

                /* uncertain
                SECTION("Reference to the same table.")
                {
                    LEXER("CREATE TABLE mytable2 ( \
                    x FLOAT, \
                    y FLOAT REFERENCES mytable2(x) \
                    );");
                    Parser parser(lexer);
                    auto stmt = as<CreateTableStmt>(parser.parse());
                    REQUIRE(diag.num_errors() == 0);
                    REQUIRE(err.str().empty());
                    Sema sema(diag);
                    sema(*stmt);

                    REQUIRE(diag.num_errors() == 0);
                    REQUIRE(err.str().empty());
                }
               */

                SECTION("Reference to an attribute with different type.")
                {
                    LEXER("CREATE TABLE mytable2 ( \
                    y FLOAT REFERENCES mytable(v) \
                    );");
                    Parser parser(lexer);
                    auto stmt = as<CreateTableStmt>(parser.parse());
                    REQUIRE(diag.num_errors() == 0);
                    REQUIRE(err.str().empty());
                    Sema sema(diag);
                    sema(*stmt);

                    REQUIRE(diag.num_errors() == 1);
                    REQUIRE(not err.str().empty());
                }

                SECTION("Reference to an attribute that does not exist.")
                {
                    LEXER("CREATE TABLE mytable2 ( \
                    y FLOAT REFERENCES mytable(x) \
                    );");
                    Parser parser(lexer);
                    auto stmt = as<CreateTableStmt>(parser.parse());
                    REQUIRE(diag.num_errors() == 0);
                    REQUIRE(err.str().empty());
                    Sema sema(diag);
                    sema(*stmt);

                    REQUIRE(diag.num_errors() == 1);
                    REQUIRE(not err.str().empty());
                }

                SECTION("Reference to a table that does not exist.")
                {
                    LEXER("CREATE TABLE mytable2 ( \
                    y FLOAT REFERENCES nonexistent(x) \
                    );");
                    Parser parser(lexer);
                    auto stmt = as<CreateTableStmt>(parser.parse());
                    REQUIRE(diag.num_errors() == 0);
                    REQUIRE(err.str().empty());
                    Sema sema(diag);
                    sema(*stmt);

                    REQUIRE(diag.num_errors() == 1);
                    REQUIRE(not err.str().empty());
                }

                SECTION("Double reference.")
                {
                    LEXER("CREATE TABLE mytable2 ( \
                    y INT(4) REFERENCES mytable(v) REFERENCES mytable(v) \
                    );");
                    Parser parser(lexer);
                    auto stmt = as<CreateTableStmt>(parser.parse());
                    REQUIRE(diag.num_errors() == 0);
                    REQUIRE(err.str().empty());
                    Sema sema(diag);
                    sema(*stmt);

                    REQUIRE(diag.num_errors() == 1);
                    REQUIRE(not err.str().empty());
                }
            }
        }
    }
}

TEST_CASE("Sema/Statements/Select", "[core][parse][sema]")
{
    Catalog::Clear();

    /* Create a dummy DB and a dummy table with a vector attribute. */
    Catalog &C = Catalog::Get();
    const char *db_name = "mydb";
    auto &DB = C.add_database(db_name);
    //C.set_database_in_use(DB);
    auto &table = DB.add_table(C.pool("mytable"));
    table.push_back(C.pool("v"), Type::Get_Integer(Type::TY_Vector, 4));

    SECTION("Select without database selected")
    {
        LEXER("SELECT * FROM mytable;");
        Parser parser(lexer);
        auto stmt = as<SelectStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 1);
        REQUIRE(not err.str().empty());
    }
}

TEST_CASE("Sema/Statements/Insert", "[core][parse][sema]")
{
    Catalog::Clear();

    /* Create a dummy DB and a dummy table with integer and boolean vectorial attribute. */
    Catalog &C = Catalog::Get();
    const char *db_name = C.pool("mydb");
    auto &DB = C.add_database(db_name);
    //C.set_database_in_use(DB);
    auto &table1 = DB.add_table(C.pool("mytable"));
    table1.push_back(C.pool("v"), Type::Get_Integer(Type::TY_Vector, 4));
    table1.push_back(C.pool("b"), Type::Get_Boolean(Type::TY_Vector));
    table1.push_back(C.pool("c"), Type::Get_Char(Type::TY_Vector, 4));
    auto &table2 = DB.add_table(C.pool("single"));
    table2.push_back(C.pool("w"), Type::Get_Integer(Type::TY_Vector, 4));



    SECTION("Insert without database in use")
    {
        LEXER("INSERT INTO mytable VALUES (5);");
        Parser parser(lexer);
        auto stmt = as<InsertStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 1);
        REQUIRE(not err.str().empty());
    }

    SECTION("Insert with database in use")
    {
        C.set_database_in_use(DB);

        SECTION("Insert into table that does not exist")
        {
            LEXER("INSERT INTO nonexistent VALUES (TRUE, 5, \"x\");");
            Parser parser(lexer);
            auto stmt = as<InsertStmt>(parser.parse());
            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            REQUIRE(diag.num_errors() == 1);
            REQUIRE(not err.str().empty());
        }

        SECTION("Insert has not enough values")
        {
            LEXER("INSERT INTO mytable VALUES (5);");
            Parser parser(lexer);
            auto stmt = as<InsertStmt>(parser.parse());
            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            REQUIRE(diag.num_errors() == 1);
            REQUIRE(not err.str().empty());
        }

        SECTION("Insert has too many values")
        {
            LEXER("INSERT INTO mytable VALUES (5, TRUE,  \"x\", 42);");
            Parser parser(lexer);
            auto stmt = as<InsertStmt>(parser.parse());
            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            REQUIRE(diag.num_errors() == 1);
            REQUIRE(not err.str().empty());
        }

        SECTION("Insert numeric value into boolean column")
        {
            LEXER("INSERT INTO mytable VALUES (5, 42,  \"x\");");
            Parser parser(lexer);
            auto stmt = as<InsertStmt>(parser.parse());
            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            REQUIRE(diag.num_errors() == 1);
            REQUIRE(not err.str().empty());
        }

        SECTION("Insert boolean value to numeric column")
        {
            LEXER("INSERT INTO mytable VALUES (TRUE, FALSE,  \"x\");");
            Parser parser(lexer);
            auto stmt = as<InsertStmt>(parser.parse());
            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            REQUIRE(diag.num_errors() == 1);
            REQUIRE(not err.str().empty());
        }

        SECTION("Insert boolean to numeric and numeric value to boolean column")
        {
            LEXER("INSERT INTO mytable VALUES (TRUE, 42,  \"x\");");
            Parser parser(lexer);
            auto stmt = as<InsertStmt>(parser.parse());
            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            REQUIRE(diag.num_errors() == 2);
            REQUIRE(not err.str().empty());
        }

        SECTION("Insert NULL is ok")
        {
            LEXER("INSERT INTO mytable VALUES (NULL, NULL, NULL);");
            Parser parser(lexer);
            auto stmt = as<InsertStmt>(parser.parse());
            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
        }

        SECTION("Insert DEFAULT is ok")
        {
            LEXER("INSERT INTO mytable VALUES (DEFAULT, DEFAULT, DEFAULT);");
            Parser parser(lexer);
            auto stmt = as<InsertStmt>(parser.parse());
            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
        }
    }
}

#if 0
NOT IMPLEMENTED IN SEMA.CPP
TEST_CASE("Sema/Statements/Update", "[core][parse][sema]")
{
    //TODO
}

TEST_CASE("Sema/Statements/Delete", "[core][parse][sema]")
{
    Catalog::Clear();

    //Create a dummy DB and a dummy table with integer and boolean vectorial attribute.
    Catalog &C = Catalog::Get();
    const char *db_name = "mydb";
    auto &DB = C.add_database(db_name);
    //C.set_database_in_use(DB);
    auto &table = DB.add_table(C.pool("mytable"));
    table.push_back(C.pool("v"), Type::Get_Integer(Type::TY_Vector, 4));
    table.push_back(C.pool("b"), Type::Get_Boolean(Type::TY_Vector));

    SECTION("DELETE all rows without deleting the table")
    {
        LEXER("DELETE FROM mytable;");
        Parser parser(lexer);
        auto stmt = as<DeleteStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
    }

    SECTION("DELETE with WHERE")
    {
        LEXER("DELETE FROM mytable WHERE v=0;");
        Parser parser(lexer);
        auto stmt = as<DeleteStmt>(parser.parse());
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        Sema sema(diag);
        sema(*stmt);

        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
    }
}
#endif

TEST_CASE("Sema/Statements/DSVImport", "[core][parse][sema]")
{
    Catalog::Clear();

    //Create a dummy DB and a dummy table with integer and boolean vectorial attribute.
    Catalog &C = Catalog::Get();
    const char *db_name = "mydb";
    auto &DB = C.add_database(db_name);
    //C.set_database_in_use(DB);
    auto &table = DB.add_table(C.pool("mytable"));
    table.push_back(C.pool("v"), Type::Get_Integer(Type::TY_Vector, 4));

    SECTION("Import without database selected")
    {
        SECTION("Import without additional info")
        {
            LEXER("IMPORT INTO mytable DSV \"test\";");
            Parser parser(lexer);
            auto stmt = as<ImportStmt>(parser.parse());
            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            REQUIRE(diag.num_errors() == 1);
            REQUIRE(not err.str().empty());
        }
    }

    SECTION("Import with database selected")
    {
        C.set_database_in_use(DB);

        SECTION("Import without additional info")
        {
            LEXER("IMPORT INTO mytable DSV \"test\";");
            Parser parser(lexer);
            auto stmt = as<ImportStmt>(parser.parse());
            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
        }

        SECTION("Import into table that does not exist")
        {
            LEXER("IMPORT INTO nonexistent DSV \"test\";");
            Parser parser(lexer);
            auto stmt = as<ImportStmt>(parser.parse());
            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            REQUIRE(diag.num_errors() == 1);
            REQUIRE(not err.str().empty());
        }

        SECTION("Import with every possible info")
        {
            LEXER("IMPORT INTO mytable DSV \"test\" \
                ROWS 42 \
                DELIMITER \",\" \
                ESCAPE \"|\" \
                QUOTE \"\'\" \
                HAS HEADER \
                SKIP HEADER;");
            Parser parser(lexer);
            auto stmt = as<ImportStmt>(parser.parse());
            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
        }

        SECTION("Delimiter has length > 1")
        {
            LEXER("IMPORT INTO mytable DSV \"test\" \
                DELIMITER \",,\";");
            Parser parser(lexer);
            auto stmt = as<ImportStmt>(parser.parse());
            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            REQUIRE(diag.num_errors() == 1);
            REQUIRE(not err.str().empty());
        }

        SECTION("Quote has length > 1")
        {
            LEXER("IMPORT INTO mytable DSV \"test\" \
                QUOTE \"''\";");
            Parser parser(lexer);
            auto stmt = as<ImportStmt>(parser.parse());
            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            REQUIRE(diag.num_errors() == 1);
            REQUIRE(not err.str().empty());
        }

        SECTION("Escape has length > 1")
        {
            LEXER("IMPORT INTO mytable DSV \"test\" \
                ESCAPE \"||\";");
            Parser parser(lexer);
            auto stmt = as<ImportStmt>(parser.parse());
            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            REQUIRE(diag.num_errors() == 1);
            REQUIRE(not err.str().empty());
        }

        SECTION("Same character for delimiter and quote")
        {
            LEXER("IMPORT INTO mytable DSV \"test\" \
                DELIMITER \",\" \
                QUOTE \",\";");
            Parser parser(lexer);
            auto stmt = as<ImportStmt>(parser.parse());
            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            REQUIRE(diag.num_errors() == 1);
            REQUIRE(not err.str().empty());
        }

        SECTION("SKIP HEADER without HAS HEADER")
        {
            LEXER("IMPORT INTO mytable DSV \"test\" SKIP HEADER;");
            Parser parser(lexer);
            auto stmt = as<ImportStmt>(parser.parse());
            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            REQUIRE(diag.num_errors() == 0);
            REQUIRE(err.str().empty());
        }
    }
}

TEST_CASE("Sema/Nested Queries", "[core][parse][sema]")
{
    Catalog::Clear();

    /* Create a dummy DB and three dummy tables with a vector attribute. */
    Catalog &Cat = Catalog::Get();
    const char *db_name = "mydb";
    auto &DB = Cat.add_database(db_name);
    Cat.set_database_in_use(DB);
    auto &A = DB.add_table(Cat.pool("A"));
    auto &B = DB.add_table(Cat.pool("B"));
    auto &C = DB.add_table(Cat.pool("C"));
    A.push_back(Cat.pool("v"), Type::Get_Integer(Type::TY_Vector, 4));
    B.push_back(Cat.pool("v"), Type::Get_Integer(Type::TY_Vector, 4));
    C.push_back(Cat.pool("w"), Type::Get_Integer(Type::TY_Vector, 4));

    SECTION("Uncorrelated Queries")
    {
        const char* queries[] = {
            "SELECT 42, (SELECT 17);",
            "SELECT 42, (SELECT MIN(v) FROM A);",
            "SELECT v, (SELECT MIN(A.v) FROM A) FROM B;",
            "SELECT v FROM B WHERE v = (SELECT 17);",
            "SELECT v FROM B WHERE v != (SELECT MIN(A.v) FROM A);",
            "SELECT v FROM B WHERE v != (SELECT MIN(w) FROM C);",
            "SELECT v FROM B GROUP BY v HAVING v < (SELECT 17);",
            "SELECT v FROM B GROUP BY v HAVING v >= (SELECT MIN(A.v) FROM A);",
            "SELECT v FROM B GROUP BY v HAVING v >= (SELECT MIN(w) FROM C);",
        };

        for (auto q : queries) {
            LEXER(q);
            Parser parser(lexer);
            auto stmt = as<SelectStmt>(parser.parse());
            CHECK(diag.num_errors() == 0);
            CHECK(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            if (diag.num_errors() != 0 or not err.str().empty())
                std::cerr << "ERROR for input \"" << q << "\": " << err.str() << std::endl;
            CHECK(diag.num_errors() == 0);
            CHECK(err.str().empty());

        }
    }

    SECTION("Correlated Queries")
    {
        const char* queries[] = {
            "SELECT 42, (SELECT MIN(A.v) FROM A WHERE A.v = B.v) FROM B;",
            "SELECT v FROM B WHERE v != (SELECT MIN(A.v) FROM A WHERE A.v = B.v);",
            "SELECT v FROM B GROUP BY v HAVING v >= (SELECT MIN(A.v) FROM A WHERE A.v = B.v);",
            "SELECT 42, (SELECT MIN(w) FROM C WHERE w = v) FROM B;",
            "SELECT v FROM B WHERE v != (SELECT MIN(w) FROM C WHERE w = v);",
            "SELECT v FROM B GROUP BY v HAVING v >= (SELECT MIN(w) FROM C WHERE w = v);",
        };

        for (auto q : queries) {
            LEXER(q);
            Parser parser(lexer);
            auto stmt = as<SelectStmt>(parser.parse());
            CHECK(diag.num_errors() == 0);
            CHECK(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            if (diag.num_errors() != 0 or not err.str().empty())
                std::cerr << "ERROR for input \"" << q << "\": " << err.str() << std::endl;
            CHECK(diag.num_errors() == 0);
            CHECK(err.str().empty());

        }
    }

    SECTION("Santity tests")
    {
        const char* queries[] = {
            /* invalid number of columns */
            "SELECT 42, (SELECT 17, 29);",
            "SELECT v FROM B WHERE v = (SELECT 17, 29);",
            "SELECT v FROM B GROUP BY v HAVING v = (SELECT 17, 29);",

            /* no single scalar value */
            "SELECT 42, (SELECT 17 FROM A);",
            "SELECT 42, (SELECT v FROM A);",
            "SELECT 42, (SELECT MIN(v) FROM A GROUP BY v);",
            "SELECT v FROM B WHERE v = (SELECT A.v FROM A);",
            "SELECT v FROM B WHERE v = (SELECT MIN(A.v) FROM A GROUP BY A.v);",
            "SELECT v FROM B GROUP BY v HAVING v = (SELECT A.v FROM A);",
            "SELECT v FROM B GROUP BY v HAVING v = (SELECT MIN(A.v) FROM A GROUP BY A.v);",

            /* invalid reference to outer table */
            "SELECT 42, (SELECT MIN(A.v) FROM A WHERE A.v = B.v) FROM A;",
            "SELECT v FROM A WHERE v != (SELECT MIN(A.v) FROM A WHERE A.v = B.v);",
            "SELECT v FROM A GROUP BY v HAVING v >= (SELECT MIN(A.v) FROM A WHERE A.v = B.v);",

            /* dublicate table name */
            "SELECT v FROM A WHERE v = (SELECT MIN(A.v) FROM A WHERE A.v = A.v);",
            "SELECT v FROM A WHERE v = (SELECT MIN(A.v) FROM B AS A WHERE A.v = A.v);",

            /* ambiguous designator */
            "SELECT 42, (SELECT MIN(v) FROM A WHERE A.v = B.v) FROM A;",
            "SELECT v FROM A WHERE v != (SELECT MIN(v) FROM A WHERE A.v = B.v);",
            "SELECT v FROM A GROUP BY v HAVING v >= (SELECT MIN(v) FROM A WHERE A.v = B.v);",

            /* invalid position for correlation */
            "SELECT 42, (SELECT B.v) FROM B;",
            "SELECT 42, (SELECT MIN(B.v) FROM A WHERE A.v = B.v) FROM B;",
            "SELECT 42, (SELECT MIN(A.v) FROM A WHERE A.v = B.v GROUP BY B.v) FROM B;",
            "SELECT 42, (SELECT MIN(A.v) FROM A WHERE A.v = B.v ORDER BY B.v) FROM B;",
            "SELECT v FROM B WHERE v != (SELECT B.v);",
            "SELECT v FROM B WHERE v != (SELECT MIN(B.v) FROM A WHERE A.v = B.v);",
            "SELECT v FROM B WHERE v != (SELECT MIN(A.v) FROM A WHERE A.v = B.v GROUP BY B.v);",
            "SELECT v FROM B WHERE v != (SELECT MIN(A.v) FROM A WHERE A.v = B.v ORDER BY B.v);",
            "SELECT v FROM B GROUP BY v HAVING v >= (SELECT B.v);",
            "SELECT v FROM B GROUP BY v HAVING v >= (SELECT MIN(B.v) FROM A WHERE A.v = B.v);",
            "SELECT v FROM B GROUP BY v HAVING v >= (SELECT MIN(A.v) FROM A WHERE A.v = B.v GROUP BY B.v);",
            "SELECT v FROM B GROUP BY v HAVING v >= (SELECT MIN(A.v) FROM A WHERE A.v = B.v ORDER BY B.v);",
            "SELECT 42 FROM B, (SELECT MIN(A.v) AS min FROM A WHERE A.v = B.v) AS Q WHERE B.v = Q.min;",

            /* invalid position for nested query */
            "SELECT v FROM B GROUP BY (SELECT 17);",
            "SELECT v FROM B ORDER BY (SELECT 17);",
        };

        for (auto q : queries) {
            LEXER(q);
            Parser parser(lexer);
            auto stmt = as<SelectStmt>(parser.parse());
            CHECK(diag.num_errors() == 0);
            CHECK(err.str().empty());
            Sema sema(diag);
            sema(*stmt);

            if (diag.num_errors() == 0)
                std::cerr << "UNEXPECTED PASS for input \"" << q << '"' << std::endl;
            CHECK_FALSE(diag.num_errors() == 0);
            CHECK_FALSE(err.str().empty());

        }
    }
}

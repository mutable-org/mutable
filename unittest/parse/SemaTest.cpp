#include "catch.hpp"

#include "catalog/Schema.hpp"
#include "parse/Parser.hpp"
#include "parse/Sema.hpp"
#include "testutil.hpp"
#include "util/Diagnostic.hpp"
#include "util/fn.hpp"
#include <iostream>


using namespace db;


TEST_CASE("Sema c'tor", "[unit][parse]")
{
    LEXER("SELECT * FROM test;");
    Sema sema(diag);
    REQUIRE(diag.num_errors() == 0);
    REQUIRE(out.str().empty());
    REQUIRE(err.str().empty());
}

TEST_CASE("Sema/expressions", "[unit][parse]")
{
    std::pair<const char*, const Type*> exprs[] = {
        /* { expression , type } */
        /* boolean constants */
        { "TRUE", Type::Get_Boolean(Type::TY_Scalar) },
        { "FALSE", Type::Get_Boolean(Type::TY_Scalar) },

        /* string literals */
        { "\"Hello, World\"", Type::Get_Char(Type::TY_Scalar, 12) }, // strlen without quotes

        /* numeric constants */
        { "42", Type::Get_Integer(Type::TY_Scalar, 4) },
        { "017", Type::Get_Integer(Type::TY_Scalar, 4) },
        { "0xC0FF33", Type::Get_Integer(Type::TY_Scalar, 4) },
        { "0xC0FF33", Type::Get_Integer(Type::TY_Scalar, 4) },

        { "017777777777", Type::Get_Integer(Type::TY_Scalar, 4) }, // 2^31 - 1, octal
        { "2147483647", Type::Get_Integer(Type::TY_Scalar, 4) }, // 2^31 - 1, decimal
        { "0x7fffffff", Type::Get_Integer(Type::TY_Scalar, 4) }, // 2^31 - 1, hexadecimal

        { "020000000000", Type::Get_Integer(Type::TY_Scalar, 8) }, // 2^31, octal
        { "2147483648", Type::Get_Integer(Type::TY_Scalar, 8) }, // 2^31, decimal
        { "0x80000000", Type::Get_Integer(Type::TY_Scalar, 8) }, // 2^31, hexadecimal

        { ".1", Type::Get_Float(Type::TY_Scalar) },
        { "0xC0F.F33", Type::Get_Float(Type::TY_Scalar) },

        /* unary expressions */
        { "~42", Type::Get_Integer(Type::TY_Scalar, 4) },
        { "+42", Type::Get_Integer(Type::TY_Scalar, 4) },
        { "-42", Type::Get_Integer(Type::TY_Scalar, 4) },

        { "~42.", Type::Get_Float(Type::TY_Scalar) },
        { "+42.", Type::Get_Float(Type::TY_Scalar) },
        { "-42.", Type::Get_Float(Type::TY_Scalar) },

        { "~ TRUE", Type::Get_Error() },
        { "+ TRUE", Type::Get_Error() },
        { "- TRUE", Type::Get_Error() },

        { "~ \"Hello, World\"", Type::Get_Error() },
        { "+ \"Hello, World\"", Type::Get_Error() },
        { "- \"Hello, World\"", Type::Get_Error() },

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

        { "2.718 + 3.14", Type::Get_Float(Type::TY_Scalar) },
        { "42 + 3.14", Type::Get_Float(Type::TY_Scalar) },
        { "3.14 + 42", Type::Get_Float(Type::TY_Scalar) },

        { "TRUE + FALSE", Type::Get_Error() },
        { "TRUE + 42", Type::Get_Error() },
        { "42 + TRUE", Type::Get_Error() },
        { "\"Hello, World\" + 42", Type::Get_Error() },
        { "42 + \"Hello, World\"", Type::Get_Error() },

        /* comparative expressions */
        { "42 < 1337", Type::Get_Boolean(Type::TY_Scalar) },
        { "42 <= 1337", Type::Get_Boolean(Type::TY_Scalar) },
        { "42 > 1337", Type::Get_Boolean(Type::TY_Scalar) },
        { "42 >= 1337", Type::Get_Boolean(Type::TY_Scalar) },
        { "42 = 1337", Type::Get_Boolean(Type::TY_Scalar) },
        { "42 != 1337", Type::Get_Boolean(Type::TY_Scalar) },
        { "3.14 < 0x80000000", Type::Get_Boolean(Type::TY_Scalar) },

        { "TRUE < FALSE", Type::Get_Error() },
        { "TRUE < 42", Type::Get_Error() },
        { "42 < TRUE", Type::Get_Error() },
        { "42 < \"Hello, World\"", Type::Get_Error() },
        { "\"Hello, World\" < 42", Type::Get_Error() },

        { "TRUE = FALSE", Type::Get_Boolean(Type::TY_Scalar) },
        { "TRUE != FALSE", Type::Get_Boolean(Type::TY_Scalar) },

        { "\"verylongtext\" = \"shorty\"", Type::Get_Boolean(Type::TY_Scalar) },

        { "TRUE = 42", Type::Get_Error() },
        { "42 = TRUE", Type::Get_Error() },
        { "TRUE = \"text\"", Type::Get_Error() },
        { "\"text\" = TRUE", Type::Get_Error() },
        { "42 = \"text\"", Type::Get_Error() },
        { "\"text\" = 42", Type::Get_Error() },
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

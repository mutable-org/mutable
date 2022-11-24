#include "catch2/catch.hpp"

#include "storage/RowStore.hpp"
#include "storage/ColumnStore.hpp"
#include "storage/PaxStore.hpp"
#include <mutable/mutable.hpp>
#include <mutable/storage/DataLayoutFactory.hpp>


using namespace m;
using namespace m::ast;
using namespace m::storage;


/*======================================================================================================================
 * Helper function.
 *====================================================================================================================*/

void test_table_is_empty(Diagnostic &diag, std::ostringstream &err, const std::string &table_name)
{
    auto stmt = statement_from_string(diag, "SELECT * FROM " + table_name + ";");
    REQUIRE(diag.num_errors() == 0);
    REQUIRE(err.str().empty());

    std::size_t num_tuples = 0;
    auto callback = std::make_unique<CallbackOperator>([&](const Schema &, const Tuple &) {
        ++num_tuples;
    });

    std::unique_ptr<SelectStmt> select_stmt(static_cast<SelectStmt *>(stmt.release()));
    execute_query(diag, *select_stmt, std::move(callback));
    REQUIRE(diag.num_errors() == 0);
    REQUIRE(err.str().empty());
    REQUIRE(num_tuples == 0);
}

/*======================================================================================================================
 * RowStore.
 *====================================================================================================================*/

TEST_CASE("RowStore/access", "[core][backend]")
{
    Catalog::Clear();
    auto &C = Catalog::Get();
    C.default_backend("Interpreter");

    auto &DB = C.add_database(C.pool("test_db"));
    auto &table = DB.add_table(C.pool("test"));

    /* Process queries. */
    C.set_database_in_use(DB);

    std::ostringstream out, err;
    Diagnostic diag(false, out, err);
    RowLayoutFactory factory;

    SECTION("null tuple")
    {
        const std::pair<const char*, const PrimitiveType*> Attributes[] = {
            { "a_i4",   Type::Get_Integer(Type::TY_Vector, 4) },
            { "b_f",    Type::Get_Float(Type::TY_Vector) },
            { "c_d",    Type::Get_Double(Type::TY_Vector) },
            { "d_c",    Type::Get_Char(Type::TY_Vector, 5) },
            { "e_b",    Type::Get_Boolean(Type::TY_Vector) },
        };
        for (auto &attr : Attributes)
            table.push_back(C.pool(attr.first), attr.second);

        /* Create and set store and set data layout. */
        table.store(std::make_unique<RowStore>(table));
        table.layout(factory);

        /* Test table without any inserted tuples. */
        test_table_is_empty(diag, err, "test");
        REQUIRE(table.store().num_rows() == 0);

        auto insertions = statement_from_string(diag, "INSERT INTO test VALUES \
            ( NULL, NULL, NULL, NULL, NULL );");
        execute_statement(diag, *insertions);

        auto stmt = statement_from_string(diag, "SELECT * FROM test;");
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());

#define IDX(ATTR) S[Schema::Identifier(table.name, C.pool(ATTR))].first

        std::size_t num_tuples = 0;
        auto callback = std::make_unique<CallbackOperator>([&](const Schema &S, const Tuple &T) {
            switch (num_tuples) {
                case 0: {
                    CHECK(T.is_null(IDX("a_i4")));
                    CHECK(T.is_null(IDX("b_f")));
                    CHECK(T.is_null(IDX("c_d")));
                    CHECK(T.is_null(IDX("d_c")));
                    CHECK(T.is_null(IDX("e_b")));
                    break;
                }

                default:
                    REQUIRE(false);
            }
            ++num_tuples;
        });

#undef IDX

        std::unique_ptr<SelectStmt> select_stmt(static_cast<SelectStmt*>(stmt.release()));
        execute_query(diag, *select_stmt, std::move(callback));
        REQUIRE(table.store().num_rows() == 1);
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        REQUIRE(num_tuples == 1);
    }

    SECTION("byte-aligned types")
    {
        const std::pair<const char*, const PrimitiveType*> Attributes[] = {
            { "a_i4",   Type::Get_Integer(Type::TY_Vector, 4) },
            { "b_f",    Type::Get_Float(Type::TY_Vector) },
            { "c_d",    Type::Get_Double(Type::TY_Vector) },
            { "d_c",    Type::Get_Char(Type::TY_Vector, 7) },
            { "e_i2",   Type::Get_Integer(Type::TY_Vector, 2) },
        };
        for (auto &attr : Attributes)
            table.push_back(C.pool(attr.first), attr.second);

        /* Create and set store and data layout. */
        table.store(std::make_unique<RowStore>(table));
        table.layout(factory);

        /* Test table without any inserted tuples. */
        test_table_is_empty(diag, err, "test");

        auto insertions = statement_from_string(diag, "INSERT INTO test VALUES \
            ( 2439, 3.14, 2.71828, \"female\", -23 ), \
            ( NULL, 6.62607015, NULL, NULL, 21);");
        execute_statement(diag, *insertions);

        auto stmt = statement_from_string(diag, "SELECT * FROM test;");
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());

#define IDX(ATTR) S[Schema::Identifier(table.name, C.pool(ATTR))].first
#define CHECK_VALUE(ATTR, TYPE, VALUE) \
    CHECK_FALSE(T.is_null(IDX(ATTR))); \
    if (not T.is_null(IDX(ATTR))) \
        CHECK((VALUE) == T.get(IDX(ATTR)).as_##TYPE())
#define CHECK_CHAR(ATTR, VALUE) \
    CHECK_FALSE(T.is_null(IDX(ATTR))); \
    if (T.is_null(IDX(ATTR))) \
        CHECK(std::string(VALUE) == reinterpret_cast<char*>(T.get(IDX(ATTR)).as_p()))

        std::size_t num_tuples = 0;
        auto callback = std::make_unique<CallbackOperator>([&](const Schema &S, const Tuple &T) {
            switch (num_tuples) {
                case 0: {
                    CHECK_VALUE("a_i4", i, 2439);
                    CHECK_VALUE("b_f",  f, 3.14f);
                    CHECK_VALUE("c_d",  d, 2.71828);
                    CHECK_CHAR("d_c", "female");
                    CHECK_VALUE("e_i2",  i, -23);
                    break;
                }

                case 1: {
                    CHECK(T.is_null(IDX("a_i4")));
                    CHECK_VALUE("b_f",  f, 6.62607015f);
                    CHECK(T.is_null(IDX("c_d")));
                    CHECK(T.is_null(IDX("d_c")));
                    CHECK_VALUE("e_i2",  i, 21);
                    break;
                }

                default:
                    REQUIRE(false);
            }
            ++num_tuples;
        });

#undef IDX
#undef CHECK_VALUE
#undef CHECK_CHAR

        std::unique_ptr<SelectStmt> select_stmt(static_cast<SelectStmt*>(stmt.release()));
        execute_query(diag, *select_stmt, std::move(callback));
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        REQUIRE(num_tuples == 2);
    }

    SECTION("not byte-aligned types")
    {
        const std::pair<const char*, const PrimitiveType*> Attributes[] = {
            { "a_i4",   Type::Get_Integer(Type::TY_Vector, 4) },
            { "b_f",    Type::Get_Float(Type::TY_Vector) },
            { "c_i2",   Type::Get_Integer(Type::TY_Vector, 2) },
            { "d_b",    Type::Get_Boolean(Type::TY_Vector) },
            { "e_d",    Type::Get_Double(Type::TY_Vector) },
            { "f_b",    Type::Get_Boolean(Type::TY_Vector) },
            { "g_c",    Type::Get_Char(Type::TY_Vector, 7) },
            { "h_b",    Type::Get_Boolean(Type::TY_Vector) },
        };
        for (auto &attr : Attributes)
            table.push_back(C.pool(attr.first), attr.second);

        /* Create and set store and data layout. */
        table.store(std::make_unique<RowStore>(table));
        table.layout(factory);

        /* Test table without any inserted tuples. */
        test_table_is_empty(diag, err, "test");

        auto insertions = statement_from_string(diag, "INSERT INTO test VALUES \
            ( 42, 3.14, 1337, TRUE, 2.71828, FALSE, \"female\", TRUE ), \
            ( NULL, NULL, -137, NULL, 6.241509074, FALSE, \"male\", TRUE );");
        execute_statement(diag, *insertions);

        auto stmt = statement_from_string(diag, "SELECT * FROM test;");
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());

#define IDX(ATTR) S[Schema::Identifier(table.name, C.pool(ATTR))].first
#define CHECK_VALUE(ATTR, TYPE, VALUE) \
    CHECK_FALSE(T.is_null(IDX(ATTR))); \
    if (not T.is_null(IDX(ATTR))) \
        CHECK((VALUE) == T.get(IDX(ATTR)).as_##TYPE())
#define CHECK_CHAR(ATTR, VALUE) \
    CHECK_FALSE(T.is_null(IDX(ATTR))); \
    if (T.is_null(IDX(ATTR))) \
        CHECK(std::string(VALUE) == reinterpret_cast<char*>(T.get(IDX(ATTR)).as_p()))

        std::size_t num_tuples = 0;
        auto callback = std::make_unique<CallbackOperator>([&](const Schema &S, const Tuple &T) {
            switch (num_tuples) {
                case 0: {
                    CHECK_VALUE("a_i4", i, 42);
                    CHECK_VALUE("b_f",  f, 3.14f);
                    CHECK_VALUE("c_i2", i, 1337);
                    CHECK_VALUE("d_b",  b, true);
                    CHECK_VALUE("e_d",  d, 2.71828);
                    CHECK_VALUE("f_b",  b, false);
                    CHECK_VALUE("h_b",  b, true);
                    CHECK_CHAR("g_c", "female");
                    break;
                }

                case 1: {
                    CHECK(T.is_null(IDX("a_i4")));
                    CHECK(T.is_null(IDX("b_f")));
                    CHECK_VALUE("c_i2", i, -137);
                    CHECK(T.is_null(IDX("d_b")));
                    CHECK_VALUE("e_d",  d, 6.241509074);
                    CHECK_VALUE("f_b",  b, false);
                    CHECK_CHAR("g_c", "male");
                    CHECK_VALUE("h_b",  b, true);
                    break;
                }

                default:
                    REQUIRE(false);
            }
            ++num_tuples;
        });

#undef IDX
#undef CHECK_VALUE
#undef CHECK_CHAR

        std::unique_ptr<SelectStmt> select_stmt(static_cast<SelectStmt*>(stmt.release()));
        execute_query(diag, *select_stmt, std::move(callback));
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        REQUIRE(num_tuples == 2);
    }

    SECTION("multiple inserts")
    {
        const std::pair<const char*, const PrimitiveType*> Attributes[] = {
            { "a_b",    Type::Get_Boolean(Type::TY_Vector) },
            { "b_f",    Type::Get_Float(Type::TY_Vector) },
            { "c_b",    Type::Get_Boolean(Type::TY_Vector) },
            { "d_c",    Type::Get_Char(Type::TY_Vector, 17) },
            { "e_i8",   Type::Get_Integer(Type::TY_Vector, 8) },
            { "f_d",    Type::Get_Double(Type::TY_Vector) },
            { "g_b",    Type::Get_Boolean(Type::TY_Vector) },
            { "h_i4",   Type::Get_Integer(Type::TY_Vector, 4) },
        };
        for (auto &attr : Attributes)
            table.push_back(C.pool(attr.first), attr.second);

        /* Create and set store and data layout. */
        table.store(std::make_unique<RowStore>(table));
        table.layout(factory);

        /* Test table without any inserted tuples. */
        test_table_is_empty(diag, err, "test");

        const std::string Values[] = {
            "( FALSE, NULL, TRUE, \"male\", -491915, 7452.64, NULL, 17 )",
            "( NULL, -174.256772, FALSE, NULL, 2929292, NULL, FALSE, 0 )",
            "( TRUE, 492, NULL, \"loooooong string\", NULL, -3.33, TRUE, NULL )",
        };

        for (auto &val : Values) {
            auto insertion = statement_from_string(diag, "INSERT INTO test VALUES " + val + ";");
            execute_statement(diag, *insertion);
        }

        auto stmt = statement_from_string(diag, "SELECT * FROM test;");
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());

#define IDX(ATTR) S[Schema::Identifier(table.name, C.pool(ATTR))].first
#define CHECK_VALUE(ATTR, TYPE, VALUE) \
    CHECK_FALSE(T.is_null(IDX(ATTR))); \
    if (not T.is_null(IDX(ATTR))) \
        CHECK((VALUE) == T.get(IDX(ATTR)).as_##TYPE())
#define CHECK_CHAR(ATTR, VALUE) \
    CHECK_FALSE(T.is_null(IDX(ATTR))); \
    if (T.is_null(IDX(ATTR))) \
        CHECK(std::string(VALUE) == reinterpret_cast<char*>(T.get(IDX(ATTR)).as_p()))

        std::size_t num_tuples = 0;
        auto callback = std::make_unique<CallbackOperator>([&](const Schema &S, const Tuple &T) {
            switch (num_tuples) {
                case 0: {
                    CHECK_VALUE("a_b", b, false);
                    CHECK(T.is_null(IDX("b_f")));
                    CHECK_VALUE("c_b", b, true);
                    CHECK_CHAR("d_c",  "male" );
                    CHECK_VALUE("e_i8",  i, -491915);
                    CHECK_VALUE("f_d",  d, 7452.64);
                    CHECK(T.is_null(IDX("g_b")));
                    CHECK_VALUE("h_i4", i, 17);
                    break;
                }

                case 1: {
                    CHECK(T.is_null(IDX("a_b")));
                    CHECK_VALUE("b_f",  f, -174.256772f);
                    CHECK_VALUE("c_b", b, false);
                    CHECK(T.is_null(IDX("d_c")) );
                    CHECK_VALUE("e_i8",  i, 2929292);
                    CHECK(T.is_null(IDX("f_d")));
                    CHECK_VALUE("g_b",  b, false);
                    CHECK_VALUE("h_i4", i, 0);
                    break;
                }

                case 2: {
                    CHECK_VALUE("a_b", b, true);
                    CHECK_VALUE("b_f",  f, 492.00001f);
                    CHECK(T.is_null(IDX("c_b")));
                    CHECK_CHAR("d_c",  "loooooong string" );
                    CHECK(T.is_null(IDX("e_i8")));
                    CHECK_VALUE("f_d",  d, -3.33);
                    CHECK_VALUE("g_b",  b, true);
                    CHECK(T.is_null(IDX("h_i4")));
                    break;
                }

                default:
                    REQUIRE(false);
            }
            ++num_tuples;
        });

#undef IDX
#undef CHECK_VALUE
#undef CHECK_CHAR

        std::unique_ptr<SelectStmt> select_stmt(static_cast<SelectStmt*>(stmt.release()));
        execute_query(diag, *select_stmt, std::move(callback));
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        REQUIRE(num_tuples == 3);
    }

    SECTION("big table")
    {
        const std::pair<const char*, const PrimitiveType*> Attributes[] = {
            { "a_b",    Type::Get_Boolean(Type::TY_Vector) },
            { "b_f",    Type::Get_Float(Type::TY_Vector) },
            { "c_b",    Type::Get_Boolean(Type::TY_Vector) },
            { "d_c17",  Type::Get_Char(Type::TY_Vector, 17) },
            { "e_i8",   Type::Get_Integer(Type::TY_Vector, 8) },
            { "f_d",    Type::Get_Double(Type::TY_Vector) },
            { "g_b",    Type::Get_Boolean(Type::TY_Vector) },
            { "h_i4",   Type::Get_Integer(Type::TY_Vector, 4) },
            { "i_i2",   Type::Get_Integer(Type::TY_Vector, 2) },
            { "j_f",    Type::Get_Float(Type::TY_Vector) },
            { "k_b",    Type::Get_Boolean(Type::TY_Vector) },
            { "l_c42",  Type::Get_Char(Type::TY_Vector, 42) },
        };
        for (auto &attr : Attributes)
            table.push_back(C.pool(attr.first), attr.second);

        /* Create and set store and data layout. */
        table.store(std::make_unique<RowStore>(table));
        table.layout(factory);

        /* Test table without any inserted tuples. */
        test_table_is_empty(diag, err, "test");

        const std::string values =
            "( FALSE, NULL, TRUE, \"male\", -491915, 7452.64, NULL, 17, -1, NULL, FALSE, \"\" ), \
            ( NULL, -174.256772, FALSE, NULL, 2929292, NULL, FALSE, 0, NULL, -1.2345, TRUE, \"test null bitmap\" ), \
            ( TRUE, 492, NULL, \"loooooong string\", NULL, -3.33, TRUE, NULL, 29, 999.999, NULL, NULL )";


        for (std::size_t i = 0; i < 4; ++i) {
            auto insertion = statement_from_string(diag, "INSERT INTO test VALUES " + values + ";");
            execute_statement(diag, *insertion);
        }

        auto stmt = statement_from_string(diag, "SELECT * FROM test;");
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());

#define IDX(ATTR) S[Schema::Identifier(table.name, C.pool(ATTR))].first
#define CHECK_VALUE(ATTR, TYPE, VALUE) \
    CHECK_FALSE(T.is_null(IDX(ATTR))); \
    if (not T.is_null(IDX(ATTR))) \
        CHECK((VALUE) == T.get(IDX(ATTR)).as_##TYPE())
#define CHECK_CHAR(ATTR, VALUE) \
    CHECK_FALSE(T.is_null(IDX(ATTR))); \
    if (T.is_null(IDX(ATTR))) \
        CHECK(std::string(VALUE) == reinterpret_cast<char*>(T.get(IDX(ATTR)).as_p()))

        std::size_t num_tuples = 0;
        auto callback = std::make_unique<CallbackOperator>([&](const Schema &S, const Tuple &T) {
            switch (num_tuples % 3) {
                case 0: {
                    CHECK_VALUE("a_b", b, false);
                    CHECK(T.is_null(IDX("b_f")));
                    CHECK_VALUE("c_b", b, true);
                    CHECK_CHAR("d_c17",  "male" );
                    CHECK_VALUE("e_i8",  i, -491915);
                    CHECK_VALUE("f_d",  d, 7452.64);
                    CHECK(T.is_null(IDX("g_b")));
                    CHECK_VALUE("h_i4", i, 17);
                    CHECK_VALUE("i_i2", i, -1);
                    CHECK(T.is_null(IDX("j_f")));
                    CHECK_VALUE("k_b", b, false);
                    CHECK_CHAR("l_c42", "");
                    break;
                }

                case 1: {
                    CHECK(T.is_null(IDX("a_b")));
                    CHECK_VALUE("b_f",  f, -174.256772f);
                    CHECK_VALUE("c_b", b, false);
                    CHECK(T.is_null(IDX("d_c17")) );
                    CHECK_VALUE("e_i8",  i, 2929292);
                    CHECK(T.is_null(IDX("f_d")));
                    CHECK_VALUE("g_b",  b, false);
                    CHECK_VALUE("h_i4", i, 0);
                    CHECK(T.is_null(IDX("i_i2")));
                    CHECK_VALUE("j_f", f, -1.2345f);
                    CHECK_VALUE("k_b", b, true);
                    CHECK_CHAR("l_c42", "test null bitmap");
                    break;
                }

                case 2: {
                    CHECK_VALUE("a_b", b, true);
                    CHECK_VALUE("b_f",  f, 492.00001f);
                    CHECK(T.is_null(IDX("c_b")));
                    CHECK_CHAR("d_c17",  "loooooong string" );
                    CHECK(T.is_null(IDX("e_i8")));
                    CHECK_VALUE("f_d",  d, -3.33);
                    CHECK_VALUE("g_b",  b, true);
                    CHECK(T.is_null(IDX("h_i4")));
                    CHECK_VALUE("i_i2", i, 29);
                    CHECK_VALUE("j_f", f, 999.999f);
                    CHECK(T.is_null(IDX("k_b")));
                    CHECK(T.is_null(IDX("l_c42")));
                    break;
                }

                default:
                    REQUIRE(false);
            }
            ++num_tuples;
        });

#undef IDX
#undef CHECK_VALUE
#undef CHECK_CHAR

        std::unique_ptr<SelectStmt> select_stmt(static_cast<SelectStmt*>(stmt.release()));
        execute_query(diag, *select_stmt, std::move(callback));
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        REQUIRE(num_tuples == 12);
    }
}


/*======================================================================================================================
 * PaxStore.
 *====================================================================================================================*/

TEST_CASE("PaxStore/access", "[core][backend]")
{
    Catalog::Clear();
    auto &C = Catalog::Get();
    C.default_backend("Interpreter");

    auto &DB = C.add_database(C.pool("test_db"));
    auto &table = DB.add_table(C.pool("test"));

    /* Process queries. */
    C.set_database_in_use(DB);

    std::ostringstream out, err;
    Diagnostic diag(false, out, err);
    PAXLayoutFactory factory;

    SECTION("null tuple")
    {
        const std::pair<const char*, const PrimitiveType*> Attributes[] = {
            { "a_i4",   Type::Get_Integer(Type::TY_Vector, 4) },
            { "b_f",    Type::Get_Float(Type::TY_Vector) },
            { "c_d",    Type::Get_Double(Type::TY_Vector) },
            { "d_c",    Type::Get_Char(Type::TY_Vector, 5) },
            { "e_b",   Type::Get_Boolean(Type::TY_Vector) },
        };
        for (auto &attr : Attributes)
            table.push_back(C.pool(attr.first), attr.second);

        /* Create and set store and data layout. */
        table.store(std::make_unique<PaxStore>(table));
        table.layout(factory);

        /* Test table without any inserted tuples. */
        test_table_is_empty(diag, err, "test");

        auto insertions = statement_from_string(diag, "INSERT INTO test VALUES \
            ( NULL, NULL, NULL, NULL, NULL );");
        execute_statement(diag, *insertions);

        auto stmt = statement_from_string(diag, "SELECT * FROM test;");
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());

#define IDX(ATTR) S[Schema::Identifier(table.name, C.pool(ATTR))].first

        std::size_t num_tuples = 0;
        auto callback = std::make_unique<CallbackOperator>([&](const Schema &S, const Tuple &T) {
            switch (num_tuples) {
                case 0: {
                    CHECK(T.is_null(IDX("a_i4")));
                    CHECK(T.is_null(IDX("b_f")));
                    CHECK(T.is_null(IDX("c_d")));
                    CHECK(T.is_null(IDX("d_c")));
                    CHECK(T.is_null(IDX("e_b")));
                    break;
                }

                default:
                    REQUIRE(false);
            }
            ++num_tuples;
        });

#undef IDX

        std::unique_ptr<SelectStmt> select_stmt(static_cast<SelectStmt*>(stmt.release()));
        execute_query(diag, *select_stmt, std::move(callback));
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        REQUIRE(num_tuples == 1);
    }

    SECTION("byte-aligned types")
    {
        const std::pair<const char*, const PrimitiveType*> Attributes[] = {
            { "a_i4",   Type::Get_Integer(Type::TY_Vector, 4) },
            { "b_f",    Type::Get_Float(Type::TY_Vector) },
            { "c_d",    Type::Get_Double(Type::TY_Vector) },
            { "d_c",    Type::Get_Char(Type::TY_Vector, 7) },
            { "e_i2",   Type::Get_Integer(Type::TY_Vector, 2) },
        };
        for (auto &attr : Attributes)
            table.push_back(C.pool(attr.first), attr.second);

        /* Create and set store and data layout. */
        table.store(std::make_unique<PaxStore>(table));
        table.layout(factory);

        /* Test table without any inserted tuples. */
        test_table_is_empty(diag, err, "test");

        auto insertions = statement_from_string(diag, "INSERT INTO test VALUES \
            ( 2439, 3.14, 2.71828, \"female\", -23 ), \
            ( NULL, 6.62607015, NULL, NULL, 21);");
        execute_statement(diag, *insertions);

        auto stmt = statement_from_string(diag, "SELECT * FROM test;");
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());

#define IDX(ATTR) S[Schema::Identifier(table.name, C.pool(ATTR))].first
#define CHECK_VALUE(ATTR, TYPE, VALUE) \
    CHECK_FALSE(T.is_null(IDX(ATTR))); \
    if (not T.is_null(IDX(ATTR))) \
        CHECK((VALUE) == T.get(IDX(ATTR)).as_##TYPE())
#define CHECK_CHAR(ATTR, VALUE) \
    CHECK_FALSE(T.is_null(IDX(ATTR))); \
    if (T.is_null(IDX(ATTR))) \
        CHECK(std::string(VALUE) == reinterpret_cast<char*>(T.get(IDX(ATTR)).as_p()))

        std::size_t num_tuples = 0;
        auto callback = std::make_unique<CallbackOperator>([&](const Schema &S, const Tuple &T) {
            switch (num_tuples) {
                case 0: {
                    CHECK_VALUE("a_i4", i, 2439);
                    CHECK_VALUE("b_f",  f, 3.14f);
                    CHECK_VALUE("c_d",  d, 2.71828);
                    CHECK_CHAR("d_c", "female");
                    CHECK_VALUE("e_i2",  i, -23);
                    break;
                }

                case 1: {
                    CHECK(T.is_null(IDX("a_i4")));
                    CHECK_VALUE("b_f",  f, 6.62607015f);
                    CHECK(T.is_null(IDX("c_d")));
                    CHECK(T.is_null(IDX("d_c")));
                    CHECK_VALUE("e_i2",  i, 21);
                    break;
                }

                default:
                    REQUIRE(false);
            }
            ++num_tuples;
        });

#undef IDX
#undef CHECK_VALUE
#undef CHECK_CHAR

        std::unique_ptr<SelectStmt> select_stmt(static_cast<SelectStmt*>(stmt.release()));
        execute_query(diag, *select_stmt, std::move(callback));
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        REQUIRE(num_tuples == 2);
    }

    SECTION("not byte-aligned types")
    {
        const std::pair<const char*, const PrimitiveType*> Attributes[] = {
            { "a_i4",   Type::Get_Integer(Type::TY_Vector, 4) },
            { "b_f",    Type::Get_Float(Type::TY_Vector) },
            { "c_i2",   Type::Get_Integer(Type::TY_Vector, 2) },
            { "d_b",    Type::Get_Boolean(Type::TY_Vector) },
            { "e_d",    Type::Get_Double(Type::TY_Vector) },
            { "f_b",    Type::Get_Boolean(Type::TY_Vector) },
            { "g_c",    Type::Get_Char(Type::TY_Vector, 7) },
            { "h_b",    Type::Get_Boolean(Type::TY_Vector) },
        };
        for (auto &attr : Attributes)
            table.push_back(C.pool(attr.first), attr.second);

        /* Create and set store and data layout. */
        table.store(std::make_unique<PaxStore>(table));
        table.layout(factory);

        /* Test table without any inserted tuples. */
        test_table_is_empty(diag, err, "test");

        auto insertions = statement_from_string(diag, "INSERT INTO test VALUES \
            ( 42, 3.14, 1337, TRUE, 2.71828, FALSE, \"female\", TRUE ), \
            ( NULL, NULL, -137, NULL, 6.241509074, FALSE, \"male\", TRUE );");
        execute_statement(diag, *insertions);

        auto stmt = statement_from_string(diag, "SELECT * FROM test;");
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());

#define IDX(ATTR) S[Schema::Identifier(table.name, C.pool(ATTR))].first
#define CHECK_VALUE(ATTR, TYPE, VALUE) \
    CHECK_FALSE(T.is_null(IDX(ATTR))); \
    if (not T.is_null(IDX(ATTR))) \
        CHECK((VALUE) == T.get(IDX(ATTR)).as_##TYPE())
#define CHECK_CHAR(ATTR, VALUE) \
    CHECK_FALSE(T.is_null(IDX(ATTR))); \
    if (T.is_null(IDX(ATTR))) \
        CHECK(std::string(VALUE) == reinterpret_cast<char*>(T.get(IDX(ATTR)).as_p()))

        std::size_t num_tuples = 0;
        auto callback = std::make_unique<CallbackOperator>([&](const Schema &S, const Tuple &T) {
            switch (num_tuples) {
                case 0: {
                    CHECK_VALUE("a_i4", i, 42);
                    CHECK_VALUE("b_f",  f, 3.14f);
                    CHECK_VALUE("c_i2", i, 1337);
                    CHECK_VALUE("d_b",  b, true);
                    CHECK_VALUE("e_d",  d, 2.71828);
                    CHECK_VALUE("f_b",  b, false);
                    CHECK_VALUE("h_b",  b, true);
                    CHECK_CHAR("g_c", "female");
                    break;
                }

                case 1: {
                    CHECK(T.is_null(IDX("a_i4")));
                    CHECK(T.is_null(IDX("b_f")));
                    CHECK_VALUE("c_i2", i, -137);
                    CHECK(T.is_null(IDX("d_b")));
                    CHECK_VALUE("e_d",  d, 6.241509074);
                    CHECK_VALUE("f_b",  b, false);
                    CHECK_CHAR("g_c", "male");
                    CHECK_VALUE("h_b",  b, true);
                    break;
                }

                default:
                    REQUIRE(false);
            }
            ++num_tuples;
        });

#undef IDX
#undef CHECK_VALUE
#undef CHECK_CHAR

        std::unique_ptr<SelectStmt> select_stmt(static_cast<SelectStmt*>(stmt.release()));
        execute_query(diag, *select_stmt, std::move(callback));
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        REQUIRE(num_tuples == 2);
    }

    SECTION("multiple inserts")
    {
        const std::pair<const char*, const PrimitiveType*> Attributes[] = {
            { "a_b",    Type::Get_Boolean(Type::TY_Vector) },
            { "b_f",    Type::Get_Float(Type::TY_Vector) },
            { "c_b",    Type::Get_Boolean(Type::TY_Vector) },
            { "d_c",    Type::Get_Char(Type::TY_Vector, 17) },
            { "e_i8",   Type::Get_Integer(Type::TY_Vector, 8) },
            { "f_d",    Type::Get_Double(Type::TY_Vector) },
            { "g_b",    Type::Get_Boolean(Type::TY_Vector) },
            { "h_i4",   Type::Get_Integer(Type::TY_Vector, 4) },
        };
        for (auto &attr : Attributes)
            table.push_back(C.pool(attr.first), attr.second);

        /* Create and set store and data layout. */
        table.store(std::make_unique<PaxStore>(table));
        table.layout(factory);

        /* Test table without any inserted tuples. */
        test_table_is_empty(diag, err, "test");

        const std::string Values[] = {
                "( FALSE, NULL, TRUE, \"male\", -491915, 7452.64, NULL, 17 )",
                "( NULL, -174.256772, FALSE, NULL, 2929292, NULL, FALSE, 0 )",
                "( TRUE, 492, NULL, \"loooooong string\", NULL, -3.33, TRUE, NULL )",
        };

        for (auto &val : Values) {
            auto insertion = statement_from_string(diag, "INSERT INTO test VALUES " + val + ";");
            execute_statement(diag, *insertion);
        }

        auto stmt = statement_from_string(diag, "SELECT * FROM test;");
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());

#define IDX(ATTR) S[Schema::Identifier(table.name, C.pool(ATTR))].first
#define CHECK_VALUE(ATTR, TYPE, VALUE) \
    CHECK_FALSE(T.is_null(IDX(ATTR))); \
    if (not T.is_null(IDX(ATTR))) \
        CHECK((VALUE) == T.get(IDX(ATTR)).as_##TYPE())
#define CHECK_CHAR(ATTR, VALUE) \
    CHECK_FALSE(T.is_null(IDX(ATTR))); \
    if (T.is_null(IDX(ATTR))) \
        CHECK(std::string(VALUE) == reinterpret_cast<char*>(T.get(IDX(ATTR)).as_p()))

        std::size_t num_tuples = 0;
        auto callback = std::make_unique<CallbackOperator>([&](const Schema &S, const Tuple &T) {
            switch (num_tuples) {
                case 0: {
                    CHECK_VALUE("a_b", b, false);
                    CHECK(T.is_null(IDX("b_f")));
                    CHECK_VALUE("c_b", b, true);
                    CHECK_CHAR("d_c",  "male" );
                    CHECK_VALUE("e_i8",  i, -491915);
                    CHECK_VALUE("f_d",  d, 7452.64);
                    CHECK(T.is_null(IDX("g_b")));
                    CHECK_VALUE("h_i4", i, 17);
                    break;
                }

                case 1: {
                    CHECK(T.is_null(IDX("a_b")));
                    CHECK_VALUE("b_f",  f, -174.256772f);
                    CHECK_VALUE("c_b", b, false);
                    CHECK(T.is_null(IDX("d_c")) );
                    CHECK_VALUE("e_i8",  i, 2929292);
                    CHECK(T.is_null(IDX("f_d")));
                    CHECK_VALUE("g_b",  b, false);
                    CHECK_VALUE("h_i4", i, 0);
                    break;
                }

                case 2: {
                    CHECK_VALUE("a_b", b, true);
                    CHECK_VALUE("b_f",  f, 492.00001f);
                    CHECK(T.is_null(IDX("c_b")));
                    CHECK_CHAR("d_c",  "loooooong string" );
                    CHECK(T.is_null(IDX("e_i8")));
                    CHECK_VALUE("f_d",  d, -3.33);
                    CHECK_VALUE("g_b",  b, true);
                    CHECK(T.is_null(IDX("h_i4")));
                    break;
                }

                default:
                    REQUIRE(false);
            }
            ++num_tuples;
        });

#undef IDX
#undef CHECK_VALUE
#undef CHECK_CHAR

        std::unique_ptr<SelectStmt> select_stmt(static_cast<SelectStmt*>(stmt.release()));
        execute_query(diag, *select_stmt, std::move(callback));
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        REQUIRE(num_tuples == 3);
    }

    SECTION("big table")
    {
        const std::pair<const char*, const PrimitiveType*> Attributes[] = {
            { "a_b",    Type::Get_Boolean(Type::TY_Vector) },
            { "b_f",    Type::Get_Float(Type::TY_Vector) },
            { "c_b",    Type::Get_Boolean(Type::TY_Vector) },
            { "d_c17",  Type::Get_Char(Type::TY_Vector, 17) },
            { "e_i8",   Type::Get_Integer(Type::TY_Vector, 8) },
            { "f_d",    Type::Get_Double(Type::TY_Vector) },
            { "g_b",    Type::Get_Boolean(Type::TY_Vector) },
            { "h_i4",   Type::Get_Integer(Type::TY_Vector, 4) },
            { "i_i2",   Type::Get_Integer(Type::TY_Vector, 2) },
            { "j_f",    Type::Get_Float(Type::TY_Vector) },
            { "k_b",    Type::Get_Boolean(Type::TY_Vector) },
            { "l_c42",  Type::Get_Char(Type::TY_Vector, 42) },
        };
        for (auto &attr : Attributes)
            table.push_back(C.pool(attr.first), attr.second);

        /* Create and set store and data layout. */
        table.store(std::make_unique<PaxStore>(table));
        table.layout(factory);

        /* Test table without any inserted tuples. */
        test_table_is_empty(diag, err, "test");

        const std::string values =
            "( FALSE, NULL, TRUE, \"male\", -491915, 7452.64, NULL, 17, -1, NULL, FALSE, \"\" ), \
            ( NULL, -174.256772, FALSE, NULL, 2929292, NULL, FALSE, 0, NULL, -1.2345, TRUE, \"test null bitmap\" ), \
            ( TRUE, 492, NULL, \"loooooong string\", NULL, -3.33, TRUE, NULL, 29, 999.999, NULL, NULL )";


        for (std::size_t i = 0; i < 4; ++i) {
            auto insertion = statement_from_string(diag, "INSERT INTO test VALUES " + values + ";");
            execute_statement(diag, *insertion);
        }

        auto stmt = statement_from_string(diag, "SELECT * FROM test;");
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());

#define IDX(ATTR) S[Schema::Identifier(table.name, C.pool(ATTR))].first
#define CHECK_VALUE(ATTR, TYPE, VALUE) \
    CHECK_FALSE(T.is_null(IDX(ATTR))); \
    if (not T.is_null(IDX(ATTR))) \
        CHECK((VALUE) == T.get(IDX(ATTR)).as_##TYPE())
#define CHECK_CHAR(ATTR, VALUE) \
    CHECK_FALSE(T.is_null(IDX(ATTR))); \
    if (T.is_null(IDX(ATTR))) \
        CHECK(std::string(VALUE) == reinterpret_cast<char*>(T.get(IDX(ATTR)).as_p()))

        std::size_t num_tuples = 0;
        auto callback = std::make_unique<CallbackOperator>([&](const Schema &S, const Tuple &T) {
            switch (num_tuples % 3) {
                case 0: {
                    CHECK_VALUE("a_b", b, false);
                    CHECK(T.is_null(IDX("b_f")));
                    CHECK_VALUE("c_b", b, true);
                    CHECK_CHAR("d_c17",  "male" );
                    CHECK_VALUE("e_i8",  i, -491915);
                    CHECK_VALUE("f_d",  d, 7452.64);
                    CHECK(T.is_null(IDX("g_b")));
                    CHECK_VALUE("h_i4", i, 17);
                    CHECK_VALUE("i_i2", i, -1);
                    CHECK(T.is_null(IDX("j_f")));
                    CHECK_VALUE("k_b", b, false);
                    CHECK_CHAR("l_c42", "");
                    break;
                }

                case 1: {
                    CHECK(T.is_null(IDX("a_b")));
                    CHECK_VALUE("b_f",  f, -174.256772f);
                    CHECK_VALUE("c_b", b, false);
                    CHECK(T.is_null(IDX("d_c17")) );
                    CHECK_VALUE("e_i8",  i, 2929292);
                    CHECK(T.is_null(IDX("f_d")));
                    CHECK_VALUE("g_b",  b, false);
                    CHECK_VALUE("h_i4", i, 0);
                    CHECK(T.is_null(IDX("i_i2")));
                    CHECK_VALUE("j_f", f, -1.2345f);
                    CHECK_VALUE("k_b", b, true);
                    CHECK_CHAR("l_c42", "test null bitmap");
                    break;
                }

                case 2: {
                    CHECK_VALUE("a_b", b, true);
                    CHECK_VALUE("b_f",  f, 492.00001f);
                    CHECK(T.is_null(IDX("c_b")));
                    CHECK_CHAR("d_c17",  "loooooong string" );
                    CHECK(T.is_null(IDX("e_i8")));
                    CHECK_VALUE("f_d",  d, -3.33);
                    CHECK_VALUE("g_b",  b, true);
                    CHECK(T.is_null(IDX("h_i4")));
                    CHECK_VALUE("i_i2", i, 29);
                    CHECK_VALUE("j_f", f, 999.999f);
                    CHECK(T.is_null(IDX("k_b")));
                    CHECK(T.is_null(IDX("l_c42")));
                    break;
                }

                default:
                    REQUIRE(false);
            }
            ++num_tuples;
        });

#undef IDX
#undef CHECK_VALUE
#undef CHECK_CHAR

        std::unique_ptr<SelectStmt> select_stmt(static_cast<SelectStmt*>(stmt.release()));
        execute_query(diag, *select_stmt, std::move(callback));
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        REQUIRE(num_tuples == 12);
    }

    SECTION("rows exceed block (with given block size)")
    {
        const std::pair<const char*, const PrimitiveType*> Attributes[] = {
            { "a_i4",   Type::Get_Integer(Type::TY_Vector, 4) },
            { "b_b",    Type::Get_Boolean(Type::TY_Vector) },
        };
        for (auto &attr : Attributes)
            table.push_back(C.pool(attr.first), attr.second);

        /* Create and set store and data layout. */
        constexpr uint32_t BLOCK_SIZE = 64; // only 14 rows fit in one block
        table.store(std::make_unique<PaxStore>(table, BLOCK_SIZE));
        PAXLayoutFactory factory(PAXLayoutFactory::NBytes, BLOCK_SIZE);
        table.layout(factory);

        /* Test table without any inserted tuples. */
        test_table_is_empty(diag, err, "test");

        /* Insert 10-times 3 tuples each to test stride jumps between blocks. */
        for (std::size_t i = 0; i < 10; ++i) {
            auto insertions = statement_from_string(diag, "INSERT INTO test VALUES (NULL, TRUE), (17, NULL), (1, FALSE);");
            execute_statement(diag, *insertions);
        }

        auto stmt = statement_from_string(diag, "SELECT * FROM test;");
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());

#define IDX(ATTR) S[Schema::Identifier(table.name, C.pool(ATTR))].first
#define CHECK_VALUE(ATTR, TYPE, VALUE) \
    CHECK_FALSE(T.is_null(IDX(ATTR))); \
    if (not T.is_null(IDX(ATTR))) \
        CHECK((VALUE) == T.get(IDX(ATTR)).as_##TYPE())
#define CHECK_CHAR(ATTR, VALUE) \
    CHECK_FALSE(T.is_null(IDX(ATTR))); \
    if (T.is_null(IDX(ATTR))) \
        CHECK(std::string(VALUE) == reinterpret_cast<char*>(T.get(IDX(ATTR)).as_p()))

        std::size_t num_tuples = 0;
        auto callback = std::make_unique<CallbackOperator>([&](const Schema &S, const Tuple &T) {
            switch (num_tuples % 3) {
                case 0: {
                    CHECK(T.is_null(IDX("a_i4")));
                    CHECK_VALUE("b_b", b, true);
                    break;
                }

                case 1: {
                    CHECK_VALUE("a_i4", i, 17);
                    CHECK(T.is_null(IDX("b_b")));
                    break;
                }

                case 2: {
                    CHECK_VALUE("a_i4", i, 1);
                    CHECK_VALUE("b_b", b, false);
                    break;
                }

                default:
                    REQUIRE(false);
            }
            ++num_tuples;
        });

#undef IDX
#undef CHECK_VALUE
#undef CHECK_CHAR

        std::unique_ptr<SelectStmt> select_stmt(static_cast<SelectStmt*>(stmt.release()));
        execute_query(diag, *select_stmt, std::move(callback));
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        REQUIRE(num_tuples == 30);
    }

    SECTION("rows exceed block (with given number of tuples)")
    {
        const std::pair<const char*, const PrimitiveType*> Attributes[] = {
            { "a_i4",   Type::Get_Integer(Type::TY_Vector, 4) },
            { "b_b",    Type::Get_Boolean(Type::TY_Vector) },
        };
        for (auto &attr : Attributes)
            table.push_back(C.pool(attr.first), attr.second);

        /* Create and set store and data layout. */
        table.store(std::make_unique<PaxStore>(table));
        PAXLayoutFactory factory(PAXLayoutFactory::NTuples, 11);
        table.layout(factory);

        /* Test table without any inserted tuples. */
        test_table_is_empty(diag, err, "test");

        /* Insert 10-times 3 tuples each to test stride jumps between blocks. */
        for (std::size_t i = 0; i < 10; ++i) {
            auto insertions = statement_from_string(diag, "INSERT INTO test VALUES (NULL, TRUE), (17, NULL), (1, FALSE);");
            execute_statement(diag, *insertions);
        }

        auto stmt = statement_from_string(diag, "SELECT * FROM test;");
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());

#define IDX(ATTR) S[Schema::Identifier(table.name, C.pool(ATTR))].first
#define CHECK_VALUE(ATTR, TYPE, VALUE) \
    CHECK_FALSE(T.is_null(IDX(ATTR))); \
    if (not T.is_null(IDX(ATTR))) \
        CHECK((VALUE) == T.get(IDX(ATTR)).as_##TYPE())
#define CHECK_CHAR(ATTR, VALUE) \
    CHECK_FALSE(T.is_null(IDX(ATTR))); \
    if (T.is_null(IDX(ATTR))) \
        CHECK(std::string(VALUE) == reinterpret_cast<char*>(T.get(IDX(ATTR)).as_p()))

        std::size_t num_tuples = 0;
        auto callback = std::make_unique<CallbackOperator>([&](const Schema &S, const Tuple &T) {
            switch (num_tuples % 3) {
                case 0: {
                    CHECK(T.is_null(IDX("a_i4")));
                    CHECK_VALUE("b_b", b, true);
                    break;
                }

                case 1: {
                    CHECK_VALUE("a_i4", i, 17);
                    CHECK(T.is_null(IDX("b_b")));
                    break;
                }

                case 2: {
                    CHECK_VALUE("a_i4", i, 1);
                    CHECK_VALUE("b_b", b, false);
                    break;
                }

                default:
                    REQUIRE(false);
            }
            ++num_tuples;
        });

#undef IDX
#undef CHECK_VALUE
#undef CHECK_CHAR

        std::unique_ptr<SelectStmt> select_stmt(static_cast<SelectStmt*>(stmt.release()));
        execute_query(diag, *select_stmt, std::move(callback));
        REQUIRE(diag.num_errors() == 0);
        REQUIRE(err.str().empty());
        REQUIRE(num_tuples == 30);
    }
}

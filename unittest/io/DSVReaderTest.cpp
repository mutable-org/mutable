#include "catch2/catch.hpp"

#include "backend/Interpreter.hpp"
#include "io/Reader.hpp"
#include "storage/ColumnStore.hpp"
#include "storage/RowStore.hpp"
#include "mutable/storage/Store.hpp"


using namespace m;


typedef std::vector< std::tuple<int16_t, int32_t, float, const char*> > tuple_list;


/*======================================================================================================================
 * Helper function.
 *====================================================================================================================*/


namespace {


Table & create_table()
{
    Catalog::Clear();
    auto &C = Catalog::Get();

    auto &DB = C.add_database(C.pool("test_db"));
    auto &table = DB.add_table(C.pool("test"));

    /* Construct a table definition. */
    table.push_back(C.pool("i2"),      Type::Get_Integer(Type::TY_Vector, 2));
    table.push_back(C.pool("i4"),      Type::Get_Integer(Type::TY_Vector, 4));
    table.push_back(C.pool("f"),       Type::Get_Float(Type::TY_Vector));
    table.push_back(C.pool("char15"),  Type::Get_Char(Type::TY_Vector, 15));

    table.store(std::make_unique<RowStore>(table));
    return table;
}

void test_table_imports(const Table &table, const tuple_list &rows)
{
    Schema S;
    for (auto &attr : table) S.add({table.name, attr.name}, attr.type);
    Tuple tup = Tuple(S);
    auto W = std::make_unique<StackMachine>(Interpreter::compile_load(S, table.store().linearization()));

    for (auto row : rows) {
        Tuple *args[] = { &tup };
        (*W)(args);
        REQUIRE(tup.get(0).as<int64_t>() == std::get<0>(row));
        REQUIRE(tup.get(1).as<int64_t>() == std::get<1>(row));
        REQUIRE(tup.get(2).as<float>() == std::get<2>(row));
        REQUIRE(std::strcmp(tup.get(3).as<const char*>(), std::get<3>(row)) == 0);
    }
}

std::string format_string(const char *str, const char delimiter = ',', const char escape = '\\',
                          const char quote = '"')
{
    std::string char15;

    /* check if quotes are necessary */
    if (std::strchr(str, delimiter)
        or std::strchr(str, quote)
        or std::strchr(str, '\n')) {
        char15 += quote;
        while (*str) {
            if (*str == quote)
                char15 += escape;
            char15 += *str++;
        }
        char15 += quote;
    } else {
        char15 = std::string(str);
    }

    return char15;
}

}

/*======================================================================================================================
 * HEADER.
 *====================================================================================================================*/

TEST_CASE("DSVReader HEADER", "[core][io][unit]")
{
    SECTION("no header")
    {
        auto &table = create_table();

        /* Construct DSVReader. */
        std::ostringstream out, err;
        Diagnostic diag(false, out, err);

        DSVReader R(table, diag, ',', '\\', '"', false);

        /* Construct istream. */
        tuple_list rows {
                { 0, 81, 1.11331, "uPIGuil\\FOljtsa" },
                { 1, 57, 5.89266, "yAyrVJ8\nFG1myth" },
                { 2, 48, 0.788, "Sn3WMEpw 12Xc0K" },
                { 3, 45, 2.09507, "Q7omKtKX,ojr1wO"},
                { 4, 4, 8.05046, "ZE5jtNf\"oJIuhva"}
        };
        std::stringstream in;
        for (auto row: rows) {
            in << std::get<0>(row) << "," << std::get<1>(row) << "," << std::get<2>(row) << ","
                    << format_string(std::get<3>(row)) << "\n";
        }

        R(in, "stringstream_in");

        REQUIRE(diag.num_errors() == 0);
        REQUIRE(table.store().num_rows() == 5);
        test_table_imports(table, rows);
    }

    SECTION("consider header")
    {
        auto &table = create_table();

        /* Construct DSVReader. */
        std::ostringstream out, err;
        Diagnostic diag(false, out, err);

        DSVReader R(table, diag, ',', '\\', '"', true, false);

        /* Construct istream. */
        tuple_list rows {
                { 0, 81, 1.11331, "uPIGuil\\FOljtsa" },
                { 1, 57, 5.89266, "yAyrVJ8\nFG1myth" },
                { 2, 48, 0.788, "Sn3WMEpw 12Xc0K" },
                { 3, 45, 2.09507, "Q7omKtKX,ojr1wO"},
                { 4, 4, 8.05046, "ZE5jtNf\"oJIuhva"}
        };
        std::stringstream in;
        in << "i2,i4,f,char15\n";
        for (auto row: rows) {
            in << std::get<0>(row) << "," << std::get<1>(row) << "," << std::get<2>(row) << ","
                    << format_string(std::get<3>(row)) << "\n";
        }

        R(in, "stringstream_in");

        REQUIRE(diag.num_errors() == 0);
        REQUIRE(table.store().num_rows() == 5);
        test_table_imports(table, rows);
    }

    SECTION("consider header with permuted order")
    {
        auto &table = create_table();

        /* Construct DSVReader. */
        std::ostringstream out, err;
        Diagnostic diag(false, out, err);

        DSVReader R(table, diag, ',', '\\', '"', true, false);

        /* Construct istream. */
        tuple_list rows {
                { 0, 81, 1.11331, "uPIGuil\\FOljtsa" },
                { 1, 57, 5.89266, "yAyrVJ8\nFG1myth" },
                { 2, 48, 0.788, "Sn3WMEpw 12Xc0K" },
                { 3, 45, 2.09507, "Q7omKtKX,ojr1wO"},
                { 4, 4, 8.05046, "ZE5jtNf\"oJIuhva"}
        };
        std::stringstream in;
        in << "f,i2,char15,i4\n";
        for (auto row: rows) {
            in << std::get<2>(row) << "," << std::get<0>(row) << "," << format_string(std::get<3>(row)) << ","
                << std::get<1>(row) << "\n";
        }

        R(in, "stringstream_in");

        REQUIRE(diag.num_errors() == 0);
        REQUIRE(table.store().num_rows() == 5);
        test_table_imports(table, rows);
    }

    SECTION("consider header with fewer columns than table attributes")
    {
        auto &table = create_table();

        /* Construct DSVReader. */
        std::ostringstream out, err;
        Diagnostic diag(false, out, err);

        DSVReader R(table, diag, ',', '\\', '"', true, false);

        /* Construct istream. */
        tuple_list rows {
                { 0, 81, 1.11331, "uPIGuil\\FOljtsa" },
                { 1, 57, 5.89266, "yAyrVJ8\nFG1myth" },
                { 2, 48, 0.788, "Sn3WMEpw 12Xc0K" },
                { 3, 45, 2.09507, "Q7omKtKX,ojr1wO"},
                { 4, 4, 8.05046, "ZE5jtNf\"oJIuhva"}
        };
        std::stringstream in;
        in << "i2,f,char15\n";
        for (auto row: rows) {
            in << std::get<0>(row) << "," << std::get<2>(row) << "," << format_string(std::get<3>(row)) << "\n";
        }

        R(in, "stringstream_in");

        REQUIRE(diag.num_errors() == 0);
        REQUIRE(table.store().num_rows() == 5);
        Schema S;
        for (auto &attr : table) S.add({table.name, attr.name}, attr.type);
        Tuple tup = Tuple(S);
        std::unique_ptr<StackMachine> W;

        for (unsigned i = 0; i < rows.size(); ++i) {
            W = std::make_unique<StackMachine>(Interpreter::compile_load(S, table.store().linearization(), i));
            Tuple *args[] = { &tup };
            (*W)(args);
            REQUIRE(tup.get(0).as<int64_t>() == std::get<0>(rows[i]));
            REQUIRE(tup.is_null(1));
            REQUIRE(tup.get(2).as<float>() == std::get<2>(rows[i]));
            REQUIRE(std::strcmp(tup.get(3).as<const char*>(), std::get<3>(rows[i])) == 0);
        }
    }

    SECTION("consider header with more columns than table attributes")
    {
        auto &table = create_table();

        /* Construct DSVReader. */
        std::ostringstream out, err;
        Diagnostic diag(false, out, err);

        DSVReader R(table, diag, ',', '\\', '"', true, false);

        /* Construct istream. */
        tuple_list rows {
                { 0, 81, 1.11331, "uPIGuil\\FOljtsa" },
                { 1, 57, 5.89266, "yAyrVJ8\nFG1myth" },
                { 2, 48, 0.788, "Sn3WMEpw 12Xc0K" },
                { 3, 45, 2.09507, "Q7omKtKX,ojr1wO"},
                { 4, 4, 8.05046, "ZE5jtNf\"oJIuhva"}
        };
        std::stringstream in;
        in << "i2,i4,f,char15,extra_attr\n";
        for (auto row: rows) {
            in << std::get<0>(row) << "," << std::get<1>(row) << "," << std::get<2>(row) << ","
                    << format_string(std::get<3>(row)) << ",42\n";
        }

        R(in, "stringstream_in");

        REQUIRE(diag.num_errors() == 0);
        REQUIRE(table.store().num_rows() == 5);
        test_table_imports(table, rows);
        REQUIRE_THROWS_AS(table.at("extra_attr"), std::out_of_range);
    }

    SECTION("discard header")
    {
        auto &table = create_table();

        /* Construct DSVReader. */
        std::ostringstream out, err;
        Diagnostic diag(false, out, err);

        DSVReader R(table, diag, ',', '\\', '"', true, true);

        /* Construct istream. */
        tuple_list rows {
                { 0, 81, 1.11331, "uPIGuil\\FOljtsa" },
                { 1, 57, 5.89266, "yAyrVJ8\nFG1myth" },
                { 2, 48, 0.788, "Sn3WMEpw 12Xc0K" },
                { 3, 45, 2.09507, "Q7omKtKX,ojr1wO"},
                { 4, 4, 8.05046, "ZE5jtNf\"oJIuhva"}
        };
        std::stringstream in;
        in << "i2,i4,f,char15\n";
        for (auto row: rows) {
            in << std::get<0>(row) << "," << std::get<1>(row) << "," << std::get<2>(row) << ","
                    << format_string(std::get<3>(row)) << "\n";
        }

        R(in, "stringstream_in");

        REQUIRE(diag.num_errors() == 0);
        REQUIRE(table.store().num_rows() == 5);
        test_table_imports(table, rows);
    }
}

TEST_CASE("DSVReader DELIMITER == QUOTE sanity test", "[core][io][unit]")
{
    auto &table = create_table();

    /* Construct DSVReader. */
    std::ostringstream out, err;
    Diagnostic diag(false, out, err);

    REQUIRE_THROWS_AS(DSVReader(table, diag, ',', '\\', ','), m::invalid_argument);
}

TEST_CASE("DSVReader QUOTE == ESCAPE (csv standard)", "[core][io][unit]")
{
    auto &table = create_table();

    /* Construct DSVReader. */
    std::ostringstream out, err;
    Diagnostic diag(false, out, err);

    DSVReader R(table, diag, ',', '"', '"');

    /* Construct istream. */
    tuple_list rows {
            { 0, 81, 1.11331, "uPIGuil\\FOljtsa" },
            { 1, 57, 5.89266, "yAyrVJ8\nFG1myth" },
            { 2, 48, 0.788, "Sn3WMEpw 12Xc0K" },
            { 3, 45, 2.09507, "Q7omKtKX,ojr1wO"},
            { 4, 4, 8.05046, "ZE5jtNf\"oJIuhva"}
    };
    std::stringstream in;
    for (auto row: rows) {
        in << std::get<0>(row) << "," << std::get<1>(row) << "," << std::get<2>(row) << ","
                << format_string(std::get<3>(row), ',', '"', '"') << "\n";
    }

    R(in, "stringstream_in");

    REQUIRE(diag.num_errors() == 0);
    REQUIRE(table.store().num_rows() == 5);
    test_table_imports(table, rows);
}


TEST_CASE("DSVReader explicitly set all characters", "[core][io][unit]")
{
    auto &table = create_table();

    /* Construct DSVReader. */
    std::ostringstream out, err;
    Diagnostic diag(false, out, err);

    DSVReader R(table, diag, ';', '/', '\'');

    /* Construct istream. */
    tuple_list rows {
            { 0, 81, 1.11331, "uPIGuil/FOljtsa" },
            { 1, 57, 5.89266, "yAyrVJ8\nFG1myth" },
            { 2, 48, 0.788, "Sn3WMEpw 12Xc0K" },
            { 3, 45, 2.09507, "Q7omKtKX;ojr1wO"},
            { 4, 4, 8.05046, "ZE5jtNf'oJIuhva"}
    };
    std::stringstream in;
    for (auto row: rows) {
        in << std::get<0>(row) << ";" << std::get<1>(row) << ";" << std::get<2>(row) << ";"
                << format_string(std::get<3>(row), ';', '/', '\'') << "\n";
    }

    R(in, "stringstream_in");

    REQUIRE(diag.num_errors() == 0);
    REQUIRE(table.store().num_rows() == 5);
    test_table_imports(table, rows);
}


TEST_CASE("DSVReader sanity tests", "[core][io][unit]")
{
    SECTION("missing delimiter in row")
    {
        auto &table = create_table();

        /* Construct DSVReader. */
        std::ostringstream out, err;
        Diagnostic diag(false, out, err);

        DSVReader R(table, diag);

        /* Construct istream. */
        tuple_list rows {
                { 0, 81, 1.11331, "uPIGuil\\FOljtsa" },
                { 1, 57, 5.89266, "yAyrVJ8\nFG1myth" },
                { 2, 48, 0.788, "Sn3WMEpw 12Xc0K" },
                { 3, 45, 2.09507, "Q7omKtKX,ojr1wO"},
                { 4, 4, 8.05046, "ZE5jtNf\"oJIuhva"}
        };
        std::stringstream in;
        for (unsigned i = 0; i < rows.size(); i++) {
            if (i % 2 == 0)
                in << std::get<0>(rows[i]) << "," << std::get<1>(rows[i]) << std::get<2>(rows[i]) << ","
                    << format_string(std::get<3>(rows[i])) << "\n";
            else
                in << std::get<0>(rows[i]) << "," << std::get<1>(rows[i]) << "," << std::get<2>(rows[i]) << ","
                   << format_string(std::get<3>(rows[i])) << "\n";
        }

        R(in, "stringstream_in");

        REQUIRE(diag.num_errors() == 9);
        REQUIRE(table.store().num_rows() == 2);
        test_table_imports(table, { rows[1], rows[3] } );
    }

    SECTION("missing value in row")
    {
        Catalog::Clear();
        auto &C = Catalog::Get();

        /* Construct table. */
        auto &DB = C.add_database(C.pool("test_db"));
        auto &table = DB.add_table(C.pool("test"));
        table.push_back(C.pool("int"), Type::Get_Integer(Type::TY_Vector, 4));
        table.push_back(C.pool("decimal"), Type::Get_Decimal(Type::TY_Vector, 5, 4));
        table.push_back(C.pool("float"), Type::Get_Float(Type::TY_Vector));
        table.store(std::make_unique<ColumnStore>(table));

        /* Construct DSVReader. */
        std::ostringstream out, err;
        Diagnostic diag(false, out, err);
        DSVReader R(table, diag);

        /* Construct istream. */
        std::stringstream in;
        in << "42,,\n";

        R(in, "stringstream_in");

        REQUIRE(diag.num_errors() == 0);
        REQUIRE(table.store().num_rows() == 1);

        Schema S;
        for (auto &attr : table) S.add({table.name, attr.name}, attr.type);
        Tuple tup = Tuple(S);
        std::unique_ptr<StackMachine> W;

        W = std::make_unique<StackMachine>(Interpreter::compile_load(S, table.store().linearization()));
        Tuple *args[] = { &tup };
        (*W)(args);
        REQUIRE(tup.get(0).as<int64_t>() == 42);
        REQUIRE(tup.is_null(1));
        REQUIRE(tup.is_null(2));
    }
}


TEST_CASE("DSVReader::operator()", "[core][io][unit]")
{
    SECTION("Const<Boolean>&")
    {
        Catalog::Clear();
        auto &C = Catalog::Get();

        /* Construct table. */
        auto &DB = C.add_database(C.pool("test_db"));
        auto &table = DB.add_table(C.pool("test"));
        table.push_back(C.pool("bool"), Type::Get_Boolean(Type::TY_Vector));
        table.store(std::make_unique<ColumnStore>(table));

        /* Construct DSVReader. */
        std::ostringstream out, err;
        Diagnostic diag(false, out, err);
        DSVReader R(table, diag);

        /* Construct istream. */
        std::stringstream in;
        in << "TRUE\nFALSE\n";

        R(in, "stringstream_in");

        REQUIRE(diag.num_errors() == 0);
        REQUIRE(table.store().num_rows() == 2);

        Schema S;
        for (auto &attr : table) S.add({table.name, attr.name}, attr.type);
        Tuple tup = Tuple(S);
        std::unique_ptr<StackMachine> W;

        W = std::make_unique<StackMachine>(Interpreter::compile_load(S, table.store().linearization()));
        Tuple *args[] = { &tup };
        (*W)(args);
        REQUIRE(tup.get(0).as<bool>() == true);
        (*W)(args);
        REQUIRE(tup.get(0).as<bool>() == false);
    }

    SECTION("Const<Date>&")
    {
        Catalog::Clear();
        auto &C = Catalog::Get();

        /* Construct table. */
        auto &DB = C.add_database(C.pool("test_db"));
        auto &table = DB.add_table(C.pool("test"));
        table.push_back(C.pool("date"), Type::Get_Date(Type::TY_Vector));
        table.store(std::make_unique<ColumnStore>(table));

        /* Construct DSVReader. */
        std::ostringstream out, err;
        Diagnostic diag(false, out, err);
        DSVReader R(table, diag);

        /* Construct istream. */
        std::stringstream in;
        in << "\"2015-03-12\"\n2018-08-01\n";

        R(in, "stringstream_in");

        REQUIRE(diag.num_errors() == 0);
        REQUIRE(table.store().num_rows() == 2);

        Schema S;
        for (auto &attr : table) S.add({table.name, attr.name}, attr.type);
        Tuple tup = Tuple(S);
        std::unique_ptr<StackMachine> W;

        W = std::make_unique<StackMachine>(Interpreter::compile_load(S, table.store().linearization()));
        Tuple *args[] = { &tup };
        (*W)(args);
        REQUIRE(tup.get(0).as<int64_t>() == 1031788);
        (*W)(args);
        REQUIRE(tup.get(0).as<int64_t>() == 1033473);
    }

    SECTION("Const<DateTime>&")
    {
        Catalog::Clear();
        auto &C = Catalog::Get();

        /* Construct table. */
        auto &DB = C.add_database(C.pool("test_db"));
        auto &table = DB.add_table(C.pool("test"));
        table.push_back(C.pool("datetime"), Type::Get_Datetime(Type::TY_Vector));
        table.store(std::make_unique<ColumnStore>(table));

        /* Construct DSVReader. */
        std::ostringstream out, err;
        Diagnostic diag(false, out, err);
        DSVReader R(table, diag);

        /* Construct istream. */
        std::stringstream in;
        in << "\"2017-12-07 04:20:00\"\n2019-05-09 13:37:42\n";

        R(in, "stringstream_in");

        REQUIRE(diag.num_errors() == 0);
        REQUIRE(table.store().num_rows() == 2);

        Schema S;
        for (auto &attr : table) S.add({table.name, attr.name}, attr.type);
        Tuple tup = Tuple(S);
        std::unique_ptr<StackMachine> W;

        W = std::make_unique<StackMachine>(Interpreter::compile_load(S, table.store().linearization()));
        Tuple *args[] = { &tup };
        (*W)(args);
        REQUIRE(tup.get(0).as<int64_t>() == 1512620400);
        (*W)(args);
        REQUIRE(tup.get(0).as<int64_t>() == 1557409062);
    }

    SECTION("Const<Numeric>&")
    {
        Catalog::Clear();
        auto &C = Catalog::Get();

        /* Construct table. */
        auto &DB = C.add_database(C.pool("test_db"));
        auto &table = DB.add_table(C.pool("test"));
        table.push_back(C.pool("int"), Type::Get_Integer(Type::TY_Vector, 4));
        table.push_back(C.pool("decimal"), Type::Get_Decimal(Type::TY_Vector, 5, 4));
        table.push_back(C.pool("float"), Type::Get_Float(Type::TY_Vector));
        table.store(std::make_unique<ColumnStore>(table));

        /* Construct DSVReader. */
        std::ostringstream out, err;
        Diagnostic diag(false, out, err);
        DSVReader R(table, diag);

        /* Construct istream. */
        std::stringstream in;
        in << "1234,5.4321,0.5\n"
            << "-4321,-1.2345,-0.5\n";

        R(in, "stringstream_in");

        REQUIRE(diag.num_errors() == 0);
        REQUIRE(table.store().num_rows() == 2);

        Schema S;
        for (auto &attr : table) S.add({table.name, attr.name}, attr.type);
        Tuple tup = Tuple(S);
        std::unique_ptr<StackMachine> W;

        W = std::make_unique<StackMachine>(Interpreter::compile_load(S, table.store().linearization()));
        Tuple *args[] = { &tup };
        (*W)(args);
        REQUIRE(tup.get(0).as<int64_t>() == 1234);
        // TODO: `DECIMAL` is currently broken and this test should be adjusted when fixed (see issue #79 & #145)
        // REQUIRE(tup.get(1).as<int64_t>() == 5.4321);
        REQUIRE(tup.get(2).as<float>() == 0.5);
        (*W)(args);
        REQUIRE(tup.get(0).as<int64_t>() == -4321);
        // TODO: `DECIMAL` is currently broken and this test should be adjusted when fixed (see issue #79 & #145)
        // REQUIRE(tup.get(1).as<int64_t>() == -1.2345);
        REQUIRE(tup.get(2).as<float>() == -0.5);
    }
}


TEST_CASE("DSVReader::operator() sanity tests", "[core][io][unit]")
{
    SECTION("Const<Boolean>& sanity tests")
    {
        Catalog::Clear();
        auto &C = Catalog::Get();

        /* Construct table. */
        auto &DB = C.add_database(C.pool("test_db"));
        auto &table = DB.add_table(C.pool("test"));
        table.push_back(C.pool("bool"), Type::Get_Boolean(Type::TY_Vector));
        table.store(std::make_unique<ColumnStore>(table));

        /* Construct DSVReader. */
        std::ostringstream out, err;
        Diagnostic diag(false, out, err);
        DSVReader R(table, diag);

        /* Construct istream. */
        std::stringstream in;
        in << "0\n0.5\nasdf\n";

        R(in, "stringstream_in");

        REQUIRE(diag.num_errors() == 3);
        REQUIRE(table.store().num_rows() == 3);

        Schema S;
        for (auto &attr : table) S.add({table.name, attr.name}, attr.type);
        Tuple tup = Tuple(S);
        std::unique_ptr<StackMachine> W;

        W = std::make_unique<StackMachine>(Interpreter::compile_load(S, table.store().linearization()));
        Tuple *args[] = { &tup };
        (*W)(args);
        REQUIRE(tup[0].type == m::Value::VNone);
        (*W)(args);
        REQUIRE(tup[0].type == m::Value::VNone);
        (*W)(args);
        REQUIRE(tup[0].type == m::Value::VNone);
    }

    SECTION("Const<CharacterSequence>& sanity tests")
    {
        Catalog::Clear();
        auto &C = Catalog::Get();

        /* Construct table. */
        auto &DB = C.add_database(C.pool("test_db"));
        auto &table = DB.add_table(C.pool("test"));
        table.push_back(C.pool("char15"), Type::Get_Char(Type::TY_Vector, 15));
        table.store(std::make_unique<ColumnStore>(table));

        /* Construct DSVReader. */
        std::ostringstream out, err;
        Diagnostic diag(false, out, err);
        DSVReader R(table, diag);

        /* Construct istream. */
        std::stringstream in;
        in << "as\"df\nSn3WMEpw12Xc0K\n";

        R(in, "stringstream_in");

        REQUIRE(diag.num_errors() == 1);
        REQUIRE(table.store().num_rows() == 2);

        Schema S;
        for (auto &attr : table) S.add({table.name, attr.name}, attr.type);
        Tuple tup = Tuple(S);
        std::unique_ptr<StackMachine> W;

        W = std::make_unique<StackMachine>(Interpreter::compile_load(S, table.store().linearization()));
        Tuple *args[] = { &tup };
        (*W)(args);
        REQUIRE(tup.is_null(0));
        (*W)(args);
        REQUIRE(std::strcmp(tup.get(0).as<const char*>(), "Sn3WMEpw12Xc0K") == 0);
    }

    SECTION("Const<Date>& sanity tests")
    {
        Catalog::Clear();
        auto &C = Catalog::Get();

        /* Construct table. */
        auto &DB = C.add_database(C.pool("test_db"));
        auto &table = DB.add_table(C.pool("test"));
        table.push_back(C.pool("date"), Type::Get_Date(Type::TY_Vector));
        table.store(std::make_unique<ColumnStore>(table));

        /* Construct DSVReader. */
        std::ostringstream out, err;
        Diagnostic diag(false, out, err);
        DSVReader R(table, diag);

        /* Construct istream. */
        std::stringstream in;
        in << "2018-08-01 12:00:00\n\"2015-03-12\n2018-08-01\n";

        R(in, "stringstream_in");

        REQUIRE(diag.num_errors() == 2);
        REQUIRE(table.store().num_rows() == 3);

        Schema S;
        for (auto &attr : table) S.add({table.name, attr.name}, attr.type);
        Tuple tup = Tuple(S);
        std::unique_ptr<StackMachine> W;

        W = std::make_unique<StackMachine>(Interpreter::compile_load(S, table.store().linearization()));
        Tuple *args[] = { &tup };
        (*W)(args);
        REQUIRE(tup.is_null(0));
        (*W)(args);
        REQUIRE(tup.is_null(0));
        (*W)(args);
        REQUIRE(tup.get(0).as<int64_t>() == 1033473);
    }

    SECTION("Const<DateTime>&")
    {
        Catalog::Clear();
        auto &C = Catalog::Get();

        /* Construct table. */
        auto &DB = C.add_database(C.pool("test_db"));
        auto &table = DB.add_table(C.pool("test"));
        table.push_back(C.pool("datetime"), Type::Get_Datetime(Type::TY_Vector));
        table.store(std::make_unique<ColumnStore>(table));

        /* Construct DSVReader. */
        std::ostringstream out, err;
        Diagnostic diag(false, out, err);
        DSVReader R(table, diag);

        /* Construct istream. */
        std::stringstream in;
        in << "2019-05-09\n\"2017-12-07 04:20:00\n2019-05-09 13:37:42\n";

        R(in, "stringstream_in");

        REQUIRE(diag.num_errors() == 2);
        REQUIRE(table.store().num_rows() == 3);

        Schema S;
        for (auto &attr : table) S.add({table.name, attr.name}, attr.type);
        Tuple tup = Tuple(S);
        std::unique_ptr<StackMachine> W;

        W = std::make_unique<StackMachine>(Interpreter::compile_load(S, table.store().linearization()));
        Tuple *args[] = { &tup };
        (*W)(args);
        REQUIRE(tup.is_null(0));
        (*W)(args);
        REQUIRE(tup.is_null(0));
        (*W)(args);
        REQUIRE(tup.get(0).as<int64_t>() == 1557409062);
    }

    SECTION("Const<Numeric>&")
    {
        Catalog::Clear();
        auto &C = Catalog::Get();

        /* Construct table. */
        auto &DB = C.add_database(C.pool("test_db"));
        auto &table = DB.add_table(C.pool("test"));
        table.push_back(C.pool("int"), Type::Get_Integer(Type::TY_Vector, 4));
        table.push_back(C.pool("decimal"), Type::Get_Decimal(Type::TY_Vector, 5, 4));
        table.push_back(C.pool("float"), Type::Get_Float(Type::TY_Vector));
        table.store(std::make_unique<ColumnStore>(table));

        /* Construct DSVReader. */
        std::ostringstream out, err;
        Diagnostic diag(false, out, err);
        DSVReader R(table, diag);

        /* Construct istream. */
        std::stringstream in;
        in << "qwer,asdf,yxcv\n"
           << "0.5,0.asdf,0.yxcv\n";

        R(in, "stringstream_in");

        REQUIRE(diag.num_errors() == 6);
        REQUIRE(table.store().num_rows() == 2);

        Schema S;
        for (auto &attr : table) S.add({table.name, attr.name}, attr.type);
        Tuple tup = Tuple(S);
        std::unique_ptr<StackMachine> W;

        W = std::make_unique<StackMachine>(Interpreter::compile_load(S, table.store().linearization()));
        Tuple *args[] = { &tup };
        (*W)(args);
        REQUIRE(tup.is_null(0));
        REQUIRE(tup.is_null(1));
        REQUIRE(tup.is_null(2));
        (*W)(args);
        REQUIRE(tup.is_null(0));
        REQUIRE(tup.is_null(1));
        REQUIRE(tup.is_null(2));
    }
}

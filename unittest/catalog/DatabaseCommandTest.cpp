#include "catch2/catch.hpp"

#include <mutable/catalog/Catalog.hpp>
#include <mutable/catalog/DatabaseCommand.hpp>
#include <sstream>


using namespace m;


TEST_CASE("DropDatabase::execute()", "[core][command]")
{
    Catalog::Clear();

    Catalog &C = Catalog::Get();
    std::ostringstream out, err;
    Diagnostic diag(false, out, err);

    const char *db_name = C.pool("mydb");
    C.add_database(db_name);

    auto cmd = DropDatabase(db_name);

    CHECK(C.has_database(db_name));
    cmd.execute(diag);
    CHECK_FALSE(C.has_database(db_name));
}

TEST_CASE("DropTable::execute()", "[core][command]")
{
    Catalog::Clear();

    Catalog &C = Catalog::Get();
    std::ostringstream out, err;
    Diagnostic diag(false, out, err);

    auto &DB = C.add_database(C.pool("mydb"));
    C.set_database_in_use(DB);

    const char *table0_name = C.pool("mytable0");
    auto &table0 = DB.add_table(table0_name);
    table0.push_back(C.pool("v"), Type::Get_Boolean(Type::TY_Vector));
    const char *table1_name = C.pool("mytable1");
    auto &table1 = DB.add_table(table1_name);
    table1.push_back(C.pool("v"), Type::Get_Boolean(Type::TY_Vector));

    SECTION("single table")
    {
        std::vector<const char*> table_names = { table0_name };
        auto cmd = DropTable(table_names);

        CHECK(DB.has_table(table0_name));
        CHECK(DB.has_table(table1_name));
        cmd.execute(diag);
        CHECK_FALSE(DB.has_table(table0_name));
        CHECK(DB.has_table(table1_name));
    }

    SECTION("multiple tables")
    {
        std::vector<const char*> table_names = { table0_name, table1_name };
        auto cmd = DropTable(table_names);

        CHECK(DB.has_table(table0_name));
        CHECK(DB.has_table(table1_name));
        cmd.execute(diag);
        CHECK_FALSE(DB.has_table(table0_name));
        CHECK_FALSE(DB.has_table(table1_name));
    }
}

#include "catch2/catch.hpp"

#include "catalog/SpnWrapper.hpp"
#include <mutable/mutable.hpp>
#include <mutable/util/Diagnostic.hpp>
#include "util/Spn.hpp"


using namespace m;


TEST_CASE("spn/learning","[core][util][spn]")
{
    Catalog::Clear();
    Catalog &C = Catalog::Get();
    auto &db = C.add_database(C.pool("db"));
    C.set_database_in_use(db);

    std::ostringstream out, err;
    Diagnostic diag(false, out, err);

    SECTION("empty table")
    {
        std::ostringstream oss;
        oss << "CREATE TABLE table ("
            << "id INT(4) PRIMARY KEY,"
            << "column_1 INT(4)"
            << ");";
        auto stmt = statement_from_string(diag, oss.str());
        execute_statement(diag, *stmt);

        auto spn = SpnWrapper::learn_spn_table("db", "table");

        /* Expect a leaf as the root */
        CHECK(spn.height() == 0);
        CHECK(spn.degree() == 0);
        CHECK(spn.breadth() == 1);
    }

    SECTION("product split 1 attribute")
    {
        std::ostringstream oss;
        oss << "CREATE TABLE table ("
            << "id INT(4) PRIMARY KEY,"
            << "column_1 INT(4)"
            << ");";
        auto stmt = statement_from_string(diag, oss.str());
        execute_statement(diag, *stmt);
        auto &table = db.get_table(C.pool("table"));

        std::ostringstream oss_insert;
        oss_insert << "INSERT INTO table VALUES (0, 1);";
        auto insert_stmt = statement_from_string(diag, oss_insert.str());
        execute_statement(diag, *insert_stmt);

        auto spn = SpnWrapper::learn_spn_table("db", "table");

        /* Expect a leaf as the root */
        CHECK(spn.height() == 0);
        CHECK(spn.degree() == 0);
        CHECK(spn.breadth() == 1);
    }

    SECTION("product split 2 attributes")
    {
        std::ostringstream oss;
        oss << "CREATE TABLE table ("
            << "id INT(4) PRIMARY KEY,"
            << "column_1 INT(4),"
            << "column_2 INT(4)"
            << ");";
        auto stmt = statement_from_string(diag, oss.str());
        execute_statement(diag, *stmt);
        auto &table = db.get_table(C.pool("table"));

        std::ostringstream oss_insert;
        oss_insert << "INSERT INTO table VALUES (0, 1, 2);";
        auto insert_stmt = statement_from_string(diag, oss_insert.str());
        execute_statement(diag, *insert_stmt);

        auto spn = SpnWrapper::learn_spn_table("db", "table");

        /* Expect a product node as the root to split the attributes */
        CHECK(spn.height() == 1);
        CHECK(spn.degree() == 2);
        CHECK(spn.breadth() == 2);
    }

    SECTION("product split 3 attributes")
    {
        std::ostringstream oss;
        oss << "CREATE TABLE table ("
            << "id INT(4) PRIMARY KEY,"
            << "column_1 INT(4),"
            << "column_2 INT(4),"
            << "column_3 INT(4)"
            << ");";
        auto stmt = statement_from_string(diag, oss.str());
        execute_statement(diag, *stmt);
        auto &table = db.get_table(C.pool("table"));

        std::ostringstream oss_insert;
        oss_insert << "INSERT INTO table VALUES (0, 1, 2, 3);";
        auto insert_stmt = statement_from_string(diag, oss_insert.str());
        execute_statement(diag, *insert_stmt);

        auto spn = SpnWrapper::learn_spn_table("db", "table");

        /* Expect a product node as the root to split the attributes */
        CHECK(spn.height() == 1);
        CHECK(spn.degree() == 3);
        CHECK(spn.breadth() == 3);
    }

    SECTION("sum cluster 10 rows")
    {
        std::ostringstream oss;
        oss << "CREATE TABLE table ("
            << "id INT(4) PRIMARY KEY,"
            << "column_1 INT(4),"
            << "column_2 INT(4)"
            << ");";
        auto stmt = statement_from_string(diag, oss.str());
        execute_statement(diag, *stmt);
        auto &table = db.get_table(C.pool("table"));

        for (int i = 0; i < 10; i++) {
            std::ostringstream oss_insert;
            if (i < 5) {
                oss_insert << "INSERT INTO table VALUES (" << i << ", " << i+1 << "00, 0);";
            }
            else {
                oss_insert << "INSERT INTO table VALUES (" << i << ", " << i+1 << "000, 1);";
            }
            auto insert_stmt = statement_from_string(diag, oss_insert.str());
            execute_statement(diag, *insert_stmt);
        }

        auto spn = SpnWrapper::learn_spn_table("db", "table");

        /* Expect a sum node as the root to cluster into 2 clusters */
        CHECK(spn.height() == 2);
        CHECK(spn.degree() == 2);
        CHECK(spn.breadth() == 4);
    }
}

TEST_CASE("spn/inference","[core][util][spn]")
{
    Catalog::Clear();
    Catalog &C = Catalog::Get();
    auto &db = C.add_database(C.pool("db"));
    C.set_database_in_use(db);

    std::ostringstream out, err;
    Diagnostic diag(false, out, err);

    std::ostringstream oss;
    oss << "CREATE TABLE table ("
        << "id INT(4) PRIMARY KEY,"
        << "column_1 INT(4),"
        << "column_2 INT(4),"
        << "column_3 INT(4)"
        << ");";
    auto stmt = statement_from_string(diag, oss.str());
    execute_statement(diag, *stmt);
    auto &table = db.get_table(C.pool("table"));

    for (int i = 0; i < 100; i++) {
        std::ostringstream oss_insert;
        oss_insert << "INSERT INTO table VALUES (" << i << ", 1, " << i * 10 << ", " << i << ");";
        auto insert_stmt = statement_from_string(diag, oss_insert.str());
        execute_statement(diag, *insert_stmt);
    }

    std::vector<Spn::LeafType> leaf_types_discrete = {Spn::DISCRETE, Spn::DISCRETE, Spn::DISCRETE};
    auto spn_discrete = SpnWrapper::learn_spn_table("db", "table", leaf_types_discrete);

    std::vector<Spn::LeafType> leaf_types_continuous = {Spn::CONTINUOUS, Spn::CONTINUOUS, Spn::CONTINUOUS};
    auto spn_continuous = SpnWrapper::learn_spn_table("db", "table", leaf_types_continuous);

    SECTION("EQUAL")
    {
        /* P(column_1 = 1) should be 1 */
        std::unordered_map<const char*, std::pair<Spn::SpnOperator, float>> filter;
        filter.emplace(C.pool("column_1"), std::make_pair(Spn::EQUAL, 1));
        CHECK(spn_discrete.likelihood(filter) >= 0.999f);
        CHECK(spn_continuous.likelihood(filter) >= 0.999f);
    }

    SECTION("LESS")
    {
        /* P(column_2 < 2000) should be 1 */
        std::unordered_map<const char*, std::pair<Spn::SpnOperator, float>> filter;
        filter.emplace(C.pool("column_2"), std::make_pair(Spn::LESS, 2000));
        CHECK(spn_discrete.likelihood(filter) >= 0.999f);
        CHECK(spn_continuous.likelihood(filter) >= 0.999f);
    }

    SECTION("LESS_EQUAL")
    {
        /* P(column_3 <= 101) should be 1 */
        std::unordered_map<const char*, std::pair<Spn::SpnOperator, float>> filter;
        filter.emplace(C.pool("column_3"), std::make_pair(Spn::LESS_EQUAL, 101));
        CHECK(spn_discrete.likelihood(filter) >= 0.999f);
        CHECK(spn_continuous.likelihood(filter) >= 0.999f);
    }

    SECTION("GREATER")
    {
        /* P(column_3 > -1) should be 1 */
        std::unordered_map<const char*, std::pair<Spn::SpnOperator, float>> filter;
        filter.emplace(C.pool("column_3"), std::make_pair(Spn::GREATER, -1));
        CHECK(spn_discrete.likelihood(filter) >= 0.999f);
        CHECK(spn_continuous.likelihood(filter) >= 0.999f);
    }

    SECTION("GREATER_EQUAL")
    {
        /* P(column_2 >= 0) should be 1 */
        std::unordered_map<const char*, std::pair<Spn::SpnOperator, float>> filter;
        filter.emplace(C.pool("column_2"), std::make_pair(Spn::GREATER_EQUAL, 0));
        CHECK(spn_discrete.likelihood(filter) >= 0.999f);
        CHECK(spn_continuous.likelihood(filter) >= 0.999f);
    }

    SECTION("IS_NULL")
    {
        /* P(column_3 being NULL) should be 0 */
        std::unordered_map<const char*, std::pair<Spn::SpnOperator, float>> filter;
        filter.emplace(C.pool("column_3"), std::make_pair(Spn::IS_NULL, 0));
        CHECK(spn_discrete.likelihood(filter) <= 0.001f);
        CHECK(spn_continuous.likelihood(filter) <= 0.001f);
    }

    SECTION("EXPECTATION")
    {
        /* E(column_1) should be 1 */
        std::unordered_map<const char*, std::pair<Spn::SpnOperator, float>> filter;
        CHECK(spn_discrete.expectation(C.pool("column_1"), filter) == 1.f);
    }
}

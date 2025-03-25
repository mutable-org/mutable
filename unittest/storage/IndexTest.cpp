#include "catch2/catch.hpp"

#include <mutable/catalog/Catalog.hpp>
#include <mutable/mutable.hpp>
#include <mutable/storage/Index.hpp>
#include <mutable/util/concepts.hpp>
#include <mutable/util/Diagnostic.hpp>
#include "storage/PaxStore.hpp"


using namespace m;
using namespace m::idx;


TEST_CASE("ArrayIndex::finalized()", "[core][storage][index]")
{
    /* Create empty index. */
    ArrayIndex<int64_t> idx;
    REQUIRE_FALSE(idx.finalized());

    /* Querying not allowerd, not finalized yet. */
    REQUIRE_THROWS(idx.lower_bound(13));
    REQUIRE_THROWS(idx.upper_bound(13));

    /* Adding another key/value-pair should invalidate index. */
    idx.add(19, 4);
    REQUIRE_FALSE(idx.finalized());
    idx.finalize();
    REQUIRE(idx.finalized());

    /* Index is finalized, should not throw exception. */
    REQUIRE_NOTHROW(idx.lower_bound(13));
    REQUIRE_NOTHROW(idx.upper_bound(13));
}

TEMPLATE_TEST_CASE("ArrayIndex::add() with Numeric types", "[core][storage][index]",
                    int8_t, int16_t, int32_t, int64_t, float, double)
{
    /* Create empty index. */
    ArrayIndex<TestType> idx;

    /* Add keys/value-pairs to index. */
    std::vector<TestType> keys = { 0, 42, 15 };
    std::size_t i = 0;
    for (auto key : keys)
        idx.add(key, i++);

    /* Finalize index. */
    REQUIRE_FALSE(idx.finalized());
    idx.finalize();

    /* Check contents of index. */
    i = 0;
    for (auto key : keys) {
        auto it = idx.lower_bound(key);
        REQUIRE(it->first == key);
        REQUIRE(it->second == i);
        i++;
    }

    /* Check sortedness of iterator. */
    for (auto it = idx.cbegin() + 1; it != idx.cend(); ++it)
        REQUIRE((it - 1)->first <= it->first);

}

TEMPLATE_TEST_CASE("ArrayIndex::bulkload() with Numeric types", "[core][storage][index]",
                    int8_t, int16_t, int32_t, int64_t, float, double)
{
    Catalog::Clear();
    Diagnostic diag(false, std::cout, std::cerr);

    /* Create and use a DB. */
    Catalog &C = Catalog::Get();
    ThreadSafePooledString db_name = C.pool("db");
    auto &DB = C.add_database(db_name);
    C.set_database_in_use(DB);
    auto &table = DB.add_table(C.pool("t"));

    /* Create a table with a single attribute. */
    table.push_back(C.pool("val"), []() {
        if constexpr(integral<TestType>)
            return Type::Get_Integer(Type::TY_Vector, sizeof(TestType));
        else if constexpr(std::same_as<TestType, float>)
            return Type::Get_Float(Type::TY_Vector);
        else // double
            return Type::Get_Double(Type::TY_Vector);
    }());
    table.layout(C.data_layout());
    table.store(C.create_store(table));

    /* Build and execute insert statement. */
    std::vector<TestType> keys = { 0, 42, 15 };
    std::ostringstream oss2;
    oss2 << "INSERT INTO t VALUES ";
    for (auto key : keys)
        oss2 << "(" << +key << "), ";
    oss2 << " (NULL);";

    auto insert_stmt = statement_from_string(diag, oss2.str());
    execute_statement(diag, *insert_stmt);

    /* Create empty index. */
    ArrayIndex<TestType> idx;

    /* Bulkload index from table. */
    idx.bulkload(table, table.schema());
    REQUIRE(idx.finalized()); // bulkloading should automatically finalize index.

    /* Check contents of index. */
    std::size_t i = 0;
    for (auto key : keys) {
        auto it = idx.lower_bound(key);
        REQUIRE(it->first == key);
        REQUIRE(it->second == i);
        i++;
    }

    /* Index should not contain NULL. */
    REQUIRE(idx.num_entries() == keys.size());
}

#include "catch2/catch.hpp"

#include "storage/ColumnStore.hpp"
#include "storage/PaxStore.hpp"
#include "storage/RowStore.hpp"
#include <mutable/catalog/Catalog.hpp>
#include <mutable/catalog/Schema.hpp>
#include <mutable/storage/Store.hpp>
#include <mutable/util/memory.hpp>


using namespace m;


namespace {

struct TestStore : Store
{
    private:
    memory::Memory memory_;
    public:
    TestStore(const Table &table) : Store(table) { }

    virtual std::size_t num_rows() const override { return 0; }
    void append() override { }
    void drop() override { }
    const memory::Memory & memory(std::size_t attr_id) const override { return memory_; }
    void dump(std::ostream &out) const override { }
};

}


TEST_CASE("Store", "[core][storage]")
{
    Catalog &C = Catalog::Get();
    /* Construct a table definition. */
    Table table("mytable");
    table.push_back("i1", Type::Get_Integer(Type::TY_Vector, 1)); // 1 byte

    SECTION("Create from string")
    {
#define TEST(NAME) { \
    auto store = C.create_store(#NAME, table); \
    REQUIRE(cast<NAME>(store.get())); \
}

        TEST(RowStore);
        TEST(ColumnStore);
        TEST(PaxStore);
    }

    SECTION("linearization() sanity check")
    {
        REQUIRE_THROWS_AS(TestStore(table).linearization(), m::runtime_error);
    }
}

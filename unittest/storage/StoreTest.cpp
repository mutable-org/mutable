#include "catch2/catch.hpp"

#include <mutable/catalog/Schema.hpp>
#include <mutable/storage/Store.hpp>
#include "storage/ColumnStore.hpp"
#include "storage/PaxStore.hpp"
#include "storage/RowStore.hpp"
#include <mutable/util/memory.hpp>


using namespace m;


namespace {

struct TestStore : Store
{
    public:
    TestStore(const Table &table) : Store(table) { }

    virtual std::size_t num_rows() const override { return 0; }
    void append() override { }
    void drop() override { }
    const memory::Memory & memory(std::size_t attr_id) const override { return memory::Memory(); }
    void accept(StoreVisitor &v) override { }
    void accept(ConstStoreVisitor &v) const override { }
    void dump(std::ostream &out) const override { }
};

}


TEST_CASE("Store", "[core][storage][store]")
{
    /* Construct a table definition. */
    Table table("mytable");
    table.push_back("i1", Type::Get_Integer(Type::TY_Vector, 1)); // 1 byte

    SECTION("Create from Store::kind_t")
    {
#define M_STORE(NAME, _) REQUIRE(cast<NAME>(&*Store::Create(Store::S_ ## NAME, table)));
#include <mutable/tables/Store.tbl>
#undef M_STORE
    }

    SECTION("Create from string")
    {
#define M_STORE(NAME, _) REQUIRE(cast<NAME>(&*Store::Create(#NAME, table)));
#include <mutable/tables/Store.tbl>
#undef M_STORE
    }

    SECTION("linearization() sanity check")
    {
        REQUIRE_THROWS_AS(TestStore(table).linearization(), m::runtime_error);
    }
}

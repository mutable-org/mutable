#include "catch2/catch.hpp"

#include "storage/ColumnStore.hpp"
#include <mutable/catalog/Catalog.hpp>
#include <mutable/storage/Store.hpp>


using namespace m;


TEST_CASE("ColumnStore", "[core][storage][columnstore]")
{
    auto &C = Catalog::Get();
    /* Construct a table definition. */
    ConcreteTable table(C.pool("mytable"));
    table.push_back(C.pool("i1"),      Type::Get_Integer(Type::TY_Vector, 1)); // 1 byte
    table.push_back(C.pool("i2"),      Type::Get_Integer(Type::TY_Vector, 2)); // 2 byte
    table.push_back(C.pool("i4"),      Type::Get_Integer(Type::TY_Vector, 4)); // 4 byte
    table.push_back(C.pool("i8"),      Type::Get_Integer(Type::TY_Vector, 8)); // 8 byte
    table.push_back(C.pool("decimal"), Type::Get_Decimal(Type::TY_Vector, 8, 2)); // 4 byte
    table.push_back(C.pool("f"),       Type::Get_Float(Type::TY_Vector)); // 4 byte
    table.push_back(C.pool("d"),       Type::Get_Double(Type::TY_Vector)); // 8 byte
    table.push_back(C.pool("char3"),   Type::Get_Char(Type::TY_Vector, 3)); // 3 byte
    table.push_back(C.pool("b0"),      Type::Get_Boolean(Type::TY_Vector)); // 1 bit
    table.push_back(C.pool("b1"),      Type::Get_Boolean(Type::TY_Vector)); // 1 bit

    constexpr std::size_t ROW_SIZE =
        64 + // i8
        64 + // d
        32 + // i4
        32 + // decimal
        32 + // f
        16 + // i2
        8  + // i1
        24 + // char3
        2 +  // b0 & b1
        10;  // bitmap

    ColumnStore store(table);

    auto &i1 = table[C.pool("i1")];
    auto &i2 = table[C.pool("i2")];
    auto &i4 = table[C.pool("i4")];
    auto &i8 = table[C.pool("i8")];
    auto &decimal = table[C.pool("decimal")];
    auto &f = table[C.pool("f")];
    auto &d = table[C.pool("d")];
    auto &char3 = table[C.pool("char3")];
    auto &b0 = table[C.pool("b0")];
    auto &b1 = table[C.pool("b1")];

    SECTION("ctor")
    {
        REQUIRE(store.num_rows() == 0);
        REQUIRE(store.row_size() == ROW_SIZE);
    }

    SECTION("append")
    {
        store.append();
        REQUIRE(store.num_rows() == 1);
        store.append();
        REQUIRE(store.num_rows() == 2);
    }

    SECTION("drop")
    {
        store.append();
        store.append();
        store.drop();
        REQUIRE(store.num_rows() == 1);
        store.drop();
        REQUIRE(store.num_rows() == 0);
    }
}

TEST_CASE("ColumnStore sanity checks", "[core][storage][columnstore]")
{
    auto &C = Catalog::Get();
    /* Construct a table definition. */
    ConcreteTable table(C.pool("mytable"));
    table.push_back(C.pool("char2048"), Type::Get_Char(Type::TY_Vector, 2048)); // 2048 byte

    ColumnStore store(table);

    SECTION("append")
    {
        std::size_t capacity = ColumnStore::ALLOCATION_SIZE / 2048;
        while (store.num_rows() < capacity) store.append();
        REQUIRE_THROWS_AS(store.append(), std::logic_error);
    }
}

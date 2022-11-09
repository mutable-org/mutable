#include "catch2/catch.hpp"

#include "storage/PaxStore.hpp"
#include <mutable/storage/Store.hpp>


using namespace m;


TEST_CASE("PaxStore", "[core][storage][paxstore]")
{
    /* Construct a table definition. */
    Table table("mytable");
    table.push_back("i1",      Type::Get_Integer(Type::TY_Vector, 1)); // 1 byte
    table.push_back("i2",      Type::Get_Integer(Type::TY_Vector, 2)); // 2 byte
    table.push_back("i4",      Type::Get_Integer(Type::TY_Vector, 4)); // 4 byte
    table.push_back("i8",      Type::Get_Integer(Type::TY_Vector, 8)); // 8 byte
    table.push_back("decimal", Type::Get_Decimal(Type::TY_Vector, 8, 2)); // 4 byte
    table.push_back("f",       Type::Get_Float(Type::TY_Vector)); // 4 byte
    table.push_back("d",       Type::Get_Double(Type::TY_Vector)); // 8 byte
    table.push_back("char3",   Type::Get_Char(Type::TY_Vector, 3)); // 3 byte
    table.push_back("b0",      Type::Get_Boolean(Type::TY_Vector)); // 1 bit
    table.push_back("b1",      Type::Get_Boolean(Type::TY_Vector)); // 1 bit
    constexpr std::size_t ROW_SIZE =
        64 + // i8
        64 + // d
        32 + // i4
        32 + // decimal
        32 + // f
        16 + // i2
        8  + // i1
        24 + // char3
        2  + // b0 & b1
        10;  // bitmap
    constexpr uint32_t BLOCK_SIZE = 1UL << 12; // 4 KiB

    PaxStore store(table, BLOCK_SIZE);

    auto &i1 = table["i1"];
    auto &i2 = table["i2"];
    auto &i4 = table["i4"];
    auto &i8 = table["i8"];
    auto &decimal = table["decimal"];
    auto &f = table["f"];
    auto &d = table["d"];
    auto &char3 = table["char3"];
    auto &b0 = table["b0"];
    auto &b1 = table["b1"];

    SECTION("ctor")
    {
        REQUIRE(store.num_rows() == 0);
        REQUIRE(store.num_rows_per_block() * ROW_SIZE <= BLOCK_SIZE * 8);
        auto num_rows_per_block = store.num_rows_per_block();
        REQUIRE(store.offset(i8)      == 0 * num_rows_per_block);
        REQUIRE(store.offset(d)       == 64 * num_rows_per_block);
        REQUIRE(store.offset(i4)      == 128 * num_rows_per_block);
        REQUIRE(store.offset(decimal) == 160 * num_rows_per_block);
        REQUIRE(store.offset(f)       == 192 * num_rows_per_block);
        REQUIRE(store.offset(i2)      == 224 * num_rows_per_block);
        REQUIRE(store.offset(i1)      == 240 * num_rows_per_block);
        REQUIRE(store.offset(char3)   == 248 * num_rows_per_block);
        REQUIRE(store.offset(b0)      == 272 * num_rows_per_block);
        auto b1_offset_without_padding = 273 * num_rows_per_block;
        if (auto remainder = b1_offset_without_padding % 8)
            REQUIRE(store.offset(b1)  == b1_offset_without_padding + 8 - remainder); // add padding
        else
            REQUIRE(store.offset(b1)  == b1_offset_without_padding);
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

TEST_CASE("PaxStore sanity checks", "[core][storage][columnstore]")
{
    /* Construct a table definition. */
    Table table("mytable");
    table.push_back("char2048", Type::Get_Char(Type::TY_Vector, 2048)); // 2048 byte

    constexpr uint32_t BLOCK_SIZE = 1UL << 13; // 8 KiB
    PaxStore store(table, BLOCK_SIZE);
    size_t num_not_byte_aligned = 0;
    size_t row_size = 0;
    for (auto &attr : table) {
        row_size += attr.type->size();
        if (attr.type->size() % 8) ++num_not_byte_aligned;
    }
    row_size += table.num_attrs(); // add size of NULL bitmap
    std::size_t num_rows_per_block = (BLOCK_SIZE * 8 - num_not_byte_aligned * 7) / row_size;
    std::size_t capacity = (PaxStore::ALLOCATION_SIZE / BLOCK_SIZE) * num_rows_per_block // entire blocks
            + ((PaxStore::ALLOCATION_SIZE % BLOCK_SIZE) * 8) / row_size;  // last partial filled block

    SECTION("append")
    {
        while (store.num_rows() < capacity) store.append();
        REQUIRE_THROWS_AS(store.append(), std::logic_error);
    }
}

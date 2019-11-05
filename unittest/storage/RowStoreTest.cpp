#include "catch.hpp"

#include "storage/RowStore.hpp"
#include "storage/Store.hpp"


using namespace db;


TEST_CASE("RowStore", "[unit]")
{
    /* Construct a table definition. */
    Table table("mytable");
    table.push_back(Type::Get_Integer(Type::TY_Vector, 1),      "i1"); // 1 byte
    table.push_back(Type::Get_Integer(Type::TY_Vector, 2),      "i2"); // 2 byte
    table.push_back(Type::Get_Integer(Type::TY_Vector, 4),      "i4"); // 4 byte
    table.push_back(Type::Get_Integer(Type::TY_Vector, 8),      "i8"); // 8 byte
    table.push_back(Type::Get_Decimal(Type::TY_Vector, 8, 2),   "decimal"); // 4 byte
    table.push_back(Type::Get_Float(Type::TY_Vector),           "f"); // 4 byte
    table.push_back(Type::Get_Double(Type::TY_Vector),          "d"); // 8 byte
    table.push_back(Type::Get_Char(Type::TY_Vector, 3),         "char3"); // 3 byte
    table.push_back(Type::Get_Boolean(Type::TY_Vector),         "b0"); // 1 bit
    table.push_back(Type::Get_Boolean(Type::TY_Vector),         "b1"); // 1 bit
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
        10; // bitmap
    constexpr std::size_t ROW_SIZE_WITH_PADDING = ROW_SIZE + (ROW_SIZE % 64 ? 64 - ROW_SIZE % 64 : 0);

    RowStore store(table);

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
        REQUIRE(store.row_size() == ROW_SIZE_WITH_PADDING);
        REQUIRE(store.offset(i8)      == 0);
        REQUIRE(store.offset(d)       == 64);
        REQUIRE(store.offset(i4)      == 128);
        REQUIRE(store.offset(decimal) == 160);
        REQUIRE(store.offset(f)       == 192);
        REQUIRE(store.offset(i2)      == 224);
        REQUIRE(store.offset(i1)      == 240);
        REQUIRE(store.offset(char3)   == 248);
        REQUIRE(store.offset(b0)      == 272);
        REQUIRE(store.offset(b1)      == 273);
    }

    SECTION("append")
    {
        store.append();
        REQUIRE(store.num_rows() == 1);
        store.append();
        REQUIRE(store.num_rows() == 2);
    }

    SECTION("row/set+get")
    {
        auto row = store.append();

        row->set(i1, 42);
        row->set(i2, 43);
        row->set(i4, 44);
        row->set(i8, 45);
        row->set(decimal, 1337); // 13.37
        row->set(f, 13.37f);
        row->set(d, 42.42);
        row->set(char3, std::string("OK"));
        row->set(b0, true);
        row->set(b1, false);

        REQUIRE(row->get<int8_t>(i1) == 42);
        REQUIRE(row->get<int16_t>(i2) == 43);
        REQUIRE(row->get<int32_t>(i4) == 44);
        REQUIRE(row->get<int64_t>(i8) == 45);
        REQUIRE(row->get<int32_t>(decimal) == 1337);
        REQUIRE(row->get<float>(f) == 13.37f);
        REQUIRE(row->get<double>(d) == 42.42);
        REQUIRE(streq(row->get<std::string>(char3).c_str(), "OK"));
        REQUIRE(row->get<bool>(b0) == true);
        REQUIRE(row->get<bool>(b1) == false);
    }

    SECTION("row/NULL")
    {
        auto row = store.append();

        REQUIRE(row->isnull(i4));
        row->set(i4, 42);
        REQUIRE(not row->isnull(i4));
        REQUIRE(row->get<int32_t>(i4) == 42);
        row->setnull(i4);
        REQUIRE(row->isnull(i4));
    }
}

#include "catch2/catch.hpp"

#include "storage/PaxStore.hpp"
#include "storage/store_manip.hpp"
#include <mutable/catalog/Catalog.hpp>
#include <set>


using namespace m;
using namespace m::storage;


#define NUM_ROWS 257
#define NUM_DISTINCT_VALUES 10


/*======================================================================================================================
 * Helper function.
 *====================================================================================================================*/


namespace {


Table & create_table()
{
    Catalog::Clear();
    auto &C = Catalog::Get();

    auto &DB = C.add_database(C.pool("$test_db"));
    auto &table = DB.add_table(C.pool("$test"));

    /* Construct a table definition. */
    table.push_back(C.pool("id"),       Type::Get_Integer(Type::TY_Vector, 4));
    table.push_back(C.pool("i1"),       Type::Get_Integer(Type::TY_Vector, 1));
    table.push_back(C.pool("i2"),       Type::Get_Integer(Type::TY_Vector, 2));
    table.push_back(C.pool("i4"),       Type::Get_Integer(Type::TY_Vector, 4));
    table.push_back(C.pool("i8"),       Type::Get_Integer(Type::TY_Vector, 8));
    table.push_back(C.pool("f"),        Type::Get_Float(Type::TY_Vector));
    table.push_back(C.pool("d"),        Type::Get_Double(Type::TY_Vector));
    table.push_back(C.pool("dec"),      Type::Get_Decimal(Type::TY_Vector, 4, 2));
    table.push_back(C.pool("char15"),   Type::Get_Char(Type::TY_Vector, 15));

    table.store(C.create_store("PaxStore", table));
    PAXLayoutFactory factory(PAXLayoutFactory::NTuples, NUM_ROWS);
    table.layout(factory);
    return table;
}

/** Returns the offset in bytes of the `idx`-th column in the `DataLayout` `layout` which is considered to  represent
 * a **PAX**-layout. */
uint64_t get_column_offset_in_bytes(const DataLayout &layout, std::size_t idx)
{
    auto &child = as<const DataLayout::INode>(layout.child()).at(idx);
    M_insist(as<DataLayout::Leaf>(child.ptr.get())->index() == idx, "index of entry must match index in leaf");
    M_insist(child.offset_in_bits % 8 == 0, "column must be byte-aligned");
    return child.offset_in_bits / 8;
}

}

/*======================================================================================================================
 * Unit Tests.
 *====================================================================================================================*/

TEST_CASE("Null Bitmap Manipulation", "[core][storage][store_manip]")
{
    auto &table = create_table();
    uint8_t *mem_ptr = reinterpret_cast<uint8_t*>(table.store().memory().addr());
    uint8_t *null_bitmap_column = mem_ptr + get_column_offset_in_bytes(table.layout(), table.num_attrs());

    SECTION("set_all_null & set_all_not_null")
    {
        const std::size_t num_attrs = table.num_attrs();
        for (unsigned i = 0; i != NUM_ROWS; ++i) table.store().append();

        const std::size_t column_size_bytes = (NUM_ROWS * num_attrs) / 8;
        const std::size_t column_size_bits = (NUM_ROWS * num_attrs) % 8;

        for (uint8_t *ptr = null_bitmap_column, *end = ptr + column_size_bytes; ptr < end; ptr++)
            REQUIRE(*ptr == 0);
        if (column_size_bits) {
            uint8_t byte = *(null_bitmap_column + column_size_bytes);
            uint8_t mask = (1U << column_size_bits) - 1U;
            REQUIRE((byte & mask) == 0);
        }

        set_all_null(null_bitmap_column, num_attrs, 0, NUM_ROWS);

        for (uint8_t *ptr = null_bitmap_column, *end = ptr + column_size_bytes; ptr < end; ptr++)
            REQUIRE(*ptr == uint8_t(~0));
        if (column_size_bits) {
            uint8_t byte = *(null_bitmap_column + column_size_bytes);
            uint8_t mask = ~((1U << column_size_bits) - 1U);
            REQUIRE((byte | mask) == uint8_t(~0));
        }

        set_all_not_null(null_bitmap_column, num_attrs, 0, NUM_ROWS);

        for (uint8_t *ptr = null_bitmap_column, *end = ptr + column_size_bytes; ptr < end; ptr++)
            REQUIRE(*ptr == 0);
        if (column_size_bits) {
            uint8_t byte = *(null_bitmap_column + column_size_bytes);
            uint8_t mask = (1U << column_size_bits) - 1U;
            REQUIRE((byte & mask) == 0);
        }
    }
}


TEST_CASE("Store Data Manipulation", "[core][storage][store_manip]")
{
    auto &table = create_table();

    SECTION("generate_primary_keys")
    {
        auto &C = Catalog::Get();

        for (unsigned i = 0; i != NUM_ROWS; ++i) table.store().append();

        uint8_t *mem_ptr = reinterpret_cast<uint8_t*>(table.store().memory().addr());

        {
            auto &attr_i32 = table.at(C.pool("i4"));
            int32_t *attr_i32_column = reinterpret_cast<int32_t*>(mem_ptr + get_column_offset_in_bytes(table.layout(), attr_i32.id));

            generate_primary_keys(attr_i32_column, *attr_i32.type, 0, NUM_ROWS);

            int32_t i = 0;
            for (int32_t *ptr = attr_i32_column, *end = ptr + NUM_ROWS; ptr < end; ++ptr)
                REQUIRE(*ptr == i++);
        }

        {
            auto &attr_i64 = table.at(C.pool("i8"));
            int64_t *attr_i64_column = reinterpret_cast<int64_t*>(mem_ptr + get_column_offset_in_bytes(table.layout(), attr_i64.id));

            generate_primary_keys(attr_i64_column, *attr_i64.type, 0, NUM_ROWS);

            int64_t i = 0;
            for (int64_t *ptr = attr_i64_column, *end = ptr + NUM_ROWS; ptr < end; ++ptr)
                REQUIRE(*ptr == i++);
        }
    }

    SECTION("fill_uniform")
    {
        auto &C = Catalog::Get();

        for (unsigned i = 0; i != NUM_ROWS; ++i) table.store().append();

        uint8_t *mem_ptr = reinterpret_cast<uint8_t*>(table.store().memory().addr());

        auto &attr = table.at(C.pool("i4"));
        int32_t *attr_column = reinterpret_cast<int32_t*>(mem_ptr + get_column_offset_in_bytes(table.layout(), attr.id));

        std::vector<int32_t> values{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        std::array<int32_t, 10> num_values{};
        fill_uniform(attr_column, values, 0, NUM_ROWS);

        for (int32_t *ptr = attr_column, *end = ptr + NUM_ROWS; ptr < end; ++ptr)
            num_values[*ptr]++;

        REQUIRE(std::max_element(num_values.begin(), num_values.end())
                - std::min_element(num_values.begin(), num_values.end()) <= 1);
    }

    SECTION("generate_column_data")
    {
        auto &C = Catalog::Get();

        for (unsigned i = 0; i != NUM_ROWS; ++i) table.store().append();

        uint8_t *mem_ptr = reinterpret_cast<uint8_t*>(table.store().memory().addr());

        SECTION("special case: attr == id")
        {
            auto &attr = table.at(C.pool("id"));
            int32_t *attr_column = reinterpret_cast<int32_t*>(mem_ptr + get_column_offset_in_bytes(table.layout(), attr.id));

            generate_column_data(attr_column, attr, NUM_DISTINCT_VALUES, 0, NUM_ROWS);

            int32_t i = 0;
            for (int32_t *ptr = attr_column, *end = ptr + NUM_ROWS; ptr < end; ++ptr)
                REQUIRE(*ptr == i++);
        }

        SECTION("INT(1)")
        {
            auto &attr = table.at(C.pool("i1"));
            int8_t *attr_column = reinterpret_cast<int8_t*>(mem_ptr + get_column_offset_in_bytes(table.layout(), attr.id));

            generate_column_data(attr_column, attr, NUM_DISTINCT_VALUES, 0, NUM_ROWS);

            std::unordered_set<int8_t> distinct_values_set;
            for (int8_t *ptr = attr_column, *end = ptr + NUM_ROWS; ptr < end; ++ptr)
                distinct_values_set.insert(*ptr);
            REQUIRE(distinct_values_set.size() == NUM_DISTINCT_VALUES);
        }

        SECTION("INT(2)")
        {
            auto &attr = table.at(C.pool("i2"));
            int16_t *attr_column = reinterpret_cast<int16_t*>(mem_ptr + get_column_offset_in_bytes(table.layout(), attr.id));

            generate_column_data(attr_column, attr, NUM_DISTINCT_VALUES, 0, NUM_ROWS);

            std::unordered_set<int16_t> distinct_values_set;
            for (int16_t *ptr = attr_column, *end = ptr + NUM_ROWS; ptr < end; ++ptr)
                distinct_values_set.insert(*ptr);
            REQUIRE(distinct_values_set.size() == NUM_DISTINCT_VALUES);
        }

        SECTION("INT(4)")
        {
            auto &attr = table.at(C.pool("i4"));
            int32_t *attr_column = reinterpret_cast<int32_t*>(mem_ptr + get_column_offset_in_bytes(table.layout(), attr.id));

            generate_column_data(attr_column, attr, NUM_DISTINCT_VALUES, 0, NUM_ROWS);

            std::unordered_set<int32_t> distinct_values_set;
            for (int32_t *ptr = attr_column, *end = ptr + NUM_ROWS; ptr < end; ++ptr)
                distinct_values_set.insert(*ptr);
            REQUIRE(distinct_values_set.size() == NUM_DISTINCT_VALUES);
        }

        SECTION("INT(8)")
        {
            auto &attr = table.at(C.pool("i8"));
            int64_t *attr_column = reinterpret_cast<int64_t*>(mem_ptr + get_column_offset_in_bytes(table.layout(), attr.id));

            generate_column_data(attr_column, attr, NUM_DISTINCT_VALUES, 0, NUM_ROWS);

            std::unordered_set<int64_t> distinct_values_set;
            for (int64_t *ptr = attr_column, *end = ptr + NUM_ROWS; ptr < end; ++ptr)
                distinct_values_set.insert(*ptr);
            REQUIRE(distinct_values_set.size() == NUM_DISTINCT_VALUES);
        }

        SECTION("FLOAT")
        {
            auto &attr = table.at(C.pool("f"));
            float *attr_column = reinterpret_cast<float*>(mem_ptr + get_column_offset_in_bytes(table.layout(), attr.id));

            generate_column_data(attr_column, attr, NUM_DISTINCT_VALUES, 0, NUM_ROWS);

            std::unordered_set<float> distinct_values_set;
            for (float *ptr = attr_column, *end = ptr + NUM_ROWS; ptr < end; ++ptr)
                distinct_values_set.insert(*ptr);
            REQUIRE(distinct_values_set.size() == NUM_DISTINCT_VALUES);
        }

        SECTION("DOUBLE")
        {
            auto &attr = table.at(C.pool("d"));
            double *attr_column = reinterpret_cast<double*>(mem_ptr + get_column_offset_in_bytes(table.layout(), attr.id));

            generate_column_data(attr_column, attr, NUM_DISTINCT_VALUES, 0, NUM_ROWS);

            std::unordered_set<double> distinct_values_set;
            for (double *ptr = attr_column, *end = ptr + NUM_ROWS; ptr < end; ++ptr)
                distinct_values_set.insert(*ptr);
            REQUIRE(distinct_values_set.size() == NUM_DISTINCT_VALUES);
        }
    }

    SECTION("generate_correlated_column_data")
    {
        /* set section specific variables. */
        const size_t num_rows_1 = NUM_ROWS;
        const size_t num_rows_2 = NUM_ROWS * 2;
        const size_t num_distinct_values_1 = NUM_DISTINCT_VALUES;
        const size_t num_distinct_values_2 = NUM_DISTINCT_VALUES * 2;
        const size_t num_matching_distinct_values = NUM_DISTINCT_VALUES / 2;

        Catalog::Clear();
        auto &C = Catalog::Get();
        auto &DB = C.add_database(C.pool("$test_db"));

        /* Construct a table definitions. */
        auto &table1 = DB.add_table(C.pool("$test1"));
        table1.push_back(C.pool("i1"), Type::Get_Integer(Type::TY_Vector, 1));
        table1.push_back(C.pool("i2"), Type::Get_Integer(Type::TY_Vector, 2));
        table1.push_back(C.pool("i4"), Type::Get_Integer(Type::TY_Vector, 4));
        table1.push_back(C.pool("i8"), Type::Get_Integer(Type::TY_Vector, 8));
        table1.store(std::make_unique<PaxStore>(table1));
        PAXLayoutFactory factory1(PAXLayoutFactory::NTuples, num_rows_1);
        table1.layout(factory1);
        for (unsigned i = 0; i != num_rows_1; ++i) table1.store().append();
        uint8_t *mem1_ptr = reinterpret_cast<uint8_t*>(table1.store().memory().addr());

        auto &table2 = DB.add_table(C.pool("$test2"));
        table2.push_back(C.pool("i1"), Type::Get_Integer(Type::TY_Vector, 1));
        table2.push_back(C.pool("i2"), Type::Get_Integer(Type::TY_Vector, 2));
        table2.push_back(C.pool("i4"), Type::Get_Integer(Type::TY_Vector, 4));
        table2.push_back(C.pool("i8"), Type::Get_Integer(Type::TY_Vector, 8));
        table2.store(std::make_unique<PaxStore>(table2));
        PAXLayoutFactory factory2(PAXLayoutFactory::NTuples, num_rows_2);
        table2.layout(factory2);
        for (unsigned i = 0; i != num_rows_2; ++i) table2.store().append();
        uint8_t *mem2_ptr = reinterpret_cast<uint8_t*>(table2.store().memory().addr());

        SECTION("INT(1)")
        {
            auto &attr1 = table1.at(C.pool("i1"));
            int8_t *attr1_column = reinterpret_cast<int8_t*>(mem1_ptr + get_column_offset_in_bytes(table1.layout(), attr1.id));
            auto &attr2 = table2.at(C.pool("i1"));
            int8_t *attr2_column = reinterpret_cast<int8_t*>(mem2_ptr + get_column_offset_in_bytes(table2.layout(), attr2.id));

            generate_correlated_column_data(attr1_column, attr2_column, attr1,
                                            num_distinct_values_1, num_distinct_values_2,
                                            num_rows_1, num_rows_2,
                                            num_matching_distinct_values);

            std::set<int8_t> distinct_values_set_1;
            for (int8_t *ptr = attr1_column, *end = ptr + num_rows_1; ptr < end; ++ptr)
                distinct_values_set_1.insert(*ptr);

            std::set<int8_t> distinct_values_set_2;
            for (int8_t *ptr = attr2_column, *end = ptr + num_rows_2; ptr < end; ++ptr)
                distinct_values_set_2.insert(*ptr);

            REQUIRE(distinct_values_set_1.size() == num_distinct_values_1);
            REQUIRE(distinct_values_set_2.size() == num_distinct_values_2);

            std::set<int8_t> matching_distinct_values_set;
            std::set_intersection(distinct_values_set_1.begin(), distinct_values_set_1.end(),
                                  distinct_values_set_2.begin(), distinct_values_set_2.end(),
                                  std::inserter(matching_distinct_values_set, matching_distinct_values_set.begin()));
            REQUIRE(matching_distinct_values_set.size() == num_matching_distinct_values);
        }

        SECTION("INT(2)")
        {
            auto &attr1 = table1.at(C.pool("i2"));
            int16_t *attr1_column = reinterpret_cast<int16_t*>(mem1_ptr + get_column_offset_in_bytes(table1.layout(), attr1.id));
            auto &attr2 = table2.at(C.pool("i2"));
            int16_t *attr2_column = reinterpret_cast<int16_t*>(mem2_ptr + get_column_offset_in_bytes(table2.layout(), attr2.id));

            generate_correlated_column_data(attr1_column, attr2_column, attr1,
                                            num_distinct_values_1, num_distinct_values_2,
                                            num_rows_1, num_rows_2,
                                            num_matching_distinct_values);

            std::set<int16_t> distinct_values_set_1;
            for (int16_t *ptr = attr1_column, *end = ptr + num_rows_1; ptr < end; ++ptr)
                distinct_values_set_1.insert(*ptr);

            std::set<int16_t> distinct_values_set_2;
            for (int16_t *ptr = attr2_column, *end = ptr + num_rows_2; ptr < end; ++ptr)
                distinct_values_set_2.insert(*ptr);

            REQUIRE(distinct_values_set_1.size() == num_distinct_values_1);
            REQUIRE(distinct_values_set_2.size() == num_distinct_values_2);

            std::set<int16_t> matching_distinct_values_set;
            std::set_intersection(distinct_values_set_1.begin(), distinct_values_set_1.end(),
                                  distinct_values_set_2.begin(), distinct_values_set_2.end(),
                                  std::inserter(matching_distinct_values_set, matching_distinct_values_set.begin()));
            REQUIRE(matching_distinct_values_set.size() == num_matching_distinct_values);
        }

        SECTION("INT(4)")
        {
            auto &attr1 = table1.at(C.pool("i4"));
            int32_t *attr1_column = reinterpret_cast<int32_t*>(mem1_ptr + get_column_offset_in_bytes(table1.layout(), attr1.id));
            auto &attr2 = table2.at(C.pool("i4"));
            int32_t *attr2_column = reinterpret_cast<int32_t*>(mem2_ptr + get_column_offset_in_bytes(table2.layout(), attr2.id));

            generate_correlated_column_data(attr1_column, attr2_column, attr1,
                                            num_distinct_values_1, num_distinct_values_2,
                                            num_rows_1, num_rows_2,
                                            num_matching_distinct_values);

            std::set<int32_t> distinct_values_set_1;
            for (int32_t *ptr = attr1_column, *end = ptr + num_rows_1; ptr < end; ++ptr)
                distinct_values_set_1.insert(*ptr);

            std::set<int32_t> distinct_values_set_2;
            for (int32_t *ptr = attr2_column, *end = ptr + num_rows_2; ptr < end; ++ptr)
                distinct_values_set_2.insert(*ptr);

            REQUIRE(distinct_values_set_1.size() == num_distinct_values_1);
            REQUIRE(distinct_values_set_2.size() == num_distinct_values_2);

            std::set<int32_t> matching_distinct_values_set;
            std::set_intersection(distinct_values_set_1.begin(), distinct_values_set_1.end(),
                                  distinct_values_set_2.begin(), distinct_values_set_2.end(),
                                  std::inserter(matching_distinct_values_set, matching_distinct_values_set.begin()));
            REQUIRE(matching_distinct_values_set.size() == num_matching_distinct_values);
        }

        SECTION("INT(8)")
        {
            auto &attr1 = table1.at(C.pool("i8"));
            int64_t *attr1_column = reinterpret_cast<int64_t*>(mem1_ptr + get_column_offset_in_bytes(table1.layout(), attr1.id));
            auto &attr2 = table2.at(C.pool("i8"));
            int64_t *attr2_column = reinterpret_cast<int64_t*>(mem2_ptr + get_column_offset_in_bytes(table2.layout(), attr2.id));

            generate_correlated_column_data(attr1_column, attr2_column, attr1,
                                            num_distinct_values_1, num_distinct_values_2,
                                            num_rows_1, num_rows_2,
                                            num_matching_distinct_values);

            std::set<int64_t> distinct_values_set_1;
            for (int64_t *ptr = attr1_column, *end = ptr + num_rows_1; ptr < end; ++ptr)
                distinct_values_set_1.insert(*ptr);

            std::set<int64_t> distinct_values_set_2;
            for (int64_t *ptr = attr2_column, *end = ptr + num_rows_2; ptr < end; ++ptr)
                distinct_values_set_2.insert(*ptr);

            REQUIRE(distinct_values_set_1.size() == num_distinct_values_1);
            REQUIRE(distinct_values_set_2.size() == num_distinct_values_2);

            std::set<int64_t> matching_distinct_values_set;
            std::set_intersection(distinct_values_set_1.begin(), distinct_values_set_1.end(),
                                  distinct_values_set_2.begin(), distinct_values_set_2.end(),
                                  std::inserter(matching_distinct_values_set, matching_distinct_values_set.begin()));
            REQUIRE(matching_distinct_values_set.size() == num_matching_distinct_values);
        }
    }
}

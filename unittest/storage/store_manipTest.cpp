#include "catch2/catch.hpp"

#include "storage/ColumnStore.hpp"
#include "storage/store_manip.hpp"
#include <mutable/catalog/Catalog.hpp>
#include <set>


using namespace m;


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

    table.store(std::make_unique<ColumnStore>(table));
    return table;
}

}

/*======================================================================================================================
 * Unit Tests.
 *====================================================================================================================*/

TEST_CASE("Null Bitmap Manipulation", "[core][storage][store_manip]")
{
    auto &table = create_table();
    auto &store = as<ColumnStore>(table.store());

    SECTION("set_null_bitmap")
    {
        const std::size_t num_attrs = table.size();
        for (unsigned i = 0; i != NUM_ROWS; ++i) store.append();

        uint8_t *null_bitmap_col = store.memory(num_attrs).as<uint8_t *>();
        const std::size_t null_bitmap_size_in_bytes = ( num_attrs + 7 ) / 8;
        const std::size_t num_bytes_in_null_bitmap = NUM_ROWS * null_bitmap_size_in_bytes;

        for (uint8_t *ptr = null_bitmap_col, *end = ptr + num_bytes_in_null_bitmap;
             ptr < end;
             ptr++) {
            REQUIRE(*ptr == 0);
        }

        std::vector<uint8_t> bytes;
        bytes.reserve(null_bitmap_size_in_bytes);
        unsigned i;
        for (i = num_attrs; i >= 8; i -= 8) bytes.emplace_back(uint8_t(0b10101010));
        if (i) {
            M_insist(i < 8);
            bytes.emplace_back(uint8_t((( 1U << i ) - 1 ) & 0b10101010));
        }

        set_null_bitmap(store, bytes, 0, NUM_ROWS);

        for (uint8_t *ptr = null_bitmap_col, *end = ptr + num_bytes_in_null_bitmap;
             ptr < end;
             ptr += null_bitmap_size_in_bytes) {
            for (i = 0; i < bytes.size(); ++i)
                REQUIRE(*ptr+i == *bytes.data()+i);
        }
    }

    SECTION("set_all_null & set_all_not_null")
    {
        const std::size_t num_attrs = table.size();
        for (unsigned i = 0; i != NUM_ROWS; ++i) store.append();

        uint8_t *null_bitmap_col = store.memory(num_attrs).as<uint8_t *>();
        const std::size_t null_bitmap_size_in_bytes = ( num_attrs + 7 ) / 8;
        const std::size_t num_bytes_in_null_bitmap = NUM_ROWS * null_bitmap_size_in_bytes;

        for (uint8_t *ptr = null_bitmap_col, *end = ptr + num_bytes_in_null_bitmap;
             ptr < end;
             ptr++) {
            REQUIRE(*ptr == 0);
        }

        set_all_not_null(store, 0, NUM_ROWS);

        for (uint8_t *ptr = null_bitmap_col, *end = ptr + num_bytes_in_null_bitmap;
             ptr < end;
             ptr += null_bitmap_size_in_bytes) {
            unsigned i;
            uint8_t *tmp_ptr = ptr;
            for (i = num_attrs; i >= 8; i -= 8) {
                REQUIRE(*tmp_ptr == uint8_t(0b11111111));
                tmp_ptr++;
            }
            if (i) {
                REQUIRE(*tmp_ptr == uint8_t((1U << i) -1));
            }
        }

        set_all_null(store, 0, NUM_ROWS);

        for (uint8_t *ptr = null_bitmap_col, *end = ptr + num_bytes_in_null_bitmap;
             ptr < end;
             ptr++) {
            REQUIRE(*ptr == 0);
        }
    }
}


TEST_CASE("Store Data Manipulation", "[core][storage][store_manip]")
{
    auto &table = create_table();
    auto &store = as<ColumnStore>(table.store());

    SECTION("generate_primary_keys") {
        auto &C = Catalog::Get();
        for (unsigned i = 0; i != NUM_ROWS; ++i) store.append();

        auto &attr_i32 = table.at(C.pool("i4"));
        uint32_t *attr_i32_col = store.memory(attr_i32.id).as<uint32_t *>();
        generate_primary_keys(store, attr_i32, 0, NUM_ROWS);

        uint32_t i = 0;
        for (uint32_t *ptr = attr_i32_col, *end = ptr + NUM_ROWS;
             ptr < end;
             ++ptr) {
            REQUIRE(*ptr == i++);
        }

        auto &attr_i64 = table.at(C.pool("i8"));
        uint64_t *attr_i64_col = store.memory(attr_i64.id).as<uint64_t *>();
        generate_primary_keys(store, attr_i64, 0, NUM_ROWS);

        uint64_t j = 0;
        for (uint64_t *ptr = attr_i64_col, *end = ptr + NUM_ROWS;
             ptr < end;
             ++ptr) {
            REQUIRE(*ptr == j++);
        }
    }

    SECTION("fill_uniform")
    {
        auto &C = Catalog::Get();
        for (unsigned i = 0; i != NUM_ROWS; ++i) store.append();

        auto &attr = table.at(C.pool("i4"));
        uint32_t *attr_col = store.memory(attr.id).as<uint32_t*>();
        std::vector<uint32_t> values{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        std::array<uint32_t, 10> num_values{};

        fill_uniform(store, attr, values, 0, NUM_ROWS);
        for (uint32_t *ptr = attr_col, *end = ptr + NUM_ROWS;
             ptr < end;
             ++ptr) {
            num_values[*ptr]++;
        }

        REQUIRE(std::max_element(num_values.begin(), num_values.end())
                - std::min_element(num_values.begin(), num_values.end()) <= 1);
    }

    SECTION("generate_column_data")
    {
        auto &C = Catalog::Get();
        for (unsigned i = 0; i != NUM_ROWS; ++i) store.append();

        SECTION("special case: attr == id")
        {
            auto &attr_id = table.at(C.pool("id"));
            uint32_t *attr_id_col = store.memory(attr_id.id).as<uint32_t *>();
            generate_column_data(store, attr_id, NUM_DISTINCT_VALUES, 0, NUM_ROWS);

            uint32_t i = 0;
            for (uint32_t *ptr = attr_id_col, *end = ptr + NUM_ROWS;
                 ptr < end;
                 ++ptr) {
                REQUIRE(*ptr == i++);
            }
        }

        SECTION("INT(1)")
        {
            auto &attr_i8 = table.at(C.pool("i1"));
            int8_t *attr_i8_col = store.memory(attr_i8.id).as<int8_t *>();
            std::unordered_set<int8_t> distinct_values_set;
            generate_column_data(store, attr_i8, NUM_DISTINCT_VALUES, 0, NUM_ROWS);

            for (int8_t *ptr = attr_i8_col, *end = ptr + NUM_ROWS;
                 ptr < end;
                 ++ptr) {
                distinct_values_set.insert(*ptr);
            }
            REQUIRE(distinct_values_set.size() == NUM_DISTINCT_VALUES);
        }

        SECTION("INT(2)")
        {
            auto &attr_i16 = table.at(C.pool("i2"));
            int16_t *attr_i16_col = store.memory(attr_i16.id).as<int16_t *>();
            std::unordered_set<int16_t> distinct_values_set;
            generate_column_data(store, attr_i16, NUM_DISTINCT_VALUES, 0, NUM_ROWS);

            for (int16_t *ptr = attr_i16_col, *end = ptr + NUM_ROWS;
                 ptr < end;
                 ++ptr) {
                distinct_values_set.insert(*ptr);
            }
            REQUIRE(distinct_values_set.size() == NUM_DISTINCT_VALUES);
        }

        SECTION("INT(4)")
        {
            auto &attr_i32 = table.at(C.pool("i4"));
            int32_t *attr_i32_col = store.memory(attr_i32.id).as<int32_t *>();
            std::unordered_set<int16_t> distinct_values_set;
            generate_column_data(store, attr_i32, NUM_DISTINCT_VALUES, 0, NUM_ROWS);

            for (int32_t *ptr = attr_i32_col, *end = ptr + NUM_ROWS;
                 ptr < end;
                 ++ptr) {
                distinct_values_set.insert(*ptr);
            }
            REQUIRE(distinct_values_set.size() == NUM_DISTINCT_VALUES);
        }

        SECTION("INT(8)")
        {
            auto &attr_i64 = table.at(C.pool("i8"));
            int64_t *attr_i64_col = store.memory(attr_i64.id).as<int64_t *>();
            std::unordered_set<int64_t> distinct_values_set;
            generate_column_data(store, attr_i64, NUM_DISTINCT_VALUES, 0, NUM_ROWS);

            for (int64_t *ptr = attr_i64_col, *end = ptr + NUM_ROWS;
                 ptr < end;
                 ++ptr) {
                distinct_values_set.insert(*ptr);
            }
            REQUIRE(distinct_values_set.size() == NUM_DISTINCT_VALUES);
        }

        SECTION("FLOAT")
        {
            auto &attr_f = table.at(C.pool("f"));
            float *attr_f_col = store.memory(attr_f.id).as<float *>();
            std::unordered_set<float> distinct_values_set;
            generate_column_data(store, attr_f, NUM_DISTINCT_VALUES, 0, NUM_ROWS);

            for (float *ptr = attr_f_col, *end = ptr + NUM_ROWS;
                 ptr < end;
                 ++ptr) {
                distinct_values_set.insert(*ptr);
            }
            REQUIRE(distinct_values_set.size() == NUM_DISTINCT_VALUES);
        }

        SECTION("DOUBLE")
        {
            auto &attr_d = table.at(C.pool("d"));
            double *attr_d_col = store.memory(attr_d.id).as<double *>();
            std::unordered_set<double> distinct_values_set;
            generate_column_data(store, attr_d, NUM_DISTINCT_VALUES, 0, NUM_ROWS);

            for (double *ptr = attr_d_col, *end = ptr + NUM_ROWS;
                 ptr < end;
                 ++ptr) {
                distinct_values_set.insert(*ptr);
            }
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
        table1.store(std::make_unique<ColumnStore>(table1));
        auto &store1 = as<ColumnStore>(table1.store());
        for (unsigned i = 0; i != num_rows_1; ++i) store1.append();

        auto &table2 = DB.add_table(C.pool("$test2"));
        table2.push_back(C.pool("i1"), Type::Get_Integer(Type::TY_Vector, 1));
        table2.push_back(C.pool("i2"), Type::Get_Integer(Type::TY_Vector, 2));
        table2.push_back(C.pool("i4"), Type::Get_Integer(Type::TY_Vector, 4));
        table2.push_back(C.pool("i8"), Type::Get_Integer(Type::TY_Vector, 8));
        table2.store(std::make_unique<ColumnStore>(table2));
        auto &store2 = as<ColumnStore>(table2.store());
        for (unsigned i = 0; i != num_rows_2; ++i) store2.append();


        SECTION("INT(1)")
        {
            auto &attr_i1_1 = table1.at(C.pool("i1"));
            int8_t *attr_i1_1_col = store1.memory(attr_i1_1.id).as<int8_t *>();
            auto &attr_i1_2 = table2.at(C.pool("i1"));
            int8_t *attr_i1_2_col = store2.memory(attr_i1_2.id).as<int8_t *>();

            generate_correlated_column_data(store1, store2, attr_i1_1,
                                            num_distinct_values_1, num_distinct_values_2,
                                            num_rows_1, num_rows_2,
                                            num_matching_distinct_values);

            std::set<int8_t> distinct_values_set_1;
            for (int8_t *ptr = attr_i1_1_col, *end = ptr + num_rows_1;
                 ptr < end;
                 ++ptr) {
                distinct_values_set_1.insert(*ptr);
            }

            std::set<int8_t> distinct_values_set_2;
            for (int8_t *ptr = attr_i1_2_col, *end = ptr + num_rows_2;
                 ptr < end;
                 ++ptr) {
                distinct_values_set_2.insert(*ptr);
            }

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
            auto &attr_i2_1 = table1.at(C.pool("i2"));
            int16_t *attr_i2_1_col = store1.memory(attr_i2_1.id).as<int16_t *>();
            auto &attr_i2_2 = table2.at(C.pool("i2"));
            int16_t *attr_i2_2_col = store2.memory(attr_i2_2.id).as<int16_t *>();

            generate_correlated_column_data(store1, store2, attr_i2_1,
                                            num_distinct_values_1, num_distinct_values_2,
                                            num_rows_1, num_rows_2,
                                            num_matching_distinct_values);

            std::set<int16_t> distinct_values_set_1;
            for (int16_t *ptr = attr_i2_1_col, *end = ptr + num_rows_1;
                 ptr < end;
                 ++ptr) {
                distinct_values_set_1.insert(*ptr);
            }

            std::set<int16_t> distinct_values_set_2;
            for (int16_t *ptr = attr_i2_2_col, *end = ptr + num_rows_2;
                 ptr < end;
                 ++ptr) {
                distinct_values_set_2.insert(*ptr);
            }

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
            auto &attr_i4_1 = table1.at(C.pool("i4"));
            int32_t *attr_i4_1_col = store1.memory(attr_i4_1.id).as<int32_t *>();
            auto &attr_i4_2 = table2.at(C.pool("i4"));
            int32_t *attr_i4_2_col = store2.memory(attr_i4_2.id).as<int32_t *>();

            generate_correlated_column_data(store1, store2, attr_i4_1,
                                            num_distinct_values_1, num_distinct_values_2,
                                            num_rows_1, num_rows_2,
                                            num_matching_distinct_values);

            std::set<int32_t> distinct_values_set_1;
            for (int32_t *ptr = attr_i4_1_col, *end = ptr + num_rows_1;
                 ptr < end;
                 ++ptr) {
                distinct_values_set_1.insert(*ptr);
            }

            std::set<int32_t> distinct_values_set_2;
            for (int32_t *ptr = attr_i4_2_col, *end = ptr + num_rows_2;
                 ptr < end;
                 ++ptr) {
                distinct_values_set_2.insert(*ptr);
            }

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
            auto &attr_i8_1 = table1.at(C.pool("i8"));
            int64_t *attr_i8_1_col = store1.memory(attr_i8_1.id).as<int64_t *>();
            auto &attr_i8_2 = table2.at(C.pool("i8"));
            int64_t *attr_i8_2_col = store2.memory(attr_i8_2.id).as<int64_t *>();

            generate_correlated_column_data(store1, store2, attr_i8_1,
                                            num_distinct_values_1, num_distinct_values_2,
                                            num_rows_1, num_rows_2,
                                            num_matching_distinct_values);

            std::set<int64_t> distinct_values_set_1;
            for (int64_t *ptr = attr_i8_1_col, *end = ptr + num_rows_1;
                 ptr < end;
                 ++ptr) {
                distinct_values_set_1.insert(*ptr);
            }

            std::set<int64_t> distinct_values_set_2;
            for (int64_t *ptr = attr_i8_2_col, *end = ptr + num_rows_2;
                 ptr < end;
                 ++ptr) {
                distinct_values_set_2.insert(*ptr);
            }

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

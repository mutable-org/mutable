#include "store_manip.hpp"

#include "catalog/Schema.hpp"
#include "globals.hpp"
#include "mutable/util/fn.hpp"
#include "storage/ColumnStore.hpp"
#include "util/datagen.hpp"
#include <algorithm>
#include <chrono>
#include <cstring>
#include <limits>
#include <random>
#include <unordered_set>


using namespace m;


//======================================================================================================================
// HELPER FUNCTIONS
//======================================================================================================================

namespace {

/** Generate `count` many distinct numbers of type `T` in the range of [`min`, `max`).
*
* @author Immanuel Haffner
*/
template<typename T>
std::vector<T> generate_distinct_numbers(const T min, const T max, const std::size_t count)
{
    using uniform_distibution_t = std::conditional_t<std::is_integral_v<T>,
            std::uniform_int_distribution<T>,
            std::uniform_real_distribution<T>>;
    static_assert(std::is_arithmetic_v<T>, "T must be an arithmetic type");
    if (std::is_integral_v<T>)
        insist(T(max - count) < min, "range is too small to provide enough distinct values");

    std::vector<T> values;
    values.reserve(count);

    std::unordered_set<T> taken;
    T counter = max - T(count);

    std::mt19937_64 g(0);
    uniform_distibution_t dist(min, counter);

    for (std::size_t i = 0; i != count; ++i) {
        T val = dist(g);
        auto[_, success] = taken.insert(val);
        if (success) {
            values.push_back(val);
        } else {
            values.push_back(++counter);
        }
    }

    return values;
}

/** Generates data for an numeric column `attr` of type `T` and writes it directly to the `ColumnStore` `store`.  The
* rows must have been allocated before calling this function.
*
* @author Immanuel Haffner
*/
template<typename T>
void generate_numeric_column(ColumnStore &store, const Attribute &attr, const std::size_t num_distinct_values,
                             const std::size_t begin, const std::size_t end)
{
    static_assert(std::is_arithmetic_v<T>, "T must be an arithmetic type");
    insist(begin < store.num_rows(), "begin out of bounds");
    insist(end <= store.num_rows(), "end out of bounds");
    insist(begin <= end);

    const std::size_t count = end - begin;

    /* Generate distinct values. */
    std::vector<T> values;
    if (std::is_integral_v<T>)
        values = datagen::generate_uniform_distinct_numbers<T>(std::numeric_limits<T>::lowest() + std::is_signed_v<T>,
                                                               std::numeric_limits<T>::max(),
                                                               num_distinct_values);
    else
        values = datagen::generate_uniform_distinct_numbers<T>(T(0), T(1), num_distinct_values);
    insist(values.size() == num_distinct_values);

    /* Write distinct values repeatedly in arbitrary order to column. */
    auto &mem = store.memory(attr.id);
    auto ptr = mem.as<T*>() + begin;

    std::mt19937_64 g(0);
    for (std::size_t i = 0; i != count;) {
        /* Shuffle the vector before writing its values to the column. */
        std::shuffle(values.begin(), values.end(), g);
        for (auto v : values) {
            *ptr++ = v;
            ++i;
            if (i == count)
                goto exit;
        }
    }
exit:
    insist(ptr - mem.as<T*>() == long(end), "incorrect number of elements written");
}

template<typename T>
void generate_correlated_numeric_columns(ColumnStore &store_left,
                                         ColumnStore &store_right,
                                         const Attribute &attr,
                                         const std::size_t num_distinct_values_left,
                                         const std::size_t num_distinct_values_right,
                                         const std::size_t count_left,
                                         const std::size_t count_right,
                                         const std::size_t num_distinct_values_matching)
{
    static_assert(std::is_arithmetic_v<T>, "T must be an arithmetic type");
    insist(num_distinct_values_left >= num_distinct_values_matching,
           "num_distinct_values_left must be larger than num_distinct_values_matching");
    insist(num_distinct_values_right >= num_distinct_values_matching,
           "num_distinct_values_right must be larger than num_distinct_values_matching");

    /* Generate a single set of distinct values to draw from to fill left and right side, with an overlap of exactly
     * `num_distinct_values_matching` values. */
    const std::vector<T> values = generate_distinct_numbers<T>(
            /* min=   */ std::numeric_limits<T>::lowest(),
            /* max=   */ std::numeric_limits<T>::max(),
            /* count= */ num_distinct_values_left + num_distinct_values_right - num_distinct_values_matching
    );

    std::mt19937_64 g(0);

    /* Fill `store_left`. */
    {
        /* Draw first `num_distinct_values_left` values from `values`. */
        std::vector<T> values_left(values.begin(), values.begin() + num_distinct_values_left);

        /* Write distinct values repeatedly in arbitrary order to column. */
        auto &mem_left = store_left.memory(attr.id);
        auto left = mem_left.as<T *>();

        for (std::size_t i = 0; i != count_left;) {
            /* Shuffle the vector before writing its values to the column. */
            std::shuffle(values_left.begin(), values_left.end(), g);
            for (auto v : values_left) {
                *left++ = v;
                ++i;
                if (i == count_left)
                    goto exit_left;
            }
        }
        exit_left:
        insist(left - mem_left.as<T *>() == long(count_left), "incorrect number of elements written to left store");
    }

    /* Fill `store_right`. */
    {
        /* Draw last `num_distinct_values_right` values from `values`. */
        std::vector<T> values_right(values.rbegin(), values.rbegin() + num_distinct_values_right);

        auto &mem_right = store_right.memory(attr.id);
        auto right = mem_right.as<T *>();

        for (std::size_t i = 0; i != count_right;) {
            /* Shuffle the vector before writing its values to the column. */
            std::shuffle(values_right.begin(), values_right.end(), g);
            for (auto v : values_right) {
                *right++ = v;
                ++i;
                if (i == count_right)
                    goto exit_right;
            }
        }
        exit_right:
        insist(right - mem_right.as<T *>() ==
               long(count_right), "incorrect number of elements written to right store");
    }
}

}

void m::set_null_bitmap(ColumnStore &store, const std::vector<uint8_t> &bytes,
                        const std::size_t begin, const std::size_t end)
{
    insist(end <= store.num_rows(), "end out of bounds");
    insist(begin <= end, "begin out of bounds");

    const std::size_t num_attrs = store.table().size();
    uint8_t *null_bitmap_col = store.memory(num_attrs).as<uint8_t*>();

    const std::size_t null_bitmap_size_in_bytes = (num_attrs + 7) / 8;
    insist(bytes.size() == null_bitmap_size_in_bytes, "invalid number of bytes to set");

    const std::size_t num_rows_to_set = end - begin;
    const std::size_t first_byte_to_set = null_bitmap_size_in_bytes * begin;
    const std::size_t num_bytes_to_set = num_rows_to_set * null_bitmap_size_in_bytes;

    for (uint8_t *ptr = null_bitmap_col + first_byte_to_set, *end = ptr + num_bytes_to_set;
         ptr < end;
         ptr += null_bitmap_size_in_bytes)
    {
        std::memcpy(ptr, bytes.data(), null_bitmap_size_in_bytes);
    }
}

void m::set_all_null(ColumnStore &store, const std::size_t begin, const std::size_t end)
{
    const std::size_t num_attrs = store.table().size();
    const std::size_t null_bitmap_size_in_bytes = (num_attrs + 7) / 8;

    std::vector<uint8_t> bytes(null_bitmap_size_in_bytes, uint8_t(0U));
    set_null_bitmap(store, bytes, begin, end);
}

void m::set_all_not_null(ColumnStore &store, const std::size_t begin, const std::size_t end)
{
    const std::size_t num_attrs = store.table().size();
    const std::size_t null_bitmap_size_in_bytes = (num_attrs + 7) / 8;

    std::vector<uint8_t> bytes;
    bytes.reserve(null_bitmap_size_in_bytes);
    std::size_t i;
    for (i = num_attrs; i >= 8; i -= 8)
        bytes.emplace_back(uint8_t(~0U));
    if (i) {
        insist(i < 8);
        bytes.emplace_back(uint8_t((1U << i) - 1));
    }

    set_null_bitmap(store, bytes, begin, end);
}


void m::generate_primary_keys(ColumnStore &store, const Attribute &attr, const std::size_t begin, const std::size_t end)
{
    insist(begin < store.num_rows(), "begin out of bounds");
    insist(end <= store.num_rows(), "end out of bounds");
    insist(begin <= end);
    insist(attr.type->is_integral(), "primary key columns must be of integral type");

    auto n = as<const Numeric>(attr.type);
    switch (n->size()) {
        default:
            unreachable("unsupported size of primary key");

        case 32: {
            auto &mem = store.memory(attr.id);
            auto ptr = mem.as<int32_t*>();
            std::iota<int32_t*, int32_t>(ptr + begin, ptr + end, begin);
            break;
        }

        case 64: {
            auto &mem = store.memory(attr.id);
            auto ptr = mem.as<int64_t*>();
            std::iota<int64_t*, int64_t>(ptr + begin, ptr + end, begin);
            break;
        }
    }
}

/** Generates data for the column `attr` and writes it directly to the `ColumnStore` `store`.  The rows must have been
 * allocated before calling this function.
 *
 * @author Immanuel Haffner
 */
void m::generate_column_data(m::ColumnStore &store, const m::Attribute &attr, const std::size_t num_distinct_values,
                             const std::size_t begin, const std::size_t end)
{
    insist(end >= begin);
    if (streq(attr.name, "id")) { // Generate primary key.
        generate_primary_keys(store, attr, begin, end);
    } else if (auto n = cast<const Numeric>(attr.type)) {
        switch (n->kind) {
            case m::Numeric::N_Int:
                switch (n->size()) {
#define CASE(N) case N: \
                ::generate_numeric_column<int##N##_t>(store, attr, num_distinct_values, begin, end); \
                break
                    CASE(8);
                    CASE(16);
                    CASE(32);
                    CASE(64);
#undef CASE
                }
                break;
            case m::Numeric::N_Float:
                switch (n->size()) {
                    case 32:
                        ::generate_numeric_column<float>(store, attr, num_distinct_values, begin, end);
                        break;
                    case 64:
                        ::generate_numeric_column<double>(store, attr, num_distinct_values, begin, end);
                        break;
                }
                break;
            case m::Numeric::N_Decimal:
                unreachable("unsupported type");
        }

    } else {
        unreachable("unsupported type");
    }
}

/** Generates data for two columns `attr` correlated by `num_distinct_values_matching` and writes the data directly to
 * the `ColumnStore`s `store_left` and `store_right`. The rows must have been allocated before calling this function.
 */
void m::generate_correlated_column_data(ColumnStore &store_left,
                                        ColumnStore &store_right,
                                        const Attribute &attr,
                                        const std::size_t num_distinct_values_left,
                                        const std::size_t num_distinct_values_right,
                                        const std::size_t count_left,
                                        const std::size_t count_right,
                                        const std::size_t num_distinct_values_matching)
{
    if (streq(attr.name, "id")) { // primary keys cannot be correlated.
        unreachable("primary keys unsupported");
    } else if (auto n = cast<const Numeric>(attr.type)) {
        switch (n->size()) {
#define CASE(N) case N: \
                generate_correlated_numeric_columns<int##N##_t>(store_left, store_right, attr, \
                                                                num_distinct_values_left, num_distinct_values_right, \
                                                                count_left, count_right, \
                                                                num_distinct_values_matching); \
                break
            CASE(8);
            CASE(16);
            CASE(32);
            CASE(64);
#undef CASE
        }
    } else {
        unreachable("unsupported type");
    }
}

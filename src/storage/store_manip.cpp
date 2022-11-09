#include "store_manip.hpp"

#include "storage/ColumnStore.hpp"
#include "util/datagen.hpp"
#include <algorithm>
#include <chrono>
#include <cstring>
#include <limits>
#include <mutable/catalog/Schema.hpp>
#include <mutable/Options.hpp>
#include <mutable/util/fn.hpp>
#include <random>
#include <unordered_set>


using namespace m;


//======================================================================================================================
// HELPER FUNCTIONS
//======================================================================================================================

namespace {

/** Generate `count` many distinct numbers of type `T` in the range of [`min`, `max`). */
template<typename T>
std::vector<T> generate_distinct_numbers(const T min, const T max, const std::size_t count)
{
    using uniform_distibution_t = std::conditional_t<std::is_integral_v<T>,
            std::uniform_int_distribution<T>,
            std::uniform_real_distribution<T>>;
    static_assert(std::is_arithmetic_v<T>, "T must be an arithmetic type");
    if (std::is_integral_v<T>)
        M_insist(T(max - count) > min, "range is too small to provide enough distinct values");

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

/** Generates data for a numeric column at address `column_ptr` of type `T` and writes it directly to memory.  The
* rows must have been allocated before calling this function. */
template<typename T>
void generate_numeric_column(T *column_ptr, std::size_t num_distinct_values, std::size_t begin, std::size_t end)
{
    static_assert(std::is_arithmetic_v<T>, "T must be an arithmetic type");
    M_insist(begin < end, "must set at least one row");

    const auto count = end - begin;

    /* Generate distinct values. */
    std::vector<T> values;
    if (std::is_integral_v<T>)
        values = datagen::generate_uniform_distinct_numbers<T>(
            /* min=   */ std::numeric_limits<T>::lowest() + std::is_signed_v<T>,
            /* max=   */ std::numeric_limits<T>::max(),
            /* count= */ num_distinct_values
        );
    else
        values = datagen::generate_uniform_distinct_numbers<T>(T(0), T(1), num_distinct_values);
    M_insist(values.size() == num_distinct_values);

    /* Write distinct values repeatedly in arbitrary order to column. */
    auto ptr = column_ptr + begin;
    std::mt19937_64 g(0);
    for (std::size_t i = 0; i != count; ) {
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
    M_insist(ptr - column_ptr == long(count), "incorrect number of elements written");
}

template<typename T>
void generate_correlated_numeric_columns(T *left_ptr, T *right_ptr, std::size_t num_distinct_values_left,
                                         std::size_t num_distinct_values_right, std::size_t count_left,
                                         std::size_t count_right, std::size_t num_distinct_values_matching)
{
    static_assert(std::is_arithmetic_v<T>, "T must be an arithmetic type");
    M_insist(num_distinct_values_left >= num_distinct_values_matching,
           "num_distinct_values_left must be larger than num_distinct_values_matching");
    M_insist(num_distinct_values_right >= num_distinct_values_matching,
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
        auto left = left_ptr;
        for (std::size_t i = 0; i != count_left; ) {
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
        M_insist(left - left_ptr == long(count_left),
                 "incorrect number of elements written to left store");
    }

    /* Fill `store_right`. */
    {
        /* Draw last `num_distinct_values_right` values from `values`. */
        std::vector<T> values_right(values.rbegin(), values.rbegin() + num_distinct_values_right);

        /* Write distinct values repeatedly in arbitrary order to column. */
        auto right = right_ptr;
        for (std::size_t i = 0; i != count_right; ) {
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
        M_insist(right - right_ptr == long(count_right),
                "incorrect number of elements written to right store");
    }
}

}

void m::set_all_null(uint8_t *column_ptr, std::size_t num_attrs, std::size_t begin, std::size_t end)
{
    M_insist(begin < end, "must set at least one row");

    auto begin_bytes = (num_attrs * begin) / 8U;
    const auto begin_bits = (num_attrs * begin) % 8U;
    const auto end_bytes = (num_attrs * end) / 8U;
    const auto end_bits = (num_attrs * end) % 8U;

    M_insist(begin_bytes != end_bytes, "the `begin`-th row and `end`-th row must be in different bytes");

    /* Set the `begin_bits` most significant bits to 1. */
    if (begin_bits) {
        *(column_ptr + begin_bytes) |= ~((1U << (8U - begin_bits)) - 1U); // set respective bits to 1
        ++begin_bytes; // advance to first byte which can be written entirely
    }

    /* Set the `end_bits` least significant bits to 1. */
    if (end_bits)
        *(column_ptr + end_bytes) |= (1U << end_bits) - 1U; // set respective bits to 1

    const auto num_bytes = end_bytes - begin_bytes;
    std::memset(column_ptr + begin_bytes, uint8_t(~0), num_bytes);
}

void m::set_all_not_null(uint8_t *column_ptr, std::size_t num_attrs, std::size_t begin, std::size_t end)
{
    M_insist(begin < end, "must set at least one row");

    auto begin_bytes = (num_attrs * begin) / 8U;
    const auto begin_bits = (num_attrs * begin) % 8U;
    const auto end_bytes = (num_attrs * end) / 8U;
    const auto end_bits = (num_attrs * end) % 8U;

    M_insist(begin_bytes != end_bytes, "the `begin`-th row and `end`-th row must be in different bytes");

    /* Set the `begin_bits` most significant bits to 0. */
    if (begin_bits) {
        *(column_ptr + begin_bytes) &= (1U << begin_bits) - 1U; // set respective bits to 0
        ++begin_bytes; // advance to first byte which can be written entirely
    }

    /* Set the `end_bits` least significant bits to 0. */
    if (end_bits)
        *(column_ptr + end_bytes) &= ~((1U << (8U - end_bits)) - 1U); // set respective bits to 0

    const auto num_bytes = end_bytes - begin_bytes;
    std::memset(column_ptr + begin_bytes, uint8_t(0), num_bytes);
}


void m::generate_primary_keys(void *column_ptr, const Type &type, std::size_t begin, std::size_t end)
{
    M_insist(begin < end, "must set at least one row");
    M_insist(type.is_integral(), "primary key columns must be of integral type");

    auto &n = as<const Numeric>(type);
    switch (n.size()) {
        default:
            M_unreachable("unsupported size of primary key");

        case 32: {
            auto ptr = reinterpret_cast<int32_t*>(column_ptr);
            std::iota<int32_t*, int32_t>(ptr + begin, ptr + end, begin);
            break;
        }

        case 64: {
            auto ptr = reinterpret_cast<int64_t*>(column_ptr);
            std::iota<int64_t*, int64_t>(ptr + begin, ptr + end, begin);
            break;
        }
    }
}

void m::generate_column_data(void *column_ptr, const Attribute &attr, std::size_t num_distinct_values,
                             std::size_t begin, std::size_t end)
{
    M_insist(begin < end, "must set at least one row");

    if (streq(attr.name, "id")) { // generate primary key
        generate_primary_keys(column_ptr, *attr.type, begin, end);
    } else if (auto n = cast<const Numeric>(attr.type)) {
        switch (n->kind) {
            case m::Numeric::N_Int:
                switch (n->size()) {
#define CASE(N) case N: \
                ::generate_numeric_column(reinterpret_cast<int##N##_t*>(column_ptr), num_distinct_values, begin, end); \
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
                        ::generate_numeric_column(reinterpret_cast<float*>(column_ptr), num_distinct_values, begin, end);
                        break;
                    case 64:
                        ::generate_numeric_column(reinterpret_cast<double*>(column_ptr), num_distinct_values, begin, end);
                        break;
                }
                break;
            case m::Numeric::N_Decimal:
                M_unreachable("unsupported type");
        }

    } else {
        M_unreachable("unsupported type");
    }
}

void m::generate_correlated_column_data(void *left_ptr, void *right_ptr, const Attribute &attr,
                                        std::size_t num_distinct_values_left, std::size_t num_distinct_values_right,
                                        std::size_t count_left, std::size_t count_right,
                                        std::size_t num_distinct_values_matching)
{
    if (streq(attr.name, "id")) { // primary keys cannot be correlated
        M_unreachable("primary keys unsupported");
    } else if (auto n = cast<const Numeric>(attr.type)) {
        switch (n->size()) {
#define CASE(N) case N: \
                generate_correlated_numeric_columns(reinterpret_cast<int##N##_t*>(left_ptr), \
                                                    reinterpret_cast<int##N##_t*>(right_ptr), \
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
        M_unreachable("unsupported type");
    }
}

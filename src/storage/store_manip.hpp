#pragma once

#include "storage/ColumnStore.hpp"
#include <mutable/catalog/Schema.hpp>
#include <random>
#include <utility>
#include <vector>


namespace m {

/** Sets all attributes of the `begin`-th row (including) to the `end`-th row (excluding) of column at
 * address `column_ptr` to NULL. */
void set_all_null(uint8_t *column_ptr, std::size_t num_attrs, std::size_t begin, std::size_t end);

/** Sets all attributes of the `begin`-th row (including) to the `end`-th row (excluding) of column at
 * address `column_ptr` to NOT NULL. */
void set_all_not_null(uint8_t *column_ptr, std::size_t num_attrs, std::size_t begin, std::size_t end);

/** Generates primary keys of `Type` `type` for the `begin`-th row (including) to the `end`-th row (excluding)
 * of column at address `column_ptr`. */
void generate_primary_keys(void *column_ptr, const Type &type, std::size_t begin, std::size_t end);

/** Fills column at address `column_ptr` from `begin`-th row (including) to `end`-th row (excluding) with data from
 * `values`.  Initially, shuffles `values` with `g` as URBG.  Then, uses each value of `values` exactly once.  After
 * all elements of `values` were used exactly once, shuffles `values` again with `g` as URBG and repeats.  */
template<typename T, typename Generator = std::mt19937_64>
std::enable_if_t<std::is_arithmetic_v<T>, void>
M_EXPORT
fill_uniform(T *column_ptr, std::vector<T> values, std::size_t begin, std::size_t end, Generator &&g = Generator()) {
    M_insist(begin < end, "must set at least one row");

    const auto count = end - begin;

    /* Write distinct values repeatedly in arbitrary order to column. */
    auto ptr = column_ptr + begin;
    for (std::size_t i = 0; i != count; ) {
        /* Shuffle the vector before writing its values to the column. */
        std::shuffle(values.begin(), values.end(), std::forward<Generator>(g));
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

/** Generates data for the column at address `column_ptr` from `begin`-th row (including) to `end`-th row (excluding)
 * and writes it directly to memory.  The rows must have been allocated before calling this function. */
void generate_column_data(void *column_ptr, const Attribute &attr, std::size_t num_distinct_values, std::size_t begin,
                          std::size_t end);

/** Generates data for two columns at addresses `left_ptr` and `right_ptr` correlated by `num_distinct_values_matching`
 * and writes the data directly to memory. The rows must have been allocated before calling this function. */
void generate_correlated_column_data(void *left_ptr, void *right_ptr, const Attribute &attr,
                                     std::size_t num_distinct_values_left, std::size_t num_distinct_values_right,
                                     std::size_t count_left, std::size_t count_right,
                                     std::size_t num_distinct_values_matching);

}

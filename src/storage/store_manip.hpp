#pragma once

#include "catalog/Schema.hpp"
#include "storage/ColumnStore.hpp"
#include <random>
#include <utility>
#include <vector>


namespace m {

/** Sets the NULL bitmap of all rows in `store`, from the `begin`-th row (including) to the `end`-th row (excluding), to
 * `bytes`.  `bytes` must have the same length as there are bytes in the `store`'s NULL bitmap. */
void set_null_bitmap(ColumnStore &store, const std::vector<uint8_t> &bytes,
                     const std::size_t begin, const std::size_t end);

/** Sets all attributes of the `begin`-th row (including) to the `end`-th row (excluding) to NULL. */
void set_all_null(ColumnStore &store, const std::size_t begin, std::size_t end);

/** Sets all attributes of the `begin`-th row (including) to the `end`-th row (excluding) not NULL. */
void set_all_not_null(ColumnStore &store, const std::size_t begin, std::size_t end);

/** Generates primary keys for `attr` of `store` for the `begin`-th row (including) to the `end`-th row (excluding).  */
void generate_primary_keys(ColumnStore &store, const Attribute &attr, const std::size_t begin, const std::size_t end);

/** Fills column `attr` of `store` from `begin`-th row (including) to `end`-th row (excluding) with data from `values`.
 * Initially, shuffles `values` with `g` as URBG.  Then, uses each value of `values` exactly once.  After all elements
 * of `values` were used exactly once, shuffles `values` again with `g` as URBG and repeats.  */
template<typename T, typename Generator = std::mt19937_64>
std::enable_if_t<std::is_arithmetic_v<T>, void>
M_EXPORT
fill_uniform(ColumnStore &store, const Attribute &attr, std::vector<T> values,
             const std::size_t begin, const std::size_t end, Generator &&g = Generator())
{
    M_insist(end <= store.num_rows(), "end out of bounds");
    M_insist(begin <= end, "begin out of bounds");

    const std::size_t count = end - begin;

    /* Write distinct values repeatedly in arbitrary order to column. */
    auto &mem = store.memory(attr.id);
    auto ptr = mem.as<T*>() + begin;

    for (std::size_t i = 0; i != count;) {
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
    M_insist(ptr - mem.as<T*>() == (long long)(end), "incorrect number of elements written");
}

void generate_column_data(ColumnStore &store, const Attribute &attr, const std::size_t num_distinct_values,
                          const std::size_t begin, std::size_t end);

void generate_correlated_column_data(ColumnStore &store_left,
                                     ColumnStore &store_right,
                                     const Attribute &attr,
                                     const std::size_t num_distinct_values_left,
                                     const std::size_t num_distinct_values_right,
                                     const std::size_t count_left,
                                     const std::size_t count_right,
                                     const std::size_t num_distinct_values_matching);

}

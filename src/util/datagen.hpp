#pragma once

#include "util/GridSearch.hpp"
#include <random>
#include <type_traits>
#include <utility>


namespace m {

/** Utilities to help generate data, e.g. for experiment setups. */
namespace datagen {

/** Generate `count` many distinct numbers of type `T`, chosen uniformly at random from the range [`min`, `max`].  Uses
 * generator `g` for randomness in the data.  */
template<typename T, typename Generator = std::mt19937_64>
std::vector<T> generate_uniform_distinct_numbers(const T min, const T max, const std::size_t count,
                                                 Generator &&g = Generator())
{
    static_assert(std::is_arithmetic_v<T>, "T must be an arithmetic type");
    M_insist(min <= max);
    M_insist(is_range_wide_enough<T>(min, max, count), "range [min, max] does not have count distinct values");

    if (count == 1) return std::vector<T>(1, min);
    std::vector<T> values = gs::LinearSpace<T>(min, max, count-1).sequence();

    std::shuffle(values.begin(), values.end(), std::forward<Generator>(g));

    return values;
}

}

}

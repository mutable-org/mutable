#pragma once


#include <mutable/util/macro.hpp>
#include <algorithm>
#include <cstddef>
#include <type_traits>
#include <utility>


namespace m {


/** Verifies that `mid` splits the range `begin` to `end` (exclusive) into two partitions.
 *
 * @return  `true` iff `mid` splits the range `begin` to `end` (exclusive) into two partitions.
 */
template<typename It>
bool verify_partition(It begin, It mid, It end)
{
    using T = std::remove_reference_t<decltype(*begin)>;
    T max_lo{std::numeric_limits<T>::lowest()};
    T min_hi{std::numeric_limits<T>::max()};
    M_insist(max_lo < min_hi);
    for (It p = begin; p != mid; ++p)
        max_lo = std::max(max_lo, *p);
    for (It p = mid; p != end; ++p)
        min_hi = std::min(min_hi, *p);
    return max_lo <= min_hi;
}

/** Partitions the range `begin` to `end` (exclusive) into two partitions using `pivot`.
 *
 * @return  pointer to the beginning of the second partition
 */
struct partition_predicated_naive
{
    template<typename T, typename It>
    It operator()(const T pivot, It begin, It end)
    {
#ifndef NDEBUG
        const It the_end = end;
#endif
        using std::prev;
        while (begin < end) {
            const T left = *begin;
            const T right = *prev(end);
            *begin = right;
            *prev(end) = left;
            const ptrdiff_t adv_lo = right <= pivot;
            const ptrdiff_t adv_hi = left >= pivot;
            begin += adv_lo;
            end   -= adv_hi;
        }
#ifndef NDEBUG
        M_insist(begin == the_end or *begin >= pivot);
#endif
        return begin;
    }
};

template<typename Partitioning, typename It>
void qsort(It begin, It end, Partitioning p)
{
    using std::swap;
    using std::min, std::max;
    using std::distance;
    using std::prev, std::next;
    M_insist(begin <= end);

    while (distance(begin, end) > 2) {
        /* Compute median of three. */
        auto pm = begin + (end - begin) / 2;
        bool left_le_mid   = *begin <= *pm;
        bool left_le_right = *begin <= *prev(end);
        bool mid_le_right  = *pm <= *prev(end);
        if (left_le_mid) {
            if (left_le_right) {
                if (mid_le_right)
                    swap(*begin, *pm);
                else
                    swap(*begin, *prev(end));
            } else {
                /* nothing to be done */
            }
        } else {
            if (mid_le_right) {
                if (left_le_right) {
                    /* nothing to be done */
                } else {
                    swap(*begin, *prev(end));
                }
            } else {
                swap(*begin, *pm);
            }
        }

        It mid = p(*begin, next(begin), end);
        M_insist(mid <= end);
        M_insist(mid >= next(begin));
        M_insist(verify_partition(next(begin), mid, end));
        swap(*begin, *prev(mid));

        if (distance(mid, end) >= 2) qsort(mid, end, p); // recurse to the right
        M_insist(std::is_sorted(mid, end));
        end = prev(mid);
    }

    if (distance(begin, end) == 2) {
        if (*begin > *prev(end))
            swap(*begin, *prev(end));
    }
};

}

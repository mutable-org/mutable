#pragma once

#include "mutable/util/ADT.hpp"


/** A sorted list of elements.  Allows duplicates. */
template<typename T>
struct sorted_list
{
    using vector_type = std::vector<T>; ///< the type of the internal container of elements
    using value_type = T;
    using size_type = typename vector_type::size_type;

    private:
    vector_type v_; ///< the internal container of elements

    public:
    sorted_list() { }

    auto begin() { return v_.begin(); }
    auto end()   { return v_.end(); }
    auto begin() const { return v_.begin(); }
    auto end()   const { return v_.end(); }
    auto cbegin() const { return v_.cbegin(); }
    auto cend()   const { return v_.cend(); }

    /** Returns `true` iff the `sorted_list` has no elements. */
    auto empty() const { return v_.empty(); }
    /** Returns the number of elements in this `sorted_list`. */
    auto size() const { return v_.size(); }
    /** Reserves space for `new_cap` elements in this `sorted_list`. */
    auto reserve(size_type new_cap) { return v_.reserve(new_cap); }

    /** Returns `true` iff this `sorted_list` contains an element that is equal to `value`. */
    bool contains(const T &value) const {
        auto pos = std::lower_bound(begin(), end(), value);
        return pos != end() and *pos == value;
    }

    /** Inserts `value` into this `sorted_list`.  Returns an `iterator` pointing to the inserted element. */
    auto insert(T value) { return v_.insert(std::lower_bound(begin(), end(), value), value); }

    /** Inserts elements in the range from `first` (including) to `last` (excluding) into this `sorted_list. */
    template<typename InsertIt>
    void insert(InsertIt first, InsertIt last) {
        while (first != last)
            insert(*first++);
    }
};

/** Enumerate all subsets of size `k` based on superset of size `n`.
 *  See http://programmingforinsomniacs.blogspot.com/2018/03/gospers-hack-explained.html. */
struct GospersHack
{
    private:
    uint64_t set_;
    uint64_t limit_;

    GospersHack() { }

    public:
    /** Create an instance of `GospersHack` that enumerates all subsets of size `k` of a set of `n` elements. */
    static GospersHack enumerate_all(uint64_t k, uint64_t n) {
        insist(k <= n, "invalid enumeration");
        GospersHack GH;
        GH.set_ = (1UL << k) - 1;
        GH.limit_ = 1UL << n;
        return GH;
    }
    /** Create an instance of `GospersHack` that enumerates all remaining subsets of a set of `n` elements, starting at
     * subset `set`. */
    static GospersHack enumerate_from(SmallBitset set, uint64_t n) {
        GospersHack GH;
        GH.set_ = set;
        GH.limit_ = 1UL << n;
        insist(set <= GH.limit_, "set exceeds the limit");
        return GH;
    }

    /** Advance to the next subset. */
    GospersHack & operator++() {
        uint64_t c = set_ & -set_;
        uint64_t r = set_ + c;
        set_ = (((r ^ set_) >> 2) / c) | r;
        return *this;
    }

    /** Returns `false` iff all subsets have been enumerated. */
    operator bool() const { return set_ < limit_; }

    /** Returns the current subset. */
    SmallBitset operator*() const { return SmallBitset(set_); }
};

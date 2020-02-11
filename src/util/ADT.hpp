#pragma once

#include "util/fn.hpp"
#include "util/macro.hpp"
#include <algorithm>
#include <iostream>


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

/** Implements a small and efficient set over integers in the range of `0` to `63` (including). */
struct SmallBitset
{
    static constexpr std::size_t CAPACITY = 64; ///< the maximum capacity of a `SmallBitset`

    private:
    uint64_t bits_; ///< the bit vector representing the set

    struct iterator
    {
        private:
        uint64_t bits_;

        public:
        iterator(uint64_t bits) : bits_(bits) { }

        bool operator==(iterator other) const { return this->bits_ == other.bits_; }
        bool operator!=(iterator other) const { return not operator==(other); }

        iterator & operator++() { bits_ = bits_ & (bits_ - 1); /* reset lowest set bit */ return *this; }
        iterator operator++(int) { auto clone = *this; ++clone; return clone; }

        std::size_t operator*() const { insist(bits_ != 0); return __builtin_ctzl(bits_); }
    };

    public:
    SmallBitset() : bits_(0) { };
    explicit SmallBitset(uint64_t bits) : bits_(bits) { };

    /** Set the `offset`-th bit to `1`. */
    void set(std::size_t offset) {
        insist(offset < CAPACITY, "offset is out-of-bounds");
        setbit(&bits_, 1, offset);
    }

    /** Returns `true` iff the `offset`-th bit is set to `1`. */
    bool contains(std::size_t offset) const {
        insist(offset < CAPACITY, "offset is out-of-bounds");
        return (bits_ >> offset) & 0b1;
    }

    /** Returns `true` iff the `offset`-th bit is set to `1`. */
    bool operator()(std::size_t offset) const { return contains(offset); }

    /** Returns the maximum capacity. */
    constexpr std::size_t capacity() { return CAPACITY; }
    /** Returns the number of elements in this `SmallBitset`. */
    std::size_t size() const { return __builtin_popcountl(bits_); }
    /** Returns `true` if there are no elements in this `SmallBitset`. */
    bool empty() const { return size() == 0; }

    auto begin() const { return iterator(bits_); }
    auto cbegin() const { return begin(); }
    auto end() const { return iterator(0); }
    auto cend() const { return end(); }

    /** Convert the `SmallBitset` type to `uint64_t`. */
    operator uint64_t() const { return bits_; }

    /** Returns `true` if the set represented by `other` is a subset of `this`, i.e.\ `other` ⊆ `this`. */
    bool is_subset(SmallBitset other) const { return this->bits_ == (other.bits_ & this->bits_); }

    /** Returns the union of `left` and `right`, i.e.\ `left` ∪ `right`. */
    friend SmallBitset unify(SmallBitset left, SmallBitset right) { return SmallBitset(left.bits_ | right.bits_); }
    /** Returns the intersection of `left` and `right`, i.e.\ `left` ∩ `right`. */
    friend SmallBitset intersect(SmallBitset left, SmallBitset right) { return SmallBitset(left.bits_ & right.bits_); }
    /** Returns the set where the elements of `left` have been subtracted from `right`, i.e.\ `left` - `right`. */
    friend SmallBitset subtract(SmallBitset left, SmallBitset right) { return SmallBitset(left.bits_ & ~right.bits_); }
    /** Returns the union of `left` and `right`, i.e.\ `left` ∪ `right`. */
    friend SmallBitset operator|(SmallBitset left, SmallBitset right) { return unify(left, right); }
    /** Returns the intersection of `left` and `right`, i.e.\ `left` ∩ `right`. */
    friend SmallBitset operator&(SmallBitset left, SmallBitset right) { return intersect(left, right); }
    /** Returns the set where the elements of `right` have been subtracted from `left`, i.e.\ `left` - `right`. */
    friend SmallBitset operator-(SmallBitset left, SmallBitset right) { return subtract(left, right); }

    SmallBitset & operator|=(SmallBitset other) { return *this = *this | other; }
    SmallBitset & operator&=(SmallBitset other) { return *this = *this & other; }
    SmallBitset & operator-=(SmallBitset other) { return *this = *this - other; }

    /** Write a textual representation of `s` to `out`. */
    friend std::ostream & operator<<(std::ostream &out, SmallBitset s) {
        for (uint64_t mask = 1UL << (SmallBitset::CAPACITY - 1); mask; mask >>= 1)
            out << bool(s & SmallBitset(mask));
        return out;
    }

    void dump(std::ostream &out) const;
    void dump() const;
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

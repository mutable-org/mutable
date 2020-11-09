#pragma once

#include "mutable/util/exception.hpp"
#include "mutable/util/fn.hpp"
#include "mutable/util/macro.hpp"
#include <algorithm>
#include <iostream>


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

    /** Set the `offset`-th bit to `val`. */
    void set(std::size_t offset, bool val) {
        if (offset >= CAPACITY)
            throw m::out_of_range("offset is out of bounds");
        setbit(&bits_, val, offset);
    }

    /** Set the `offset`-th bit to `1`. */
    void set(std::size_t offset) { set(offset, true); }

    /** Set the `offset`-th bit to `0`. */
    void clear(std::size_t offset) { set(offset, false); }

    /** Returns `true` iff the `offset`-th bit is set to `1`. */
    bool contains(std::size_t offset) const {
        if (offset >= CAPACITY)
            throw m::out_of_range("offset is out of bounds");
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

    /** Returns `true` if the set represented by `this` is a subset of `other`, i.e.\ `this` ⊆ `other`. */
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

    /** Print a textual representation of `this` with `size` bits to `out`. */
    void print_fixed_length(std::ostream &out, std::size_t size) const {
        for (uint64_t mask = 1UL << (size - 1); mask; mask >>= 1)
            out << bool(*this & SmallBitset(mask));
    }

    void dump(std::ostream &out) const;
    void dump() const;
};

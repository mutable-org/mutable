#pragma once

#include "mutable/util/exception.hpp"
#include "mutable/util/fn.hpp"
#include "mutable/util/macro.hpp"
#include <algorithm>
#include <iostream>
#include <memory>
#include <type_traits>


namespace m {

/** Implements a small and efficient set over integers in the range of `0` to `63` (including). */
struct SmallBitset
{
    static constexpr std::size_t CAPACITY = 64; ///< the maximum capacity of a `SmallBitset`

    /** A proxy to access single elements in `SmallBitset`. */
    template<bool C>
    struct Proxy
    {
        static constexpr bool Is_Const = C;

        private:
        using reference_type = std::conditional_t<Is_Const, const SmallBitset&, SmallBitset&>;

        reference_type S_;
        std::size_t offset_;

        public:
        Proxy(reference_type S, std::size_t offset) : S_(S), offset_(offset) { insist(offset_ < CAPACITY); }

        operator bool() const { return (S_.bits_ >> offset_) & 0b1; }

        template<bool C_ = Is_Const>
        std::enable_if_t<not C_, Proxy&>
        operator=(bool val) { setbit(&S_.bits_, val, offset_); return *this; }
    };

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

    /** Returns the `offset`-th bit.  Requires that `offset` is in range `[0; CAPACITY)`. */
    Proxy<true> operator()(std::size_t offset) const { return Proxy<true>(*this, offset); }

    /** Returns the `offset`-th bit.  Requires that `offset` is in range `[0; CAPACITY)`. */
    Proxy<false> operator()(std::size_t offset) { return Proxy<false>(*this, offset); }

    /** Returns the `offset`-th bit.  Requires that `offset` is in range `[0; CAPACITY)`. */
    Proxy<true> operator[](std::size_t offset) const { return operator()(offset); }

    /** Returns the `offset`-th bit.  Requires that `offset` is in range `[0; CAPACITY)`. */
    Proxy<false> operator[](std::size_t offset) { return operator()(offset); }

    /** Returns a proxy to the bit at offset `offset`.  Throws `m::out_of_range` if `offset` is not in range
     * `[0; CAPACITY)`. */
    Proxy<true> at(std::size_t offset) const {
        if (offset >= CAPACITY)
            throw m::out_of_range("offset is out of bounds");
        return operator()(offset);
    }

    /** Returns a proxy to the bit at offset `offset`.  Throws `m::out_of_range` if `offset` is not in range
     * `[0; CAPACITY)`. */
    Proxy<false> at(std::size_t offset) {
        if (offset >= CAPACITY)
            throw m::out_of_range("offset is out of bounds");
        return operator()(offset);
    }

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
    explicit operator uint64_t() const { return bits_; }
    explicit operator bool() const { return not empty(); }

    bool operator==(SmallBitset other) const { return this->bits_ == other.bits_; }
    bool operator!=(SmallBitset other) const { return not operator==(other); }

    /** Inverts all bits in the bitset. */
    SmallBitset operator~() const { return SmallBitset(~bits_); }

    /** Returns `true` if the set represented by `this` is a subset of `other`, i.e.\ `this` ⊆ `other`. */
    bool is_subset(SmallBitset other) const { return this->bits_ == (other.bits_ & this->bits_); }

    /** Converts a singleton set to a mask for all bits lower than the single, set bit. */
    SmallBitset singleton_to_lo_mask() const {
        insist((bits_ & (bits_ - 1UL)) == 0UL, "not a singleton set");
        return bits_ ? SmallBitset(bits_ - 1UL) : SmallBitset(0UL);
    }

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
        for (uint64_t i = CAPACITY; i --> 0;)
            out << s(i);
        return out;
    }

    /** Print a textual representation of `this` with `size` bits to `out`. */
    void print_fixed_length(std::ostream &out, std::size_t size) const {
        for (uint64_t i = size; i --> 0;)
            out << (*this)(i);
    }

    void dump(std::ostream &out) const;
    void dump() const;
};


/** Returns the least subset of a given `set`, i.e.\ the set represented by the lowest 1 bit. */
inline SmallBitset least_subset(SmallBitset S) { return SmallBitset(uint64_t(S) & -uint64_t(S)); }

/** Returns the next subset of a given `subset` and `set. */
inline SmallBitset next_subset(SmallBitset subset, SmallBitset set)
{
    return SmallBitset(uint64_t(subset) - uint64_t(set)) & set;
}

/** Implements an array of dynamic but fixed size. */
template<typename T>
struct dyn_array
{
    using value_type = T;
    using size_type = std::size_t;
    using iterator = value_type*;
    using const_iterator = const value_type*;

    private:
    std::unique_ptr<value_type[]> arr_;
    std::size_t size_;

    public:
    /** Constructs an array of size 0. */
    dyn_array() : size_(0) { }

    /** Constructs an array of size `size`. */
    explicit dyn_array(std::size_t size)
        : arr_(std::make_unique<value_type[]>(size))
        , size_(size)
    { }

    /** Constructs an array with the elements in range `[begin, end)`.  The size of the array will be
     * `std::distance(begin, end)`.  */
    template<typename It>
    dyn_array(It begin, It end)
        : dyn_array(std::distance(begin, end))
    {
        auto ptr = data();
        for (auto it = begin; it != end; ++it)
            new (ptr++) value_type(*it);
    }

    /** Copy-constructs an array. */
    explicit dyn_array(const dyn_array &other)
        : dyn_array(other.size())
    {
        for (std::size_t i = 0; i != other.size(); ++i)
            new (&data()[i]) value_type(other[i]);
    }

    dyn_array(dyn_array&&) = default;
    dyn_array & operator=(dyn_array&&) = default;

    /** Returns the size of this array, i.e. the number of elements. */
    size_type size() const { return size_; }

    /** Returns a pointer to the beginning of the array. */
    const value_type * data() const { return arr_.get(); }
    /** Returns a pointer to the beginning of the array. */
    value_type * data() { return arr_.get(); }

    /** Returns a reference to the element at position `pos`.  Requires that `pos` is in bounds. */
    const value_type & operator[](std::size_t pos) const {
        insist(pos < size(), "index out of bounds");
        return data()[pos];
    }

    /** Returns a reference to the element at position `pos`.  Requires that `pos` is in bounds. */
    value_type & operator[](std::size_t pos) {
        insist(pos < size(), "index out of bounds");
        return data()[pos];
    }

    /** Returns a reference to the element at position `pos`.  Throws `m::out_of_range` if `pos` is out of bounds. */
    const value_type & at(std::size_t pos) const {
        if (pos >= size())
            throw m::out_of_range("index out of bounds");
        return (*this)[pos];
    }

    /** Returns a reference to the element at position `pos`.  Throws `m::out_of_range` if `pos` is out of bounds. */
    value_type & at(std::size_t pos) {
        if (pos >= size())
            throw m::out_of_range("index out of bounds");
        return (*this)[pos];
    }

    iterator begin() { return data(); }
    iterator end() { return data() + size(); }
    const_iterator begin() const { return data(); }
    const_iterator end() const { return data() + size(); }
    const_iterator cbegin() const { return begin(); }
    const_iterator cend() const { return end(); }

    /** Returns `true` iff the contents of `this` and `other` are equal, that is, they have the same number of elements
     * and each element in `this` compares equal with the element in `other` at the same position. */
    bool operator==(const dyn_array &other) const {
        if (this->size() != other.size())
            return false;

        for (std::size_t i = 0; i != size(); ++i) {
            if ((*this)[i] != other[i])
                return false;
        }

        return true;
    }
    /** Returns `false` iff the contents of `this` and `other` are equal, that is, they have the same number of elements
     * and each element in `this` compares equal with the element in `other` at the same position. */
    bool operator!=(const dyn_array &other) const { return not operator==(other); }
};

}

#pragma once

#include "util/fn.hpp"
#include "util/macro.hpp"
#include <algorithm>
#include <iostream>


/** A sorted list of elements.  Allows duplicates. */
template<typename T>
struct sorted_list
{
    using vector_type = std::vector<T>;
    using value_type = T;
    using size_type = typename vector_type::size_type;

    private:
    vector_type v_;

    public:
    sorted_list() { }

    auto begin() { return v_.begin(); }
    auto end()   { return v_.end(); }
    auto begin() const { return v_.begin(); }
    auto end()   const { return v_.end(); }
    auto cbegin() const { return v_.cbegin(); }
    auto cend()   const { return v_.cend(); }

    auto empty() const { return v_.empty(); }
    auto size() const { return v_.size(); }
    auto reserve(size_type new_cap) { return v_.reserve(new_cap); }

    bool contains(const T &value) const {
        auto pos = std::lower_bound(begin(), end(), value);
        return pos != end() and *pos == value;
    }

    auto insert(T value) { return v_.insert(std::lower_bound(begin(), end(), value), value); }

    template<typename InsertIt>
    void insert(InsertIt first, InsertIt last) {
        while (first != last)
            insert(*first++);
    }
};

/** Implements a small and efficient set over integers in the range of 0 to 63 (including).  Useful for tracking
 *  indices. */
struct SmallBitset
{
    static constexpr std::size_t CAPACITY = 64;

    private:
    uint64_t bits_;

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

    void set(std::size_t offset) {
        insist(offset < CAPACITY, "offset is out-of-bounds");
        setbit(&bits_, 1, offset);
    }

    bool contains(std::size_t offset) const {
        insist(offset < CAPACITY, "offset is out-of-bounds");
        return (bits_ >> offset) & 0b1;
    }

    bool operator()(std::size_t offset) const { return contains(offset); }

    constexpr std::size_t capacity() { return CAPACITY; }
    std::size_t size() const { return __builtin_popcountl(bits_); }
    bool empty() const { return size() == 0; }

    auto begin() const { return iterator(bits_); }
    auto cbegin() const { return begin(); }
    auto end() const { return iterator(0); }
    auto cend() const { return end(); }

    operator uint64_t() const { return bits_; }

    bool is_subset(SmallBitset other) const { return this->bits_ == (other.bits_ & this->bits_); }

    friend SmallBitset unify(SmallBitset left, SmallBitset right) { return SmallBitset(left.bits_ | right.bits_); }
    friend SmallBitset intersect(SmallBitset left, SmallBitset right) { return SmallBitset(left.bits_ & right.bits_); }
    friend SmallBitset subtract(SmallBitset left, SmallBitset right) { return SmallBitset(left.bits_ & ~right.bits_); }
    friend SmallBitset operator|(SmallBitset left, SmallBitset right) { return unify(left, right); }
    friend SmallBitset operator&(SmallBitset left, SmallBitset right) { return intersect(left, right); }
    friend SmallBitset operator-(SmallBitset left, SmallBitset right) { return subtract(left, right); }

    friend std::ostream & operator<<(std::ostream &out, SmallBitset s) {
        for (uint64_t mask = 1UL << (SmallBitset::CAPACITY - 1); mask; mask >>= 1)
            out << bool(s & SmallBitset(mask));
        return out;
    }

    void dump(std::ostream &out) const;
    void dump() const;
};

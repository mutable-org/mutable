#pragma once

#include <algorithm>
#include <climits>
#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <mutable/util/exception.hpp>
#include <mutable/util/fn.hpp>
#include <mutable/util/macro.hpp>
#include <mutable/util/malloc_allocator.hpp>
#include <sstream>
#include <string>
#include <type_traits>
#include <utility>
#ifdef __BMI2__
#include <x86intrin.h>
#endif


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
        Proxy(reference_type S, std::size_t offset) : S_(S), offset_(offset) { M_insist(offset_ < CAPACITY); }

        operator bool() const { return (S_.bits_ >> offset_) & 0b1; }

        template <bool C_ = Is_Const>
        requires (not C_)
        Proxy& operator=(bool val) { setbit(&S_.bits_, val, offset_); return *this; }

        Proxy & operator=(const Proxy &other) {
            static_assert(not Is_Const, "can only assign to proxy of non-const SmallBitset");
            return operator=(bool(other));
        }
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

        iterator & operator++() {
#ifdef __BMI__
            bits_ = _blsr_u64(bits_); // BMI1: reset lowest set bit
#else
            bits_ = bits_ & (bits_ - 1UL); // reset lowest set bit
#endif
            return *this;
        }
        iterator operator++(int) { auto clone = *this; operator++(); return clone; }

        std::size_t operator*() const { M_insist(bits_ != 0); return std::countr_zero(bits_); }
        SmallBitset as_set() const {
#ifdef __BMI__
            return SmallBitset(_blsi_u64(bits_)); // BMI1: extract lowest set isolated bit
#else
            return SmallBitset(bits_ & -bits_); // extract lowest set isolated bit
#endif
        }
    };

    struct reverse_iterator
    {
        private:
        uint64_t bits_;

        public:
        reverse_iterator(uint64_t bits) : bits_(bits) { }

        bool operator==(reverse_iterator other) const { return this->bits_ == other.bits_; }
        bool operator!=(reverse_iterator other) const { return not operator==(other); }

        reverse_iterator & operator++() { bits_ = bits_ & ~(1UL << operator*()); return *this; }
        reverse_iterator operator++(int) { auto clone = *this; operator++(); return clone; }

        std::size_t operator*() const {
            M_insist(bits_ != 0);
            const unsigned lz = std::countl_zero(bits_);
            return CHAR_BIT * sizeof(bits_) - 1UL - lz;
        }
        SmallBitset as_set() const { return SmallBitset(1UL << operator*()); }
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
    std::size_t size() const { return std::popcount(bits_); }
    /** Returns `true` if there are no elements in this `SmallBitset`. */
    bool empty() const { return bits_ == 0; }
    /* Returns `true` if this set is a singleton set, i.e. the set contains exactly one element. */
    bool singleton() const { return size() == 1; }

    /** Returns the highest set bit as a `SmallBitset`. */
    SmallBitset hi() const {
        unsigned lz = std::countl_zero(bits_);
        return SmallBitset(1UL << (CHAR_BIT * sizeof(bits_) - lz - 1U));
    }

    auto begin() const { return iterator(bits_); }
    auto cbegin() const { return begin(); }
    auto end() const { return iterator(0); }
    auto cend() const { return end(); }

    auto rbegin() const { return reverse_iterator(bits_); }
    auto crbegin() const { return rbegin(); }
    auto rend() const { return reverse_iterator(0); }
    auto crend() const { return rend(); }

    /** Convert the `SmallBitset` type to `uint64_t`. */
    explicit operator uint64_t() const { return bits_; }
    explicit operator bool() const { return not empty(); }

    bool operator==(SmallBitset other) const { return this->bits_ == other.bits_; }
    bool operator!=(SmallBitset other) const { return not operator==(other); }

    /** Inverts all bits in the bitset. */
    SmallBitset operator~() const { return SmallBitset(~bits_); }

    /** Returns `true` if the set represented by `this` is a subset of `other`, i.e.\ `this` ⊆ `other`. */
    bool is_subset(SmallBitset other) const { return this->bits_ == (other.bits_ & this->bits_); }

    /** Returns a mask up to and including the lowest set bit. */
    SmallBitset mask_to_lo() const {
        M_insist(not empty());
#ifdef __BMI__
        return SmallBitset(_blsmsk_u64(bits_)); // BMI1: get mask up to lowest set bit
#else
        return SmallBitset(bits_ ^ (bits_ - 1UL)); // get mask up to lowest set bit
#endif
    }

    /** Converts a singleton set to a mask up to -- but not including -- the single, set bit. */
    SmallBitset singleton_to_lo_mask() const {
        M_insist(singleton(), "not a singleton set");
        return SmallBitset(bits_ - 1UL);
    }

    /** Returns the union of `left` and `right`, i.e.\ `left` ∪ `right`. */
    friend SmallBitset unify(SmallBitset left, SmallBitset right) { return SmallBitset(left.bits_ | right.bits_); }
    /** Returns the intersection of `left` and `right`, i.e.\ `left` ∩ `right`. */
    friend SmallBitset intersect(SmallBitset left, SmallBitset right) { return SmallBitset(left.bits_ & right.bits_); }
    /** Returns the set where the elements of `right` have been subtracted from `left`, i.e.\ `left` - `right`. */
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

    /** Treat this `SmallBitset` as an element of the power set of *2^n* bits, where *n* is the number of bits that fit
     * into `SmallBitset`.  Then this method advances to the next set in the power set.  The behavior is undefined if
     * all bits are already set. */
    SmallBitset & operator++() { M_insist(~bits_ != 0UL, "must not have all bits set"); bits_ += 1UL; return *this; }
    /** Treat this `SmallBitset` as an element of the power set of *2^n* bits, where *n* is the number of bits that fit
     * into `SmallBitset`.  Then this method advances to the next set in the power set.  The behavior is undefined if
     * all bits are already set. */
    SmallBitset operator++(int) { SmallBitset clone(*this); operator++(); return clone; }

    /** Treat this `SmallBitset` as an element of the power set of *2^n* bits, where *n* is the number of bits that fit
     * into `SmallBitset`.  Then this method advances to the previous set in the power set.  The behavior is undefined
     * if no bits are set. */
    SmallBitset & operator--() { M_insist(bits_, "at least one bit must be set"); bits_ -= 1UL; return *this; }
    /** Treat this `SmallBitset` as an element of the power set of *2^n* bits, where *n* is the number of bits that fit
     * into `SmallBitset`.  Then this method advances to the previous set in the power set.  The behavior is undefined
     * if no bits are set. */
    SmallBitset operator--(int) { SmallBitset clone(*this); operator--(); return clone; }

    M_LCOV_EXCL_START
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
    M_LCOV_EXCL_STOP
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
    template<typename InputIt>
    dyn_array(InputIt begin, InputIt end)
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
        M_insist(pos < size(), "index out of bounds");
        return data()[pos];
    }

    /** Returns a reference to the element at position `pos`.  Requires that `pos` is in bounds. */
    value_type & operator[](std::size_t pos) {
        M_insist(pos < size(), "index out of bounds");
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

/** Implements a doubly-linked list with an overhead of just a single pointer per element. */
template<
    typename T,
    is_allocator Allocator = malloc_allocator
>
struct doubly_linked_list
{
    using value_type = T;
    using size_type = std::size_t;
    using allocator_type = Allocator;

    using reference_type = T&;
    using const_reference_type = const T&;

    struct node_type
    {
        std::uintptr_t ptrxor_;
        T value_;
    };

    private:
    ///> the memory allocator
    mutable allocator_type allocator_;

    ///> points to the first node
    node_type *head_ = nullptr;
    ///> points to the last node
    node_type *tail_ = nullptr;
    ///> the number of elements/nodes in the list
    size_type size_ = 0;

    template<bool C>
    struct the_iterator
    {
        friend struct doubly_linked_list;

        static constexpr bool Is_Const = C;

        using iterator_category = std::bidirectional_iterator_tag;
        using value_type = T;
        using difference_type = std::ptrdiff_t;
        using pointer = std::conditional_t<Is_Const, const T*, T*>;
        using reference = std::conditional_t<Is_Const, const T&, T&>;

        private:
        node_type *node_;
        std::uintptr_t prev_;

        public:
        the_iterator(node_type *node, std::uintptr_t prev) : node_(node), prev_(prev) { }

        the_iterator & operator++() {
            M_insist(node_, "cannot advance a past-the-end iterator");
            node_type *curr = node_;
            node_ = reinterpret_cast<node_type*>(prev_ ^ node_->ptrxor_);
            prev_ = reinterpret_cast<std::uintptr_t>(curr);
            return *this;
        }

        the_iterator operator++(int) { the_iterator clone = *this; operator++(); return clone; }

        the_iterator & operator--() {
            node_type *prev = reinterpret_cast<node_type*>(prev_);
            M_insist(prev, "cannot retreat past the beginning");
            prev_ = prev->ptrxor_ ^ reinterpret_cast<std::uintptr_t>(node_);
            node_ = prev;
            return *this;
        }

        the_iterator operator--(int) { the_iterator clone = *this; operator--(); return clone; }

        reference operator*() const { return node_->value_; }
        pointer operator->() const { return &node_->value_; }

        bool operator==(const the_iterator &other) const {
            return this->node_ == other.node_ and this->prev_ == other.prev_;
        }
        bool operator!=(const the_iterator &other) const { return not operator==(other); }

        operator the_iterator<true>() const { return the_iterator<true>(node_, prev_); }
    };

    public:
    using iterator = the_iterator<false>;
    using const_iterator = the_iterator<true>;

    friend void swap(doubly_linked_list &first, doubly_linked_list &second) {
        using std::swap;
        swap(first.head_,      second.head_);
        swap(first.tail_,      second.tail_);
        swap(first.size_,      second.size_);
        swap(first.allocator_, second.allocator_);
    }

    /*----- Constructors & Destructor --------------------------------------------------------------------------------*/
    doubly_linked_list() : doubly_linked_list(allocator_type()) { }

    template<is_allocator A = allocator_type>
    explicit doubly_linked_list(A &&allocator)
        : allocator_(std::forward<A>(allocator))
    { }

    template<typename InputIt>
    doubly_linked_list(InputIt begin, InputIt end, allocator_type &&allocator = allocator_type())
        : allocator_(std::forward<allocator_type>(allocator))
    {
        for (auto it = begin; it != end; ++it)
            push_back(*it);
    }

    ~doubly_linked_list() { clear(); }

    explicit doubly_linked_list(const doubly_linked_list &other) : doubly_linked_list() {
        insert(begin(), other.cbegin(), other.cend());
    }

    doubly_linked_list(doubly_linked_list &&other) : doubly_linked_list() { swap(*this, other); }

    doubly_linked_list & operator=(doubly_linked_list other) { swap(*this, other); return *this; }

    allocator_type & get_allocator() const noexcept { return allocator_; }

    /*----- Element access -------------------------------------------------------------------------------------------*/
    reference_type front() { M_insist(head_); return head_->value_; }
    const_reference_type front() const { M_insist(head_); return head_->value_; }
    reference_type back() { M_insist(tail_); return tail_->value_; }
    const_reference_type back() const { M_insist(tail_); return tail_->value_; }

    /*----- Iterators ------------------------------------------------------------------------------------------------*/
    iterator begin() { return iterator(head_, 0); }
    iterator end() { return iterator(nullptr, reinterpret_cast<std::uintptr_t>(tail_)); }
    const_iterator begin() const { return const_iterator(head_, 0); }
    const_iterator end() const { return const_iterator(nullptr, reinterpret_cast<std::uintptr_t>(tail_)); }
    const_iterator cbegin() const { return begin(); }
    const_iterator cend() const { return end(); }

    iterator rbegin() { return iterator(tail_, 0); }
    iterator rend() { return iterator(nullptr, reinterpret_cast<std::uintptr_t>(head_)); }
    const_iterator rbegin() const { return const_iterator(tail_, 0); }
    const_iterator rend() const { return const_iterator(nullptr, reinterpret_cast<std::uintptr_t>(head_)); }
    const_iterator crbegin() const { return rbegin(); }
    const_iterator crend() const { return rend(); }

    /*----- Capacity -------------------------------------------------------------------------------------------------*/
    bool empty() const { return size_ == 0; }
    size_type size() const { return size_; }
    size_type max_size() const { return std::numeric_limits<size_type>::max(); }

    /*----- Modifiers ------------------------------------------------------------------------------------------------*/
    template<typename... Args>
    iterator emplace(const_iterator pos, Args&&... args) {
        node_type *node = allocate_node();
        new (&node->value_) value_type(std::forward<Args>(args)...);
        node->ptrxor_ = pos.prev_ ^ reinterpret_cast<std::uintptr_t>(pos.node_);

        node_type *prev = reinterpret_cast<node_type*>(pos.prev_);
        if (prev)
            prev->ptrxor_ ^= reinterpret_cast<std::uintptr_t>(pos.node_) ^ reinterpret_cast<std::uintptr_t>(node);
        else // insert at front
            head_ = node;
        if (pos.node_)
            pos.node_->ptrxor_ ^= pos.prev_ ^ reinterpret_cast<std::uintptr_t>(node);
        else // insert at end
            tail_ = node;

        ++size_;
        return iterator(node, pos.prev_);
    }

    template<typename... Args>
    reference_type emplace_back(Args&&... args) {
        auto it = emplace(end(), std::forward<Args>(args)...);
        return *it;
    }

    template<typename... Args>
    reference_type emplace_front(Args&&... args) {
        auto it = emplace(begin(), std::forward<Args>(args)...);
        return *it;
    }

    void push_back(const value_type &value) { emplace_back(value); }
    void push_back(value_type &&value) { emplace_back(std::move(value)); }
    void push_front(const value_type &value) { emplace_front(value); }
    void push_front(value_type &&value) { emplace_front(std::move(value)); }

    iterator insert(const_iterator pos, const value_type &value) { return emplace(pos, value); }
    iterator insert(const_iterator pos, value_type &&value) { return emplace(pos, std::move(value)); }
    iterator insert(const_iterator pos, size_type count, const value_type &value) {
        iterator it(pos.node_, pos.prev_);
        while (count--) it = insert(it, value);
        return it;
    }

    template<typename InputIt,
             typename = decltype(*std::declval<InputIt&>(), std::declval<InputIt&>()++, void())>
    iterator insert(const_iterator pos, InputIt first, InputIt last) {
        if (first == last) return iterator(pos.node_, pos.prev_);

        iterator begin = insert(pos, *first++);
        M_insist(begin != end());
        M_insist(begin.node_);
        iterator it = begin;
        while (first != last) it = insert(++it, *first++);

        return begin;
    }

    iterator insert(const_iterator pos, std::initializer_list<value_type> ilist) {
        return insert(pos, ilist.begin(), ilist.end());
    }

    iterator erase(iterator pos) {
        M_insist(pos.node_);
        M_insist(size_);
        node_type *prev = reinterpret_cast<node_type*>(pos.prev_);
        node_type *next = reinterpret_cast<node_type*>(pos.node_->ptrxor_ ^ pos.prev_);
        if (prev)
            prev->ptrxor_ ^= reinterpret_cast<std::uintptr_t>(pos.node_) ^ reinterpret_cast<std::uintptr_t>(next);
        else // erased first node
            head_ = next;
        if (next)
            next->ptrxor_ ^= reinterpret_cast<std::uintptr_t>(pos.node_) ^ reinterpret_cast<std::uintptr_t>(prev);
        else // erased last node
            tail_ = prev;
        deallocate_node(pos.node_);
        --size_;
        return iterator(next, pos.prev_);
    }

    iterator erase(const_iterator pos) { return erase(iterator(pos.node_, pos.prev_)); }

    value_type pop_back() {
        reverse();
        value_type value = pop_front();
        reverse();
        return value;
    }

    value_type pop_front() {
        M_insist(head_);
        M_insist(tail_);
        M_insist(size_);
        value_type value = std::move(head_->value_);
        erase(begin());
        return value;
    }

    void clear() { while (head_) pop_front(); M_insist(size_ == 0); }

    /*----- Operations -----------------------------------------------------------------------------------------------*/
    void reverse() { std::swap(head_, tail_); }

    /*----- Text -----------------------------------------------------------------------------------------------------*/
M_LCOV_EXCL_START
    friend std::ostream & operator<<(std::ostream &out, const doubly_linked_list &L) {
        if (L.empty())
            return out << "+-+";

        out << "+-> ";
        for (auto it = L.cbegin(); it != L.cend(); ++it) {
            if (it != L.cbegin()) out << " <-> ";
            if constexpr (is_streamable<std::ostream&, decltype(*it)>::value)
                out << *it;
            else
                out << 'o';
        }
        return out << " <-+";
    }

    friend std::string to_string(const doubly_linked_list &L) {
        std::ostringstream oss;
        oss << L;
        return oss.str();
    }

    void dump(std::ostream &out) const { out << *this << std::endl; }
    void dump() const { dump(std::cerr); }
M_LCOV_EXCL_STOP

    private:
    node_type * allocate_node() { return allocator_.template allocate<node_type>(); }
    void deallocate_node(node_type *ptr) { allocator_.template deallocate<node_type>(ptr); }
};

template<typename It>
struct range
{
    using iterator_type = It;

    private:
    It begin_, end_;

    public:
    range() { }
    range(It begin, It end) : begin_(begin), end_(end) { }

    It begin() const { return begin_; }
    It end() const { return end_; }
};

template<typename It, typename Fn>
struct view;

template<typename It, typename ReturnType>
struct view<It, ReturnType&(It)>
{
    using iterator_type = It;
    using projection_type = std::function<ReturnType&(It)>;

    private:
    range<It> range_;
    projection_type project_;

    struct iterator
    {
        using difference_type = typename It::difference_type;
        using value_type = ReturnType;
        using pointer = value_type*;
        using reference = value_type&;
        using iterator_category = std::random_access_iterator_tag;

        private:
        It it_;
        const projection_type &project_;

        public:
        iterator(It it, const projection_type &project) : it_(it), project_(project) { }

        template<typename = decltype(std::declval<It>() == std::declval<It>())>
        bool operator==(iterator other) const { return this->it_ == other.it_; }
        template<typename = decltype(std::declval<It>() != std::declval<It>())>
        bool operator!=(iterator other) const { return this->it_ != other.it_; }

        template<typename = decltype(++std::declval<It>())>
        iterator & operator++() { ++it_; return *this; }
        template<typename = decltype(std::declval<It>()++)>
        iterator operator++(int) { return it_++; }

        template<typename = decltype(--std::declval<It>())>
        iterator & operator--() { --it_; return *this; }
        template<typename = decltype(std::declval<It>()--)>
        iterator operator--(int) { return it_--; }

        template<typename = decltype(std::declval<It>() += int())>
        iterator & operator+=(int offset) { it_ += offset; return *this; }
        template<typename = decltype(std::declval<It>() -= int())>
        iterator & operator-=(int offset) { it_ -= offset; return *this; }

        template<typename = decltype(std::declval<It>() - std::declval<It>())>
        difference_type operator-(iterator other) const { return this->it_ - other.it_; }

        template<typename = decltype(std::declval<projection_type>()(std::declval<It>()))>
        reference operator*() const { return project_(it_); }
        template<typename = decltype(std::declval<projection_type>()(std::declval<It>()))>
        reference operator->() const { return project_(it_); }
    };

    public:
    view(range<It> range, projection_type project) : range_(range), project_(project) { }
    view(It begin, It end, projection_type project) : range_(begin, end), project_(project) { }
    template<typename Fn>
    view(range<It> range, Fn &&fn) : range_(range), project_(std::forward<Fn>(fn)) { }
    template<typename Fn>
    view(It begin, It end, Fn &&fn) : range_(begin, end), project_(std::forward<Fn>(fn)) { }

    iterator begin() const { return iterator(range_.begin(), project_); }
    iterator end() const { return iterator(range_.end(), project_); }
};

// class template argument deduction guides
template<typename It, typename Fn>
view(range<It>, Fn&&) -> view<It, std::invoke_result_t<Fn&&, It>(It)>;
template<typename It, typename Fn>
view(It, It, Fn&&) -> view<It, std::invoke_result_t<Fn&&, It>(It)>;

/** A sorted list of elements.  Allows duplicates. */
template<typename T, typename Compare = std::less<T>>
struct sorted_vector
{
    using vector_type = std::vector<T>; ///< the type of the internal container of elements
    using value_type = T;
    using size_type = typename vector_type::size_type;

    private:
    Compare comp_;
    vector_type v_; ///< the internal container of elements

    public:
    sorted_vector(Compare comp = Compare()) : comp_(comp) { }

    auto begin() { return v_.begin(); }
    auto end()   { return v_.end(); }
    auto begin() const { return v_.begin(); }
    auto end()   const { return v_.end(); }
    auto cbegin() const { return v_.cbegin(); }
    auto cend()   const { return v_.cend(); }

    /** Returns `true` iff the `sorted_vector` has no elements. */
    auto empty() const { return v_.empty(); }
    /** Returns the number of elements in this `sorted_vector`. */
    auto size() const { return v_.size(); }
    /** Reserves space for `new_cap` elements in this `sorted_vector`. */
    auto reserve(size_type new_cap) { return v_.reserve(new_cap); }

    /** Returns `true` iff this `sorted_vector` contains an element that is equal to `value`. */
    bool contains(const T &value) const {
        auto pos = std::lower_bound(begin(), end(), value, comp_);
        return pos != end() and *pos == value;
    }

    /** Inserts `value` into this `sorted_vector`.  Returns an `iterator` pointing to the inserted element. */
    auto insert(T value) { return v_.insert(std::lower_bound(begin(), end(), value), value, comp_); }

    /** Inserts elements in the range from `first` (including) to `last` (excluding) into this `sorted_vector. */
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
    SmallBitset set_;
    uint64_t limit_;

    GospersHack() { }

    public:
    /** Create an instance of `GospersHack` that enumerates all subsets of size `k` of a set of `n` elements. */
    static GospersHack enumerate_all(uint64_t k, uint64_t n) {
        M_insist(k <= n, "invalid enumeration");
        M_insist(n < 64, "n exceeds range");
        GospersHack GH;
        GH.set_ = SmallBitset((1UL << k) - 1);
        GH.limit_ = 1UL << n;
        return GH;
    }
    /** Create an instance of `GospersHack` that enumerates all remaining subsets of a set of `n` elements, starting at
     * subset `set`. */
    static GospersHack enumerate_from(SmallBitset set, uint64_t n) {
        M_insist(n < 64, "n exceeds range");
        GospersHack GH;
        GH.set_ = set;
        GH.limit_ = 1UL << n;
        M_insist(uint64_t(set) <= GH.limit_, "set exceeds the limit");
        return GH;
    }

    /** Advance to the next subset. */
    GospersHack & operator++() {
        const uint64_t s(set_);
#ifdef __BMI__
        const uint64_t c = _blsi_u64(s); // BMI1: extract lowest set isolated bit -> c is a power of 2
#else
        const uint64_t c = s & -s; // extract lowest set isolated bit -> c is a power of 2
#endif
        const uint64_t r = s + c; // flip lowest block of 1-bits and following 0-bit
        const uint64_t m = r ^ s; // mask flipped bits, i.e. lowest block of 1-bits and following 0-bit
#ifdef __BMI2__
        const uint64_t l = _pext_u64(m, m); // BMI2: deposit all set bits in the low bits
#else
        const uint64_t l = (1UL << __builtin_popcount(m)) - 1UL; // deposit all set bits in the low bits
#endif
        set_ = SmallBitset((l >> 2U) | r); // instead of divide by c, rshift by log2(c)
        return *this;
    }

    /** Returns `false` iff all subsets have been enumerated. */
    operator bool() const { return uint64_t(set_) < limit_; }

    /** Returns the current subset. */
    SmallBitset operator*() const { return SmallBitset(set_); }
};

/** This class efficiently enumerates all subsets of a given size. */
struct SubsetEnumerator
{
    private:
    ///> the set to compute the power set of
    SmallBitset set_;
    ///> used to enumerate the power set of numbers 0 to n-1
    GospersHack GH_;

    public:
    SubsetEnumerator(SmallBitset set, uint64_t size)
        : set_(set)
        , GH_(GospersHack::enumerate_all(size, set.size()))
    {
        M_insist(size <= set.size());
    }

    SubsetEnumerator & operator++() { ++GH_; return *this; }
    operator bool() const { return bool(GH_); }
    SmallBitset operator*() const {
        auto gh_set = *GH_;
        return SmallBitset(_pdep_u64(uint64_t(gh_set), uint64_t(set_)));
    }
};

}

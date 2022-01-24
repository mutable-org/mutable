#pragma once

#include <mutable/util/fn.hpp>
#include <mutable/util/macro.hpp>
#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <utility>


namespace m {


/*======================================================================================================================
 * This class implements an open addressing hash map that uses reference counting for fast collision resolution.
 *====================================================================================================================*/

template<
    typename Key,
    typename Value,
    typename Hash = std::hash<Key>,
    typename KeyEqual = std::equal_to<Key>
>
struct RefCountingHashMap
{
    using key_type          = Key;
    using mapped_type       = Value;
    using value_type        = std::pair<const Key, Value>;
    using hasher            = Hash;
    using key_equal         = KeyEqual;
    using pointer           = value_type*;
    using const_pointer     = const value_type*;
    using reference         = value_type&;
    using const_reference   = const value_type&;
    using size_type         = std::size_t;
    using difference_type   = std::ptrdiff_t;

    private:
    struct entry_type
    {
        /** Counts the length of the probe sequence. */
        uint32_t probe_length = 0;
        /** The value stored in this entry. */
        value_type value;
    };

    template<bool C>
    struct the_iterator
    {
        friend struct RefCountingHashMap;

        static constexpr bool Is_Const = C;

        using map_type = std::conditional_t<Is_Const, const RefCountingHashMap, RefCountingHashMap>;
        using bucket_type = std::conditional_t<Is_Const, const typename map_type::entry_type, typename map_type::entry_type>;
        using pointer = std::conditional_t<Is_Const, const typename map_type::const_pointer, typename map_type::pointer>;
        using reference = std::conditional_t<Is_Const, const typename map_type::const_reference, typename map_type::reference>;

        private:
        map_type &map_;
        bucket_type *bucket_ = nullptr;

        public:
        the_iterator(map_type &map, entry_type *bucket) : map_(map), bucket_(bucket) { }

        the_iterator & operator++() {
            do {
                ++bucket_;
            } while (bucket_ != map_.table_ + map_.capacity_ and bucket_->probe_length == 0);
            return *this;
        }

        the_iterator operator++(int) {
            the_iterator tmp = *this;
            operator++();
            return tmp;
        }

        reference operator*() const { return bucket_->value; }
        pointer operator->() const { return &bucket_->value; }

        bool operator==(the_iterator other) const { return this->bucket_ == other.bucket_; }
        bool operator!=(the_iterator other) const { return not operator==(other); }
    };

    template<bool C>
    struct the_bucket_iterator
    {
        friend struct RefCountingHashMap;

        static constexpr bool Is_Const = C;

        using map_type = std::conditional_t<Is_Const, const RefCountingHashMap, RefCountingHashMap>;
        using bucket_type = std::conditional_t<Is_Const, const typename map_type::entry_type, typename map_type::entry_type>;
        using pointer = std::conditional_t<Is_Const, const typename map_type::const_pointer, typename map_type::pointer>;
        using reference = std::conditional_t<Is_Const, const typename map_type::const_reference, typename map_type::reference>;
        using size_type = typename map_type::size_type;

        private:
        map_type &map_;
        size_type bucket_index_;
        size_type step_ = 0;
        size_type max_step_;

        public:
        the_bucket_iterator(map_type &map, size_type bucket_index)
            : map_(map)
            , bucket_index_(bucket_index)
        {
            max_step_ = bucket()->probe_length;
        }

        bool has_next() const { return step_ < max_step_; }

        the_bucket_iterator & operator++() {
            ++step_;
            bucket_index_ = map_.masked(bucket_index_ + step_);
            return *this;
        }

        the_bucket_iterator & operator++(int) {
            the_bucket_iterator tmp = *this;
            operator++();
            return tmp;
        }

        reference operator*() const { return map_.table_[bucket_index_].value; }
        pointer operator->() const { return &map_.table_[bucket_index_].value; }

        size_type probe_length() const { return step_; }
        size_type probe_distance() const { return (step_ * (step_ + 1)) / 2; }
        size_type current_index() const { return bucket_index_; }
        size_type bucket_index() const { return map_.masked(current_index() - probe_distance()); }

        private:
        bucket_type * bucket() const { return map_.table_ + current_index(); }
    };

    public:
    using iterator = the_iterator<false>;
    using const_iterator = the_iterator<true>;

    using bucket_iterator = the_bucket_iterator<false>;
    using const_bucket_iterator = the_bucket_iterator<true>;

    private:
    const hasher h_;
    const key_equal eq_;

    /** A pointer to the beginning of the table. */
    entry_type *table_ = nullptr;
    /** The total number of entries allocated in the table. */
    size_type capacity_ = 0;
    /** The number of occupied entries in the table. */
    size_type size_ = 0;
    /** The maximum size before resizing. */
    size_type watermark_high_;
    /** The maximum load factor before resizing. */
    float max_load_factor_ = .85;

    public:
    RefCountingHashMap(size_type bucket_count,
                       const hasher &hash = hasher(),
                       const key_equal &equal = key_equal())
        : h_(hash)
        , eq_(equal)
        , capacity_(ceil_to_pow_2(bucket_count))
    {
        if (bucket_count == 0)
            throw std::invalid_argument("bucket_count must not be zero");

        /* Allocate and initialize table. */
        table_ = allocate(capacity_);
        initialize();

        /* Compute high watermark. */
        watermark_high_ = capacity_ * max_load_factor_;
    }

    ~RefCountingHashMap() {
        for (auto p = table_, end = table_ + capacity_; p != end; ++p) {
            if (p->probe_length != 0)
                p->~entry_type();
        }
        free(table_);
    }

    size_type capacity() const { return capacity_; }
    size_type size() const { return size_; }
    size_type mask() const { return capacity_ - size_type(1); }
    size_type masked(size_type index) const { return index & mask(); }
    float max_load_factor() const { return max_load_factor_; }
    void max_load_factor(float ml) {
        max_load_factor_ = std::clamp(ml, .0f, .99f);
        watermark_high_ = max_load_factor_ * capacity_;
    }
    size_type watermark_high() const { return watermark_high_; }

    iterator begin() { return iterator(*this, table_); }
    iterator end()   { return iterator(*this, table_ + capacity()); }
    const_iterator begin() const { return const_iterator(*this, table_); }
    const_iterator end()   const { return const_iterator(*this, table_ + capacity()); }
    const_iterator cbegin() const { return begin(); }
    const_iterator cend()   const { return end(); }

    iterator insert_with_duplicates(key_type key, mapped_type value) {
        if (size_ >= watermark_high_)
            resize(2 * capacity_);

        const auto hash = h_(key);
        const size_type index = masked(hash);
        entry_type * const bucket = table_ + index;

        if (bucket->probe_length == 0) [[likely]] { // bucket is free
            ++bucket->probe_length;
            new (&bucket->value) value_type(std::move(key), std::move(value));
            ++size_;
            return iterator(*this, bucket);
        }

        /* Compute distance to end of probe sequence. */
        size_type distance = (bucket->probe_length * bucket->probe_length + bucket->probe_length) >> 1;
        M_insist(distance > 0, "the distance must not be 0, otherwise we would have run into the likely case above");

        /* Search next free slot in bucket's probe sequence. */
        entry_type *probe = table_ + masked(index + distance);
        M_insist(probe != bucket, "the probed slot must not be the original bucket as the distance is not 0 and always "
                                 "less than capacity");
        while (probe->probe_length != 0) {
            ++bucket->probe_length;
            distance += bucket->probe_length;
            probe = table_ + masked(index + distance);
        }

        /* Found free slot in bucket's probe sequence.  Place element in slot and update probe length. */
        ++probe->probe_length; // set probe_length from 0 to 1
        ++bucket->probe_length;
        new (&probe->value) value_type(std::move(key), std::move(value));
        ++size_;
        return iterator(*this, probe);
    }

    std::pair<iterator, bool> insert_without_duplicates(key_type key, mapped_type value) {
        if (size_ >= watermark_high_)
            resize(2 * capacity_);

        const auto hash = h_(key);
        const size_type index = masked(hash);
        entry_type * const bucket = table_ + index;

        entry_type *probe = bucket;
        size_type insertion_probe_length = 0;
        size_type insertion_probe_distance = 0;
        while (probe->probe_length != 0) {
            if (eq_(key, probe->value.first))
                return std::make_pair(iterator(*this, probe), false); // duplicate key
            ++insertion_probe_length;
            insertion_probe_distance += insertion_probe_length;
            probe = table_ + masked(index + insertion_probe_distance);
        }

        ++probe->probe_length; // set probe_length from 0 to 1
        bucket->probe_length = insertion_probe_length + 1;
        new (&probe->value) value_type(std::move(key), std::move(value));
        ++size_;
        return std::make_pair(iterator(*this, probe), true);
    }

    iterator find(const key_type &key) {
        const auto hash = h_(key);
        const size_type index = masked(hash);
        entry_type * const bucket = table_ + index;

#if 1
        /* Search the probe sequence in natural order. */
        entry_type *probe = bucket;
        size_type lookup_probe_length = 0;
        size_type lookup_probe_distance = 0;
        while (probe->probe_length != 0 and lookup_probe_length < bucket->probe_length) {
            if (eq_(key, probe->value.first))
                return iterator(*this, probe);
            ++lookup_probe_length;
            lookup_probe_distance += lookup_probe_length;
            probe = table_ + masked(index + lookup_probe_distance);
        }
#else
        /* Search the probe sequence in inversed order, starting at the last element of this bucket. */
        size_type lookup_probe_length = bucket->probe_length;
        size_type lookup_probe_distance = (lookup_probe_length * (lookup_probe_length - 1)) >> 1;
        while (lookup_probe_length != 0) {
            entry_type *probe = table_ + masked(index + lookup_probe_distance);
            if (eq_(key, probe->value.first))
                return iterator(*this, probe);
            --lookup_probe_length;
            lookup_probe_distance -= lookup_probe_length;
        }
#endif
        return end();
    }
    const_iterator find(const key_type &key) const {
        return const_iterator(*this, const_cast<RefCountingHashMap*>(this)->find(key).bucket_);
    }

    bucket_iterator bucket(const key_type &key) {
        const auto hash = h_(key);
        const size_type index = masked(hash);
        return bucket_iterator(*this, index);
    }
    const_bucket_iterator bucket(const key_type &key) const {
        return const_bucket_iterator(*this, const_cast<RefCountingHashMap*>(this)->bucket(key).bucket_index_);
    }

    void for_all(const key_type &key, std::function<void(value_type&)> callback) {
        for (auto it = bucket(key); it.has_next(); ++it) {
            if (eq_(key, it->first))
                callback(*it);
        }
    }
    void for_all(const key_type &key, std::function<void(const value_type&)> callback) const {
        for (auto it = bucket(key); it.has_next(); ++it) {
            if (eq_(key, it->first))
                callback(*it);
        }
    }

    size_type count(const key_type &key) const {
        size_type cnt = 0;
        for_all(key, [&cnt](auto) { ++cnt; });
        return cnt;
    }

    /** Rehash all elements. */
    private:
    void rehash(std::size_t new_capacity) {
        M_insist((new_capacity & (new_capacity - 1)) == 0, "not a power of 2");
        M_insist(size_ <= watermark_high_, "there are more elements to rehash than the high watermark allows");

        auto old_table = table_;
        table_ = allocate(new_capacity);
        auto old_capacity = capacity_;
        capacity_ = new_capacity;
        size_ = 0;
        initialize();

        for (auto runner = old_table, end = old_table + old_capacity; runner != end; ++runner) {
            if (runner->probe_length) {
                auto &key_ref = const_cast<key_type&>(runner->value.first); // hack around the `const key_type`
                insert_with_duplicates(std::move(key_ref), std::move(runner->value.second));
            }
        }

        free(old_table);
    }

    public:
    void rehash() { rehash(capacity_); }

    void resize(std::size_t new_capacity) {
        new_capacity = std::max<decltype(new_capacity)>(new_capacity, std::ceil(size() / max_load_factor()));
        new_capacity = ceil_to_pow_2(new_capacity);

        if (new_capacity != capacity_) {
            watermark_high_ = new_capacity * max_load_factor();
            M_insist(watermark_high() >= size());
            rehash(new_capacity);
        }
    }

    void shrink_to_fit() { resize(size()); }

M_LCOV_EXCL_START
    friend std::ostream & operator<<(std::ostream &out, const RefCountingHashMap &map) {
        size_type log2 = log2_ceil(map.capacity());
        size_type log10 = size_type(std::ceil(double(log2) / 3.322));
        for (size_type i = 0; i != map.capacity_; ++i) {
            auto &entry = map.table_[i];
            out << '[' << std::setw(log10) << i << "]: probe length = " << std::setw(log10) << entry.probe_length;
            if (entry.probe_length) {
                out << ", value = (" << entry.value.first << ", " << entry.value.second << ')';
            }
            out << '\n';
        }
        return out;
    }

    void dump(std::ostream &out) const { out << *this; out.flush(); }
    void dump() const { dump(std::cerr); }
M_LCOV_EXCL_STOP

    private:
    static entry_type * allocate(size_type n, entry_type *hint = nullptr) {
        auto p = static_cast<entry_type*>(realloc(hint, n * sizeof(entry_type)));
        if (p == nullptr)
            throw std::runtime_error("allocation failed");
        return p;
    }

    void initialize() {
        for (auto runner = table_, end = table_ + capacity_; runner != end; ++runner)
            new (runner) entry_type();
    }
};

}

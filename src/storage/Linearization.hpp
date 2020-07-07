#pragma once

#include "util/macro.hpp"
#include <cstdint>
#include <memory>
#include <type_traits>


namespace db {

struct Attribute;

/** A `Linearization` describes a mapping from a logical schema to a physical data layout. */
struct Linearization
{
    friend void swap(Linearization &first, Linearization &second) {
        using std::swap;

        swap(first.num_tuples_, second.num_tuples_);
        swap(first.size_,       second.size_ );
        swap(first.capacity_,   second.capacity_);
        swap(first.offsets_,    second.offsets_);
        swap(first.strides_,    second.strides_);
        swap(first.sequences_,  second.sequences_);
    }

    private:
    template<bool C>
    struct the_iterator
    {
        static constexpr bool Is_Const = C;
        using reference_type = std::conditional_t<Is_Const, const Linearization&, Linearization&>;

        struct entry
        {
            uint32_t offset;
            uint32_t stride;
            uintptr_t seq;

            entry(uint32_t offset, uint32_t stride, uintptr_t seq)
                : offset(offset), stride(stride), seq(seq)
            { }

            /** Returns `true` iff the sequence is a sequence of `Attribute` values. */
            bool is_attribute() const { return seq & 0x1UL; }

            /** Returns `true` iff the sequence is a sequence of `Linearization`s. */
            bool is_linearization() const { return not is_attribute(); }

            const Attribute & as_attribute() const {
                insist(is_attribute(), "not a pointer to Attribute");
                return *reinterpret_cast<Attribute*>(seq & ~0x1UL);
            }

            Linearization & as_linearization() const {
                insist(is_linearization(), "not a pointer to Linearization");
                return *reinterpret_cast<Linearization*>(seq);
            }
        };

        private:
        reference_type lin_;
        std::size_t idx_;

        public:
        the_iterator(reference_type lin, std::size_t initial_index = 0)
            : lin_(lin)
            , idx_(initial_index)
        { }

        the_iterator & operator++() { ++idx_; return *this; }
        the_iterator operator++(int) { auto old = *this; ++idx_; return old; }

        bool operator==(the_iterator other) const { return this->idx_ == other.idx_; }
        bool operator!=(the_iterator other) const { return not operator==(other); }

        entry operator*() const { return entry(lin_.offsets_[idx_], lin_.strides_[idx_], lin_.sequences_[idx_]); }
    };
    public:
    using iterator = the_iterator<false>;
    using const_iterator = the_iterator<true>;

    private:
    std::size_t num_tuples_; ///< number of tuples, 0 means infinite
    std::size_t size_ = 0; ///< number of sequences
    std::size_t capacity_; ///< maximal number of sequences this linearization can handle
    uint32_t *offsets_ = nullptr; ///< offsets of the sequences, specified in bits
    uint32_t *strides_ = nullptr; ///< strides of the sequences, specified in bits
    uintptr_t *sequences_ = nullptr; ///< array of nested `Linearization`s or `Attribute`s, LSB is 1 iff an `Attribute` is stored

    public:
    static Linearization CreateInfiniteSequence(std::size_t num_sequences) {
        return Linearization(num_sequences, 0);
    }
    static Linearization CreateFiniteSequence(std::size_t num_sequences, std::size_t num_tuples) {
        insist(num_tuples != 0, "Finite sequences have to contain at least 1 tuple.");
        return Linearization(num_sequences, num_tuples);
    }

    private:
    /** Creates a `Linearization` which can handle at most `num_sequences` sequences. */
    Linearization(std::size_t num_sequences, std::size_t num_tuples)
        : num_tuples_(num_tuples)
        , capacity_(num_sequences)
        , offsets_(new uint32_t[num_sequences])
        , strides_(new uint32_t[num_sequences])
        , sequences_(new uintptr_t[num_sequences]())
    { }

    public:
    Linearization() = default;
    Linearization(const Linearization&) = delete;
    Linearization(Linearization &&other) { swap(*this, other); }

    ~Linearization() {
        delete[] offsets_;
        delete[] strides_;
        for (std::size_t idx = 0; idx != size_; ++idx) {
            if (is_linearization(idx))
                delete reinterpret_cast<Linearization*>(sequences_[idx]);
        }
        delete[] sequences_;
    }

    Linearization & operator=(Linearization &other) { swap(*this, other); return *this; }

    std::size_t num_tuples() const { return num_tuples_; }
    std::size_t num_sequences() const { return size_; }

    /** Returns `true` iff the sequence at index `idx` is a sequence of `Attribute` values. */
    bool is_attribute(std::size_t idx) const { return sequences_[idx] & 0x1UL; }

    /** Returns `true` iff the sequence at index `idx` is a sequence of `Linearization`s. */
    bool is_linearization(std::size_t idx) const { return not is_attribute(idx); }

    iterator begin() { return iterator(*this, 0); }
    iterator end()   { return iterator(*this, size_); }
    const_iterator begin() const { return const_iterator(*this, 0); }
    const_iterator end()   const { return const_iterator(*this, size_); }
    const_iterator cbegin() const { return begin(); }
    const_iterator cend()   const { return end(); }

    void add_sequence(uint32_t offset, uint32_t stride, std::unique_ptr<Linearization> lin) {
        insist(size_ < capacity_, "maximum capacity reached");
        auto idx = size_++;
        offsets_[idx] = offset;
        strides_[idx] = stride;
        sequences_[idx] = reinterpret_cast<uintptr_t>(lin.release());
    }

    void add_sequence(uint32_t offset, uint32_t stride, const Attribute &attr) {
        insist(size_ < capacity_, "maximum capacity reached");
        auto idx = size_++;
        offsets_[idx] = offset;
        strides_[idx] = stride;
        sequences_[idx] = reinterpret_cast<uintptr_t>(&attr) | 0x1UL;
    }

    friend std::ostream & operator<<(std::ostream &out, const Linearization &lin) {
        lin.print(out);
        return out;
    };

    void dump() const;
    void dump(std::ostream &out) const;

    private:
    void print(std::ostream &out, unsigned indentation = 0) const;
    std::ostream & indent(std::ostream &out, unsigned indentation) const;
};

}

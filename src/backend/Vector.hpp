#pragma once

#include "catalog/Type.hpp"
#include "IR/Operator.hpp"
#include <cstdint>
#include <cstdlib>
#include <type_traits>


namespace m {

/*======================================================================================================================
 * Vector
 *====================================================================================================================*/

template<std::size_t N>
struct Vector
{
    static constexpr std::size_t CAPACITY = N;

    const Table &schema;
    private:
    uint8_t *null_bitmap_;
    void **columns_;
    std::size_t num_columns_;
    uint64_t mask_ = 0x0;
    static_assert(N <= sizeof(mask_) * 8, "maximum vector size exceeded");

    template<bool C>
    struct the_iterator
    {
        static constexpr bool IsConst = C;

        private:
        std::conditional_t<IsConst, const Vector<N>, Vector<N>> *vec_;
        uint64_t mask_;

        public:
        the_iterator(Vector<N> *vec, uint64_t mask) : vec_(vec), mask_(mask) { }

        bool operator==(the_iterator other) const { return this->mask_ == other.mask_; }
        bool operator!=(the_iterator other) const { return not operator==(other); }

        the_iterator & operator++() { mask_ = mask_ & (mask_ - 1); /* reset lowest set bit */ return *this; }
        the_iterator operator++(int) { auto clone = *this; ++clone; return clone; }

        std::size_t index() const { M_insist(mask_ != 0); return __builtin_ctzl(mask_); }

        /** Returns true iff the attribute with id `attr_id` is NULL. */
        bool is_null(std::size_t attr_id) const;

        /** Sets the attribute with id `attr_id` to NULL. */
        void set_null(std::size_t attr_id, bool isnull);

        auto get_i8 (std::size_t attr_id) const { return get<int8_t>(attr_id); }
        auto get_i16(std::size_t attr_id) const { return get<int16_t>(attr_id); }
        auto get_i32(std::size_t attr_id) const { return get<int32_t>(attr_id); }
        auto get_i64(std::size_t attr_id) const { return get<int64_t>(attr_id); }
        auto get_f  (std::size_t attr_id) const { return get<float>(attr_id); }
        auto get_d  (std::size_t attr_id) const { return get<double>(attr_id); }
        auto get_b  (std::size_t attr_id) const { return get<bool>(attr_id); }
        auto get_s  (std::size_t attr_id) const { return get<std::string>(attr_id); }

        void set_i8 (std::size_t attr_id, int8_t value)  { return set<int8_t>(attr_id, value); }
        void set_i16(std::size_t attr_id, int16_t value) { return set<int16_t>(attr_id, value); }
        void set_i32(std::size_t attr_id, int32_t value) { return set<int32_t>(attr_id, value); }
        void set_i64(std::size_t attr_id, int64_t value) { return set<int64_t>(attr_id, value); }
        void set_f  (std::size_t attr_id, float value)   { return set<float>(attr_id, value); }
        void set_d  (std::size_t attr_id, double value)  { return set<double>(attr_id, value); }
        void set_b  (std::size_t attr_id, bool value)    { return set<bool>(attr_id, value); }
        void set_s  (std::size_t attr_id, const std::string &str) { return set(attr_id, str); }

        private:
        template<typename T> T get(std::size_t attr_id) const;
        template<typename T> void set(std::size_t attr_id, T value);

        /*----- Template specilizations ------------------------------------------------------------------------------*/

        template<>
        bool get<bool>(std::size_t attr_id) const {
            M_insist(vec_->alive(index()));
            M_insist(attr_id < vec_->schema.size(), "invalid attribute id");
            M_insist(not is_null(attr_id), "value is NULL");
            auto val_col = vec_->columns_[attr_id];
            const auto idx = index();
            const std::size_t bytes = idx / 8;
            const std::size_t bits = idx % 8;
            return (reinterpret_cast<uint8_t*>(val_col)[bytes] >> bits) & 0x1;
        }

        template<>
        std::string get<std::string>(std::size_t attr_id) const {
            M_insist(vec_->alive(index()));
            M_insist(attr_id < vec_->schema.size(), "invalid attribute id");
            M_insist(not is_null(attr_id), "value is NULL");
            auto val_col = vec_->columns_[attr_id];
            auto ty = vec_->schema[attr_id].type;
            auto len = as<const CharacterSequence>(ty)->length;
            auto start = reinterpret_cast<char*>(val_col) + index() * len;
            len = strnlen(start, len);
            return std::string(start, start + len);
        }

        template<>
        void set<bool>(std::size_t attr_id, bool value) {
            M_insist(attr_id < vec_->schema.size(), "invalid attribute id");
            set_null(attr_id, false);
            auto val_col = vec_->columns_[attr_id];
            const auto idx = index();
            const std::size_t bytes = idx / 8;
            const std::size_t bits = idx % 8;
            setbit(reinterpret_cast<uint8_t*>(val_col) + bytes, value, bits);
        }

        void set(std::size_t attr_id, const std::string &str) {
            M_insist(attr_id < vec_->schema.size(), "invalid attribute id");
            set_null(attr_id, false);
            auto val_col = vec_->columns_[attr_id];
            auto ty = vec_->schema[attr_id].type;
            auto len = as<const CharacterSequence>(ty)->length;
            auto start = reinterpret_cast<char*>(val_col) + index() * len;
            strncpy(start, str.c_str(), len);
        }
    };

    public:
    using iterator = the_iterator<false>;
    using const_iterator = the_iterator<true>;

    Vector(const Table &schema);
    Vector(const Vector&) = delete;
    Vector(Vector&&) = delete;
    ~Vector();

    iterator begin() { return iterator(this, mask_); }
    iterator end()   { return iterator(this, 0); }
    const_iterator begin() const { return iterator(this, mask_); }
    const_iterator end()   const { return iterator(this, 0); }
    const_iterator cbegin() const { return begin(); }
    const_iterator cend()   const { return end(); }

    /** Returns the capacity of the vector. */
    static constexpr std::size_t capacity() { return CAPACITY; }
    /** Returns the current size of the vector, i.e. the number of alive tuples. */
    std::size_t size() const { return __builtin_popcountl(mask_); }
    /** Returns the index of the next dead tuple. */
    std::size_t last() const { return mask_ == 0 ? 0 : (8 * sizeof(mask_)) - __builtin_clzl(mask_); }

    /** Returns an iterator, starting at the given `index`.  The iterator starts *exactly* at `index`, no matter whether
     * the value is alive or dead. */
    iterator at(std::size_t index) {
        M_insist(index < capacity());
        const uint64_t index_bit = 1UL << index;
        return iterator(this, (mask_ & ~(index_bit - 1UL)) | index_bit);
    }

    /** Return a pointer to the start of the NULL bitmap. */
    uint8_t * null_bitmap() { return null_bitmap_; }
    /** Return a pointer to the start of the NULL bitmap. */
    const uint8_t * null_bitmap() const { return null_bitmap_; }

    /** Check whether the tuple at the given `index` is alive. */
    bool alive(std::size_t index) const {
        M_insist(index < capacity());
        return mask_ & (1UL << index);
    }

    /** Returns true iff all tuples are dead. */
    bool empty() const { return mask() == 0; }

    /** Get the mask of the vector. */
    uint64_t mask() const { return mask_; }
    /** Set the mask of the vector. */
    void mask(uint64_t new_mask) { mask_ = new_mask; }

    private:
    static constexpr uint64_t AllOnes() { return -1LU >> (8 * sizeof(mask_) - capacity()); }

    public:
    /** Make all tuples in the vector alive. */
    void fill() { mask_ = AllOnes(); M_insist(size() == capacity()); }

    /** Erase a tuple at the given `index` from the vector. */
    void erase(std::size_t index) {
        M_insist(index < capacity(), "index out of bounds");
        setbit(&mask_, false, index);
    }

    /** Clears the vector, i.e. makes all tuples dead. */
    void clear() { mask_ = 0; }

M_EXCLUDE_FROM_COVERAGE_BEGIN
    friend std::ostream & operator<<(std::ostream &out, const Vector &vec) {
        out << "Vector<" << N << "> with " << vec.size() << " rows:\n";
        for (std::size_t i = 0; i != vec.capacity(); ++i) {
            out << "    " << std::setw(2) << i << ": ";
            if (vec.alive(i))
                out << "[alive]";
            else
                out << "[dead]";
            out << '\n';
        }
        return out;
    }

    void dump(std::ostream &out) const { out << *this; out.flush(); }
    void dump() const { dump(std::cerr); }
M_EXCLUDE_FROM_COVERAGE_END
};

template<std::size_t N>
Vector<N>::Vector(const Table &schema)
    : schema(schema)
    , num_columns_(schema.size())
{
    columns_ = new void*[num_columns_];
    null_bitmap_ = new uint8_t[(N * schema.size() + 7) / 8]; // ceil to multiple of 8
    for (auto &attr : schema) {
        if (attr.type->is_none()) { /* this column has no type, hence it must be NULL */
            for (std::size_t i = 0; i != capacity(); ++i)
                at(i).set_null(attr.id, true);
            columns_[attr.id] = nullptr;
        } else {
            auto elem_size = attr.type->size(); // size in bits
            columns_[attr.id] = malloc((N * elem_size + 7) / 8); // ceil to multiple of 8
        }
    }
}

template<std::size_t N>
Vector<N>::~Vector()
{
    for (auto col = columns_, end = columns_ + num_columns_; col != end; ++col)
        free(*col);
    delete[] columns_;
    delete[] null_bitmap_;
}

template<std::size_t N>
template<bool C>
bool Vector<N>::the_iterator<C>::is_null(std::size_t attr_id) const
{
    M_insist(attr_id < vec_->schema.size(), "invalid attribute id");
    auto bitmap_col = vec_->null_bitmap();
    const std::size_t bits_per_tuple = vec_->schema.size(); // one bit per attribute
    const std::size_t bit_offset = bits_per_tuple * index() + attr_id;
    const std::size_t bytes = bit_offset / 8;
    const std::size_t bits = bit_offset % 8;
    return not ((bitmap_col[bytes] >> bits) & 0x1);
}

template<std::size_t N>
template<bool C>
void Vector<N>::the_iterator<C>::set_null(std::size_t attr_id, bool isnull)
{
    M_insist(attr_id < vec_->schema.size(), "invalid attribute id");
    auto bitmap_col = vec_->null_bitmap();
    const std::size_t bits_per_tuple = vec_->schema.size(); // one bit per attribute
    const std::size_t bit_offset = bits_per_tuple * index() + attr_id;
    const std::size_t bytes = bit_offset / 8;
    const std::size_t bits = bit_offset % 8;
    setbit(bitmap_col + bytes, not isnull, bits);
}

template<std::size_t N>
template<bool C>
template<typename T>
T Vector<N>::the_iterator<C>::get(std::size_t attr_id) const
{
    M_insist(vec_->alive(index()));
    M_insist(attr_id < vec_->schema.size(), "invalid attribute id");
    M_insist(not is_null(attr_id), "value is NULL");
    auto val_col = vec_->columns_[attr_id];
    return reinterpret_cast<T*>(val_col)[index()];
}

template<std::size_t N>
template<bool C>
template<typename T>
void Vector<N>::the_iterator<C>::set(std::size_t attr_id, T value) {
    M_insist(attr_id < vec_->schema.size(), "invalid attribute id");
    set_null(attr_id, false);
    auto val_col = vec_->columns_[attr_id];
    reinterpret_cast<T*>(val_col)[index()] = value;
}

}

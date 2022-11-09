#pragma once

#include <mutable/catalog/Schema.hpp>
#include <mutable/storage/Store.hpp>
#include <mutable/util/memory.hpp>


namespace m {

/** This class implements a row store. */
struct RowStore : Store
{
#ifndef NDEBUG
    static constexpr std::size_t ALLOCATION_SIZE = 1UL << 30; ///< 1 GiB
#else
    static constexpr std::size_t ALLOCATION_SIZE = 1UL << 37; ///< 128 GiB
#endif

    private:
    memory::LinearAllocator allocator_; ///< the memory allocator
    memory::Memory data_; ///< the underlying memory containing the data
    std::size_t num_rows_ = 0; ///< the number of rows in use
    std::size_t capacity_; ///< the number of available rows
    uint32_t *offsets_; ///< the offsets from the first column, in bits, of all columns
    uint32_t row_size_; ///< the size of a row, in bits; includes NULL bitmap and other meta data

    public:
    RowStore(const Table &table);
    ~RowStore();

    virtual std::size_t num_rows() const override { return num_rows_; }

    int offset(uint32_t idx) const {
        M_insist(idx <= table().num_attrs(), "index out of range");
        return offsets_[idx];
    }
    int offset(const Attribute &attr) const { return offset(attr.id); }

    /** Returns the effective size of a row, in bits. */
    std::size_t row_size() const { return row_size_; }

    void append() override {
        if (num_rows_ == capacity_)
            throw std::logic_error("row store exceeds capacity");
        ++num_rows_;
    }

    void drop() override {
        M_insist(num_rows_);
        --num_rows_;
    }

    /** Returns the memory of the store. */
    const memory::Memory & memory() const override { return data_; }
    /** Sets the memory of the store to `memory`. */
    void memory(memory::Memory memory) { data_ = std::move(memory); }

    void dump(std::ostream &out) const override;
    using Store::dump;

    private:
    /** Computes the offsets of the attributes within a row.  Tries to minimize the row size by storing the attributes
     * in descending order of their size, avoiding padding.  */
    void compute_offsets();

    /** Return a pointer to the `idx`th row. */
    uintptr_t at(std::size_t idx) const { return data_.as<uintptr_t>() + row_size_/8 * idx; }
};

}

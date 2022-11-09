#pragma once

#include <mutable/catalog/Schema.hpp>
#include <mutable/storage/Store.hpp>
#include <mutable/util/memory.hpp>


namespace m {

/** This class implements a generic PAX store. */
struct PaxStore : Store
{
#ifndef NDEBUG
    static constexpr std::size_t ALLOCATION_SIZE = 1UL << 30; ///< 1 GiB
#else
    static constexpr std::size_t ALLOCATION_SIZE = 1UL << 37; ///< 128 GiB
#endif

    static constexpr uint32_t BLOCK_SIZE = 1UL << 12; ///< 4 KiB

    private:
    memory::LinearAllocator allocator_; ///< the memory allocator
    memory::Memory data_; ///< the underlying memory containing the data
    std::size_t num_rows_ = 0; ///< the number of rows in use
    std::size_t capacity_; ///< the number of available rows
    uint32_t *offsets_; ///< the offsets of each column within a PAX block, in bits
    uint32_t block_size_; ///< the size of a PAX block, in bytes; includes padding
    std::size_t num_rows_per_block_; ///< the number of rows within a PAX block

    public:
    PaxStore(const Table &table, uint32_t block_size_in_bytes = BLOCK_SIZE);
    ~PaxStore();

    virtual std::size_t num_rows() const override { return num_rows_; }
    std::size_t num_rows_per_block() const { return num_rows_per_block_; }
    uint32_t block_size() const { return block_size_; }

    uint32_t offset(uint32_t idx) const {
        M_insist(idx <= table().num_attrs(), "index out of range");
        return offsets_[idx];
    }
    uint32_t offset(const Attribute &attr) const { return offset(attr.id); }

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

    void dump(std::ostream &out) const override;
    using Store::dump;

    private:
    /** Computes the offsets of the columns within a PAX block, the rows size, the number of rows that fit in a PAX
     * block, and the capacity.  Tries to maximize the number of rows within a PAX block by storing the attributes in
     * descending order of their size, avoiding padding.  */
    void compute_block_offsets();
};

}

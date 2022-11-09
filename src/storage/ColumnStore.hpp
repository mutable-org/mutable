#pragma once

#include <mutable/catalog/Schema.hpp>
#include <mutable/storage/Store.hpp>
#include <mutable/util/memory.hpp>


namespace m {

/** This class implements a column store. */
struct ColumnStore : Store
{
#ifndef NDEBUG
    static constexpr std::size_t ALLOCATION_SIZE = 1UL << 30; ///< 1 GiB
#else
    static constexpr std::size_t ALLOCATION_SIZE = 1UL << 37; ///< 128 GiB
#endif

    private:
    memory::LinearAllocator allocator_; ///< the memory allocator
    memory::Memory data_;
    std::size_t num_rows_ = 0;
    std::size_t capacity_;
    std::size_t row_size_ = 0;

    public:
    ColumnStore(const Table &table);
    ~ColumnStore();

    virtual std::size_t num_rows() const override { return num_rows_; }

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
    /** Returns the memory address where the column assigned to the attribute with id `attr_id` starts.
     * Return the address of the NULL bitmap column if `attr_id == table().size()`. */
    void * memory(std::size_t attr_id) const {
        M_insist(attr_id <= table().num_attrs());
        auto offset = ALLOCATION_SIZE * attr_id;
        return reinterpret_cast<uint8_t*>(data_.addr()) + offset;
    }

    void dump(std::ostream &out) const override;
    using Store::dump;
};

}

#include "storage/PaxStore.hpp"

#include <mutable/storage/Linearization.hpp>
#include <algorithm>
#include <exception>
#include <fstream>
#include <iomanip>
#include <numeric>


using namespace m;


PaxStore::PaxStore(const Table &table, uint32_t block_size_in_bytes)
        : Store(table)
        , block_size_(block_size_in_bytes)
        , offsets_(new uint32_t[table.size() + 1]) // add one slot for the offset of the meta data
{
    compute_block_offsets();

    data_ = allocator_.allocate(ALLOCATION_SIZE);

    /* Initialize linearization. */
    auto lin = std::make_unique<Linearization>(Linearization::CreateInfinite(1));
    auto block = std::make_unique<Linearization>(Linearization::CreateFinite(table.size() + 1, num_rows_per_block_));
    for (auto &attr : table)
        block->add_sequence(offset(attr), attr.type->size(), attr);
    block->add_null_bitmap(offset(table.size()), table.size());
    lin->add_sequence(uintptr_t(memory().addr()), block_size_, std::move(block));
    linearization(std::move(lin));
}

PaxStore::~PaxStore()
{
    delete[] offsets_;
}

void PaxStore::compute_block_offsets()
{
    using std::max;

    const auto num_attrs = table().size();
    const Attribute **attrs = new const Attribute*[num_attrs];

    for (uint32_t pos = 0; pos != num_attrs; ++pos)
        attrs[pos] = &table()[pos];

    /* Sort attributes by their alignment requirement in descending order. */
    std::stable_sort(attrs, attrs + num_attrs, [](const Attribute *first, const Attribute *second) {
        return first->type->alignment() > second->type->alignment();
    });

    /* Compute offsets (for a single row). */
    uint32_t off = 0;
    uint32_t alignment = 8;
    std::size_t num_not_byte_aligned = 0; // number of attribute columns which are not necessarily byte-aligned
    for (uint32_t pos = 0; pos != num_attrs; ++pos) {
        const Attribute &attr = *attrs[pos];
        offsets_[attr.id] = off;
        off += attr.type->size();
        alignment = max(alignment, attr.type->alignment());
        if (attr.type->size() % 8)
            ++num_not_byte_aligned;
    }
    /* Add space for meta data. */
    offsets_[num_attrs] = off;
    off += num_attrs; // reserve space for the NULL bitmap
    uint32_t row_size_ = off;

#if 1
    /* Compute number of rows within a PAX block. Consider worst case padding of 7 bits (because each column within
     * a PAX block must be byte-aligned) for every possibly not byte-aligned attribute column. Null bitmap column is
     * ignored since it is the last column. */
    num_rows_per_block_ = (block_size_ * 8 - num_not_byte_aligned * 7) / row_size_;
#else
    /* Compute number of rows within a PAX block. Assert that the stride jumps to the next block are whole multiples
     * of a byte by packing potentially less rows into one block. Therefore, padding can be ignored. */
    auto max_num_rows_per_block = (block_size_ * 8) / row_size_;
    num_rows_per_block_ = (max_num_rows_per_block / 8) * 8; // round down to next multiple of 8
#endif

    /* Compute offsets (for all rows in a PAX block) by multiplying the offset for a single row by the number of rows
     * within a PAX block. Add padding to next byte if necessary. */
    for (uint32_t pos = 0; pos != num_attrs + 1; ++pos) {
        offsets_[pos] *= num_rows_per_block_;
        if (auto bit_offset = offsets_[pos] % 8; bit_offset)
            offsets_[pos] += 8 - bit_offset;
    }

    /* Compute capacity. */
    capacity_ = (ALLOCATION_SIZE / block_size_) * num_rows_per_block_ // entire blocks
                + ((ALLOCATION_SIZE % block_size_) * 8) / row_size_;  // last partial filled block

    delete[] attrs;
}

void PaxStore::dump(std::ostream &out) const
{
    out << "PaxStore at " << data_.addr() << " for table \"" << table().name << "\": " << num_rows_ << '/' << capacity_
        << " rows, " << block_size_ << " bytes per block, " << num_rows_per_block_ << " rows per block, offsets in bits [";
    for (uint32_t i = 0, end = table().size(); i != end; ++i) {
        if (i != 0) out << ", ";
        out << offsets_[i];
    }
    out << ']' << std::endl;
}

std::unique_ptr<Store> Store::CreatePaxStore(const Table &table) { return std::make_unique<PaxStore>(table); }

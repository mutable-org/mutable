#include "storage/PaxStore.hpp"

#include <algorithm>
#include <exception>
#include <fstream>
#include <iomanip>
#include <mutable/catalog/Catalog.hpp>
#include <numeric>


using namespace m;


PaxStore::PaxStore(const Table &table, uint32_t block_size_in_bytes)
    : Store(table)
    , offsets_(new uint32_t[table.num_attrs() + 1]) // add one slot for the offset of the meta data
    , block_size_(block_size_in_bytes)
{
    compute_block_offsets();

    data_ = allocator_.allocate(ALLOCATION_SIZE);
}

PaxStore::~PaxStore()
{
    delete[] offsets_;
}

void PaxStore::compute_block_offsets()
{
    using std::max;

    const auto num_attrs = table().num_attrs();
    const Attribute **attrs = new const Attribute*[num_attrs];

    for (uint32_t pos = 0; pos != num_attrs; ++pos)
        attrs[pos] = &table()[pos];

    /* Sort attributes by their alignment requirement in descending order. */
    std::stable_sort(attrs, attrs + num_attrs, [](const Attribute *first, const Attribute *second) {
        return first->type->alignment() > second->type->alignment();
    });

    /* Compute offsets (for a single row). */
    uint64_t off = 0;
    uint64_t alignment = 8;
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
    uint64_t row_size_ = off;

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
    uint32_t running_bit_offset = 0;
    for (uint32_t pos = 0; pos != num_attrs; ++pos) {
        const Attribute &attr = *attrs[pos];
        offsets_[attr.id] = offsets_[attr.id] * num_rows_per_block_ + running_bit_offset;
        M_insist(offsets_[attr.id] % 8 == 0, "attribute column must be byte aligned");
        if (auto bit_offset = (offsets_[attr.id] + attr.type->size() * num_rows_per_block_) % 8; bit_offset)
            running_bit_offset += 8 - bit_offset;
    }
    offsets_[num_attrs] = offsets_[num_attrs] * num_rows_per_block_ + running_bit_offset;
    M_insist(offsets_[num_attrs] % 8 == 0, "NULL bitmap column must be byte aligned");
    M_insist(offsets_[num_attrs] + num_attrs * num_rows_per_block_ <= block_size_ * 8);

    /* Compute capacity. */
    capacity_ = (ALLOCATION_SIZE / block_size_) * num_rows_per_block_;

    delete[] attrs;
}

M_LCOV_EXCL_START
void PaxStore::dump(std::ostream &out) const
{
    out << "PaxStore at " << data_.addr() << " for table \"" << table().name << "\": " << num_rows_ << '/' << capacity_
        << " rows, " << block_size_ << " bytes per block, " << num_rows_per_block_ << " rows per block, offsets in bits [";
    for (uint32_t i = 0, end = table().num_attrs(); i != end; ++i) {
        if (i != 0) out << ", ";
        out << offsets_[i];
    }
    out << ']' << std::endl;
}
M_LCOV_EXCL_STOP

__attribute__((constructor(202)))
static void register_store()
{
    Catalog &C = Catalog::Get();
    C.register_store<PaxStore>("PaxStore", "stores attributes using PAX layout");
}

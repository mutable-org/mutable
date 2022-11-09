#include "storage/ColumnStore.hpp"

#include "backend/StackMachine.hpp"
#include <mutable/catalog/Catalog.hpp>
#include <numeric>


using namespace m;


ColumnStore::ColumnStore(const Table &table)
    : Store(table)
{
    uint64_t max_attr_size = 0;

    /* Allocate memory for the attributes columns and the null bitmap column. */
    data_ = allocator_.allocate(ALLOCATION_SIZE * (table.num_attrs() + 1));

    /* Compute the capacity depending on the column with the largest attribute size. */
    for (auto &attr : table) {
        auto size = attr.type->size();
        row_size_ += size;
        max_attr_size = std::max(max_attr_size, size);
    }
    uint32_t num_attrs = table.num_attrs();
    row_size_ += num_attrs;
    uint64_t null_bitmap_size = /* pad_null_bitmap= */ 1 ? ((num_attrs + 7) / 8) * 8 : num_attrs;
    max_attr_size = std::max(max_attr_size, null_bitmap_size);

    capacity_ = (ALLOCATION_SIZE * 8) / max_attr_size;
}

ColumnStore::~ColumnStore() { }

M_LCOV_EXCL_START
void ColumnStore::dump(std::ostream &out) const
{
    out << "ColumnStore for table \"" << table().name << "\": " << num_rows_ << '/' << capacity_
        << " rows, " << row_size_ << " bits per row" << std::endl;
}
M_LCOV_EXCL_STOP

__attribute__((constructor(202)))
static void register_store()
{
    Catalog &C = Catalog::Get();
    C.register_store<ColumnStore>("ColumnStore", "stores attributes in column-major order");
}

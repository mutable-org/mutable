#include "storage/RowStore.hpp"

#include "backend/StackMachine.hpp"
#include <algorithm>
#include <exception>
#include <fstream>
#include <iomanip>
#include <mutable/catalog/Catalog.hpp>
#include <mutable/catalog/Type.hpp>
#include <mutable/storage/Linearization.hpp>
#include <mutable/util/fn.hpp>
#include <typeinfo>


using namespace m;


RowStore::RowStore(const Table &table)
    : Store(table)
    , offsets_(new uint32_t[table.size() + 1]) // add one slot for the offset of the meta data
{
    compute_offsets();
    capacity_ = ALLOCATION_SIZE / (row_size_ / 8);
    data_ = allocator_.allocate(ALLOCATION_SIZE);

    /* Initialize linearization. */
    auto lin = std::make_unique<Linearization>(Linearization::CreateInfinite(1));
    auto child = std::make_unique<Linearization>(Linearization::CreateFinite(table.size() + 1, 1));
    for (auto &attr : table)
        child->add_sequence(offset(attr), 0, attr);
    child->add_null_bitmap(offset(table.size()), 0);
    lin->add_sequence(uintptr_t(memory().addr()), row_size_ / 8, std::move(child));
    linearization(std::move(lin));
}

RowStore::~RowStore()
{
    delete[] offsets_;
}

void RowStore::compute_offsets()
{
    /* TODO: use `PhysicalSchema` with additional bitmap-type to compute offsets. */
    using std::max;

    const auto num_attrs = table().size();
    const Attribute **attrs = new const Attribute*[num_attrs];

    for (uint32_t pos = 0; pos != num_attrs; ++pos)
        attrs[pos] = &table()[pos];

    /* Sort attributes by their alignment requirement in descending order. */
    std::stable_sort(attrs, attrs + num_attrs, [](const Attribute *first, const Attribute *second) {
        return first->type->alignment() > second->type->alignment();
    });

    /* Compute offsets. */
    uint32_t off = 0;
    uint32_t alignment = 8;
    for (uint32_t pos = 0; pos != num_attrs; ++pos) {
        const Attribute &attr = *attrs[pos];
        offsets_[attr.id] = off;
        off += attr.type->size();
        alignment = max(alignment, attr.type->alignment());
    }
    /* Add space for meta data. */
    offsets_[num_attrs] = off;
    off += num_attrs; // reserve space for the NULL bitmap
    if (off % alignment)
        off += (alignment - off % alignment); // the offset is padded to fulfill the alignment requirements
    row_size_ = off;

    delete[] attrs;
}

M_LCOV_EXCL_START
void RowStore::dump(std::ostream &out) const
{
    out << "RowStore at " << data_.addr() << " for table \"" << table().name << "\": " << num_rows_ << '/' << capacity_
        << " rows, " << row_size_ << " bits per row, offsets [";
    for (uint32_t i = 0, end = table().size(); i != end; ++i) {
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
    C.register_store<RowStore>("RowStore", "stores attributes in row-major order");
}

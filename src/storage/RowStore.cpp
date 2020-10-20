#include "storage/RowStore.hpp"

#include "backend/StackMachine.hpp"
#include "catalog/Type.hpp"
#include "util/fn.hpp"
#include "storage/Linearization.hpp"
#include <algorithm>
#include <exception>
#include <fstream>
#include <iomanip>
#include <typeinfo>


using namespace db;


RowStore::RowStore(const Table &table)
    : Store(table)
    , offsets_(new uint32_t[table.size() + 1]) // add one slot for the offset of the meta data
{
    auto &allocator = Catalog::Get().allocator();
    compute_offsets();
    capacity_ = ALLOCATION_SIZE / (row_size_ / 8);
    data_ = allocator.allocate(ALLOCATION_SIZE);

    /* Initialize linearization. */
    auto lin = std::make_unique<Linearization>(Linearization::CreateInfiniteSequence(1));
    auto child = std::make_unique<Linearization>(Linearization::CreateFiniteSequence(table.size() + 1, 1));
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

std::unique_ptr<Store> Store::CreateRowStore(const Table &table) { return std::make_unique<RowStore>(table); }

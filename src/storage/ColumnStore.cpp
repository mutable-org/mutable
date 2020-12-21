#include "storage/ColumnStore.hpp"

#include "backend/StackMachine.hpp"
#include "mutable/storage/Linearization.hpp"
#include <numeric>


using namespace m;


ColumnStore::ColumnStore(const Table &table)
    : Store(table)
{
    uint32_t max_attr_size = 0;

    auto &allocator = Catalog::Get().allocator();

    /* Allocate columns for the attributes. */
    columns_.reserve(table.size() + 1);
    for (auto &attr : table) {
        columns_.emplace_back(allocator.allocate(ALLOCATION_SIZE));
        auto size = attr.type->size();
        row_size_ += size;
        max_attr_size = std::max(max_attr_size, size);
    }

    /* Allocate a column for the null bitmap. */
    columns_.emplace_back(allocator.allocate(ALLOCATION_SIZE));

    insist(columns_.size() == table.size() + 1);
    capacity_ = (ALLOCATION_SIZE * 8) / max_attr_size;

    /* Initialize linearization. */
    auto lin = std::make_unique<Linearization>(Linearization::CreateInfinite(table.size() + 1));
    for (auto &attr : table) {
        if (attr.type->is_boolean()) {
            /* Pack 8 booleans into a sequence of 1 byte. */
            auto seq = std::make_unique<Linearization>(Linearization::CreateFinite(1, 8));
            seq->add_sequence(0, 1, attr);
            lin->add_sequence(uintptr_t(memory(attr.id).addr()), 1, std::move(seq));
        } else {
            auto seq = std::make_unique<Linearization>(Linearization::CreateFinite(1, 1));
            seq->add_sequence(0, 0, attr);
            lin->add_sequence(uintptr_t(memory(attr.id).addr()), attr.type->size() / 8, std::move(seq));
        }
    }
#if 1
    /* Pad null bitmap to next byte. */
    auto seq = std::make_unique<Linearization>(Linearization::CreateFinite(1, 1));
    seq->add_null_bitmap(0, 0);
    lin->add_sequence(uintptr_t(memory(table.size()).addr()), (table.size() + 7 ) / 8, std::move(seq));
#else
    /* Pack null bitmaps consecutively to save space. */
    const std::size_t bits_per_pack = std::lcm(table.size(), 8); // least common multiple
    const std::size_t num_bitmaps_per_pack = bits_per_pack / table.size();
    auto seq = std::make_unique<Linearization>(Linearization::CreateFinite(1, num_bitmaps_per_pack));
    seq->add_null_bitmap(0, num_bitmaps_per_pack == 1 ? 0 : table.size());
    lin->add_sequence(uintptr_t(memory(table.size()).addr()), bits_per_pack / 8, std::move(seq));
#endif
    linearization(std::move(lin));
}

ColumnStore::~ColumnStore() { }

void ColumnStore::dump(std::ostream &out) const
{
    out << "ColumnStore for table \"" << table().name << "\": " << num_rows_ << '/' << capacity_
        << " rows, " << row_size_ << " bits per row" << std::endl;
}

std::unique_ptr<Store> Store::CreateColumnStore(const Table &table) { return std::make_unique<ColumnStore>(table); }

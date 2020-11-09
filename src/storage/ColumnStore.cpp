#include "storage/ColumnStore.hpp"

#include "backend/StackMachine.hpp"
#include "mutable/storage/Linearization.hpp"


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
    auto lin = std::make_unique<Linearization>(Linearization::CreateInfiniteSequence(table.size() + 1));
    for (auto &attr : table) {
        if (attr.type->is_boolean()) {
            /* Pack 8 booleans into a sequence of 1 byte. */
            auto seq = std::make_unique<Linearization>(Linearization::CreateFiniteSequence(1, 8));
            seq->add_sequence(0, 1, attr);
            lin->add_sequence(uintptr_t(memory(attr.id).addr()), 1, std::move(seq));
        } else {
            auto seq = std::make_unique<Linearization>(Linearization::CreateFiniteSequence(1, 1));
            seq->add_sequence(0, 0, attr);
            lin->add_sequence(uintptr_t(memory(attr.id).addr()), attr.type->size() / 8, std::move(seq));
        }
    }
    auto seq = std::make_unique<Linearization>(Linearization::CreateFiniteSequence(1, 1));
    seq->add_null_bitmap(0, 0);
    lin->add_sequence(uintptr_t(memory(table.size()).addr()), (table.size() + 7 ) / 8, std::move(seq));
    linearization(std::move(lin));
}

ColumnStore::~ColumnStore() { }

void ColumnStore::dump(std::ostream &out) const
{
    out << "ColumnStore for table \"" << table().name << "\": " << num_rows_ << '/' << capacity_
        << " rows, " << row_size_ << " bits per row" << std::endl;
}

std::unique_ptr<Store> Store::CreateColumnStore(const Table &table) { return std::make_unique<ColumnStore>(table); }

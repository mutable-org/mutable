#include "storage/ColumnStore.hpp"

#include "backend/StackMachine.hpp"


using namespace db;


/*======================================================================================================================
 * ColumnStore
 *====================================================================================================================*/

ColumnStore::ColumnStore(const Table &table)
    : Store(table)
{
    uint32_t max_attr_size = 0;

    auto &allocator = Catalog::Get().allocator();

    /* Allocate columns for the attributes. */
    columns_.reserve(table.size());
    for (auto &attr : table) {
        columns_.emplace_back(allocator.allocate(ALLOCATION_SIZE));
        auto size = attr.type->size();
        row_size_ += size;
        max_attr_size = std::max(max_attr_size, size);
    }

    /* Allocate a column for the null bitmap. */
    columns_.emplace_back(allocator.allocate(ALLOCATION_SIZE));

    insist(columns_.size() == table.size() + 1);
    capacity_ = ALLOCATION_SIZE / (max_attr_size / 8);
}

ColumnStore::~ColumnStore() { }

StackMachine ColumnStore::loader(const Schema &schema) const
{
    StackMachine sm;
    std::size_t out_idx = 0;

    /* Add row id to context. */
    auto row_id_idx = sm.add(int64_t(0));

    /* Add address of null bitmap column to context. */
    const auto null_bitmap_col_addr = columns_.back().as<uintptr_t>();
    auto null_bitmap_col_addr_idx = sm.add(int64_t(null_bitmap_col_addr));

    for (auto &e : schema) {
        auto &attr = table().at(e.id.name);

        /* Load row id to stack. */
        sm.emit_Ld_Ctx(row_id_idx);

        /* Load address of null bitmap column to stack. */
        sm.emit_Ld_Ctx(null_bitmap_col_addr_idx);

        /* Load column address to stack. */
        const auto col_addr = columns_[attr.id].as<uintptr_t>();
        sm.add_and_emit_load(int64_t(col_addr));

        /* Load attribute id to stack. */
        sm.add_and_emit_load(int64_t(attr.id));

        /* Emit load from store instruction. */
        auto ty = attr.type;
        if (ty->is_boolean()) {
            sm.emit_Ld_CS_b();
        } else if (auto n = cast<const Numeric>(ty)) {
            switch (n->kind) {
                case Numeric::N_Int:
                case Numeric::N_Decimal: {
                    switch (n->size()) {
                        default: unreachable("illegal integer type");
                        case  8: sm.emit_Ld_CS_i8();  break;
                        case 16: sm.emit_Ld_CS_i16(); break;
                        case 32: sm.emit_Ld_CS_i32(); break;
                        case 64: sm.emit_Ld_CS_i64(); break;
                    }
                    break;
                }

                case Numeric::N_Float: {
                    if (n->size() == 32)
                        sm.emit_Ld_CS_f();
                    else
                        sm.emit_Ld_CS_d();
                    break;
                }
            }
        } else if (auto cs = cast<const CharacterSequence>(ty)) {
            sm.add_and_emit_load(int64_t(cs->length));
            sm.emit_Ld_CS_s();
        } else {
            unreachable("illegal type");
        }
        sm.emit_St_Tup(0, out_idx++, attr.type);
    }

    /* Update row id. */
    sm.emit_Ld_Ctx(row_id_idx);
    sm.emit_Inc();
    sm.emit_Upd_Ctx(row_id_idx);

    return sm;
}

StackMachine ColumnStore::writer(const std::vector<const Attribute*> &attrs) const
{
    Schema in;
    for (auto attr : attrs)
        in.add({"attr"}, attr->type);
    StackMachine sm(in);

    /* Get row id.  Allocate a slot in the context, that is to be set from the user of this StackMachine. */
    auto row_id_idx = sm.add(int64_t(0));

    /* Add address of null bitmap column to context. */
    const auto null_bitmap_col_addr = columns_.back().as<uintptr_t>();
    auto null_bitmap_col_addr_idx = sm.add(int64_t(null_bitmap_col_addr));

    uint8_t tuple_idx = 0;
    for (auto attr : attrs) {
        if (not attr) continue;

        /* Load the next value from the tuple to the stack. */
        sm.emit_Ld_Tup(0, tuple_idx++);

        /* Load row id from first tuple to stack. */
        sm.emit_Ld_Ctx(row_id_idx);

        /* Load address of null bitmap column to stack. */
        sm.emit_Ld_Ctx(null_bitmap_col_addr_idx);

        /* Load column address to stack. */
        const auto col_addr = columns_[attr->id].as<uintptr_t>();
        sm.add_and_emit_load(int64_t(col_addr));

        /* Load attribute id to stack. */
        sm.add_and_emit_load(int64_t(attr->id));

        /* Emit store to store instruction. */
        auto ty = attr->type;
        if (ty->is_boolean()) {
            sm.emit_St_CS_b();
        } else if (auto n = cast<const Numeric>(ty)) {
            switch (n->kind) {
                case Numeric::N_Int:
                case Numeric::N_Decimal: {
                    switch (n->size()) {
                        default: unreachable("illegal type");
                        case  8: sm.emit_St_CS_i8();  break;
                        case 16: sm.emit_St_CS_i16(); break;
                        case 32: sm.emit_St_CS_i32(); break;
                        case 64: sm.emit_St_CS_i64(); break;
                    }
                    break;
                }

                case Numeric::N_Float: {
                    if (n->size() == 32)
                        sm.emit_St_CS_f();
                    else
                        sm.emit_St_CS_d();
                    break;
                }
            }
        } else if (auto cs = cast<const CharacterSequence>(ty)) {
            sm.add_and_emit_load(int64_t(cs->length));
            sm.emit_St_CS_s();
        } else {
            unreachable("illegal type");
        }
    }

    return sm;
}

void ColumnStore::dump(std::ostream &out) const
{
    out << "ColumnStore for table \"" << table().name << "\": " << num_rows_ << '/' << capacity_
        << " rows, " << row_size_ << " bits per row" << std::endl;
}

std::unique_ptr<Store> Store::CreateColumnStore(const Table &table) { return std::make_unique<ColumnStore>(table); }

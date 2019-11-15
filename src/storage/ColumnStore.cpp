#include "storage/ColumnStore.hpp"

#include "IR/Interpreter.hpp"

#include <sys/mman.h>


using namespace db;


/*======================================================================================================================
 * ColumnStore
 *====================================================================================================================*/

ColumnStore::ColumnStore(const Table &table)
    : Store(table)
{
    uint32_t max_attr_size = 0;

    /* Allocate columns for the attributes. */
    /* XXX All bools into one columns or one column per boolean? */
    columns_.reserve(table.size());
    for (auto &attr : table) {
        auto col = mmap(nullptr, ALLOCATION_SIZE, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS|MAP_NORESERVE, 0, 0);
        if (col == MAP_FAILED)
            throw std::runtime_error("RowStore failed to allocate memory");
        columns_.push_back(col);
        auto size = attr.type->size();
        row_size_ += size;
        max_attr_size = std::max(max_attr_size, size);
    }

    /* Allocate a column for the null bitmap. */
    {
        auto col = mmap(nullptr, ALLOCATION_SIZE, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS|MAP_NORESERVE, 0, 0);
        if (col == MAP_FAILED)
            throw std::runtime_error("RowStore failed to allocate memory");
        columns_.push_back(col);
    }
    insist(columns_.size() == table.size() + 1);

    capacity_ = ALLOCATION_SIZE / (max_attr_size / 8);
}

ColumnStore::~ColumnStore()
{
    for (auto col : columns_)
        munmap(col, ALLOCATION_SIZE);
}

std::size_t ColumnStore::load(std::filesystem::path path)
{
    unreachable("not implemented");
}

void ColumnStore::save(std::filesystem::path path) const
{
    unreachable("not implemented");
}

StackMachine ColumnStore::loader(const OperatorSchema &schema) const
{
    StackMachine sm;

    /* Add row id to context. */
    auto row_id_idx = sm.add(int64_t(0));

    /* Add address of null bitmap column to context. */
    const auto null_bitmap_col_addr = reinterpret_cast<uintptr_t>(columns_.back());
    auto null_bitmap_col_addr_idx = sm.add(int64_t(null_bitmap_col_addr));

    for (auto &attr_ident : schema) {
        auto &attr = table().at(attr_ident.first.attr_name);

        /* Load row id to stack. */
        sm.emit_Ld_Ctx(row_id_idx);

        /* Load address of null bitmap column to stack. */
        sm.emit_Ld_Ctx(null_bitmap_col_addr_idx);

        /* Load column address to stack. */
        const auto col_addr = reinterpret_cast<uintptr_t>(columns_[attr.id]);
        sm.add_and_emit_load(int64_t(col_addr));

        /* Load attribute id to stack. */
        sm.add_and_emit_load(int64_t(attr.id));

        /* Emit load from store instruction. */
        auto ty = attr.type;
        if (ty->is_boolean()) {
            sm.emit_Ld_CS_b();
        } else if (auto n = cast<const Numeric>(ty)) {
            switch (n->kind) {
                case Numeric::N_Int: {
                    switch (n->precision) {
                        default: unreachable("illegal integer type");
                        case 1: sm.emit_Ld_CS_i8();  break;
                        case 2: sm.emit_Ld_CS_i16(); break;
                        case 4: sm.emit_Ld_CS_i32(); break;
                        case 8: sm.emit_Ld_CS_i64(); break;
                    }
                    break;
                }

                case Numeric::N_Float: {
                    if (n->precision == 32)
                        sm.emit_Ld_CS_f();
                    else
                        sm.emit_Ld_CS_d();
                    break;
                }

                case Numeric::N_Decimal: {
                    const auto p = ceil_to_pow_2(n->size());
                    switch (p) {
                        default: unreachable("illegal precision of decimal type");
                        case 1: sm.emit_Ld_CS_i8();  break;
                        case 2: sm.emit_Ld_CS_i16(); break;
                        case 4: sm.emit_Ld_CS_i32(); break;
                        case 8: sm.emit_Ld_CS_i64(); break;
                    }
                    break;
                }
            }
        } else if (auto cs = cast<const CharacterSequence>(ty)) {
            sm.add_and_emit_load(int64_t(cs->length));
            sm.emit_Ld_CS_s();
        } else {
            unreachable("illegal type");
        }
    }

    /* Update row id. */
    sm.emit_Ld_Ctx(row_id_idx);
    sm.add_and_emit_load(int64_t(1));
    sm.emit_Add_i();
    sm.emit_Upd_Ctx(row_id_idx);
    sm.emit_Pop();

    return sm;
}

StackMachine ColumnStore::writer(const std::vector<const Attribute*> &attrs) const
{
    StackMachine sm;

    /* Add row id to context. */
    auto row_id_idx = sm.add(int64_t(0));

    /* Add address of null bitmap column to context. */
    const auto null_bitmap_col_addr = reinterpret_cast<uintptr_t>(columns_.back());
    auto null_bitmap_col_addr_idx = sm.add(int64_t(null_bitmap_col_addr));

    uint8_t tuple_idx = 0;
    for (auto attr : attrs) {
        if (not attr) continue;

        /* Load the next value to the stack. */
        sm.emit_Ld_Tup(tuple_idx++);

        /* Load row id to stack. */
        sm.emit_Ld_Ctx(row_id_idx);

        /* Load address of null bitmap column to stack. */
        sm.emit_Ld_Ctx(null_bitmap_col_addr_idx);

        /* Load column address to stack. */
        const auto col_addr = reinterpret_cast<uintptr_t>(columns_[attr->id]);
        sm.add_and_emit_load(int64_t(col_addr));

        /* Load attribute id to stack. */
        sm.add_and_emit_load(int64_t(attr->id));

        /* Emit store to store instruction. */
        auto ty = attr->type;
        if (ty->is_boolean()) {
            sm.emit_St_CS_b();
        } else if (auto n = cast<const Numeric>(ty)) {
            switch (n->kind) {
                case Numeric::N_Int: {
                    switch (n->precision) {
                        default: unreachable("illegal integer type");
                        case 1: sm.emit_St_CS_i8();  break;
                        case 2: sm.emit_St_CS_i16(); break;
                        case 4: sm.emit_St_CS_i32(); break;
                        case 8: sm.emit_St_CS_i64(); break;
                    }
                    break;
                }

                case Numeric::N_Float: {
                    if (n->precision == 32)
                        sm.emit_St_CS_f();
                    else
                        sm.emit_St_CS_d();
                    break;
                }

                case Numeric::N_Decimal: {
                    const auto p = ceil_to_pow_2(n->size());
                    switch (p) {
                        default: unreachable("illegal precision of decimal type");
                        case 1: sm.emit_St_CS_i8();  break;
                        case 2: sm.emit_St_CS_i16(); break;
                        case 4: sm.emit_St_CS_i32(); break;
                        case 8: sm.emit_St_CS_i64(); break;
                    }
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

    /* Update row id. */
    sm.emit_Ld_Ctx(row_id_idx);
    sm.add_and_emit_load(int64_t(1));
    sm.emit_Add_i();
    sm.emit_Upd_Ctx(row_id_idx);
    sm.emit_Pop();

    return sm;
}

void ColumnStore::dump(std::ostream &out) const
{
    out << "ColumnStore for table \"" << table().name << "\": " << num_rows_ << '/' << capacity_
        << " rows, " << row_size_ << " bits per row" << std::endl;
}

/*======================================================================================================================
 * ColumnStore::Row
 *====================================================================================================================*/

void ColumnStore::Row::dispatch(callback_t callback) const
{
    struct TypeDispatch : ConstTypeVisitor
    {
        callback_t callback;
        const Attribute &attr;
        const ColumnStore::Row &row;

        TypeDispatch(callback_t callback, const Attribute &attr, const ColumnStore::Row &row)
            : callback(callback)
            , attr(attr)
            , row(row)
        { }

        using ConstTypeVisitor::operator();
        void operator()(Const<ErrorType>&) { unreachable("error type"); }
        void operator()(Const<Boolean>&) { callback(attr, row.get_generic<bool>(attr)); }
        void operator()(Const<CharacterSequence> &ty) {
            insist(not ty.is_varying, "varying length character sequences are not supported by this store");
            callback(attr, row.get_generic<std::string>(attr));
        }
        void operator()(Const<Numeric> &ty) {
            switch (ty.kind) {
                case Numeric::N_Int:
                case Numeric::N_Decimal:
                    callback(attr, row.get_generic<int64_t>(attr));
                    return;

                case Numeric::N_Float:
                    if (ty.precision == 32)
                        callback(attr, row.get_generic<float>(attr));
                    else
                        callback(attr, row.get_generic<double>(attr));
                    return;
            }
        }
        void operator()(Const<FnType>&) { unreachable("fn type"); }
    };

    for (auto &attr : store.table()) {
        if (isnull(attr)) {
            callback(attr, value_type());
            continue;
        }

        TypeDispatch dispatcher(callback, attr, *this);
        dispatcher(*attr.type);
    }
}

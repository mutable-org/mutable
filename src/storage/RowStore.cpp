#include "storage/RowStore.hpp"

#include "catalog/Type.hpp"
#include "util/fn.hpp"
#include <algorithm>
#include <exception>
#include <fstream>
#include <iomanip>
#include <typeinfo>

#include <sys/mman.h>


using namespace db;


/*======================================================================================================================
 * RowStore
 *====================================================================================================================*/

RowStore::RowStore(const Table &table)
    : Store(table)
    , offsets_(new uint32_t[table.size() + 1]) // add one slot for the offset of the meta data
{
    compute_offsets();
    capacity_ = ALLOCATION_SIZE / (row_size_ / 8);
    data_ = mmap(nullptr, ALLOCATION_SIZE, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS|MAP_NORESERVE, 0, 0);
    if (data_ == MAP_FAILED)
        throw std::runtime_error("RowStore failed to allocate memory");
}

RowStore::~RowStore()
{
    delete[] offsets_;
    munmap(data_, ALLOCATION_SIZE);
}

void RowStore::save(std::filesystem::path path) const
{
    std::ofstream out(path, std::ios_base::binary);

    out << "store\n" << typeid(*this).name()
        << "\ntable\n" << table().name << '\n'
        << num_rows_ << '\n';

    std::size_t num_bytes = num_rows_ * row_size_/8;
    out.write(reinterpret_cast<char*>(data_), num_bytes);
}

std::size_t RowStore::load(std::filesystem::path path)
{
    std::string buf;
    std::ifstream in(path, std::ios_base::binary);

    in >> buf;
    if (buf != "store")
        throw std::invalid_argument("not a storage file");
    in >> buf;
    if (buf != typeid(*this).name())
        throw std::invalid_argument("this storeage file is of a different type");
    in >> buf;
    if (buf != "table")
        throw std::invalid_argument("missing table name");
    in >> buf;
    if (buf != table().name)
        throw std::invalid_argument("this storage file is for a different table");
    std::size_t num_fresh_rows;
    in >> num_fresh_rows;
    if (capacity_ - num_fresh_rows < num_rows_)
        throw std::runtime_error("not enough capacity to load data from storage file");
    in.get(); // skip new line

    std::size_t num_bytes = num_fresh_rows * row_size_/8;
    in.read(reinterpret_cast<char*>(data_) + num_rows_ * row_size_/8, num_bytes);
    num_rows_ += num_fresh_rows;
    return num_fresh_rows;
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
        row_size_ = off + (alignment - off % alignment); // the row size is padded to fulfill the alignment requirements

    delete[] attrs;
}

void RowStore::dump(std::ostream &out) const
{
    out << "RowStore at " << data_ << " for table \"" << table().name << "\": " << num_rows_ << '/' << capacity_
        << " rows, " << row_size_ << " bits per row, offsets [";
    for (uint32_t i = 0, end = table().size(); i != end; ++i) {
        if (i != 0) out << ", ";
        out << offsets_[i];
    }
    out << ']' << std::endl;
}

/*======================================================================================================================
 * RowStore::Row
 *====================================================================================================================*/

void RowStore::Row::dispatch(callback_t callback) const
{
    struct TypeDispatch : ConstTypeVisitor
    {
        callback_t callback;
        const Attribute &attr;
        const RowStore::Row &row;

        TypeDispatch(callback_t callback, const Attribute &attr, const RowStore::Row &row)
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

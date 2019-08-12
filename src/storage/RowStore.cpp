#include "storage/Store.hpp"

#include "catalog/Type.hpp"
#include "util/fn.hpp"
#include <algorithm>
#include <exception>
#include <fstream>
#include <typeinfo>

#include <sys/mman.h>


using namespace db;


/*======================================================================================================================
 * Store
 *====================================================================================================================*/

void Store::dump() const { dump(std::cerr); }

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
 * Row
 *====================================================================================================================*/

template<bool C>
std::ostream & db::operator<<(std::ostream &out, RowStore::the_row<C> row)
{
    struct {
        std::ostream &out;

        void operator()(bool b) const { out << (b ? "TRUE" : "FALSE"); }
        void operator()(int64_t i) const { out << i; }
        void operator()(float f) const { out << f; }
        void operator()(double d) const { out << d; }
        void operator()(int64_t pre, int64_t post) const { out << pre << '.' << post; }
        void operator()(const char *str) const { out << '"' << escape_string(str) << '"'; }
        void operator()(std::string str) const { out << '"' << str << '"'; }
    } printer{out};

    auto &T = row.store.table();
    for (auto it = T.begin(), end = T.end(); it != end; ++it) {
        if (it != T.begin()) out << ", ";
        row.dispatch(*it, printer);
    }

    return out;
}
template std::ostream & db::operator<<(std::ostream &out, RowStore::the_row<false> row);
template std::ostream & db::operator<<(std::ostream &out, RowStore::the_row<true> row);

#include "backend/RuntimeStruct.hpp"

#include <mutable/catalog/Schema.hpp>


using namespace m;


RuntimeStruct::RuntimeStruct(std::initializer_list<const Type*> fields) : RuntimeStruct(Schema(), fields) { }

RuntimeStruct::RuntimeStruct(const Schema &schema, std::initializer_list<const Type*> additional_fields)
    : types_(new const Type*[schema.num_entries() + additional_fields.size()])
    , offsets_(new offset_type[schema.num_entries() + additional_fields.size()])
{
    /* Collect schema and additional fields in one array of fields. */
    num_fields_ = schema.num_entries() + additional_fields.size();
    std::pair<index_type, const Type*> fields[num_fields_];
    {
        index_type idx = 0;
        for (auto &e : schema)
            fields[idx] = { idx, e.type }, ++idx;
        for (auto f : additional_fields)
            fields[idx] = { idx, f }, ++idx;
    }

    /* Sort fields by their alignment requirement in descending order to minimize padding. */
    std::stable_sort(fields, fields + num_fields_,
        [](const std::pair<index_type, const Type*> &left, const std::pair<index_type, const Type*> &right) {
            return left.second->alignment() > right.second->alignment();
    });

    /* Compute offsets of the fields. */
    using std::max;
    offset_type off = 0;
    offset_type alignment = 8;
    for (index_type pos = 0; pos != num_fields_; ++pos) {
        const auto &field = fields[pos];
        offsets_[field.first] = off;
        types_[field.first] = field.second;
        off += field.second->size();
        alignment = max(alignment, field.second->alignment());
    }
    if (off % alignment)
        off += (alignment - off % alignment); // the offset is padded to fulfill the alignment requirements
    size_ = off;
}

RuntimeStruct::~RuntimeStruct()
{
    delete[] types_;
    delete[] offsets_;
}

std::ostream & m::operator<<(std::ostream &out, const RuntimeStruct &PS) {
    out << "RuntimeStruct [";
    for (RuntimeStruct::index_type i = 0; i != PS.num_fields_; ++i) {
        if (i != 0) out << ", ";
        out << PS.offset(i);
    }
    return out << "] (" << PS.size_in_bits() << " bits)";
}

void RuntimeStruct::dump(std::ostream &out) const { out << *this << std::endl; }
void RuntimeStruct::dump() const { dump(std::cerr); }

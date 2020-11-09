#include "mutable/storage/Linearization.hpp"

#include "catalog/Schema.hpp"
#include <cmath>
#include <iomanip>


using namespace m;


namespace {

/** Start a new line with proper indentation. */
std::ostream & indent(std::ostream &out, unsigned indentation) {
    out << '\n' << std::string(4 * indentation - 4, ' ');
    return out;
}

}

void Linearization::dump() const { dump(std::cerr); }
void Linearization::dump(std::ostream &out) const { out << *this << std::endl; }

void Linearization::print(std::ostream &out, unsigned indentation) const {
    if (not indentation) {
        out << "Linearization of ";
        if (num_tuples_)
            out << num_tuples_;
        else
            out << "infinite";
        out << " tuples";
    }

    const std::size_t decimal_places = std::ceil(size_ / Numeric::DECIMAL_TO_BINARY_DIGITS);

    ++indentation;
    auto it = cbegin();
    for (std::size_t i = 0; i != size_; ++i, ++it) {
        indent(out, indentation) << "[" << std::setw(decimal_places) << i << "]: ";

        auto seq = *it;
        if (seq.is_attribute()) {
            auto &attr = seq.as_attribute();
            out << "Attribute \"" << attr.name << "\" of type " << *attr.type << " with bit offset " << seq.offset
                << " and bit stride " << seq.stride;
        } else if (seq.is_linearization()) {
            auto &lin = seq.as_linearization();
            out << "Linearization of ";
            if (lin.num_tuples())
                out << lin.num_tuples();
            else
                out << "infinite";
            out << " tuples with byte offset ";
            if (seq.offset & 0xFFFFFF0000000000U) // identify addresses on x64
                out << reinterpret_cast<void*>(static_cast<uintptr_t>(seq.offset));
            else
                out << seq.offset;
            out << " and byte stride " << seq.stride;
            lin.print(out, indentation + 1);
        } else {
            insist(seq.is_null_bitmap());
            out << "Null bitmap with bit offset " << seq.offset << " and bit stride " << seq.stride;
        }
    }
    --indentation;
}

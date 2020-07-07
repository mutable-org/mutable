#include "storage/Linearization.hpp"

#include "catalog/Schema.hpp"


using namespace db;


void Linearization::dump() const { dump(std::cerr); }
void Linearization::dump(std::ostream &out) const { out << *this << std::endl; }

void Linearization::print(std::ostream &out, unsigned indentation) const {
    indent(out, indentation) << "Sequences of ";
    if (num_tuples_)
        out << num_tuples_;
    else
        out << "infinite";
    out << " tuples";
    for (auto s : *this) {
        if (s.is_attribute()) {
            indent(out, indentation + 1) << "Attribute " << s.as_attribute() << " at offset " << s.offset
                << " and stride " << s.stride;
        } else {
            indent(out, indentation + 1) << "at offset " << s.offset << " and stride " << s.stride;
            s.as_linearization().print(out, indentation + 2);
        }
    }
}

/** Start a new line with proper indentation. */
std::ostream & Linearization::indent(std::ostream &out, unsigned indentation) const {
    if (indentation)
        out << '\n' << std::string(2 * indentation - 2, ' ') << "` ";
    return out;
}

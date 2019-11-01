#include "storage/Store.hpp"

#include <cmath>


using namespace db;


/*======================================================================================================================
 * Store
 *====================================================================================================================*/

void Store::dump() const { dump(std::cerr); }

void Store::Row::print(std::ostream &out) const
{
    dispatch([&](const Attribute &attr, value_type value) {
        if (attr.id != 0) out << ", "; // preceeded by comma, if not the first attribute
        db::print(out, attr.type, value);
    });
}

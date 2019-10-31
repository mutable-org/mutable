#include "storage/Store.hpp"

#include <cmath>


using namespace db;


void db::print(std::ostream &out, const Type *type, value_type value)
{
    /* Check if NULL. */
    if (std::holds_alternative<null_type>(value)) {
        out << "NULL";
        return;
    }

    if (type->is_boolean()) {
        out << (std::get<bool>(value) ? "TRUE" : "FALSE");
        return;
    }

    /* Put character sequences in quotes. */
    if (type->is_character_sequence()) {
        out << '"' << value << '"';
        return;
    }

    /* Print decimals with decimal places. */
    if (auto n = cast<const Numeric>(type); n and n->kind == Numeric::N_Decimal) {
        using std::setw, std::setfill;
        int64_t v = std::get<int64_t>(value);
        int64_t shift = pow(10, n->scale);
        int64_t pre = v / shift;
        int64_t post = std::abs(v) % shift;
        out << pre << '.' << setw(n->scale) << setfill('0') << post;
        return;
    }

    /* If not a corner case, simply print. */
    out << value;
}

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

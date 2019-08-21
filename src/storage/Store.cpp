#include "storage/Store.hpp"


using namespace db;


void print(std::ostream &out, const Attribute &attr, value_type value)
{
    /* Check if NULL. */
    if (std::holds_alternative<null_type>(value)) {
        out << "NULL";
        return;
    }

    if (attr.type->is_boolean()) {
        out << (std::get<bool>(value) ? "TRUE" : "FALSE");
        return;
    }

    /* Put character sequences in quotes. */
    if (attr.type->is_character_sequence()) {
        out << '"' << value << '"';
        return;
    }

    /* Print decimals with decimal places. */
    if (auto n = cast<const Numeric>(attr.type); n and n->kind == Numeric::N_Decimal) {
        using std::setw, std::setfill;
        int64_t v = std::get<int64_t>(value);
        int64_t shift = pow(10, n->scale);
        int64_t pre = v / shift;
        int64_t post = v % shift;
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
        ::print(out, attr, value);
    });
}

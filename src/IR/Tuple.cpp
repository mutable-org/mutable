#include <mutable/IR/Tuple.hpp>

#include <ctime>
#include <mutable/catalog/Schema.hpp>
#include <mutable/catalog/Type.hpp>
#include <mutable/util/fn.hpp>


using namespace m;


/*======================================================================================================================
  Value
 *====================================================================================================================*/

M_LCOV_EXCL_START
void Value::print(std::ostream &out, const Type &ty) const
{
    visit(overloaded {
        [this, &out](const Boolean&) { out << (as_b() ? "TRUE" : "FALSE"); },
        [this, &out](const CharacterSequence &cs) { out << '"'; out.write(as<const char*>(), cs.length); out << '"'; },
        [this, &out](const Date&) {
            const int32_t date = as_i(); // signed because year is signed
            const auto oldfill = out.fill('0');
            const auto oldfmt = out.flags();
            out << std::internal
                << std::setw(date >> 9 > 0 ? 4 : 5) << (date >> 9) << '-'
                << std::setw(2) << ((date >> 5) & 0xF) << '-'
                << std::setw(2) << (date & 0x1F);
            out.fill(oldfill);
            out.flags(oldfmt);
        },
        [this, &out](const DateTime&) {
            const time_t time = as_i();
            std::tm tm;
            gmtime_r(&time, &tm);
            out << put_tm(tm);
        },
        [this, &out](const Numeric &n) {
            switch (n.kind) {
                case Numeric::N_Int:
                    out << as_i();
                    break;

                case Numeric::N_Decimal: {
                    const int64_t div = powi(10L, n.scale);
                    const int64_t pre = as_i() / div;
                    const int64_t post = as_i() % div;
                    out << pre << '.';
                    auto old_fill = out.fill('0');
                    out << std::setw(n.scale) << post;
                    out.fill(old_fill);
                    break;
                }

                case Numeric::N_Float:
                    if (n.size() == 32)
                        out << as_f();
                    else
                        out << as_d();
                    break;
            }
        },
        [](auto&&) { M_unreachable("invalid value type"); }
    }, ty);
}

void Value::dump(std::ostream &out) const { out << *this << std::endl; }
void Value::dump() const { dump(std::cerr); }
M_LCOV_EXCL_STOP


/*======================================================================================================================
  Tuple
 *====================================================================================================================*/

Tuple::Tuple(const Schema &S)
#ifdef M_ENABLE_SANITY_FIELDS
    : num_values_(S.num_entries())
#endif
{
    std::size_t additional_bytes = 0;
    for (auto &e : S) {
        if (auto cs = cast<const CharacterSequence>(e.type))
            additional_bytes += cs->length + 1;
    }
    values_ = (Value*) malloc(S.num_entries() * sizeof(Value) + additional_bytes);
    uint8_t *p = reinterpret_cast<uint8_t*>(values_) + S.num_entries() * sizeof(Value);
    for (std::size_t i = 0; i != S.num_entries(); ++i) {
        if (auto cs = cast<const CharacterSequence>(S[i].type)) {
            new (&values_[i]) Value(p);
            *p = '\0'; // terminating NUL byte
            p += cs->length + 1;
        } else {
            new (&values_[i]) Value();
        }
    }
    clear();
}

Tuple::Tuple(std::vector<const Type*> types)
#ifdef M_ENABLE_SANITY_FIELDS
    : num_values_(types.size())
#endif
{
    std::size_t additional_bytes = 0;
    for (auto &ty : types) {
        if (auto cs = cast<const CharacterSequence>(ty))
            additional_bytes += cs->length + 1;
    }
    values_ = (Value*) malloc(types.size() * sizeof(Value) + additional_bytes);
    uint8_t *p = reinterpret_cast<uint8_t*>(values_) + types.size() * sizeof(Value);
    for (std::size_t i = 0; i != types.size(); ++i) {
        if (auto cs = cast<const CharacterSequence>(types[i])) {
            new (&values_[i]) Value(p);
            p += cs->length + 1;
        } else {
            new (&values_[i]) Value();
        }
    }
    clear();
}

Tuple Tuple::clone(const Schema &S) const
{
    Tuple cpy(S);
    for (std::size_t i = 0; i != S.num_entries(); ++i) {
        if (S[i].type->is_character_sequence()) {
            strcpy(reinterpret_cast<char*>(cpy[i].as_p()), reinterpret_cast<char*>((*this)[i].as_p()));
            cpy.not_null(i);
        } else {
            cpy.set(i, (*this)[i]);
        }
    }
    return cpy;
}

M_LCOV_EXCL_START
void Tuple::print(std::ostream &out, const Schema &schema) const
{
    for (std::size_t i = 0; i != schema.num_entries(); ++i) {
        if (i != 0) out << ',';
        if (is_null(i))
            out << "NULL";
        else
            values_[i].print(out, *schema[i].type);
    }
}

void Tuple::dump(std::ostream &out) const { out << *this << std::endl; }
void Tuple::dump() const { dump(std::cerr); }
M_LCOV_EXCL_STOP

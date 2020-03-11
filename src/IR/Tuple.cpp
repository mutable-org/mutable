#include "IR/Tuple.hpp"

#include "catalog/Schema.hpp"
#include "catalog/Type.hpp"


using namespace db;


/*======================================================================================================================
  Value
 *====================================================================================================================*/

struct value_printer : ConstTypeVisitor
{
    private:
    std::ostream &out_;
    Value val_;

    public:
    value_printer(std::ostream &out, Value val) : out_(out), val_(val) { }

    using ConstTypeVisitor::operator();
    void operator()(Const<ErrorType>&) override { unreachable("cannot print value of erroneous type"); }
    void operator()(Const<Boolean>&) override { out_ << (val_.as<bool>() ? "TRUE" : "FALSE"); }

    void operator()(Const<CharacterSequence>&) override {
        std::string str(reinterpret_cast<char*>(val_.as_p()));
        out_ << '"' << escape(str) << '"';
    }

    void operator()(Const<Numeric> &n) override {
        switch (n.kind) {
            case Numeric::N_Int:
                out_ << val_.as_i();
                break;

            case Numeric::N_Decimal: {
                const int64_t div = powi(10L, n.scale);
                const int64_t pre = val_.as<int64_t>() / div;
                const int64_t post = val_.as<int64_t>() % div;
                out_ << pre << '.';
                auto old_fill = out_.fill('0');
                out_ << std::setw(n.scale) << post;
                out_.fill(old_fill);
                break;
            }

            case Numeric::N_Float:
                if (n.size() == 32)
                    out_ << val_.as<float>();
                else
                    out_ << val_.as<double>();
                break;
        }
    }
    void operator()(Const<FnType>&) override { unreachable("value cannot have function type"); }
};

void Value::print(std::ostream &out, const Type &ty) const
{
    value_printer(out, *this)(ty);
}

void Value::dump(std::ostream &out) const { out << *this << std::endl; }
void Value::dump() const { dump(std::cerr); }


/*======================================================================================================================
  Tuple
 *====================================================================================================================*/

Tuple::Tuple(const Schema &S)
#ifndef NDEBUG
    : num_values_(S.num_entries())
#endif
{
    std::size_t additional_bytes = 0;
    for (auto &e : S) {
        if (auto cs = cast<const CharacterSequence>(e.type))
            additional_bytes += cs->size() / 8;
    }
    values_ = (Value*) malloc(S.num_entries() * sizeof(Value) + additional_bytes);
    uint8_t *p = reinterpret_cast<uint8_t*>(values_) + S.num_entries() * sizeof(Value);
    for (std::size_t i = 0; i != S.num_entries(); ++i) {
        if (auto cs = cast<const CharacterSequence>(S[i].type)) {
            new (&values_[i]) Value(p);
            p += cs->size() / 8;
        } else {
            new (&values_[i]) Value();
        }
    }
    clear();
}

Tuple::Tuple(std::vector<const Type*> types)
#ifndef NDEBUG
    : num_values_(types.size())
#endif
{
    std::size_t additional_bytes = 0;
    for (auto &ty : types) {
        if (auto cs = cast<const CharacterSequence>(ty))
            additional_bytes += cs->size() / 8;
    }
    values_ = (Value*) malloc(types.size() * sizeof(Value) + additional_bytes);
    uint8_t *p = reinterpret_cast<uint8_t*>(values_) + types.size() * sizeof(Value);
    for (std::size_t i = 0; i != types.size(); ++i) {
        if (auto cs = cast<const CharacterSequence>(types[i])) {
            new (&values_[i]) Value(p);
            p += cs->size() / 8;
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

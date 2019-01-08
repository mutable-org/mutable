#include "catalog/Schema.hpp"

#include "util/fn.hpp"
#include <algorithm>
#include <cmath>
#include <iterator>
#include <stdexcept>


using namespace db;


static constexpr double LOG_2_OF_10 = 3.321928094887362; ///> factor to convert count of decimal digits to binary digits

constexpr const char * Numeric::KIND_TO_STR_[]; ///> declaration for constexpr static field, see C++17 inline variables

/*======================================================================================================================
 * SQL Types
 *====================================================================================================================*/

Pool<Type> Type::types_;

void Type::dump() const { dump(std::cerr); }

/*===== Factory Methods ==============================================================================================*/

const ErrorType * Type::Get_Error()
{
    static ErrorType err;
    return &err;
}

const Boolean * Type::Get_Boolean()
{
    static Boolean b;
    return &b;
}

const CharacterSequence * Type::Get_Char(std::size_t length)
{
    return static_cast<const CharacterSequence*>(types_(CharacterSequence(length, false)));
}

const CharacterSequence * Type::Get_Varchar(std::size_t length)
{
    return static_cast<const CharacterSequence*>(types_(CharacterSequence(length, true)));
}

const Numeric * Type::Get_Decimal(unsigned digits, unsigned scale)
{
    return static_cast<const Numeric*>(types_(Numeric(Numeric::N_Decimal, digits, scale)));
}

const Numeric * Type::Get_Integer(unsigned num_bytes)
{
    return static_cast<const Numeric*>(types_(Numeric(Numeric::N_Int, num_bytes, 0)));
}

const Numeric * Type::Get_Float()
{
    static Numeric f(Numeric::N_Float, 32, 0);
    return &f;
}

const Numeric * Type::Get_Double()
{
    static Numeric d(Numeric::N_Float, 64, 0);
    return &d;
}

/*===== Comparison ===================================================================================================*/

bool ErrorType::operator==(const Type &other) const
{
    return dynamic_cast<const ErrorType*>(&other) != nullptr;
}

bool Boolean::operator==(const Type &other) const
{
    return dynamic_cast<const Boolean*>(&other) != nullptr;
}

bool CharacterSequence::operator==(const Type &other) const
{
    if (auto o = dynamic_cast<const CharacterSequence*>(&other))
        return this->is_varying == o->is_varying and this->length == o->length;
    return false;
}

bool Numeric::operator==(const Type &other) const
{
    if (auto o = dynamic_cast<const Numeric*>(&other)) {
        return this->kind == o->kind and
               this->precision == o->precision and
               this->scale == o->scale;
    }
    return false;
}

/*===== Hash =========================================================================================================*/

uint64_t ErrorType::hash() const { return 0; }

uint64_t Boolean::hash() const { return 0; }

uint64_t CharacterSequence::hash() const { return uint64_t(is_varying) | uint64_t(length) << 1; }

uint64_t Numeric::hash() const { return (uint64_t(precision) << 32 | scale) * uint64_t(kind); }

/*===== Pretty Printing ==============================================================================================*/

void ErrorType::print(std::ostream &out) const { out << "[ErrorType]"; }

void Boolean::print(std::ostream &out) const { out << "BOOL"; }

void CharacterSequence::print(std::ostream &out) const
{
    out << ( is_varying ? "VARCHAR" : "CHAR" ) << '(' << length << ')';
}

void Numeric::print(std::ostream &out) const
{
    switch (kind) {
        case N_Int:
            out << "INT(" << precision << ')';
            break;

        case N_Float:
            if (precision == 32) out << "FLOAT";
            else if (precision == 64) out << "DOUBLE";
            else out << "[IllegalFloatingPoint]";
            break;

        case N_Decimal: {
            out << "DECIMAL(" << precision << ", " << scale << ')';
            break;
        }
    }
}


/*===== Dump =========================================================================================================*/

void ErrorType::dump(std::ostream &out) const { out << "[ErrorType]" << std::endl; }

void Boolean::dump(std::ostream &out) const { out << "Boolean" << std::endl; }

void CharacterSequence::dump(std::ostream &out) const
{
    out << "CharacterSequence{ is_varying = " << (is_varying ? "true" : "false") << ", length = " << length << " }"
        << std::endl;
}

void Numeric::dump(std::ostream &out) const
{
    out << "Numeric{ kind = " << Numeric::KIND_TO_STR_[kind] << ", precision = " << precision << ", scale = " << scale
        << " }" << std::endl;
}

/*======================================================================================================================
 * Attribute
 *====================================================================================================================*/

void Attribute::dump(std::ostream &out) const
{
    out << "Attribute `" << relation.name << "`.`" << name << "`, "
        << "id " << id << ", "
        << "type " << *type
        << std::endl;
}

void Attribute::dump() const { dump(std::cerr); }

/*======================================================================================================================
 * Relation
 *====================================================================================================================*/

Relation::~Relation() { }

const Attribute & Relation::operator[](std::size_t i) const
{
    return attrs_.at(i);
}

const Attribute & Relation::operator[](const char *name) const
{
    return *name_to_attr_.at(name);
}

const Attribute & Relation::push_back(const Type *type, const char *name)
{
    if (name_to_attr_.count(name)) throw std::invalid_argument("attribute with that name already exists");
    attrs_.emplace_back(Attribute(attrs_.size(), *this, type, name));
    name_to_attr_.emplace(name, std::prev(attrs_.end()));
    return attrs_.back();
}

void Relation::dump(std::ostream &out) const
{
    out << "Relation `" << name << '`';
    for (const auto &attr : attrs_)
        out << "\n` " << attr.id << ": `" << attr.name << "` " << *attr.type;
    out << std::endl;
}

void Relation::dump() const { dump(std::cerr); }

/*======================================================================================================================
 * Schema
 *====================================================================================================================*/

Schema::~Schema()
{
    for (auto &r : relations_)
        delete r.second;
}

#include "catalog/Schema.hpp"

#include "util/fn.hpp"
#include <algorithm>
#include <cmath>
#include <iterator>
#include <stdexcept>


using namespace db;


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

const FnType * Type::Get_Function(const Type *return_type, std::vector<const Type*> parameter_types)
{
    return static_cast<const FnType*>(types_(FnType(return_type, parameter_types)));
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

bool FnType::operator==(const Type &other) const
{
    if (auto o = dynamic_cast<const FnType*>(&other)) {
        if (this->return_type != o->return_type) return false; // return types must match
        if (this->parameter_types.size() != o->parameter_types.size()) return false; // parameter count must match
        for (std::size_t i = 0, end = parameter_types.size(); i != end; ++i)
            if (this->parameter_types[i] != o->parameter_types[i]) return false; // parameters must have same type
    }
    return true;
}

/*===== Hash =========================================================================================================*/

uint64_t ErrorType::hash() const { return 0; }

uint64_t Boolean::hash() const { return 0; }

uint64_t CharacterSequence::hash() const { return uint64_t(is_varying) | uint64_t(length) << 1; }

uint64_t Numeric::hash() const { return (uint64_t(precision) << 32 | scale) * uint64_t(kind); }

uint64_t FnType::hash() const
{
    auto h = return_type->hash();
    for (auto p : parameter_types)
        h = (h << 7) ^ p->hash();
    return h;
}

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

void FnType::print(std::ostream &out) const
{
    out << *return_type << '(';
    for (auto it = parameter_types.cbegin(), end = parameter_types.cend(); it != end; ++it) {
        if (it != parameter_types.cbegin()) out << ", ";
        out << **it;
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

void FnType::dump(std::ostream &out) const
{
    out << "FnType{\n    return_type: ";
    return_type->dump(out);
    out << "    parameter_types: {\n";
    for (auto p :parameter_types) {
        out << "        ";
        p->dump(out);
    }
    out << '}' << std::endl;
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

const Attribute & Relation::push_back(const Type *type, const char *name)
{
    if (name_to_attr_.count(name)) throw std::invalid_argument("attribute with that name already exists");
    name_to_attr_.emplace(name, attrs_.size());
    attrs_.emplace_back(Attribute(attrs_.size(), *this, type, name));
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

Schema::Schema(const char *name)
    : name(name)
{
}

Schema::~Schema()
{
    for (auto &r : relations_)
        delete r.second;
}

/*======================================================================================================================
 * Catalog
 *====================================================================================================================*/

Catalog::Catalog()
{
    /* Initialize standard functions. */
#define DB_FUNCTION(NAME) { \
    auto name = pool(#NAME); \
    auto res = standard_functions_.emplace(name, new Function(name, Function::FN_ ## NAME)); \
    insist(res.second, "function already defined"); \
}
#include "tables/Functions.tbl"
#undef DB_FUNCTION
}

Catalog::~Catalog()
{
    for (auto &s : schemas_)
        delete s.second;
}

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

const Boolean * Type::Get_Boolean(category_t category)
{
    static Boolean b_scalar(Type::TY_Scalar);
    static Boolean b_vector(Type::TY_Vector);
    return category == TY_Scalar ? &b_scalar : &b_vector;
}

const CharacterSequence * Type::Get_Char(category_t category, std::size_t length)
{
    return static_cast<const CharacterSequence*>(types_(CharacterSequence(category, length, false)));
}

const CharacterSequence * Type::Get_Varchar(category_t category, std::size_t length)
{
    return static_cast<const CharacterSequence*>(types_(CharacterSequence(category, length, true)));
}

const Numeric * Type::Get_Decimal(category_t category, unsigned digits, unsigned scale)
{
    return static_cast<const Numeric*>(types_(Numeric(category, Numeric::N_Decimal, digits, scale)));
}

const Numeric * Type::Get_Integer(category_t category, unsigned num_bytes)
{
    return static_cast<const Numeric*>(types_(Numeric(category, Numeric::N_Int, num_bytes, 0)));
}

const Numeric * Type::Get_Float(category_t category)
{
    static Numeric f(category, Numeric::N_Float, 32, 0);
    return &f;
}

const Numeric * Type::Get_Double(category_t category)
{
    static Numeric d(category, Numeric::N_Float, 64, 0);
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
    if (auto o = dynamic_cast<const Boolean*>(&other))
        return this->category == o->category;
    return false;
}

bool CharacterSequence::operator==(const Type &other) const
{
    if (auto o = dynamic_cast<const CharacterSequence*>(&other))
        return this->category == o->category and this->is_varying == o->is_varying and this->length == o->length;
    return false;
}

bool Numeric::operator==(const Type &other) const
{
    if (auto o = dynamic_cast<const Numeric*>(&other)) {
        return this->category == o->category and
               this->kind == o->kind and
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
        return true;
    }
    return false;
}

/*===== Hash =========================================================================================================*/

uint64_t ErrorType::hash() const { return 0; }

uint64_t Boolean::hash() const { return uint64_t(category); }

uint64_t CharacterSequence::hash() const
{
    return uint64_t(length) << 2 | uint64_t(is_varying) << 1 | uint64_t(category);
}

uint64_t Numeric::hash() const
{
    return ((uint64_t(precision) << 32) ^ (uint64_t(scale) << 3) ^ (uint64_t(kind) << 1) << uint64_t(category));
}

uint64_t FnType::hash() const
{
    auto h = return_type->hash();
    for (auto p : parameter_types)
        h = (h << 7) ^ p->hash();
    return h;
}

/*===== Scalar & Vector Conversion ===================================================================================*/

const PrimitiveType * Boolean::as_scalar() const
{
    if (is_scalar()) return this;
    return Type::Get_Boolean(TY_Scalar);
}
const PrimitiveType * Boolean::as_vectorial() const
{
    if (is_vectorial()) return this;
    return Type::Get_Boolean(TY_Vector);
}

const PrimitiveType * CharacterSequence::as_scalar() const
{
    if (is_scalar()) return this;
    return static_cast<const CharacterSequence*>(types_(CharacterSequence(TY_Scalar, length, is_varying)));
}
const PrimitiveType * CharacterSequence::as_vectorial() const
{
    if (is_vectorial()) return this;
    return static_cast<const CharacterSequence*>(types_(CharacterSequence(TY_Vector, length, is_varying)));
}

const PrimitiveType * Numeric::as_scalar() const
{
    return static_cast<const Numeric*>(types_(Numeric(TY_Scalar, kind, precision, scale)));
}
const PrimitiveType * Numeric::as_vectorial() const
{
    return static_cast<const Numeric*>(types_(Numeric(TY_Vector, kind, precision, scale)));
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
    for (auto it = parameter_types.cbegin(), end = parameter_types.cend(); it != end; ++it) {
        if (it != parameter_types.cbegin()) out << ", ";
        out << **it;
    }
    out << " -> " << *return_type;
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
 * Function
 *====================================================================================================================*/

constexpr const char * Function::FNID_TO_STR_[];
constexpr const char * Function::KIND_TO_STR_[];

void Function::dump(std::ostream &out) const
{
    out << "Function{ name = \"" << name << "\", fnid = " << FNID_TO_STR_[fnid] << ", kind = " << KIND_TO_STR_[kind]
        << "}" << std::endl;
}

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
#define DB_FUNCTION(NAME, KIND) { \
    auto name = pool(#NAME); \
    auto res = standard_functions_.emplace(name, new Function(name, Function::FN_ ## NAME, Function::KIND)); \
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

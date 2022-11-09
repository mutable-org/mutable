#include <mutable/catalog/Type.hpp>


using namespace m;



constexpr const char * Numeric::KIND_TO_STR_[]; ///> declaration for constexpr static field, see C++17 inline variables

/*======================================================================================================================
 * SQL Types
 *====================================================================================================================*/

constexpr const char *Type::CATEGORY_TO_STR_[];

Pool<Type> Type::types_;

M_LCOV_EXCL_START
void Type::dump() const { dump(std::cerr); }
M_LCOV_EXCL_STOP

const Numeric * m::arithmetic_join(const Numeric *lhs, const Numeric *rhs)
{
    static constexpr double LOG_2_OF_10 = 3.321928094887362; ///> factor to convert count of decimal digits to binary digits

    /* Combining a vector with a scalar yields a vector. */
    Type::category_t category = std::max(lhs->category, rhs->category);

    /* N_Decimal is always "more precise" than N_Float.  N_Float is always more precise than N_Int.  */
    Numeric::kind_t kind = std::max(lhs->kind, rhs->kind);

    /* Compute the precision in bits. */
    unsigned precision_lhs, precision_rhs;
    switch (lhs->kind) {
        case Numeric::N_Int:     precision_lhs = 8 * lhs->precision; break;
        case Numeric::N_Float:   precision_lhs = lhs->precision; break;
        case Numeric::N_Decimal: precision_lhs = std::ceil(LOG_2_OF_10 * lhs->precision); break;
    }
    switch (rhs->kind) {
        case Numeric::N_Int:     precision_rhs = 8 * rhs->precision; break;
        case Numeric::N_Float:   precision_rhs = rhs->precision; break;
        case Numeric::N_Decimal: precision_rhs = std::ceil(LOG_2_OF_10 * rhs->precision); break;
    }
    int precision = std::max(precision_lhs, precision_rhs);
    int scale = std::max(lhs->scale, rhs->scale);

    switch (kind) {
        case Numeric::N_Int: return Type::Get_Integer(category, precision / 8);
        case Numeric::N_Float: {
            if (precision == 32) return Type::Get_Float(category);
            M_insist(precision == 64, "Illegal floating-point precision");
            return Type::Get_Double(category);
        }

        case Numeric::N_Decimal: return Type::Get_Decimal(category, precision / LOG_2_OF_10, scale);
    }
}

/*===== Factory Methods ==============================================================================================*/

const ErrorType * Type::Get_Error()
{
    static ErrorType err;
    return &err;
}

const NoneType * Type::Get_None()
{
    static NoneType none;
    return &none;
}

const Boolean * Type::Get_Boolean(category_t category)
{
    static Boolean b_scalar(Type::TY_Scalar);
    static Boolean b_vector(Type::TY_Vector);
    return category == TY_Scalar ? &b_scalar : &b_vector;
}

const Bitmap * Type::Get_Bitmap(category_t category, std::size_t length)
{
    return static_cast<const Bitmap*>(types_(Bitmap(category, length)));
}

const CharacterSequence * Type::Get_Char(category_t category, std::size_t length)
{
    return static_cast<const CharacterSequence*>(types_(CharacterSequence(category, length, false)));
}

const CharacterSequence * Type::Get_Varchar(category_t category, std::size_t length)
{
    return static_cast<const CharacterSequence*>(types_(CharacterSequence(category, length, true)));
}

const Date * Type::Get_Date(category_t category)
{
    static Date d_scalar(Type::TY_Scalar);
    static Date d_vector(Type::TY_Vector);
    return category == TY_Scalar ? &d_scalar : &d_vector;
}

const DateTime * Type::Get_Datetime(category_t category)
{
    static DateTime dt_scalar(Type::TY_Scalar);
    static DateTime dt_vector(Type::TY_Vector);
    return category == TY_Scalar ? &dt_scalar : &dt_vector;
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
    return static_cast<const Numeric*>(types_(Numeric(category, Numeric::N_Float, 32, 0)));
}

const Numeric * Type::Get_Double(category_t category)
{
    return static_cast<const Numeric*>(types_(Numeric(category, Numeric::N_Float, 64, 0)));
}

const FnType * Type::Get_Function(const Type *return_type, std::vector<const Type*> parameter_types)
{
    return static_cast<const FnType*>(types_(FnType(return_type, parameter_types)));
}

/*===== Type visitor =================================================================================================*/

#define ACCEPT(TYPE) \
    void TYPE::accept(TypeVisitor &v) { v(*this); } \
    void TYPE::accept(ConstTypeVisitor &v) const { v(*this); }
M_TYPE_LIST(ACCEPT)
#undef ACCEPT

/*===== Comparison ===================================================================================================*/

bool ErrorType::operator==(const Type &other) const { return is<const ErrorType>(&other); }

bool NoneType::operator==(const Type &other) const { return is<const NoneType>(&other); }

bool Boolean::operator==(const Type &other) const
{
    if (auto o = cast<const Boolean>(&other))
        return this->category == o->category;
    return false;
}

bool Bitmap::operator==(const Type &other) const
{
    if (auto o = cast<const Bitmap>(&other))
        return this->category == o->category and this->length == o->length;
    return false;
}

bool CharacterSequence::operator==(const Type &other) const
{
    if (auto o = cast<const CharacterSequence>(&other))
        return this->category == o->category and this->is_varying == o->is_varying and this->length == o->length;
    return false;
}

bool Date::operator==(const Type &other) const
{
    if (auto o = cast<const Date>(&other))
        return this->category == o->category;
    return false;
}

bool DateTime::operator==(const Type &other) const
{
    if (auto o = cast<const DateTime>(&other))
        return this->category == o->category;
    return false;
}

bool Numeric::operator==(const Type &other) const
{
    if (auto o = cast<const Numeric>(&other)) {
        return this->category == o->category and
               this->kind == o->kind and
               this->precision == o->precision and
               this->scale == o->scale;
    }
    return false;
}

bool FnType::operator==(const Type &other) const
{
    if (auto o = cast<const FnType>(&other)) {
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

uint64_t NoneType::hash() const { return -1UL; }

uint64_t Boolean::hash() const { return 0b10UL | uint64_t(category); }

uint64_t Bitmap::hash() const
{
    return uint64_t(length) << 1 | uint64_t(category);
}

uint64_t CharacterSequence::hash() const
{
    return uint64_t(length) << 2 | uint64_t(is_varying) << 1 | uint64_t(category);
}

uint64_t Date::hash() const { return 0b100UL | uint64_t(category); }

uint64_t DateTime::hash() const { return 0b1000UL | uint64_t(category); }

uint64_t Numeric::hash() const
{
    return ((uint64_t(precision) << 32) ^ (uint64_t(scale) << 3) ^ (uint64_t(kind) << 1)) | uint64_t(category);
}

uint64_t FnType::hash() const
{
    auto h = return_type->hash();
    for (auto p : parameter_types)
        h = (h << 7) ^ p->hash();
    return h;
}

/*===== Scalar & Vector Conversion ===================================================================================*/

const PrimitiveType * Boolean::as_scalar() const { return Type::Get_Boolean(TY_Scalar); }

const PrimitiveType * Boolean::as_vectorial() const { return Type::Get_Boolean(TY_Vector); }

const PrimitiveType * Bitmap::as_scalar() const
{
    if (is_scalar()) return this;
    return static_cast<const Bitmap*>(types_(Bitmap(TY_Scalar, length)));
}
const PrimitiveType * Bitmap::as_vectorial() const
{
    if (is_vectorial()) return this;
    return static_cast<const Bitmap*>(types_(Bitmap(TY_Vector, length)));
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

const PrimitiveType * Date::as_scalar() const { return Type::Get_Date(TY_Scalar); }

const PrimitiveType * Date::as_vectorial() const { return Type::Get_Date(TY_Vector); }

const PrimitiveType * DateTime::as_scalar() const { return Type::Get_Datetime(TY_Scalar); }

const PrimitiveType * DateTime::as_vectorial() const { return Type::Get_Datetime(TY_Vector); }

const PrimitiveType * Numeric::as_scalar() const
{
    return static_cast<const Numeric*>(types_(Numeric(TY_Scalar, kind, precision, scale)));
}
const PrimitiveType * Numeric::as_vectorial() const
{
    return static_cast<const Numeric*>(types_(Numeric(TY_Vector, kind, precision, scale)));
}

/*===== Pretty Printing ==============================================================================================*/

M_LCOV_EXCL_START
void ErrorType::print(std::ostream &out) const { out << "[ErrorType]"; }

void NoneType::print(std::ostream &out) const { out << "[none]"; }

void Boolean::print(std::ostream &out) const { out << "BOOL"; }

void Bitmap::print(std::ostream &out) const { out << "BITMAP" << '(' << length << ')'; }

void CharacterSequence::print(std::ostream &out) const
{
    out << ( is_varying ? "VARCHAR" : "CHAR" ) << '(' << length << ')';
}

void Date::print(std::ostream &out) const { out << "DATE"; }

void DateTime::print(std::ostream &out) const { out << "DATETIME"; }

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
    out << '(';
    for (auto it = parameter_types.cbegin(), end = parameter_types.cend(); it != end; ++it) {
        if (it != parameter_types.cbegin()) out << ", ";
        out << **it;
    }
    out << ") -> " << *return_type;
}
M_LCOV_EXCL_STOP

/*===== Dump =========================================================================================================*/

M_LCOV_EXCL_START
void ErrorType::dump(std::ostream &out) const { out << "[ErrorType]" << std::endl; }

void NoneType::dump(std::ostream &out) const { out << "[NoneType]" << std::endl; }

void Boolean::dump(std::ostream &out) const
{
    out << "Boolean{ category = " << CATEGORY_TO_STR_[category] << " }" << std::endl;
}

void Bitmap::dump(std::ostream &out) const
{
    out << "Bitmap{ category = " << CATEGORY_TO_STR_[category] << ", length = " << length << " }" << std::endl;
}

void CharacterSequence::dump(std::ostream &out) const
{
    out << "CharacterSequence{ category = " << CATEGORY_TO_STR_[category] << ", is_varying = "
        << (is_varying ? "true" : "false") << ", length = " << length << " }" << std::endl;
}

void Date::dump(std::ostream &out) const
{
    out << "Date{ category = " << CATEGORY_TO_STR_[category] << " }" << std::endl;
}

void DateTime::dump(std::ostream &out) const
{
    out << "DateTime{ category = " << CATEGORY_TO_STR_[category] << " }" << std::endl;
}

void Numeric::dump(std::ostream &out) const
{
    out << "Numeric{ category = " << CATEGORY_TO_STR_[category] << ", kind = " << Numeric::KIND_TO_STR_[kind]
        << ", precision = " << precision << ", scale = " << scale << " }" << std::endl;
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
M_LCOV_EXCL_STOP

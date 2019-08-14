#pragma once


#include "util/fn.hpp"
#include "util/macro.hpp"
#include "util/Pool.hpp"
#include <exception>
#include <functional>


namespace db {

struct ErrorType;
struct PrimitiveType;
struct Boolean;
struct CharacterSequence;
struct Numeric;
struct FnType;

/** This class represents types in the SQL type system. */
struct Type
{
#define category_t(X) X(TY_Scalar), X(TY_Vector)
    DECLARE_ENUM(category_t); ///< a category for whether this type is scalar or vector
    protected:
    static constexpr const char *CATEGORY_TO_STR_[] = { ENUM_TO_STR(category_t) };
#undef category_t

    protected:
    static Pool<Type> types_; ///< a pool of parameterized types

    public:
    Type() = default;
    Type(const Type&) = delete;
    Type(Type&&) = default;
    virtual ~Type() { }

    virtual bool operator==(const Type &other) const = 0;
    bool operator!=(const Type &other) const { return not operator==(other); }

    bool is_error() const { return (void*) this == Get_Error(); }
    bool is_primitive() const { return is<const PrimitiveType>(this); }
    bool is_boolean() const { return is<const Boolean>(this); }
    bool is_character_sequence() const { return is<const CharacterSequence>(this); }
    bool is_numeric() const { return is<const Numeric>(this); }

    /** Compute the size in bits of an instance of this type. */
    virtual uint32_t size() const { throw std::logic_error("the size of this type is not defined"); }

    /** Compute the alignment requirement in bits of an instance of this type. */
    virtual uint32_t alignment() const { throw std::logic_error("the size of this type is not defined"); }

    virtual uint64_t hash() const = 0;

    virtual void print(std::ostream &out) const = 0;
    virtual void dump(std::ostream &out) const = 0;
    void dump() const;

    friend std::ostream & operator<<(std::ostream &out, const Type &t) {
        t.print(out);
        return out;
    }

    /* Type factory methods */
    static const ErrorType * Get_Error();
    static const Boolean * Get_Boolean(category_t category);
    static const CharacterSequence * Get_Char(category_t category, std::size_t length);
    static const CharacterSequence * Get_Varchar(category_t category, std::size_t length);
    static const Numeric * Get_Decimal(category_t category, unsigned digits, unsigned scale);
    static const Numeric * Get_Integer(category_t category, unsigned num_bytes);
    static const Numeric * Get_Float(category_t category);
    static const Numeric * Get_Double(category_t category);
    static const FnType * Get_Function(const Type *return_type, std::vector<const Type*> parameter_types);
};

}

namespace std {

template<>
struct hash<db::Type>
{
    uint64_t operator()(const db::Type &type) const { return type.hash(); }
};

}

namespace db {

/** Primitive types are used for values. */
struct PrimitiveType : Type
{
    category_t category; ///< whether this type is scalar or vector

    PrimitiveType(category_t category) : category(category) { }
    PrimitiveType(const PrimitiveType&) = delete;
    PrimitiveType(PrimitiveType&&) = default;
    virtual ~PrimitiveType() { }

    bool is_scalar() const { return category == TY_Scalar; }
    bool is_vectorial() const { return category == TY_Vector; }

    /** Convert this type to a scalar. */
    virtual const PrimitiveType *as_scalar() const = 0;

    /** Convert this type to a vectorial. */
    virtual const PrimitiveType *as_vectorial() const = 0;
};

/** The error type.  Used when parsing of a data type fails or when semantic analysis detects a type error. */
struct ErrorType: Type
{
    friend struct Type;

    private:
    ErrorType() { }

    public:
    ErrorType(ErrorType&&) = default;

    bool operator==(const Type &other) const;

    uint64_t hash() const;

    void print(std::ostream &out) const;
    void dump(std::ostream &out) const;
};

/** The boolean type. */
struct Boolean : PrimitiveType
{
    friend struct Type;

    private:
    Boolean(category_t category) : PrimitiveType(category) { }

    public:
    Boolean(Boolean&&) = default;

    bool operator==(const Type &other) const;

    uint32_t size() const { return 1; }
    uint32_t alignment() const { return 1; }

    uint64_t hash() const;

    void print(std::ostream &out) const;
    void dump(std::ostream &out) const;

    virtual const PrimitiveType *as_scalar() const;
    virtual const PrimitiveType *as_vectorial() const;
};

/** The type of character strings, both fixed length and varying. */
struct CharacterSequence : PrimitiveType
{
    friend struct Type;

    std::size_t length; ///> the maximum length of the string in bytes
    bool is_varying; ///> true if varying, false otherwise; corresponds to Char(N) and Varchar(N)

    private:
    CharacterSequence(category_t category, std::size_t length, bool is_varying)
        : PrimitiveType(category)
        , length(length)
        , is_varying(is_varying)
    { }

    public:
    CharacterSequence(CharacterSequence&&) = default;

    bool operator==(const Type &other) const;

    uint32_t size() const {
        if (is_varying)
            return 8 * sizeof(char*);
        else
            return 8 * length;
    }

    uint32_t alignment() const { return is_varying ? 8 * sizeof(const char*) : 8 * sizeof(char); }

    uint64_t hash() const;

    void print(std::ostream &out) const;
    void dump(std::ostream &out) const;

    virtual const PrimitiveType *as_scalar() const;
    virtual const PrimitiveType *as_vectorial() const;
};

/** The numeric type represents integer and floating-point types of different precision, and scale. */
struct Numeric : PrimitiveType
{
    friend struct Type;

    /** The maximal number of decimal digits that can be accurately represented by DECIMAL(p,s). */
    static constexpr std::size_t MAX_DECIMAL_PRECISION = 19;

    static constexpr float DECIMAL_TO_BINARY_DIGITS = 3.32192f;

#define kind_t(X) X(N_Int), X(N_Float), X(N_Decimal)
    DECLARE_ENUM(kind_t) kind; ///> the kind of numeric type
    private:
    static constexpr const char *KIND_TO_STR_[] = { ENUM_TO_STR(kind_t) };
#undef kind_t
    public:
    /** The precision gives the maximum number of digits that can be represented by that type.  Its interpretation
     * depends on the kind:
     *  For INT, precision is the number of bytes.
     *  For FLOAT and DOUBLE, precision is the size of the type in bits, i.e. 32 and 64, respectively.
     *  For DECIMAL, precision is the number of decimal digits that can be represented.
     */
    unsigned precision; ///> the number of bits used to represent the number
    unsigned scale; ///> the number of decimal digits right of the decimal point

    private:
    Numeric(category_t category, kind_t kind, unsigned precision, unsigned scale)
        : PrimitiveType(category)
        , kind(kind)
        , precision(precision)
        , scale(scale)
    { }

    public:
    Numeric(Numeric&&) = default;

    bool operator==(const Type &other) const;

    uint32_t size() const {
        switch (kind) {
            case N_Int: return 8 * precision;
            case N_Float: return precision;
            case N_Decimal: return ceil_to_pow_2(uint32_t(std::ceil(DECIMAL_TO_BINARY_DIGITS * precision)));
        }
        unreachable("illegal kind");
    }

    uint32_t alignment() const { return size(); }

    uint64_t hash() const;

    void print(std::ostream &out) const;
    void dump(std::ostream &out) const;

    virtual const PrimitiveType *as_scalar() const;
    virtual const PrimitiveType *as_vectorial() const;
};

/** The function type defines the type and count of the arguments and the type of the return value of a SQL function. */
struct FnType : Type
{
    friend struct Type;

    const Type *return_type; ///> the type of the return value
    std::vector<const Type *> parameter_types; ///> the types of the parameters

    private:
    FnType(const Type *return_type, std::vector<const Type*> parameter_types)
        : return_type(notnull(return_type))
        , parameter_types(parameter_types)
    { }

    public:
    FnType(FnType&&) = default;

    bool operator==(const Type &other) const;

    uint64_t hash() const;

    void print(std::ostream &out) const;
    void dump(std::ostream &out) const;
};

}

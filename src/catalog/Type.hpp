#pragma once

#include "util/fn.hpp"
#include "util/macro.hpp"
#include "util/Pool.hpp"
#include <exception>
#include <functional>
#include <vector>


namespace db {

struct ErrorType;
struct NoneType;
struct PrimitiveType;
struct Boolean;
struct CharacterSequence;
struct Numeric;
struct FnType;

// forward declare the Type visitor
template<bool C>
struct TheTypeVisitor;
using TypeVisitor = TheTypeVisitor<false>;
using ConstTypeVisitor = TheTypeVisitor<true>;

/** This class represents types in the SQL type system. */
struct Type
{
#define category_t(X) X(TY_Scalar), X(TY_Vector)
    DECLARE_ENUM(category_t); ///< a category for whether this type is *scalar* or *vectorial*
    protected:
    static constexpr const char *CATEGORY_TO_STR_[] = { ENUM_TO_STR(category_t) };
#undef category_t

    protected:
    static Pool<Type> types_; ///< a pool of internalized, parameterized types

    public:
    Type() = default;
    Type(const Type&) = delete;
    Type(Type&&) = default;
    virtual ~Type() { }

    virtual void accept(TypeVisitor &v) = 0;
    virtual void accept(ConstTypeVisitor &v) const = 0;

    virtual bool operator==(const Type &other) const = 0;
    bool operator!=(const Type &other) const { return not operator==(other); }

    bool is_error() const { return (void*) this == Get_Error(); }
    bool is_none() const { return (void*) this == Get_None(); }
    /** Returns `true` iff this `Type` is a `db::PrimitiveType`. */
    bool is_primitive() const { return is<const PrimitiveType>(this); }
    bool is_boolean() const { return is<const Boolean>(this); }
    bool is_character_sequence() const { return is<const CharacterSequence>(this); }
    /** Returns `true` iff this `Type` is a `db::Numeric` type. */
    bool is_numeric() const { return is<const Numeric>(this); }
    bool is_integral() const;
    bool is_decimal() const;
    /** Returns `true` iff this type is a floating-point type, i.e.\ `f32` or `f64`. */
    bool is_floating_point() const;
    /** Returns `true` iff this type is a 32 bit floating-point type. */
    bool is_float() const;
    /** Returns `true` iff this type is a 64 bit floating-point type. */
    bool is_double() const;

    /** Compute the size in bits of an instance of this type. */
    virtual uint32_t size() const { throw std::logic_error("the size of this type is not defined"); }

    /** Compute the alignment requirement in bits of an instance of this type. */
    virtual uint32_t alignment() const { throw std::logic_error("the size of this type is not defined"); }

    /** Compute the 64 bit hash of this `Type`. */
    virtual uint64_t hash() const = 0;

    /** Print a textual representation of this `Type` to `out`. */
    virtual void print(std::ostream &out) const = 0;

    virtual void dump(std::ostream &out) const = 0;
    void dump() const;

    /** Print a textual representation of `Type` `t` to `out`. */
    friend std::ostream & operator<<(std::ostream &out, const Type &t) {
        t.print(out);
        return out;
    }

    /*----- Type factory methods -------------------------------------------------------------------------------------*/
    /** Returns a `db::ErrorType`. */
    static const ErrorType * Get_Error();
    /** Returns a `db::NoneType`. */
    static const NoneType * Get_None();
    /** Returns a `db::Boolean` type of the given `category`. */
    static const Boolean * Get_Boolean(category_t category);
    /** Returns a `db::CharacterSequence` type of the given `category` and fixed `length`. */
    static const CharacterSequence * Get_Char(category_t category, std::size_t length);
    /** Returns a `db::CharacterSequence` type of the given `category` and varying `length`. */
    static const CharacterSequence * Get_Varchar(category_t category, std::size_t length);
    /** Returns a `db::Numeric` type for decimals of given `category`, decimal `digits`, and `scale`. */
    static const Numeric * Get_Decimal(category_t category, unsigned digits, unsigned scale);
    /** Returns a `db::Numeric` type for integrals of given `category` and `num_bytes` bytes. */
    static const Numeric * Get_Integer(category_t category, unsigned num_bytes);
    /** Returns a `db::Numeric` type of given `category` for 32 bit floating-points. */
    static const Numeric * Get_Float(category_t category);
    /** Returns a `db::Numeric` type of given `category` for 64 bit floating-points. */
    static const Numeric * Get_Double(category_t category);
    /** Returns a `db::FnType` for a function with parameter types `parameter_types` and return type `return_type`. */
    static const FnType * Get_Function(const Type *return_type, std::vector<const Type*> parameter_types);
};

template<typename T>
bool is_convertible(const Type *attr);

}

namespace std {

template<>
struct hash<db::Type>
{
    uint64_t operator()(const db::Type &type) const { return type.hash(); }
};

}

namespace db {

/** `PrimitiveType`s represent `db::Type`s of values. */
struct PrimitiveType : Type
{
    category_t category; ///< whether this type is scalar or vector

    PrimitiveType(category_t category) : category(category) { }
    PrimitiveType(const PrimitiveType&) = delete;
    PrimitiveType(PrimitiveType&&) = default;
    virtual ~PrimitiveType() { }

    /** Returns `true` iff this `PrimitiveType` is *scalar*, i.e.\ if it is for a single value. */
    bool is_scalar() const { return category == TY_Scalar; }
    /** Returns `true` iff this `PrimitiveType` is *vectorial*, i.e.\ if it is for a sequence of values. */
    bool is_vectorial() const { return category == TY_Vector; }

    /** Convert this `PrimitiveType` to its *scalar* equivalent. */
    virtual const PrimitiveType * as_scalar() const = 0;

    /** Convert this `PrimitiveType` to its *vectorial* equivalent. */
    virtual const PrimitiveType * as_vectorial() const = 0;
};

/** This `db::Type` is assigned when parsing of a data type fails or when semantic analysis detects a type error. */
struct ErrorType: Type
{
    friend struct Type;

    private:
    ErrorType() { }

    public:
    ErrorType(ErrorType&&) = default;

    void accept(TypeVisitor &v) override;
    void accept(ConstTypeVisitor &v) const override;

    bool operator==(const Type &other) const override;

    uint64_t hash() const override;

    void print(std::ostream &out) const override;
    using Type::dump;
    void dump(std::ostream &out) const override;
};

/** A `db::Type` that represents the absence of any other type.  Used to represent the type of `NULL`. */
struct NoneType: Type
{
    friend struct Type;

    private:
    NoneType() { }

    public:
    NoneType(NoneType&&) = default;

    void accept(TypeVisitor &v) override;
    void accept(ConstTypeVisitor &v) const override;

    bool operator==(const Type &other) const override;

    uint64_t hash() const override;

    void print(std::ostream &out) const override;
    using Type::dump;
    void dump(std::ostream &out) const override;
};

/** The boolean type. */
struct Boolean : PrimitiveType
{
    friend struct Type;

    private:
    Boolean(category_t category) : PrimitiveType(category) { }

    public:
    Boolean(Boolean&&) = default;

    void accept(TypeVisitor &v) override;
    void accept(ConstTypeVisitor &v) const override;

    bool operator==(const Type &other) const override;

    uint32_t size() const override { return 1; }
    uint32_t alignment() const override { return 1; }

    uint64_t hash() const override;

    void print(std::ostream &out) const override;
    using Type::dump;
    void dump(std::ostream &out) const override;

    virtual const PrimitiveType *as_scalar() const override;
    virtual const PrimitiveType *as_vectorial() const override;
};

/** The type of character strings, both fixed length and varying length. */
struct CharacterSequence : PrimitiveType
{
    friend struct Type;

    std::size_t length; ///< the maximum length of the string in bytes
    bool is_varying; ///< true if varying, false otherwise; corresponds to Char(N) and Varchar(N)

    private:
    CharacterSequence(category_t category, std::size_t length, bool is_varying)
        : PrimitiveType(category)
        , length(length)
        , is_varying(is_varying)
    { }

    public:
    CharacterSequence(CharacterSequence&&) = default;

    void accept(TypeVisitor &v) override;
    void accept(ConstTypeVisitor &v) const override;

    bool operator==(const Type &other) const override;

    /** Returns the number of bits required to store a sequence of `length` many characters and a terminating NUL byte.
     */
    uint32_t size() const override { return 8 * (length + 1); }

    uint32_t alignment() const override { return 8; }

    uint64_t hash() const override;

    void print(std::ostream &out) const override;
    using Type::dump;
    void dump(std::ostream &out) const override;

    virtual const PrimitiveType *as_scalar() const override;
    virtual const PrimitiveType *as_vectorial() const override;
};

/** The numeric type represents integer and floating-point types of different precision and scale. */
struct Numeric : PrimitiveType
{
    friend struct Type;

    /** The maximal number of decimal digits that can be accurately represented by DECIMAL(p,s). */
    static constexpr std::size_t MAX_DECIMAL_PRECISION = 19;

    /** How many binary digits fit into a single decimal digit.  Used to compute precision. */
    static constexpr float DECIMAL_TO_BINARY_DIGITS = 3.32192f;

#define kind_t(X) X(N_Int), X(N_Float), X(N_Decimal)
    DECLARE_ENUM(kind_t) kind; ///< the kind of numeric type
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
    unsigned precision; ///< the number of bits used to represent the number
    unsigned scale; ///< the number of decimal digits right of the decimal point

    private:
    Numeric(category_t category, kind_t kind, unsigned precision, unsigned scale)
        : PrimitiveType(category)
        , kind(kind)
        , precision(precision)
        , scale(scale)
    { }

    public:
    Numeric(Numeric&&) = default;

    void accept(TypeVisitor &v) override;
    void accept(ConstTypeVisitor &v) const override;

    bool operator==(const Type &other) const override;

    uint32_t size() const override {
        switch (kind) {
            case N_Int: return 8 * precision;
            case N_Float: return precision;
            case N_Decimal: return ceil_to_pow_2(uint32_t(std::ceil(DECIMAL_TO_BINARY_DIGITS * precision)));
        }
        unreachable("illegal kind");
    }

    uint32_t alignment() const override { return size(); }

    uint64_t hash() const override;

    void print(std::ostream &out) const override;
    using Type::dump;
    void dump(std::ostream &out) const override;

    virtual const PrimitiveType *as_scalar() const override;
    virtual const PrimitiveType *as_vectorial() const override;
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

    void accept(TypeVisitor &v) override;
    void accept(ConstTypeVisitor &v) const override;

    bool operator==(const Type &other) const override;

    uint64_t hash() const override;

    void print(std::ostream &out) const override;
    using Type::dump;
    void dump(std::ostream &out) const override;
};

/* Given two `db::Numeric` types, compute the `db::Numeric` type that is at least as precise as either of them. */
const Numeric * arithmetic_join(const Numeric *lhs, const Numeric *rhs);

}

inline bool db::Type::is_integral() const {
    if (auto n = cast<const Numeric>(this))
        return n->kind == Numeric::N_Int;
    return false;
}

inline bool db::Type::is_decimal() const {
    if (auto n = cast<const Numeric>(this))
        return n->kind == Numeric::N_Decimal;
    return false;
}

inline bool db::Type::is_floating_point() const {
    if (auto n = cast<const Numeric>(this))
        return n->kind == Numeric::N_Float;
    return false;
}

inline bool db::Type::is_float() const {
    if (auto n = cast<const Numeric>(this))
        return n->kind == Numeric::N_Float and n->precision == 32;
    return false;
}

inline bool db::Type::is_double() const {
    if (auto n = cast<const Numeric>(this))
        return n->kind == Numeric::N_Float and n->precision == 64;
    return false;
}

template<typename T>
bool db::is_convertible(const Type *ty) {
    /* Boolean */
    if constexpr (std::is_same_v<T, bool>)
        return is<const Boolean>(ty);

    /* CharacterSequence */
    if constexpr (std::is_same_v<T, std::string>)
        return is<const CharacterSequence>(ty);

    /* Numeric */
    if constexpr (std::is_arithmetic_v<T>)
        return is<const Numeric>(ty);

    return false;
}

namespace db {

/** The `db::Type` visitor. */
template<bool C>
struct TheTypeVisitor
{
    static constexpr bool is_constant = C;

    template<typename T>
    using Const = std::conditional_t<is_constant, const T, T>;

    virtual ~TheTypeVisitor() { }

    /* Expressions */
    void operator()(Const<Type> &ty) { ty.accept(*this); }
    virtual void operator()(Const<ErrorType> &ty) = 0;
    virtual void operator()(Const<Boolean> &ty) = 0;
    virtual void operator()(Const<CharacterSequence> &ty) = 0;
    virtual void operator()(Const<Numeric> &ty) = 0;
    virtual void operator()(Const<FnType> &ty) = 0;
};

}

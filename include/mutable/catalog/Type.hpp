#pragma once

#include <exception>
#include <functional>
#include <mutable/util/fn.hpp>
#include <mutable/util/macro.hpp>
#include <mutable/util/Pool.hpp>
#include <mutable/util/Visitor.hpp>
#include <vector>


namespace m {

struct ErrorType;
struct NoneType;
struct PrimitiveType;
struct Boolean;
struct Bitmap;
struct CharacterSequence;
struct Date;
struct DateTime;
struct Numeric;
struct FnType;

// forward declare the Type visitor
struct TypeVisitor;
struct ConstTypeVisitor;

/** This class represents types in the SQL type system. */
struct M_EXPORT Type
{
#define category_t(X) X(TY_Scalar) X(TY_Vector)
    M_DECLARE_ENUM(category_t); ///< a category for whether this type is *scalar* or *vectorial*
    protected:
    static constexpr const char *CATEGORY_TO_STR_[] = { M_ENUM_TO_STR(category_t) };
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
    /** Returns `true` iff this `Type` is a `PrimitiveType`. */
    bool is_primitive() const { return is<const PrimitiveType>(this); }
    bool is_boolean() const { return is<const Boolean>(this); }
    bool is_bitmap() const { return is<const Bitmap>(this); }
    bool is_character_sequence() const { return is<const CharacterSequence>(this); }
    bool is_date() const { return is<const Date>(this); }
    bool is_date_time() const { return is<const DateTime>(this); }
    /** Returns `true` iff this `Type` is a `Numeric` type. */
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
    virtual uint64_t size() const { throw std::logic_error("the size of this type is not defined"); }

    /** Compute the alignment requirement in bits of an instance of this type. */
    virtual uint64_t alignment() const { throw std::logic_error("the size of this type is not defined"); }

    /** Compute the 64 bit hash of this `Type`. */
    virtual uint64_t hash() const = 0;

    /** Print a textual representation of this `Type` to `out`. */
    virtual void print(std::ostream &out) const = 0;

    virtual void dump(std::ostream &out) const = 0;
    void dump() const;

    /** Print a textual representation of `Type` `t` to `out`. */
M_LCOV_EXCL_START
    friend std::ostream & operator<<(std::ostream &out, const Type &t) {
        t.print(out);
        return out;
    }
M_LCOV_EXCL_STOP

    /*----- Type factory methods -------------------------------------------------------------------------------------*/
    /** Returns a `ErrorType`. */
    static const ErrorType * Get_Error();
    /** Returns a `NoneType`. */
    static const NoneType * Get_None();
    /** Returns a `Boolean` type of the given `category`. */
    static const Boolean * Get_Boolean(category_t category);
    /** Returns a `Bitmap` type of the given `category` and `length`. */
    static const Bitmap * Get_Bitmap(category_t category, std::size_t length);
    /** Returns a `CharacterSequence` type of the given `category` and fixed `length`. */
    static const CharacterSequence * Get_Char(category_t category, std::size_t length);
    /** Returns a `CharacterSequence` type of the given `category` and varying `length`. */
    static const CharacterSequence * Get_Varchar(category_t category, std::size_t length);
    /** Returns a `Date` type of the given `category`. */
    static const Date * Get_Date(category_t category);
    /** Returns a `DateTime` type of the given `category`. */
    static const DateTime * Get_Datetime(category_t category);
    /** Returns a `Numeric` type for decimals of given `category`, decimal `digits`, and `scale`. */
    static const Numeric * Get_Decimal(category_t category, unsigned digits, unsigned scale);
    /** Returns a `Numeric` type for integrals of given `category` and `num_bytes` bytes. */
    static const Numeric * Get_Integer(category_t category, unsigned num_bytes);
    /** Returns a `Numeric` type of given `category` for 32 bit floating-points. */
    static const Numeric * Get_Float(category_t category);
    /** Returns a `Numeric` type of given `category` for 64 bit floating-points. */
    static const Numeric * Get_Double(category_t category);
    /** Returns a `FnType` for a function with parameter types `parameter_types` and return type `return_type`. */
    static const FnType * Get_Function(const Type *return_type, std::vector<const Type*> parameter_types);
};

template<typename T>
bool M_EXPORT is_convertible(const Type *attr);

/** Returns true iff both types have the same `PrimitiveType`, i.e. `Boolean`, `CharacterSequence`, `Date`, `DateTime`,
 * or `Numeric`. */
bool M_EXPORT is_comparable(const Type *first, const Type *second);

template<typename T>
const PrimitiveType * M_EXPORT get_runtime_type();

}

namespace std {

    template<>
    struct hash<m::Type>
    {
        uint64_t operator()(const m::Type &type) const { return type.hash(); }
    };

}

namespace m {

/** `PrimitiveType`s represent `Type`s of values. */
struct M_EXPORT PrimitiveType : Type
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

/** This `Type` is assigned when parsing of a data type fails or when semantic analysis detects a type error. */
struct M_EXPORT ErrorType: Type
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

/** A `Type` that represents the absence of any other type.  Used to represent the type of `NULL`. */
struct M_EXPORT NoneType: Type
{
    friend struct Type;

    private:
    NoneType() { }

    public:
    NoneType(NoneType&&) = default;

    void accept(TypeVisitor &v) override;
    void accept(ConstTypeVisitor &v) const override;

    bool operator==(const Type &other) const override;

    uint64_t size() const override { return 0; }
    uint64_t alignment() const override { return 1; }

    uint64_t hash() const override;

    void print(std::ostream &out) const override;
    using Type::dump;
    void dump(std::ostream &out) const override;
};

/** The boolean type. */
struct M_EXPORT Boolean : PrimitiveType
{
    friend struct Type;

    private:
    Boolean(category_t category) : PrimitiveType(category) { }

    public:
    Boolean(Boolean&&) = default;

    void accept(TypeVisitor &v) override;
    void accept(ConstTypeVisitor &v) const override;

    bool operator==(const Type &other) const override;

    uint64_t size() const override { return 1; }
    uint64_t alignment() const override { return 1; }

    uint64_t hash() const override;

    void print(std::ostream &out) const override;
    using Type::dump;
    void dump(std::ostream &out) const override;

    virtual const PrimitiveType *as_scalar() const override;
    virtual const PrimitiveType *as_vectorial() const override;
};

/** The bitmap type. */
struct M_EXPORT Bitmap : PrimitiveType
{
    friend struct Type;

    uint64_t length; ///< the number of elements

    private:
    Bitmap(category_t category, uint64_t length) : PrimitiveType(category), length(length) { }

    public:
    Bitmap(Bitmap&&) = default;

    void accept(TypeVisitor &v) override;
    void accept(ConstTypeVisitor &v) const override;

    bool operator==(const Type &other) const override;

    uint64_t size() const override { return length; }
    uint64_t alignment() const override { return 1; }

    uint64_t hash() const override;

    void print(std::ostream &out) const override;
    using Type::dump;
    void dump(std::ostream &out) const override;

    virtual const PrimitiveType *as_scalar() const override;
    virtual const PrimitiveType *as_vectorial() const override;
};

/** The type of character strings, both fixed length and varying length. */
struct M_EXPORT CharacterSequence : PrimitiveType
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

    /** Returns the number of bits required to store a sequence of `length` many characters.  For VARCHAR(N), a NUL byte
     * is appended.  For CHAR(N), all trailing places up to N-1 are filled with NUL bytes.  A CHAR(N) with a string of
     * length N has no terminating NUL byte.  */
    uint64_t size() const override {
        if (is_varying)
            return 8 * (length + 1);
        else
            return 8 * length;
    }

    uint64_t alignment() const override { return 8; }

    uint64_t hash() const override;

    void print(std::ostream &out) const override;
    using Type::dump;
    void dump(std::ostream &out) const override;

    virtual const PrimitiveType *as_scalar() const override;
    virtual const PrimitiveType *as_vectorial() const override;
};

/** The date type. */
struct M_EXPORT Date : PrimitiveType
{
    friend struct Type;

    private:
    Date(category_t category) : PrimitiveType(category) { }

    public:
    Date(Date&&) = default;

    void accept(TypeVisitor &v) override;
    void accept(ConstTypeVisitor &v) const override;

    bool operator==(const Type &other) const override;

    uint64_t size() const override { return 32; }
    uint64_t alignment() const override { return 32; }

    uint64_t hash() const override;

    void print(std::ostream &out) const override;
    using Type::dump;
    void dump(std::ostream &out) const override;

    virtual const PrimitiveType *as_scalar() const override;
    virtual const PrimitiveType *as_vectorial() const override;
};

/** The date type. */
struct M_EXPORT DateTime : PrimitiveType
{
    friend struct Type;

    private:
    DateTime(category_t category) : PrimitiveType(category) { }

    public:
    DateTime(DateTime&&) = default;

    void accept(TypeVisitor &v) override;
    void accept(ConstTypeVisitor &v) const override;

    bool operator==(const Type &other) const override;

    uint64_t size() const override { return 64; }
    uint64_t alignment() const override { return 64; }

    uint64_t hash() const override;

    void print(std::ostream &out) const override;
    using Type::dump;
    void dump(std::ostream &out) const override;

    virtual const PrimitiveType *as_scalar() const override;
    virtual const PrimitiveType *as_vectorial() const override;
};

/** The numeric type represents integer and floating-point types of different precision and scale. */
struct M_EXPORT Numeric : PrimitiveType
{
    friend struct Type;

    /** The maximal number of decimal digits that can be accurately represented by DECIMAL(p,s). */
    static constexpr std::size_t MAX_DECIMAL_PRECISION = 19;

    /** How many binary digits fit into a single decimal digit.  Used to compute precision. */
    static constexpr float DECIMAL_TO_BINARY_DIGITS = 3.32192f;

#define kind_t(X) X(N_Int) X(N_Float) X(N_Decimal)
    M_DECLARE_ENUM(kind_t) kind; ///< the kind of numeric type
private:
    static constexpr const char *KIND_TO_STR_[] = { M_ENUM_TO_STR(kind_t) };
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

    uint64_t size() const override {
        switch (kind) {
            case N_Int: return 8 * precision;
            case N_Float: return precision;
            case N_Decimal: return ceil_to_pow_2(uint32_t(std::ceil(DECIMAL_TO_BINARY_DIGITS * precision)));
        }
        M_unreachable("illegal kind");
    }

    uint64_t alignment() const override { return size(); }

    uint64_t hash() const override;

    void print(std::ostream &out) const override;
    using Type::dump;
    void dump(std::ostream &out) const override;

    virtual const PrimitiveType *as_scalar() const override;
    virtual const PrimitiveType *as_vectorial() const override;
};

/** The function type defines the type and count of the arguments and the type of the return value of a SQL function. */
struct M_EXPORT FnType : Type
{
    friend struct Type;

    const Type *return_type; ///> the type of the return value
    std::vector<const Type *> parameter_types; ///> the types of the parameters

    private:
    FnType(const Type *return_type, std::vector<const Type*> parameter_types)
            : return_type(M_notnull(return_type))
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

/* Given two `Numeric` types, compute the `Numeric` type that is at least as precise as either of them. */
const Numeric * arithmetic_join(const Numeric *lhs, const Numeric *rhs);

#define M_TYPE_LIST(X) \
    X(ErrorType) \
    X(NoneType) \
    X(Boolean) \
    X(Bitmap) \
    X(CharacterSequence) \
    X(Date) \
    X(DateTime) \
    X(Numeric) \
    X(FnType)

M_DECLARE_VISITOR(TypeVisitor, Type, M_TYPE_LIST)
M_DECLARE_VISITOR(ConstTypeVisitor, const Type, M_TYPE_LIST)

inline bool Type::is_integral() const {
    if (auto n = cast<const Numeric>(this))
        return n->kind == Numeric::N_Int;
    return false;
}

inline bool Type::is_decimal() const {
    if (auto n = cast<const Numeric>(this))
        return n->kind == Numeric::N_Decimal;
    return false;
}

inline bool Type::is_floating_point() const {
    if (auto n = cast<const Numeric>(this))
        return n->kind == Numeric::N_Float;
    return false;
}

inline bool Type::is_float() const {
    if (auto n = cast<const Numeric>(this))
        return n->kind == Numeric::N_Float and n->precision == 32;
    return false;
}

inline bool Type::is_double() const {
    if (auto n = cast<const Numeric>(this))
        return n->kind == Numeric::N_Float and n->precision == 64;
    return false;
}

template<typename T>
bool is_convertible(const Type *ty) {
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

inline bool is_comparable(const Type *first, const Type *second) {
    if (first->is_boolean() and second->is_boolean()) return true;
    if (first->is_character_sequence() and second->is_character_sequence()) return true;
    if (first->is_date() and second->is_date()) return true;
    if (first->is_date_time() and second->is_date_time()) return true;
    if (first->is_numeric() and second->is_numeric()) return true;
    return false;
}


/** Returns the internal runtime `Type` of mu*t*able for the compile-time type `T`. */
template<typename T>
const PrimitiveType * get_runtime_type()
{
    if constexpr (std::is_integral_v<T>)
        return Type::Get_Integer(Type::TY_Vector, sizeof(T));
    else if constexpr (std::is_same_v<T, float>)
        return Type::Get_Float(Type::TY_Vector);
    else if constexpr (std::is_same_v<T, double>)
        return Type::Get_Double(Type::TY_Vector);
    else
        static_assert(not std::is_same_v<T, T>, "unsupported compile-time type T");
}

}

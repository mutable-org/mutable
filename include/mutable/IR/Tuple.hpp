#pragma once

#include <mutable/mutable-config.hpp>
#include <mutable/util/ADT.hpp>
#include <functional>
#include <iomanip>
#include <iostream>
#include <type_traits>


namespace m {

struct Schema;
struct Type;

/** This class holds a SQL attribute value.  It **cannot** represent `NULL`. */
struct M_EXPORT Value
{
    friend std::hash<Value>;

    using val_t = union {
        bool    b;
        int64_t i;
        float   f;
        double  d;
        void   *p;
    };

#ifdef M_ENABLE_SANITY_FIELDS
    /** The `value_type` is only used in the debug build to check that the `Value` is used with the correct type.  */
    enum value_type {
        VNone,
        Vb,
        Vi,
        Vf,
        Vd,
        Vp,
    } type = VNone;
#endif

    private:
    val_t val_;

    public:
    Value() {
        memset(&val_, 0, sizeof(val_)); // initialize with 0 bytes
#ifdef M_ENABLE_SANITY_FIELDS
        type = VNone;
#endif
    }

    template<typename T>
    Value(T val) : Value() {
        static_assert(std::is_fundamental_v<T> or std::is_pointer_v<T>,
                      "type T must be a fundamental or pointer type");
#ifdef M_ENABLE_SANITY_FIELDS
#define SET_TYPE(TY) this->type = V##TY
#else
#define SET_TYPE(TY)
#endif
#define SET(TY) { val_.TY = val; SET_TYPE(TY); }
        if constexpr (std::is_same_v<T, bool>)        SET(b)
        else if constexpr (std::is_integral_v<T>)     SET(i)
        else if constexpr (std::is_same_v<T, float>)  SET(f)
        else if constexpr (std::is_same_v<T, double>) SET(d)
        else if constexpr (std::is_pointer_v<T>) { val_.p = (void*)(val); SET_TYPE(p); }
        else static_assert(not std::is_same_v<T, T>, "unspoorted type T");
#undef SET
#undef SET_TYPE
    }

    /*----- Access methods -------------------------------------------------------------------------------------------*/
    /** Returns a reference to the value interpreted as of type `T`. */
    template<typename T>
    std::conditional_t<std::is_pointer_v<T>, T, T&> as() {
#ifdef M_ENABLE_SANITY_FIELDS
#define VALIDATE_TYPE(TY) M_insist(this->type == V##TY)
#else
#define VALIDATE_TYPE(TY)
#endif
#define GET(TY) { VALIDATE_TYPE(TY); return val_.TY; }
        if constexpr (std::is_same_v<T, bool>)         GET(b)
        else if constexpr (std::is_same_v<T, int64_t>) GET(i)
        else if constexpr (std::is_same_v<T, float>)   GET(f)
        else if constexpr (std::is_same_v<T, double>)  GET(d)
        else if constexpr (std::is_pointer_v<T>) { VALIDATE_TYPE(p); return reinterpret_cast<T>(val_.p); }
        else static_assert(not std::is_same_v<T, T>, "unsupported type");
#undef GET
#undef VALIDATE_TYPE
    }

    /** Returns the value interpreted as of type `T`. */
    template<typename T>
    T as() const { return const_cast<Value*>(this)->as<T>(); }

    /** Returns a reference to the value interpreted as of type `bool`. */
    auto & as_b() { return as<bool>(); }
    /** Returns a reference to the value interpreted as of type `int64_t`. */
    auto & as_i() { return as<int64_t>(); }
    /** Returns a reference to the value interpreted as of type `float`. */
    auto & as_f() { return as<float>(); }
    /** Returns a reference to the value interpreted as of type `double`. */
    auto & as_d() { return as<double>(); }
    /** Returns a reference to the value interpreted as of type `void*`. */
    auto as_p() { return as<void*>(); }

    /** Returns the value interpreted as of type `bool`. */
    auto as_b() const { return as<bool>(); }
    /** Returns the value interpreted as of type `int64_t`. */
    auto as_i() const { return as<int64_t>(); }
    /** Returns the value interpreted as of type `float`. */
    auto as_f() const { return as<float>(); }
    /** Returns the value interpreted as of type `double`. */
    auto as_d() const { return as<double>(); }
    /** Returns the value interpreted as of type `void*`. */
    auto as_p() const { return as<void*>(); }

    explicit operator bool()    const { return as_b(); }
    explicit operator int32_t() const { return as_i(); }
    explicit operator int64_t() const { return as_i(); }
    explicit operator float()   const { return as_f(); }
    explicit operator double()  const { return as_d(); }

    template<typename T>
    explicit operator T*() const { return reinterpret_cast<T*>(as_p()); }

    /*----- Print ----------------------------------------------------------------------------------------------------*/
M_LCOV_EXCL_START
    /** Print a hexdump of `val` to `out`. */
    friend std::ostream & operator<<(std::ostream &out, Value val) {
#ifdef M_ENABLE_SANITY_FIELDS
        switch (val.type) {
            case VNone: return out << "<none>";
            case Vb:    return out << (val.as_b() ? "TRUE" : "FALSE");
            case Vi:    return out << val.as_i();
            case Vf:    return out << val.as_f();
            case Vd:    return out << val.as_d();
            case Vp:    return out << val.as_p();
        }
#else
        out << "0x" << std::hex;
        auto prev_fill = out.fill('0');
        for (uint8_t *end = reinterpret_cast<uint8_t*>(&val.val_), *ptr = end + sizeof(val.val_); ptr != end; )
            out << std::setw(2) << uint32_t(*--ptr);
        out.fill(prev_fill);
#endif
        return out << std::dec;
    }
M_LCOV_EXCL_STOP

    /** Interpret this `Value` as of `Type` `ty` and print a human-readable representation to `out`. */
    void print(std::ostream &out, const Type &ty) const;

    /** Checks whether `this` `Value` is equal to `other`.  This operation is only sane if both `Value`s are of the same
     * type. */
    bool operator==(Value other) const {
#ifdef M_ENABLE_SANITY_FIELDS
        M_insist(this->type == other.type, "comparing values of different type");
#endif
        return memcmp(&this->val_, &other.val_, sizeof(this->val_)) == 0;
    }
    /** Checks whether `this` `Value` is not equal to `other`.  This operation is only sane if both `Value`s are of the
     * same type. */
    bool operator!=(Value other) const { return not operator==(other); }

    void dump(std::ostream &out) const;
    void dump() const;
};

static_assert(std::is_move_constructible_v<Value>, "Value must be move constructible");
static_assert(std::is_trivially_destructible_v<Value>, "Value must be trivially destructible");
#ifndef M_ENABLE_SANITY_FIELDS
static_assert(sizeof(Value) == 8, "Value exceeds expected size");
#endif

struct M_EXPORT Tuple
{
    friend std::hash<Tuple>;

    friend void swap(Tuple &first, Tuple &second) {
        using std::swap;
        swap(first.values_,     second.values_);
        swap(first.null_mask_,  second.null_mask_);
#ifdef M_ENABLE_SANITY_FIELDS
        swap(first.num_values_, second.num_values_);
#endif
    }

    private:
    Value *values_ = nullptr; ///< the `Value`s in this `Tuple`
    SmallBitset null_mask_ = SmallBitset(-1UL); ///< a bit mask for the `NULL` values; `1` represents `NULL`
#ifdef M_ENABLE_SANITY_FIELDS
    std::size_t num_values_ = 0; ///< the number of `Value`s in this `Tuple`
#define INBOUNDS(VAR) M_insist((VAR) < num_values_, "index out of bounds")
#else
#define INBOUNDS(VAR)
#endif

    public:
    /** Create a fresh `Tuple` with enough memory allocated to store all attributes of `S`.  This includes allocation
     * of memory to store character sequences within the `Tuple`. */
    explicit Tuple(const Schema &S);
    explicit Tuple(std::vector<const Type*> types);

    Tuple() { }
    Tuple(const Tuple&) = delete;
    Tuple(Tuple &&other) { swap(*this, other); }

    ~Tuple() { free(values_); }

    Tuple & operator=(const Tuple&) = delete;
    Tuple & operator=(Tuple &&other) { swap(*this, other); return *this; }

    /** Returns `true` iff the `Value` at index `idx` is `NULL`. */
    bool is_null(std::size_t idx) const {
        INBOUNDS(idx);
        return null_mask_(idx);
    }

    /** Sets the `Value` at index `idx` to `NULL`. */
    void null(std::size_t idx) {
        INBOUNDS(idx);
        null_mask_(idx) = true;
    }

    /** Sets all `Value`s of this `Tuple` to `NULL`. */
    void clear() { null_mask_ = SmallBitset(~0UL); }

    /** Sets the `Value` at index `idx` to not-`NULL`. */
    void not_null(std::size_t idx) {
        INBOUNDS(idx);
        null_mask_(idx) = false;
    }

    /** Assigns the `Value` `val` to this `Tuple` at index `idx` and clears the respective `NULL` bit. */
    void set(std::size_t idx, Value val) {
        INBOUNDS(idx);
        not_null(idx);
        values_[idx] = val;
    }

    /** Assigns the `Value` `val` to this `Tuple` at index `idx` and sets the respective `NULL` bit to `is_null`. */
    void set(std::size_t idx, Value val, bool is_null) {
        INBOUNDS(idx);
        null_mask_(idx) = is_null;
        values_[idx] = val;
    }

    /** Returns a reference to the `Value` at index `idx`.  Ignores the respective `NULL` bit. */
    Value & operator[](std::size_t idx) {
        INBOUNDS(idx);
        return values_[idx];
    }

    /** Returns the `Value` at index `idx`.  Ignores the respective `NULL` bit. */
    const Value & operator[](std::size_t idx) const { return const_cast<Tuple*>(this)->operator[](idx); }

    /** Returns a reference to the `Value` at index `idx`.  Must not be `NULL`. */
    Value & get(std::size_t idx) {
        INBOUNDS(idx);
        M_insist(not is_null(idx), "Value must not be NULL");
        return values_[idx];
    }
#undef INBOUNDS

    /** Returns a reference to the `Value` at index `idx`.  Must not be `NULL`. */
    const Value & get(std::size_t idx) const { return const_cast<Tuple*>(this)->get(idx); }

    /** Inserts the `Tuple` `other` of length `len` into this `Tuple` starting at index `pos`. */
    void insert(const Tuple &other, std::size_t pos, std::size_t len) {
        for (std::size_t i = 0; i != len; ++i)
            set(pos + i, other[i], other.is_null(i));
    }

    /** Create a clone of this `Tuple` interpreted using the `Schema` `S`. */
    Tuple clone(const Schema &S) const;

M_LCOV_EXCL_START
    friend std::ostream & operator<<(std::ostream &out, const Tuple &tup) {
#ifdef M_ENABLE_SANITY_FIELDS
        out << "(";
        for (std::size_t i = 0; i != tup.num_values_; ++i) {
            if (i != 0) out << ", ";
            if (tup.is_null(i)) out << "NULL";
            else                out << tup[i];
        }
        return out << ')';
#else
        out << "Tuple:";
        SmallBitset alive(~tup.null_mask_);
        for (auto i : alive)
            out << "\n  [" << std::setw(2) << i << "]: " << tup.values_[i];
        return out;
#endif
    }
M_LCOV_EXCL_STOP

    bool operator==(const Tuple &other) const {
        if (this->null_mask_ != other.null_mask_) return false;
        SmallBitset alive(~null_mask_);
        for (auto idx : alive) {
            if (this->get(idx) != other.get(idx))
                return false;
        }
        return true;
    }
    bool operator!=(const Tuple &other) const { return not operator==(other); }

    /** Print this `Tuple` using the given `schema`. */
    void print(std::ostream &out, const Schema &schema) const;

    void dump(std::ostream &out) const;
    void dump() const;
};

}

namespace std {

template<>
struct hash<m::Value>
{
    uint64_t operator()(m::Value val) const {
        /* Inspired by FNV-1a 64 bit. */
        uint64_t hash = 0xcbf29ce484222325;
        for (uint64_t *p = reinterpret_cast<uint64_t*>(&val.val_), *end = p + sizeof(val.val_) / sizeof(uint64_t);
             p != end; ++p)
        {
            hash ^= *p;
            hash *= 1099511628211;
        }
        return hash;
    }
};

template<>
struct hash<m::Tuple>
{
    uint64_t operator()(const m::Tuple &tup) const {
        std::hash<m::Value> h;
        auto mask = m::SmallBitset(~tup.null_mask_);
        uint64_t hash = 0;
        for (auto idx : mask) {
            hash ^= h(tup[idx]);
            hash *= 1099511628211;
        }
        return hash;
    }
};

}

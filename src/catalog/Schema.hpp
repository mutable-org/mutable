#pragma once

#include "util/macro.hpp"
#include "util/Pool.hpp"
#include <functional>
#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <vector>


namespace db {

/*======================================================================================================================
 * SQL Types
 *====================================================================================================================*/

struct ErrorType;
struct Boolean;
struct CharacterSequence;
struct Numeric;

struct Type
{
    private:
    static Pool<Type> types_; ///< a pool of parameterized types

    public:
    Type() = default;
    Type(const Type&) = delete;
    Type(Type&&) = default;
    virtual ~Type() { }

    virtual bool operator==(const Type &other) const = 0;
    bool operator!=(const Type &other) const { return not operator==(other); }

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
    static const Boolean * Get_Boolean();
    static const CharacterSequence * Get_Char(std::size_t length);
    static const CharacterSequence * Get_Varchar(std::size_t length);
    static const Numeric * Get_Decimal(unsigned digits, unsigned scale);
    static const Numeric * Get_Integer(unsigned num_bytes);
    static const Numeric * Get_Float();
    static const Numeric * Get_Double();
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
struct Boolean : Type
{
    friend struct Type;

    private:
    Boolean() { }

    public:
    Boolean(Boolean&&) = default;

    bool operator==(const Type &other) const;

    uint64_t hash() const;

    void print(std::ostream &out) const;
    void dump(std::ostream &out) const;
};

/** The type of character strings, both fixed length and varying. */
struct CharacterSequence : Type
{
    friend struct Type;

    std::size_t length; ///> the maximum length of the string in bytes
    bool is_varying; ///> true if varying, false otherwise; corresponds to Char(N) and Varchar(N)

    private:
    CharacterSequence(std::size_t length, bool is_varying) : length(length), is_varying(is_varying) { }

    public:
    CharacterSequence(CharacterSequence&&) = default;

    bool operator==(const Type &other) const;

    uint64_t hash() const;

    void print(std::ostream &out) const;
    void dump(std::ostream &out) const;
};

/** The numeric type represents integer and floating-point types of different precision, scale, and exactness. */
struct Numeric : Type
{
    friend struct Type;

#define kind_t(X) X(N_Int), X(N_Float), X(N_Decimal)
    DECLARE_ENUM(kind_t) kind; ///> the kind of numeric type
    /** The precision gives the maximum number of digits that can be represented by that type.  Its interpretation
     * depends on the kind:
     *  For INT, precision is the number of bytes.
     *  For FLOAT and DOUBLE, precision is the size of the type in bits, i.e. 32 and 64, respectively.
     *  For DECIMAL, precision is the number of decimal digits that can be represented.
     */
    unsigned precision; ///> the number of bits used to represent the number
    unsigned scale; ///> the number of decimal digits right of the decimal point
    private:
    static constexpr const char *KIND_TO_STR_[] = { ENUM_TO_STR(kind_t) };

    private:
    Numeric(kind_t kind, unsigned precision, unsigned scale)
        : kind(kind), precision(precision), scale(scale)
    { }

    public:
    Numeric(Numeric&&) = default;

    bool operator==(const Type &other) const;

    uint64_t hash() const;

    void print(std::ostream &out) const;
    void dump(std::ostream &out) const;
#undef kind_t
};

/*======================================================================================================================
 * Attribute, Relation, Schema
 *====================================================================================================================*/

struct Relation;
struct Schema;
struct Catalog;

/** An attribute of a relation.  Every attribute belongs to exactly one relation.  */
struct Attribute
{
    friend struct Relation;

    std::size_t id; ///> the internal identifier of the attribute, unique within its relation
    const Relation &relation; ///> the relation the attribute belongs to
    const Type *type; ///> the type of the attribute
    const char *name; ///> the name of the attribute

    private:
    explicit Attribute(std::size_t id, const Relation &relation, const Type *type, const char *name)
        : id(id)
        , relation(relation)
        , type(notnull(type))
        , name(notnull(name))
    { }

    public:
    Attribute(const Attribute&) = delete;
    Attribute(Attribute&&) = default;

    friend std::ostream & operator<<(std::ostream &out, const Attribute &attr) {
        return out << '`' << attr.name << "` " << *attr.type;
    }

    void dump(std::ostream &out) const;
    void dump() const;
};

/** A relation is a sorted set of attributes. */
struct Relation
{
    const char *name;
    private:
    using table_type = std::vector<Attribute>;
    /** the attributes of this relation */
    table_type attrs_;
    /** maps attribute names to their position within the relation */
    std::unordered_map<const char*, table_type::iterator> name_to_attr_;

    public:
    Relation(const char *name) : name(name) { }
    ~Relation();

    std::size_t size() const { return attrs_.size(); }

    table_type::const_iterator begin()  { return attrs_.cbegin(); }
    table_type::const_iterator end()    { return attrs_.cend(); }
    table_type::const_iterator cbegin() { return attrs_.cbegin(); }
    table_type::const_iterator cend()   { return attrs_.cend(); }

    const Attribute & operator[](std::size_t i) const;
    const Attribute & operator[](const char *name) const;

    const Attribute & push_back(const Type *type, const char *name);

    void dump(std::ostream &out) const;
    void dump() const;
};

/** A schema is a description of a database.  It is a set of relations. */
struct Schema
{
    friend struct Catalog;

    public:
    const char *name;
    private:
    std::unordered_map<const char*, Relation*> relations_;

    private:
    Schema(const char *name) : name(name) { }

    public:
    ~Schema();

    std::size_t size() const { return relations_.size(); }

    Relation & add(Relation *r) {
        auto it = relations_.find(r->name);
        if (it != relations_.end()) throw std::invalid_argument("relation with that name already exists");
        it = relations_.emplace_hint(it, r->name, r);
        return *it->second;
    }
};

/** The catalog keeps track of all meta information of the database system.  There is always exactly one catalog. */
struct Catalog
{
    private:
    std::unordered_map<const char*, Schema*> schemas_;

    private:
    Catalog() { }
    Catalog(const Catalog&) = delete;

    public:
    static Catalog & Get() {
        static Catalog the_catalog_;
        return the_catalog_;
    }

    std::size_t num_schemas() const { return schemas_.size(); }

    Schema & get_or_add_database(const char *name) {
        auto it = schemas_.find(name);
        if (it == schemas_.end()) {
            it = schemas_.emplace_hint(it, name, new Schema(name));
        }
        return *it->second;
    }
    Schema & operator[](const char *name) { return get_or_add_database(name); }
    Schema & operator[](const char *name) const { return *schemas_.at(name); }
};

}

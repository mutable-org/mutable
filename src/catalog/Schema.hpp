#pragma once

#include "util/fn.hpp"
#include "util/macro.hpp"
#include "util/Pool.hpp"
#include "util/StringPool.hpp"
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
struct FnType;

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

    bool is_error() const { return (void*) this == Get_Error(); }
    bool is_boolean() const { return (void*) this == Get_Boolean(); }
    bool is_character_sequence() const { return is<const CharacterSequence>(this); }
    bool is_numeric() const { return is<const Numeric>(this); }

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

    /** The maximal number of decimal digits that can be accurately represented by DECIMAL(p,s). */
    static constexpr std::size_t MAX_DECIMAL_PRECISION = 19;

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
    std::unordered_map<const char*, table_type::size_type> name_to_attr_;

    public:
    Relation(const char *name) : name(name) { }
    ~Relation();

    std::size_t size() const { return attrs_.size(); }

    table_type::const_iterator begin()  { return attrs_.cbegin(); }
    table_type::const_iterator end()    { return attrs_.cend(); }
    table_type::const_iterator cbegin() { return attrs_.cbegin(); }
    table_type::const_iterator cend()   { return attrs_.cend(); }

    const Attribute & at(std::size_t i) const { return attrs_.at(i); }
    const Attribute & at(const char *name) const { return attrs_[name_to_attr_.at(name)]; }
    const Attribute & operator[](std::size_t i) const { return at(i); }
    const Attribute & operator[](const char *name) const { return at(name); }

    const Attribute & push_back(const Type *type, const char *name);

    void dump(std::ostream &out) const;
    void dump() const;
};

/** Defines a function.  There are functions pre-defined in the SQL standard and user-defined functions. */
struct Function
{
    enum kind_t {
#define DB_FUNCTION(NAME) FN_ ## NAME,
#include "tables/Functions.tbl"
#undef DB_FUNCTION
        FN_UDF, // for all user-defined functions
    };

    const char *name; ///> the name of the function
    kind_t kind; ///> the kind of function

    Function(const char *name, kind_t kind) : name(name), kind(kind) { }

    bool is_UDF() const { return kind == FN_UDF; }
};

/** A schema is a description of a database.  It is a set of relations. */
struct Schema
{
    friend struct Catalog;

    public:
    const char *name;
    private:
    std::unordered_map<const char*, Relation*> relations_; ///> the relations of this schema
    std::unordered_map<const char*, Function*> functions_; ///> functions defined in this schema

    private:
    Schema(const char *name);

    public:
    ~Schema();

    std::size_t size() const { return relations_.size(); }

    /*===== Relations ================================================================================================*/
    Relation & get_relation(const char *name) const { return *relations_.at(name); }
    Relation & add_relation(const char *name) {
        auto it = relations_.find(name);
        if (it != relations_.end()) throw std::invalid_argument("relation with that name already exists");
        it = relations_.emplace_hint(it, name, new Relation(name));
        return *it->second;
    }
    Relation & add(Relation *r) {
        auto it = relations_.find(r->name);
        if (it != relations_.end()) throw std::invalid_argument("relation with that name already exists");
        it = relations_.emplace_hint(it, r->name, r);
        return *it->second;
    }

    /*===== Functions ================================================================================================*/
    const Function * get_function(const char *name) const { return functions_.at(name); }
};

/** The catalog keeps track of all meta information of the database system.  There is always exactly one catalog. */
struct Catalog
{
    private:
    StringPool pool; ///> pool of strings
    std::unordered_map<const char*, Schema*> schemas_; ///> the schemas; one per database
    Schema *database_in_use_ = nullptr; ///> the currently used database
    std::unordered_map<const char*, Function*> standard_functions_; ///> functions defined by the SQL standard

    private:
    Catalog();
    Catalog(const Catalog&) = delete;

    public:
    ~Catalog();

    static Catalog & Get() {
        static Catalog the_catalog_;
        return the_catalog_;
    }

    std::size_t num_schemas() const { return schemas_.size(); }

    StringPool & get_pool() { return pool; }
    const StringPool & get_pool() const { return pool; }

    /*===== Database =================================================================================================*/
    Schema & add_database(const char *name) {
        auto it = schemas_.find(name);
        if (it != schemas_.end()) throw std::invalid_argument("database with that name already exist");
        it = schemas_.emplace_hint(it, name, new Schema(name));
        return *it->second;
    }
    Schema & get_database(const char *name) const { return *schemas_.at(name); }
    bool drop_database(const char *name) { return schemas_.erase(name) != 0; }
    bool drop_database(const Schema &S) { return drop_database(S.name); }

    bool has_database_in_use() const { return database_in_use_ != nullptr; }
    Schema & get_database_in_use() {
        if (not has_database_in_use())
            throw std::logic_error("no database currently in use");
        return *database_in_use_;
    }
    const Schema & get_database_in_use() const {
        if (not has_database_in_use())
            throw std::logic_error("no database currently in use");
        return *database_in_use_;
    }
    void set_database_in_use(Schema &s) { database_in_use_ = &s; }
    void unset_database_in_use() { database_in_use_ = nullptr; }

    /*===== Functions ================================================================================================*/
    const Function * get_function(const char *name) const { return standard_functions_.at(name); }
};

}

#pragma once

#include "catalog/Type.hpp"
#include "util/fn.hpp"
#include "util/macro.hpp"
#include "util/Pool.hpp"
#include "util/StringPool.hpp"
#include <cmath>
#include <exception>
#include <functional>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>


namespace db {

/*======================================================================================================================
 * Attribute, Table, Database
 *====================================================================================================================*/

struct Table;
struct Store;
struct Database;
struct Catalog;

/** An attribute of a table.  Every attribute belongs to exactly one table.  */
struct Attribute
{
    friend struct Table;

    std::size_t id; ///> the internal identifier of the attribute, unique within its table
    const Table &table; ///> the table the attribute belongs to
    const PrimitiveType *type; ///> the type of the attribute
    const char *name; ///> the name of the attribute

    private:
    explicit Attribute(std::size_t id, const Table &table, const PrimitiveType *type, const char *name)
        : id(id)
        , table(table)
        , type(notnull(type))
        , name(notnull(name))
    {
        insist(type->is_vectorial()); // attributes are always of vectorial type
    }

    public:
    Attribute(const Attribute&) = delete;
    Attribute(Attribute&&) = default;

    friend std::ostream & operator<<(std::ostream &out, const Attribute &attr) {
        return out << '`' << attr.name << "` " << *attr.type;
    }

    void dump(std::ostream &out) const;
    void dump() const;
};

/** Checks that the type of the `attr` matches the template type `T`.  Throws `std::logic_error` on error. */
template<typename T>
bool type_check(const Attribute &attr);

/** A table is a sorted set of attributes. */
struct Table
{
    const char *name;
    private:
    using table_type = std::vector<Attribute>;
    /** the attributes of this table */
    table_type attrs_;
    /** maps attribute names to their position within the table */
    std::unordered_map<const char*, table_type::size_type> name_to_attr_;
    /** the store backing this table */
    Store *store_ = nullptr;

    public:
    Table(const char *name) : name(name) { }
    ~Table();

    std::size_t size() const { return attrs_.size(); }

    table_type::const_iterator begin()  const { return attrs_.cbegin(); }
    table_type::const_iterator end()    const { return attrs_.cend(); }
    table_type::const_iterator cbegin() const { return attrs_.cbegin(); }
    table_type::const_iterator cend()   const { return attrs_.cend(); }

    const Attribute & at(std::size_t i) const { return attrs_.at(i); }
    const Attribute & at(const char *name) const { return attrs_[name_to_attr_.at(name)]; }
    const Attribute & operator[](std::size_t i) const { return at(i); }
    const Attribute & operator[](const char *name) const { return at(name); }

    Store & store() const { return *store_; }
    void store(Store *new_store) { store_ = notnull(new_store); }

    void push_back(const PrimitiveType *type, const char *name);

    void dump(std::ostream &out) const;
    void dump() const;
};

/** Defines a function.  There are functions pre-defined in the SQL standard and user-defined functions. */
struct Function
{
#define kind_t(X) \
    X(FN_Scalar), \
    X(FN_Aggregate)

    enum fnid_t {
#define DB_FUNCTION(NAME, KIND) FN_ ## NAME,
#include "tables/Functions.tbl"
#undef DB_FUNCTION
        FN_UDF, // for all user-defined functions
    };

    const char *name; ///> the name of the function
    fnid_t fnid; ///> the function id
    DECLARE_ENUM(kind_t) kind; ///< the function kind: Scalar, Aggregate, etc.

    Function(const char *name, fnid_t fnid, kind_t kind) : name(name), fnid(fnid), kind(kind) { }

    bool is_UDF() const { return fnid == FN_UDF; }

    bool is_scalar() const { return kind == FN_Scalar; }
    bool is_aggregate() const { return kind == FN_Aggregate; }

    void dump(std::ostream &out) const;
    void dump() const;

    private:
    static constexpr const char *FNID_TO_STR_[] = {
#define DB_FUNCTION(NAME, KIND) "FN_" #NAME,
#include "tables/Functions.tbl"
#undef DB_FUNCTION
        "FN_UDF",
    };
    static constexpr const char *KIND_TO_STR_[] = { ENUM_TO_STR(kind_t) };
#undef kind_t
};

/** A description of a database.  It is a set of tables, functions, and statistics. */
struct Database
{
    friend struct Catalog;

    public:
    const char *name;
    private:
    std::unordered_map<const char*, Table*> tables_; ///> the tables of this database
    std::unordered_map<const char*, Function*> functions_; ///> functions defined in this database

    private:
    Database(const char *name);

    public:
    ~Database();

    std::size_t size() const { return tables_.size(); }
    auto begin_tables() const { return tables_.cbegin(); }
    auto end_tables() const { return tables_.cend(); }

    /*===== Tables ===================================================================================================*/
    Table & get_table(const char *name) const { return *tables_.at(name); }
    Table & add_table(const char *name) {
        auto it = tables_.find(name);
        if (it != tables_.end()) throw std::invalid_argument("table with that name already exists");
        it = tables_.emplace_hint(it, name, new Table(name));
        return *it->second;
    }
    Table & add(Table *r) {
        auto it = tables_.find(r->name);
        if (it != tables_.end()) throw std::invalid_argument("table with that name already exists");
        it = tables_.emplace_hint(it, r->name, r);
        return *it->second;
    }

    /*===== Functions ================================================================================================*/
    const Function * get_function(const char *name) const;
};

/** The catalog keeps track of all meta information of the database system.  There is always exactly one catalog. */
struct Catalog
{
    private:
    StringPool pool_; ///> pool of strings
    std::unordered_map<const char*, Database*> databases_; ///> the databases
    Database *database_in_use_ = nullptr; ///> the currently used database
    std::unordered_map<const char*, Function*> standard_functions_; ///> functions defined by the SQL standard

    private:
    Catalog();
    Catalog(const Catalog&) = delete;

    static Catalog the_catalog_;

    public:
    ~Catalog();

    /** Return a reference to the catalog. */
    static Catalog & Get() { return the_catalog_; }

    /** Destroys the current catalog and immediately creates a new catalog instance. */
    static void Clear() {
        the_catalog_.~Catalog();
        new (&the_catalog_) Catalog();
    }

    std::size_t num_databases() const { return databases_.size(); }

    StringPool & get_pool() { return pool_; }
    const StringPool & get_pool() const { return pool_; }
    const char * pool(const char *str) { return pool_(str); }

    /*===== Database =================================================================================================*/
    Database & add_database(const char *name);
    Database & get_database(const char *name) const { return *databases_.at(name); }
    void drop_database(const char *name);
    void drop_database(const Database &S) { return drop_database(S.name); }

    bool has_database_in_use() const { return database_in_use_ != nullptr; }
    Database & get_database_in_use() {
        if (not has_database_in_use())
            throw std::logic_error("no database currently in use");
        return *database_in_use_;
    }
    const Database & get_database_in_use() const {
        if (not has_database_in_use())
            throw std::logic_error("no database currently in use");
        return *database_in_use_;
    }
    void set_database_in_use(Database &s) { database_in_use_ = &s; }
    void unset_database_in_use() { database_in_use_ = nullptr; }

    /*===== Functions ================================================================================================*/
    const Function * get_function(const char *name) const { return standard_functions_.at(name); }
};

}

template<typename T>
bool db::type_check(const Attribute &attr) {
    auto ty = attr.type;

    /* Boolean */
    if constexpr (std::is_same_v<T, bool>) {
        if (is<const Boolean>(ty))
            return true;
    }

    /* CharacterSequence */
    if constexpr (std::is_same_v<T, std::string>) {
        if (auto s = cast<const CharacterSequence>(ty)) {
            if (not s->is_varying)
                return true;
        }
    }
    if constexpr (std::is_same_v<T, const char*>) {
        if (auto s = cast<const CharacterSequence>(ty)) {
            if (not s->is_varying)
                return true;
        }
    }

    /* Numeric */
    if constexpr (std::is_arithmetic_v<T>) {
        if (auto n = cast<const Numeric>(ty)) {
            switch (n->kind) {
                case Numeric::N_Int:
                    if (std::is_integral_v<T> and sizeof(T) * 8 == ty->size())
                        return true;
                    break;

                case Numeric::N_Float:
                    if (std::is_floating_point_v<T> and sizeof(T) * 8 == ty->size())
                        return true;
                    break;

                case Numeric::N_Decimal:
                    if (std::is_integral_v<T> and ceil_to_pow_2(ty->size()) == 8 * sizeof(T))
                        return true;
                    break;
            }
        }
    }

    return false;
}

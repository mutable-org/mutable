#pragma once

#include "mutable/catalog/Type.hpp"
#include "mutable/storage/Store.hpp"
#include "mutable/util/fn.hpp"
#include "mutable/util/macro.hpp"
#include "mutable/util/memory.hpp"
#include "mutable/util/Pool.hpp"
#include "mutable/util/StringPool.hpp"
#include "mutable/util/Timer.hpp"
#include "mutable/util/ADT.hpp"
#include <cmath>
#include <exception>
#include <functional>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <vector>


namespace m {

/** A `Schema` represents a sequence of identifiers, optionally with a prefix, and their associated types.  The `Schema`
 * allows identifiers of the same name with different prefix.  */
struct Schema
{
    /** An `Identifier` is composed of a name and an optional prefix. */
    struct Identifier
    {
        const char *prefix; ///< prefix of this `Identifier`, may be `nullptr`
        const char *name; ///< the name of this `Identifier`

        Identifier(const char *prefix, const char *name) : prefix(prefix) , name(name) {
            if (prefix != nullptr and strlen(prefix) == 0)
                throw invalid_argument("prefix must not be the empty string");
        }
        Identifier(const char *name) : prefix(nullptr), name(name) { }

        bool operator==(Identifier other) const {
            return this->prefix == other.prefix and this->name == other.name;
        }
        bool operator!=(Identifier other) const { return not operator==(other); }

        friend std::ostream & operator<<(std::ostream &out, Identifier id) {
            if (id.prefix)
                out << id.prefix << '.';
            return out << id.name;
        }
    };

    struct entry_type
    {
        Identifier id;
        const Type *type;

        entry_type(Identifier id, const Type *type) : id(id), type(notnull(type)) { }
    };

    private:
    std::vector<entry_type> entries_;

    public:
    const std::vector<entry_type> & entries() const { return entries_; }

    auto begin() { return entries_.begin(); }
    auto end()   { return entries_.end(); }
    auto begin() const { return entries_.cbegin(); }
    auto end()   const { return entries_.cend(); }
    auto cbegin() const { return entries_.cbegin(); }
    auto cend()   const { return entries_.cend(); }

    /** Returns the number of entries in this `Schema`. */
    auto num_entries() const { return entries_.size(); }

    /** Returns an iterator to the entry with the given `Identifier` `id`, or `end()` if no such entry exists.  */
    decltype(entries_)::iterator find(Identifier id) {
        std::function<bool(entry_type&)> pred;
        if (id.prefix)
            pred = [&](entry_type &e) -> bool { return e.id == id; }; // match qualified
        else
            pred = [&](entry_type &e) -> bool { return e.id.name == id.name; }; // match unqualified
        auto it = std::find_if(begin(), end(), pred);
        if (it != end() and std::find_if(std::next(it), end(), pred) != end())
            throw invalid_argument("duplicate identifier, lookup ambiguous");
        return it;
    }
    /** Returns an iterator to the entry with the given `Identifier` `id`, or `end()` if no such entry exists.  */
    decltype(entries_)::const_iterator find(Identifier id) const { return const_cast<Schema*>(this)->find(id); }

    /** Returns `true` iff this `Schema` contains an entry with `Identifier` `id`. */
    bool has(Identifier id) const { return find(id) != end(); }

    /** Returns the entry at index `idx`. */
    const entry_type & operator[](std::size_t idx) const {
        if (idx >= entries_.size())
            throw out_of_range("index out of bounds");
        return entries_[idx];
    }

    /** Returns a `std::pair` of the index and a reference to the entry with `Identifier` `id`. */
    std::pair<std::size_t, const entry_type&> operator[](Identifier id) const {
        auto pos = find(id);
        if (pos == end())
            throw out_of_range("identifier not found");
        return { std::distance(begin(), pos), *pos };
    }

    /** Adds a new entry `id` of type `type` to this `Schema`. */
    void add(Identifier id, const Type *type) { entries_.emplace_back(id, type); }

    /** Adds all entries of `other` to `this` `Schema`. */
    Schema & operator+=(const Schema &other) {
        for (auto &e : other)
            entries_.emplace_back(e);
        return *this;
    }

    /** Adds all entries of `other` to `this` `Schema` using *set semantics*.  If an entry of `other` with a particular
     * `Identifier` already exists in `this`, it is not added again. */
    Schema & operator|=(const Schema &other) {
        for (auto &e : other) {
            if (not has(e.id))
                entries_.emplace_back(e);
        }
        return *this;
    }

    friend std::ostream & operator<<(std::ostream &out, const Schema &schema) {
        out << "{[";
        for (auto it = schema.begin(), end = schema.end(); it != end; ++it) {
            if (it != schema.begin()) out << ',';
            out << ' ' << it->id << " :" << *it->type;
        }
        return out << " ]}";
    }

    void dump(std::ostream &out) const;
    void dump() const;
};

inline Schema operator+(const Schema &left, const Schema &right)
{
    Schema S(left);
    S += right;
    return S;
}

/** Computes the *set intersection* of two `Schema`s. */
inline Schema operator&(const Schema &left, const Schema &right)
{
    Schema res;
    for (auto &e : left) {
        auto it = right.find(e.id);
        if (it != right.end()) {
            if (e.type != it->type)
                throw invalid_argument("type mismatch");
            res.add(e.id, e.type);
        }
    }
    return res;
}

inline Schema operator|(const Schema &left, const Schema &right)
{
    Schema res(left);
    res |= right;
    return res;
}

/*======================================================================================================================
 * Attribute, Table, Function, Database
 *====================================================================================================================*/

struct Table;

/** An attribute of a table.  Every attribute belongs to exactly one table.  */
struct Attribute
{
    friend struct Table;

    std::size_t id; ///< the internal identifier of the attribute, unique within its table
    const Table &table; ///< the table the attribute belongs to
    const PrimitiveType *type; ///< the type of the attribute
    const char *name; ///< the name of the attribute

    private:
    explicit Attribute(std::size_t id, const Table &table, const PrimitiveType *type, const char *name)
            : id(id)
            , table(table)
            , type(notnull(type))
            , name(notnull(name))
    {
        if (not type->is_vectorial())
            throw invalid_argument("attributes must be of vectorial type");
    }

    public:
    Attribute(const Attribute&) = delete;
    Attribute(Attribute&&) = default;

    /** Compares to attributes.  Attributes are equal if they have the same `id` and belong to the same `table`. */
    bool operator==(const Attribute &other) const { return &this->table == &other.table and this->id == other.id; }
    bool operator!=(const Attribute &other) const { return not operator==(other); }

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
    const char *name; ///< the name of the table
    private:
    using table_type = std::vector<Attribute>;
    table_type attrs_; ///< the attributes of this table, maintained as a sorted set
    std::unordered_map<const char*, table_type::size_type> name_to_attr_; ///< maps attribute names to attributes
    std::unique_ptr<Store> store_; ///< the store backing this table; may be `nullptr`
    SmallBitset primary_key_; ///< the primary key of this table, maintained as a `SmallBitset` over attribute id's

    public:
    Table(const char *name) : name(name) { }

    /** Returns the number of attributes in this table. */
    std::size_t size() const { return attrs_.size(); }

    table_type::const_iterator begin()  const { return attrs_.cbegin(); }
    table_type::const_iterator end()    const { return attrs_.cend(); }
    table_type::const_iterator cbegin() const { return attrs_.cbegin(); }
    table_type::const_iterator cend()   const { return attrs_.cend(); }

    /** Returns the attribute with the given `id`. */
    const Attribute & at(std::size_t id) const {
        if (id >= attrs_.size())
            throw out_of_range("id out of bounds");
        auto &attr = attrs_[id];
        insist(attr.id == id, "attribute ID mismatch");
        return attr;
    }
    /** Returns the attribute with the given `id`. */
    const Attribute & operator[](std::size_t i) const { return at(i); }

    /** Returns the attribute with the given `name`.  Throws `std::out_of_range` if no attribute with the given `name`
     * exists. */
    const Attribute & at(const char *name) const { return at(name_to_attr_.at(name)); }
    /** Returns the attribute with the given `name`.  Throws `std::out_of_range` if no attribute with the given `name`
     * exists. */
    const Attribute & operator[](const char *name) const { return at(name); }

    /** Returns a reference to the backing store. */
    Store & store() const { return *store_; }
    /** Sets the backing store for this table.  `new_store` must not be `nullptr`. */
    void store(std::unique_ptr<Store> new_store) { using std::swap; swap(store_, new_store); }

    /** Returns all attributes forming the primary key. */
    std::vector<const Attribute*> primary_key() const {
        std::vector<const Attribute*> res;
        for (auto id : primary_key_)
            res.push_back(&at(id));
        return res;
    }
    /** Adds an attribute with the given `name` to the primary key of this table. Throws `std::out_of_range` if no
     * attribute with the given `name` exists. */
    void add_primary_key(const char *name) {
        auto &attr = at(name);
        primary_key_.set(attr.id);
    }

    /** Adds a new attribute with the given `name` and `type` to the table.  Throws `std::invalid_argument` if the
     * `name` is already in use. */
    void push_back(const char *name, const PrimitiveType *type) {
        auto res = name_to_attr_.emplace(name, attrs_.size());
        if (not res.second)
            throw std::invalid_argument("attribute name already in use");
        attrs_.emplace_back(Attribute(attrs_.size(), *this, type, name));
    }

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
#include "mutable/tables/Functions.tbl"
#undef DB_FUNCTION
        FN_UDF, // for all user-defined functions
    };

    const char *name; ///< the name of the function
    fnid_t fnid; ///< the function id
    DECLARE_ENUM(kind_t) kind; ///< the function kind: Scalar, Aggregate, etc.

    Function(const char *name, fnid_t fnid, kind_t kind) : name(name), fnid(fnid), kind(kind) { }

    /** Returns `true` iff this is a user-defined function. */
    bool is_UDF() const { return fnid == FN_UDF; }

    /** Returns `true` iff this function is scalar, i.e.\ if it is evaluated *per tuple*. */
    bool is_scalar() const { return kind == FN_Scalar; }
    /** Returns `true` iff this function is an aggregation, i.e.\ if it is evaluated *on all tuples*. */
    bool is_aggregate() const { return kind == FN_Aggregate; }

    void dump(std::ostream &out) const;
    void dump() const;

    private:
    static constexpr const char *FNID_TO_STR_[] = {
#define DB_FUNCTION(NAME, KIND) "FN_" #NAME,
#include "mutable/tables/Functions.tbl"
#undef DB_FUNCTION
            "FN_UDF",
    };
    static constexpr const char *KIND_TO_STR_[] = { ENUM_TO_STR(kind_t) };
#undef kind_t
};

/** A `Database` is a set of `m::Table`s, `m::Function`s, and `m::Statistics`. */
struct Database
{
    friend struct Catalog;

    public:
    const char *name; ///< the name of the database
    private:
    std::unordered_map<const char*, Table*> tables_; ///< the tables of this database
    std::unordered_map<const char*, Function*> functions_; ///< functions defined in this database

    private:
    Database(const char *name);

    public:
    ~Database();

    /** Returns the number of tables in this `Database`. */
    std::size_t size() const { return tables_.size(); }
    auto begin_tables() const { return tables_.cbegin(); }
    auto end_tables() const { return tables_.cend(); }

    /*===== Tables ===================================================================================================*/
    /** Returns a reference to the `Table` with the given `name`.  Throws `std::out_of_range` if no `Table` with the
     * given `name` exists in this `Database`. */
    Table & get_table(const char *name) const { return *tables_.at(name); }
    /** Adds a new `Table` to this `Database`.  Throws `std::invalid_argument` if a `Table` with the given `name`
     * already exists. */
    Table & add_table(const char *name) {
        auto it = tables_.find(name);
        if (it != tables_.end()) throw std::invalid_argument("table with that name already exists");
        it = tables_.emplace_hint(it, name, new Table(name));
        return *it->second;
    }
    /** Adds a new `Table` to this `Database`.  TODO implement transfer of ownership with unique_ptr */
    Table & add(Table *r) {
        auto it = tables_.find(r->name);
        if (it != tables_.end()) throw std::invalid_argument("table with that name already exists");
        it = tables_.emplace_hint(it, r->name, r);
        return *it->second;
    }

    /*===== Functions ================================================================================================*/
    /** Returns a reference to the `m::Function` with the given `name`.  First searches this `Database` instance.  If
     * no `m::Function` with the given `name` is found, searches the global `m::Catalog`.  Throws
     * `std::invalid_argument` if no `m::Function` with the given `name` exists. */
    const Function * get_function(const char *name) const;
};

/*======================================================================================================================
 * Factory helpers
 *====================================================================================================================*/

namespace {

struct StoreFactory
{
    virtual ~StoreFactory() { }

    virtual std::unique_ptr<Store> make(const Table&) const = 0;
};

template<typename T>
struct ConcreteStoreFactory : StoreFactory
{
    static_assert(std::is_base_of_v<m::Store, T>, "not a subclass of Store");

    std::unique_ptr<Store> make(const Table &tbl) const override {
        return std::make_unique<T>(tbl);
    }
};

}

/*======================================================================================================================
 * Catalog
 *====================================================================================================================*/

/** The catalog contains all `Database`s and keeps track of all meta information of the database system.  There is
 * always exactly one catalog. */
struct Catalog
{
    private:
    std::unique_ptr<rewire::Allocator> allocator_; ///< our custom allocator
    mutable StringPool pool_; ///< pool of strings
    std::unordered_map<const char*, Database*> databases_; ///< the databases
    Database *database_in_use_ = nullptr; ///< the currently used database
    std::unordered_map<const char*, Function*> standard_functions_; ///< functions defined by the SQL standard
    Timer timer_; ///< a global timer

    /*----- Factories ------------------------------------------------------------------------------------------------*/
    std::unordered_map<const char*, StoreFactory*> store_factories_; ///< store factories to create new stores

    StoreFactory *default_store_ = nullptr; ///< the default store to use

    private:
    Catalog();
    Catalog(const Catalog&) = delete;

    static Catalog the_catalog_; ///< the single catalog instance

    public:
    ~Catalog();

    /** Return a reference to the single `Catalog` instance. */
    static Catalog & Get() { return the_catalog_; }

    /** Destroys the current `Catalog` instance and immediately replaces it by a new one. */
    static void Clear() {
        the_catalog_.~Catalog();
        new (&the_catalog_) Catalog();
    }

    /** Returns the number of `Database`s. */
    std::size_t num_databases() const { return databases_.size(); }

    /** Returns a reference to the `StringPool`. */
    StringPool & get_pool() { return pool_; }
    /** Returns a reference to the `StringPool`. */
    const StringPool & get_pool() const { return pool_; }

    /** Returns the global `Timer` instance. */
    Timer & timer() { return timer_; }
    /** Returns the global `Timer` instance. */
    const Timer & timer() const { return timer_; }

    /** Returns a reference to the `rewire::Allocator`. */
    rewire::Allocator & allocator() { return *allocator_; }
    /** Returns a reference to the `rewire::Allocator`. */
    const rewire::Allocator & allocator() const { return *allocator_; }

    /** Creates an internalized copy of the string `str` by adding it to the internal `StringPool`. */
    const char * pool(const char *str) const { return pool_(str); }

    /*===== Database =================================================================================================*/
    /** Creates a new `Database` with the given `name`. */
    Database & add_database(const char *name);
    /** Returns the `Database` with the given `name`.  Throws `std::out_of_range` if no such `Database` exists. */
    Database & get_database(const char *name) const { return *databases_.at(name); }
    /** Drops the `Database` with the given `name`.  Throws `std::out_of_range` if no such `Database` exists or if the
     * `Database` is currently in use.  See `get_database_in_use()`. */
    void drop_database(const char *name);
    /** Drops the `Database` `db`.  Throws `std::out_of_range` if the `db` is currently in use. */
    void drop_database(const Database &db) { return drop_database(db.name); }

    /** Returns `true` if *any* `Database` is currently in use. */
    bool has_database_in_use() const { return database_in_use_ != nullptr; }
    /** Returns a reference to the `Database` that is currently in use, if any.  Throws `std::logic_error` otherwise. */
    Database & get_database_in_use() {
        if (not has_database_in_use())
            throw std::logic_error("no database currently in use");
        return *database_in_use_;
    }
    /** Returns a reference to the `Database` that is currently in use, if any.  Throws `std::logic_error` otherwise. */
    const Database & get_database_in_use() const { return const_cast<Catalog*>(this)->get_database_in_use(); }
    /** Sets the `Database` `db` as the `Database` that is currently in use.  */
    void set_database_in_use(Database &db) { database_in_use_ = &db; }
    /** Unsets the `Database` that is currenly in use. */
    void unset_database_in_use() { database_in_use_ = nullptr; }

    /*===== Functions ================================================================================================*/
    /** Returns a reference to the `m::Function` with the given `name`.  Throws `std::out_of_range` if no such
     * `Function` exists. */
    const Function * get_function(const char *name) const { return standard_functions_.at(name); }

    /*===== Stores ===================================================================================================*/
    /** Registers a new `Store` with the given `name`. */
    template<typename T>
    void register_store(const char *name) {
        name = pool(name);
        auto it = store_factories_.find(name);
        if (it != store_factories_.end()) throw std::invalid_argument("store with that name already exists");
        store_factories_.emplace_hint(it, name, new ConcreteStoreFactory<T>());
    }

    /** Sets `store` as the default store to use. */
    void default_store(const char *name) {
        name = pool(name);
        auto it = store_factories_.find(name);
        if (it == store_factories_.end()) throw std::invalid_argument("store not found");
        default_store_ = it->second;
    }

    std::unique_ptr<Store> create_store(const char *name, const Table &tbl) const;
    std::unique_ptr<Store> create_store(const Table &tbl) const;
};

}

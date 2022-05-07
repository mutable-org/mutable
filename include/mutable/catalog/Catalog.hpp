#pragma once

#include <memory>
#include <mutable/catalog/CardinalityEstimator.hpp>
#include <mutable/catalog/CostFunction.hpp>
#include <mutable/catalog/Schema.hpp>
#include <mutable/util/ArgParser.hpp>
#include <mutable/util/macro.hpp>
#include <mutable/util/memory.hpp>
#include <mutable/util/Pool.hpp>
#include <mutable/util/StringPool.hpp>
#include <mutable/util/Timer.hpp>
#include <type_traits>


/*======================================================================================================================
 * Factory helpers
 *====================================================================================================================*/

namespace {

struct StoreFactory
{
    virtual ~StoreFactory() { }

    virtual std::unique_ptr<m::Store> make(const m::Table&) const = 0;
};

template<typename T>
struct ConcreteStoreFactory : StoreFactory
{
    static_assert(std::is_base_of_v<m::Store, T>, "not a subclass of m::Store");

    std::unique_ptr<m::Store> make(const m::Table &tbl) const override { return std::make_unique<T>(tbl); }
};

struct CardinalityEstimatorFactory
{
    virtual ~CardinalityEstimatorFactory() { }

    virtual std::unique_ptr<m::CardinalityEstimator> make() const = 0;
};

template<typename T>
struct ConcreteCardinalityEstimatorFactory : CardinalityEstimatorFactory
{
    static_assert(std::is_base_of_v<m::CardinalityEstimator, T>, "not a subclass of CardinalityEstimator");

    std::unique_ptr<m::CardinalityEstimator> make() const override {
        return std::make_unique<T>();
    }
};

}


/*======================================================================================================================
 * Catalog
 *====================================================================================================================*/

namespace m {

/** The catalog contains all `Database`s and keeps track of all meta information of the database system.  There is
 * always exactly one catalog. */
struct M_EXPORT Catalog
{
    private:
    /** Singleton `Catalog` instance. */
    static Catalog *the_catalog_;

    m::ArgParser arg_parser_;

    std::unique_ptr<memory::Allocator> allocator_; ///< our custom allocator
    mutable StringPool pool_; ///< pool of strings
    std::unordered_map<const char*, Database*> databases_; ///< the databases
    Database *database_in_use_ = nullptr; ///< the currently used database
    std::unordered_map<const char*, Function*> standard_functions_; ///< functions defined by the SQL standard
    Timer timer_; ///< a global timer

    /*----- Stores ---------------------------------------------------------------------------------------------------*/
    ///> store factories to create new stores
    std::unordered_map<const char*, std::unique_ptr<StoreFactory>> store_factories_;
    ///> the default store to use
    StoreFactory *default_store_ = nullptr;

    /*----- Cardinality Estimators -----------------------------------------------------------------------------------*/
    ///> cardinality estimator factories to create new cardinality estimators
    std::unordered_map<const char*, std::unique_ptr<CardinalityEstimatorFactory>> cardinality_estimator_factories_;
    ///> the default cardinality estimator to use
    CardinalityEstimatorFactory *default_cardinality_estimator_ = nullptr;

    /*----- Cost Functions -------------------------------------------------------------------------------------------*/
    std::unique_ptr<CostFunction> cost_function_; ///< the default cost function

    /*----- Backends -------------------------------------------------------------------------------------------------*/
    ///> the available backends
    std::unordered_map<const char*, std::unique_ptr<Backend>> backends_;
    ///> the default backend to use
    decltype(backends_)::iterator default_backend_;

    private:
    Catalog();
    Catalog(const Catalog&) = delete;
    Catalog & operator=(const Catalog&) = delete;

    public:
    ~Catalog();

    /** Return a reference to the single `Catalog` instance. */
    static Catalog & Get();

    /** Destroys the current `Catalog` instance and immediately replaces it by a new one. */
    static void Clear() {
        delete the_catalog_;
        the_catalog_ = nullptr;
    }

    m::ArgParser & arg_parser() { return arg_parser_; }

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

    /** Returns a reference to the `memory::Allocator`. */
    memory::Allocator & allocator() { return *allocator_; }
    /** Returns a reference to the `memory::Allocator`. */
    const memory::Allocator & allocator() const { return *allocator_; }

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
    /** Returns a reference to the `Function` with the given `name`.  Throws `std::out_of_range` if no such `Function`
     * exists. */
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
        default_store_ = it->second.get();
    }

    std::unique_ptr<Store> create_store(const char *name, const Table &tbl) const;
    std::unique_ptr<Store> create_store(const Table &tbl) const;

    /*===== Backends =================================================================================================*/
    void register_backend(const char *name, std::unique_ptr<Backend> backend) {
        name = pool(name);
        auto it = backends_.find(name);
        if (it != backends_.end()) throw std::invalid_argument("backend with that name already exists");
        backends_.emplace_hint(it, name, std::move(backend));
    }

    void default_backend(const char *name) {
        name = pool(name);
        auto it = backends_.find(name);
        if (it == backends_.end())
            throw std::invalid_argument("backend not found");
        default_backend_ = it;
    }

    /** Returns `true` iff a `Backend` is set. */
    bool has_default_backend() const { return default_backend_ != backends_.end(); }

    /** Returns the default `Backend`. */
    Backend & default_backend() const {
        M_insist(has_default_backend(), "must have set a backend");
        return *default_backend_->second;
    }

    const char * default_backend_name() const {
        M_insist(has_default_backend(), "must have set a backend");
        return default_backend_->first;
    }

    auto backends_begin() { return backends_.begin(); }
    auto backends_end() { return backends_.end(); }
    auto backends_begin() const { return backends_.begin(); }
    auto backends_end() const { return backends_.end(); }
    auto backends_cbegin() const { return backends_begin(); }
    auto backends_cend() const { return backends_end(); }

    /*===== CardinalityEstimators ====================================================================================*/
    /** Registers a new `CardinalityEstimator` with the given `name`. */
    template<typename T>
    void register_cardinality_estimator(const char *name) {
        name = pool(name);
        auto it = cardinality_estimator_factories_.find(name);
        if (it != cardinality_estimator_factories_.end())
            throw std::invalid_argument("cardinality estimator with that name already exists");
        cardinality_estimator_factories_.emplace_hint(it, name, new ConcreteCardinalityEstimatorFactory<T>());
    }

    /** Sets `name` as the default cardinality estimator to use. */
    void default_cardinality_estimator(const char *name) {
        name = pool(name);
        auto it = cardinality_estimator_factories_.find(name);
        if (it == cardinality_estimator_factories_.end())
            throw std::invalid_argument("cardinality estimator not found");
        default_cardinality_estimator_ = it->second.get();
    }

    std::unique_ptr<CardinalityEstimator> create_cardinality_estimator(const char *name) const;
    std::unique_ptr<CardinalityEstimator> create_cardinality_estimator() const;

    /*===== CostFunction =============================================================================================*/
    /** Returns the active `CostFunction`. */
    const CostFunction & cost_function() const;

    /** Sets the new `CostFunction` and returns the old one. */
    std::unique_ptr<CostFunction> cost_function(std::unique_ptr<CostFunction> cost_function);
};

}

#pragma once

#include <memory>
#include <mutable/catalog/CardinalityEstimator.hpp>
#include <mutable/catalog/CostFunction.hpp>
#include <mutable/catalog/Schema.hpp>
#include <mutable/IR/PlanEnumerator.hpp>
#include <mutable/util/ArgParser.hpp>
#include <mutable/util/macro.hpp>
#include <mutable/util/memory.hpp>
#include <mutable/util/Pool.hpp>
#include <mutable/util/StringPool.hpp>
#include <mutable/util/Timer.hpp>
#include <type_traits>


namespace {

/*======================================================================================================================
 * Factory helpers
 *====================================================================================================================*/

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

    virtual std::unique_ptr<m::CardinalityEstimator> make(const char *database) const = 0;
};

template<typename T>
struct ConcreteCardinalityEstimatorFactory : CardinalityEstimatorFactory
{
    static_assert(std::is_base_of_v<m::CardinalityEstimator, T>, "not a subclass of CardinalityEstimator");

    std::unique_ptr<m::CardinalityEstimator> make(const char *database) const override {
        return std::make_unique<T>(database);
    }
};


/*======================================================================================================================
 * Components
 *====================================================================================================================*/

template<typename T>
struct Component
{
    using type = T;

    private:
    const char *description_;
    std::unique_ptr<T> instance_;

    public:
    Component(const char *description, std::unique_ptr<T> instance)
        : description_(description), instance_(std::move(instance))
    { }

    Component(const char *description, T *instance)
        : description_(description), instance_(instance)
    { }

    const char * description() const { return description_; }
    T & operator*() const { return *instance_; }
};

template<typename T>
struct ComponentSet
{
    using type = T;

    private:
    using map_t = std::unordered_map<const char*, Component<T>>;
    ///> all components
    map_t components_;
    ///> the default component
    typename map_t::iterator default_;

    public:
    void add(const char *name, Component<T> &&component) {
        auto it = components_.find(name);
        if (it != components_.end())
            throw std::invalid_argument("component with that name already exists");
        it = components_.emplace_hint(it, name, std::move(component));
        if (default_ == components_.end())
            default_ = it;
    }

    void set_default(const char *name) {
        auto it = components_.find(name);
        if (it == components_.end())
            throw std::invalid_argument("component does not exist");
        default_ = it;
    }

    bool has_default() const { return default_ != components_.end(); }

    const char * get_default_name() const { M_insist(has_default()); return default_->first; }
    const char * get_default_description() const { return default_->second.description; }
    T & get_default() const { M_insist(has_default()); return *default_->second; }

    const char * get_description(const char *name) const {
        auto it = components_.find(name);
        if (it == components_.end())
            throw std::invalid_argument("component does not exist");
        return it->second.description;
    }

    T & get(const char *name) const {
        auto it = components_.find(name);
        if (it == components_.end())
            throw std::invalid_argument("component does not exist");
        return *it->second;
    }

    typename map_t::iterator begin() { return components_.begin(); }
    typename map_t::iterator end() { return components_.end(); }
    typename map_t::const_iterator begin() const { return components_.begin(); }
    typename map_t::const_iterator end() const { return components_.end(); }
    typename map_t::const_iterator cbegin() const { return components_.begin(); }
    typename map_t::const_iterator cend() const { return components_.end(); }
};

}

namespace m {

/*======================================================================================================================
 * Catalog
 *====================================================================================================================*/

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

    private:
    Catalog();
    Catalog(const Catalog&) = delete;
    Catalog & operator=(const Catalog&) = delete;

    public:
    ~Catalog();

    /** Return a reference to the single `Catalog` instance. */
    static Catalog & Get();

    /** Removes all content from the `Catalog` instance. */
    static void Clear() {
        if (the_catalog_) {
            for (auto DB : the_catalog_->databases_)
                delete DB.second;
            the_catalog_->databases_.clear();
            the_catalog_->database_in_use_ = nullptr;
        }
    }

    /** Destroys the current `Catalog` instance. */
    static void Destroy();

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


    /*------------------------------------------------------------------------------------------------------------------
     * Components
     *----------------------------------------------------------------------------------------------------------------*/
    private:
    ComponentSet<StoreFactory> stores_;
    ComponentSet<CardinalityEstimatorFactory> cardinality_estimators_;
    ComponentSet<PlanEnumerator> plan_enumerators_;
    ComponentSet<Backend> backends_;
    ComponentSet<CostFunction> cost_functions_;

    public:
    /*===== Stores ===================================================================================================*/
    /** Registers a new `Store` with the given `name`. */
    template<typename T>
    void register_store(const char *name, const char *description = nullptr) {
        auto c = Component<StoreFactory>(description, std::make_unique<ConcreteStoreFactory<T>>());
        stores_.add(pool(name), std::move(c));
    }
    /** Sets the default `Store` to use. */
    void default_store(const char *name) { stores_.set_default(pool(name)); }
    /** Returns `true` iff the `Catalog` has a default `Store`. */
    bool has_default_store() const { return stores_.has_default(); }
    /** Creates a new `Store` for the given `Table` `tbl`. */
    std::unique_ptr<Store> create_store(const Table &tbl) const { return stores_.get_default().make(tbl); }
    /** Creates a new `Store` of name `name` for the given `Table` `tbl`. */
    std::unique_ptr<Store> create_store(const char *name, const Table &tbl) const {
        return stores_.get(pool(name)).make(tbl);
    }
    /** Returns the name of the default `Store`. */
    const char * default_store_name() const { return stores_.get_default_name(); }

    auto stores_begin()        { return stores_.begin(); }
    auto stores_end()          { return stores_.end(); }
    auto stores_begin()  const { return stores_.begin(); }
    auto stores_end()    const { return stores_.end(); }
    auto stores_cbegin() const { return stores_.begin(); }
    auto stores_cend()   const { return stores_.end(); }

    /*===== CardinalityEstimators ====================================================================================*/
    /** Registers a new `CardinalityEstimator` with the given `name`. */
    template<typename T>
    void register_cardinality_estimator(const char *name, const char *description = nullptr) {
        auto c = Component<CardinalityEstimatorFactory>(
            description,
            std::make_unique<ConcreteCardinalityEstimatorFactory<T>>()
        );
        cardinality_estimators_.add(pool(name), std::move(c));
    }

    /** Sets the default `CardinalityEstimator` to use. */
    void default_cardinality_estimator(const char *name) { cardinality_estimators_.set_default(pool(name)); }
    /** Returns `true` iff the `Catalog` has a default `CardinalityEstimator`. */
    bool has_default_cardinality_estimator() const { return cardinality_estimators_.has_default(); }
    /** Creates a new `CardinalityEstimator`. */
    std::unique_ptr<CardinalityEstimator> create_cardinality_estimator(const char *database) const {
        return cardinality_estimators_.get_default().make(database);
    }
    /** Creates a new `CardinalityEstimator` of name `name`. */
    std::unique_ptr<CardinalityEstimator> create_cardinality_estimator(const char *name, const char *database) const {
        return cardinality_estimators_.get(pool(name)).make(database);
    }
    /** Returns the name of the default `CardinalityEstimator`. */
    const char * default_cardinality_estimator_name() const { return cardinality_estimators_.get_default_name(); }

    auto cardinality_estimators_begin()        { return cardinality_estimators_.begin(); }
    auto cardinality_estimators_end()          { return cardinality_estimators_.end(); }
    auto cardinality_estimators_begin()  const { return cardinality_estimators_.begin(); }
    auto cardinality_estimators_end()    const { return cardinality_estimators_.end(); }
    auto cardinality_estimators_cbegin() const { return cardinality_estimators_.begin(); }
    auto cardinality_estimators_cend()   const { return cardinality_estimators_.end(); }

    /*===== Plan Enumerators =========================================================================================*/
    /** Registers a new `PlanEnumerator` with the given `name`. */
    void register_plan_enumerator(const char *name, std::unique_ptr<PlanEnumerator> PE,
                                  const char *description = nullptr)
    {
        plan_enumerators_.add(pool(name), Component<PlanEnumerator>(description, std::move(PE)));
    }
    /** Sets the default `PlanEnumerator` to use. */
    void default_plan_enumerator(const char *name) { plan_enumerators_.set_default(pool(name)); }
    /** Returns `true` iff the `Catalog` has a default `PlanEnumerator`. */
    bool has_default_plan_enumerator() const { return plan_enumerators_.has_default(); }
    /** Returns a reference to the default `PlanEnumerator`. */
    PlanEnumerator & plan_enumerator() const { return plan_enumerators_.get_default(); }
    /** Returns a reference to the `PlanEnumerator` with the given `name`. */
    PlanEnumerator & plan_enumerator(const char *name) const { return plan_enumerators_.get(pool(name)); }
    /** Returns the name of the default `PlanEnumerator`. */
    const char * default_plan_enumerator_name() const { return plan_enumerators_.get_default_name(); }

    auto plan_enumerators_begin() { return plan_enumerators_.begin(); }
    auto plan_enumerators_end() { return plan_enumerators_.end(); }
    auto plan_enumerators_begin() const { return plan_enumerators_.begin(); }
    auto plan_enumerators_end() const { return plan_enumerators_.end(); }
    auto plan_enumerators_cbegin() const { return plan_enumerators_.begin(); }
    auto plan_enumerators_cend() const { return plan_enumerators_.end(); }

    /*===== Backends =================================================================================================*/
    /** Registers a new `Backend` with the given `name`. */
    void register_backend(const char *name, std::unique_ptr<Backend> backend, const char *description = nullptr) {
        backends_.add(pool(name), Component<Backend>(description, std::move(backend)));
    }
    /** Sets the default `Backend` to use. */
    void default_backend(const char *name) { backends_.set_default(pool(name)); }
    /** Returns `true` iff the `Catalog` has a default `Backend`. */
    bool has_default_backend() const { return backends_.has_default(); }
    /** Returns a reference to the default `Backend`. */
    Backend & backend() const { return backends_.get_default(); }
    /** Returns a reference to the `Backend` with the given `name`. */
    Backend & backend(const char *name) const { return backends_.get(pool(name)); }
    /** Returns the name of the default `Backend`. */
    const char * default_backend_name() const { return backends_.get_default_name(); }

    auto backends_begin()        { return backends_.begin(); }
    auto backends_end()          { return backends_.end(); }
    auto backends_begin()  const { return backends_.begin(); }
    auto backends_end()    const { return backends_.end(); }
    auto backends_cbegin() const { return backends_begin(); }
    auto backends_cend()   const { return backends_end(); }

    /*===== CostFunction =============================================================================================*/
    /** Registers a new `CostFunction` with the given `name`. */
    void register_cost_function(const char *name, std::unique_ptr<CostFunction> CF,
                                const char *description = nullptr)
    {
        cost_functions_.add(pool(name), Component<CostFunction>(description, std::move(CF)));
    }
    /** Sets the default `CostFunction` to use. */
    void default_cost_function(const char *name) { cost_functions_.set_default(pool(name)); }
    /** Returns `true` iff the `Catalog` has a default `CostFunction`. */
    bool has_default_cost_function() const { return cost_functions_.has_default(); }
    /** Returns a reference to the default `CostFunction`. */
    CostFunction & cost_function() const { return cost_functions_.get_default(); }
    /** Returns a reference to the `CostFunction` with the given `name`. */
    CostFunction & cost_function(const char *name) const { return cost_functions_.get(pool(name)); }
    /** Returns the name of the default `CostFunction`. */
    const char * default_cost_function_name() const { return cost_functions_.get_default_name(); }

    auto cost_functions_begin()        { return cost_functions_.begin(); }
    auto cost_functions_end()          { return cost_functions_.end(); }
    auto cost_functions_begin()  const { return cost_functions_.begin(); }
    auto cost_functions_end()    const { return cost_functions_.end(); }
    auto cost_functions_cbegin() const { return cost_functions_begin(); }
    auto cost_functions_cend()   const { return cost_functions_end(); }
};

}

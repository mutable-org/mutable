#pragma once

#include <concepts>
#include <memory>
#include <mutable/backend/Backend.hpp>
#include <mutable/backend/WebAssembly.hpp>
#include <mutable/catalog/CardinalityEstimator.hpp>
#include <mutable/catalog/CostFunction.hpp>
#include <mutable/catalog/DatabaseCommand.hpp>
#include <mutable/catalog/TableFactory.hpp>
#include <mutable/catalog/Scheduler.hpp>
#include <mutable/catalog/Schema.hpp>
#include <mutable/IR/PlanEnumerator.hpp>
#include <mutable/storage/DataLayoutFactory.hpp>
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
requires std::derived_from<T, m::Store>
struct ConcreteStoreFactory : StoreFactory
{
    std::unique_ptr<m::Store> make(const m::Table &tbl) const override { return std::make_unique<T>(tbl); }
};

struct CardinalityEstimatorFactory
{
    virtual ~CardinalityEstimatorFactory() { }

    virtual std::unique_ptr<m::CardinalityEstimator> make(const char *database) const = 0;
};

template<typename T>
requires std::derived_from<T, m::CardinalityEstimator>
struct ConcreteCardinalityEstimatorFactory : CardinalityEstimatorFactory
{
    std::unique_ptr<m::CardinalityEstimator> make(const char *database) const override {
        return std::make_unique<T>(database);
    }
};

struct WasmEngineFactory
{
    virtual ~WasmEngineFactory() { }

    virtual std::unique_ptr<m::WasmEngine> make() const = 0;
};

template<typename T>
requires std::derived_from<T, m::WasmEngine>
struct ConcreteWasmEngineFactory : WasmEngineFactory
{
    std::unique_ptr<m::WasmEngine> make() const override { return std::make_unique<T>(); }
};

struct BackendFactory
{
    virtual ~BackendFactory() { }

    virtual std::unique_ptr<m::Backend> make() const = 0;
};

template<typename T>
requires std::derived_from<T, m::Backend>
struct ConcreteBackendFactory : BackendFactory
{
    std::unique_ptr<m::Backend> make() const override { return std::make_unique<T>(); }
};

struct ConcreteWasmBackendFactory : BackendFactory
{
    private:
    std::unique_ptr<WasmEngineFactory> platform_factory_;

    public:
    ConcreteWasmBackendFactory(std::unique_ptr<WasmEngineFactory> platform_factory)
        : platform_factory_(std::move(platform_factory))
    { }

    std::unique_ptr<m::Backend> make() const override {
        return std::make_unique<m::WasmBackend>(platform_factory_->make());
    }
};

struct DatabaseInstructionFactory
{
    virtual ~DatabaseInstructionFactory() { }

    virtual std::unique_ptr<m::DatabaseInstruction> make(std::vector<std::string> args) const = 0;
};

template<typename T>
requires std::derived_from<T, m::DatabaseInstruction>
struct ConcreteDatabaseInstructionFactory : DatabaseInstructionFactory
{
    std::unique_ptr<m::DatabaseInstruction> make(std::vector<std::string> args) const override {
        return std::make_unique<T>(std::move(args));
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

    /** Returns the `Component` of the specified \p name.  Throws `std::invalid_argument` if no such `Component` exists
     */
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

    /** Creates an internalized copy of the string \p str by adding it to the internal `StringPool`. */
    const char * pool(const char *str) const { return pool_(str); }
    /** Creates an internalized copy of the string \p str by adding it to the internal `StringPool`. */
    const char * pool(std::string_view str) const { return pool_(str); }

    /*===== Database =================================================================================================*/
    /** Creates a new `Database` with the given `name`.  Throws `std::invalid_argument` if a `Database` with the given
     * `name` already exists. */
    Database & add_database(const char *name);
    /** Returns the `Database` with the given \p name.  Throws `std::out_of_range` if no such `Database` exists. */
    Database & get_database(const char *name) const { return *databases_.at(name); }
    /** Returns `true` iff a `Database` with the given \p name exists. */
    bool has_database(const char *name) const { return databases_.contains(name); }
    /** Drops the `Database` with the \p name.  Throws `std::out_of_range` if no such `Database` exists or if the
     * `Database` is currently in use.  See `get_database_in_use()`. */
    void drop_database(const char *name);
    /** Drops the `Database` \p db.  Throws `std::out_of_range` if the `db` is currently in use. */
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
    /** Sets the `Database` \p db as the `Database` that is currently in use.  */
    void set_database_in_use(Database &db) { database_in_use_ = &db; }
    /** Unsets the `Database` that is currenly in use. */
    void unset_database_in_use() { database_in_use_ = nullptr; }

    /*===== Functions ================================================================================================*/
    /** Returns a reference to the `Function` with the given \p name.  Throws `std::out_of_range` if no such `Function`
     * exists. */
    const Function * get_function(const char *name) const { return standard_functions_.at(name); }


    /*------------------------------------------------------------------------------------------------------------------
     * Components
     *----------------------------------------------------------------------------------------------------------------*/
    private:
    ComponentSet<StoreFactory> stores_;
    ComponentSet<storage::DataLayoutFactory> data_layouts_;
    ComponentSet<CardinalityEstimatorFactory> cardinality_estimators_;
    ComponentSet<pe::PlanEnumerator> plan_enumerators_;
    ComponentSet<BackendFactory> backends_;
    ComponentSet<CostFunction> cost_functions_;
    ComponentSet<DatabaseInstructionFactory> instructions_;
    ComponentSet<Scheduler> schedulers_;

    using TableFactoryDecoratorCallback = std::function<std::unique_ptr<TableFactory>(std::unique_ptr<TableFactory>)>;
    ComponentSet<TableFactoryDecoratorCallback> table_properties_; // stores callback functions that decorate a table with the given decorator

    using PreOptimizationCallback = std::function<void(QueryGraph &)>;
    ComponentSet<PreOptimizationCallback> pre_optimizations_;

    using PostOptimizationCallback = std::function<std::unique_ptr<Producer>(std::unique_ptr<Producer>)>;
    ComponentSet<PostOptimizationCallback> post_optimizations_;

    public:
    /*===== Stores ===================================================================================================*/
    /** Registers a new `Store` with the given `name`. */
    template<typename T>
    requires std::derived_from<T, m::Store>
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

    /*===== DataLayouts ==============================================================================================*/
    /** Registers a new `DataLayoutFactory` with the given `name`. */
    void register_data_layout(const char *name, std::unique_ptr<storage::DataLayoutFactory> data_layout,
                              const char *description = nullptr)
    {
        data_layouts_.add(pool(name), Component<storage::DataLayoutFactory>(description, std::move(data_layout)));
    }
    /** Sets the default `DataLayoutFactory` to use. */
    void default_data_layout(const char *name) { data_layouts_.set_default(pool(name)); }
    /** Returns `true` iff the `Catalog` has a default `DataLayoutFactory`. */
    bool has_default_data_layout() const { return data_layouts_.has_default(); }
    /** Returns a reference to the default `DataLayoutFactory`. */
    storage::DataLayoutFactory & data_layout() const { return data_layouts_.get_default(); }
    /** Returns a reference to the `DataLayoutFactory` with the given `name`. */
    storage::DataLayoutFactory & data_layout(const char *name) const { return data_layouts_.get(pool(name)); }
    /** Returns the name of the default `DataLayoutFactory`. */
    const char * default_data_layout_name() const { return data_layouts_.get_default_name(); }

    auto data_layouts_begin()        { return data_layouts_.begin(); }
    auto data_layouts_end()          { return data_layouts_.end(); }
    auto data_layouts_begin()  const { return data_layouts_.begin(); }
    auto data_layouts_end()    const { return data_layouts_.end(); }
    auto data_layouts_cbegin() const { return data_layouts_.begin(); }
    auto data_layouts_cend()   const { return data_layouts_.end(); }

    /*===== CardinalityEstimators ====================================================================================*/
    /** Registers a new `CardinalityEstimator` with the given `name`. */
    template<typename T>
    requires std::derived_from<T, m::CardinalityEstimator>
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
    void register_plan_enumerator(const char *name, std::unique_ptr<pe::PlanEnumerator> PE,
                                  const char *description = nullptr)
    {
        plan_enumerators_.add(pool(name), Component<pe::PlanEnumerator>(description, std::move(PE)));
    }
    /** Sets the default `PlanEnumerator` to use. */
    void default_plan_enumerator(const char *name) { plan_enumerators_.set_default(pool(name)); }
    /** Returns `true` iff the `Catalog` has a default `PlanEnumerator`. */
    bool has_default_plan_enumerator() const { return plan_enumerators_.has_default(); }
    /** Returns a reference to the default `PlanEnumerator`. */
    pe::PlanEnumerator & plan_enumerator() const { return plan_enumerators_.get_default(); }
    /** Returns a reference to the `PlanEnumerator` with the given `name`. */
    pe::PlanEnumerator & plan_enumerator(const char *name) const { return plan_enumerators_.get(pool(name)); }
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
    template<typename T>
    requires std::derived_from<T, m::Backend>
    void register_backend(const char *name, const char *description = nullptr) {
        auto c = Component<BackendFactory>(description, std::make_unique<ConcreteBackendFactory<T>>());
        backends_.add(pool(name), std::move(c));
    }
    /** Registers a new `WasmBackend` using the given `WasmEngine` with the given `name`. */
    template<typename T>
    requires std::derived_from<T, m::WasmEngine>
    void register_wasm_backend(const char *name, const char *description = nullptr) {
        auto c = Component<BackendFactory>(
            description,
            std::make_unique<ConcreteWasmBackendFactory>(std::make_unique<ConcreteWasmEngineFactory<T>>())
        );
        backends_.add(pool(name), std::move(c));
    }
    /** Sets the default `Backend` to use. */
    void default_backend(const char *name) { backends_.set_default(pool(name)); }
    /** Returns `true` iff the `Catalog` has a default `Backend`. */
    bool has_default_backend() const { return backends_.has_default(); }
    /** Returns a new `Backend`. */
    std::unique_ptr<Backend> create_backend() const { return backends_.get_default().make(); }
    /** Returns a new `Backend` of name `name`. */
    std::unique_ptr<Backend> create_backend(const char *name) const { return backends_.get(pool(name)).make(); }
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

    /*===== Instructions =============================================================================================*/
    /** Registers a new `DatabaseInstruction` with the given `name`. */
    template<typename T>
    requires std::derived_from<T, m::DatabaseInstruction>
    void register_instruction(const char *name, const char *description = nullptr)
    {
        auto I = Component<DatabaseInstructionFactory>(
            description,
            std::make_unique<ConcreteDatabaseInstructionFactory<T>>()
        );
        instructions_.add(pool(name), std::move(I));
    }
    /** Returns a reference to the `DatabaseInstruction` with the given `name`.  Throws `std::invalid_argument` if no
     * `Instruction` with the given `name` exists. */
    std::unique_ptr<DatabaseInstruction> create_instruction(const char *name,
                                                            const std::vector<std::string> &args) const
    {
        return instructions_.get(name).make(args);
    }

    auto instructions_begin() { return instructions_.begin(); }
    auto instructions_end() { return instructions_.end(); }
    auto instructions_begin() const { return instructions_.begin(); }
    auto instructions_end() const { return instructions_.end(); }
    auto instructions_cbegin() const { return instructions_.begin(); }
    auto instructions_cend() const { return instructions_.end(); }

    /*===== Schedulers =================================================================================================*/
    /** Registers a new `Scheduler` with the given `name`. */
    void register_scheduler(const char *name, std::unique_ptr<Scheduler> scheduler, const char *description = nullptr) {
        schedulers_.add(pool(name), Component<Scheduler>(description, std::move(scheduler)));
    }
    /** Sets the default `Scheduler` to use. */
    void default_scheduler(const char *name) { schedulers_.set_default(pool(name)); }
    /** Returns `true` iff the `Catalog` has a default `Scheduler`. */
    bool has_default_scheduler() const { return schedulers_.has_default(); }
    /** Returns a reference to the default `Scheduler`. */
    Scheduler & scheduler() const { return schedulers_.get_default(); }
    /** Returns a reference to the `Scheduler` with the given `name`. */
    Scheduler & scheduler(const char *name) const { return schedulers_.get(pool(name)); }
    /** Returns the name of the default `Scheduler`. */
    const char * default_scheduler_name() const { return schedulers_.get_default_name(); }

    auto schedulers_begin()        { return schedulers_.begin(); }
    auto schedulers_end()          { return schedulers_.end(); }
    auto schedulers_begin()  const { return schedulers_.begin(); }
    auto schedulers_end()    const { return schedulers_.end(); }
    auto schedulers_cbegin() const { return schedulers_begin(); }
    auto schedulers_cend()   const { return schedulers_end(); }

    /*===== Table Factories ==========================================================================================*/
    private:
    std::unique_ptr<TableFactory> table_factory_; ///< The `TableFactory` used to construct `Table`s

    public:
    /** Replaces the stored `TableFactory` with `table_factory` and returns the old `TableFactory`. */
    std::unique_ptr<TableFactory> table_factory(std::unique_ptr<TableFactory> table_factory) {
        return std::exchange(table_factory_, std::move(table_factory));
    }
    /** Returns a reference to the stored `TableFactory`. */
    TableFactory & table_factory() const { return *table_factory_; }

    /** Registers a new `TableFactoryDecorator` with the given `name`.
     * The `name` will be used as the property name of the decorator in `--table-properties`. */
    template<class T>
    requires std::derived_from<T, TableFactoryDecorator>
    void register_table_property(const char *name, const char *description = nullptr)
    {
        table_properties_.add(
                pool(name),
                Component<TableFactoryDecoratorCallback>(
                        description,
                        std::make_unique<TableFactoryDecoratorCallback>([](std::unique_ptr<TableFactory> table_factory)
                        {
                            return std::make_unique<T>(std::move(table_factory));
                        })
                ));
    }
    /** Applies the `TableFactoryDecorator` corresponding to `name` to `table_factory`.
     * Returns the decorated `TableFactory`. */
    std::unique_ptr<TableFactory> apply_table_property(const char *name, std::unique_ptr<TableFactory> table_factory) const
    {
        return table_properties_.get(pool(name)).operator()(std::move(table_factory));
    }

    auto table_properties_begin()        { return table_properties_.begin(); }
    auto table_properties_end()          { return table_properties_.end(); }
    auto table_properties_begin()  const { return table_properties_.begin(); }
    auto table_properties_end()    const { return table_properties_.end(); }
    auto table_properties_cbegin() const { return table_properties_begin(); }
    auto table_properties_cend()   const { return table_properties_end(); }

    /*===== Pre-Optimizations ========================================================================================*/
    /** Registers a new pre-optimization with the given `name`. */
    void register_pre_optimization(const char *name, PreOptimizationCallback optimization, const char *description = nullptr)
    {
        pre_optimizations_.add(
                pool(name),
                Component<PreOptimizationCallback>(description, std::make_unique<PreOptimizationCallback>(std::move(optimization)))
        );
    }

    auto pre_optimizations()              { return range(pre_optimizations_.begin(), pre_optimizations_.end()); }
    auto pre_optimizations_begin()        { return pre_optimizations_.begin(); }
    auto pre_optimizations_end()          { return pre_optimizations_.end(); }
    auto pre_optimizations_begin()  const { return pre_optimizations_.begin(); }
    auto pre_optimizations_end()    const { return pre_optimizations_.end(); }
    auto pre_optimizations_cbegin() const { return pre_optimizations_begin(); }
    auto pre_optimizations_cend()   const { return pre_optimizations_end(); }

    /*===== Post-Optimizations ========================================================================================*/
    /** Registers a new post-optimization with the given `name`. */
    void register_post_optimization(const char *name, PostOptimizationCallback optimization, const char *description = nullptr)
    {
        post_optimizations_.add(
                pool(name),
                Component<PostOptimizationCallback>(description, std::make_unique<PostOptimizationCallback>(std::move(optimization)))
        );
    }

    auto post_optimizations()              { return range(post_optimizations_.begin(), post_optimizations_.end()); }
    auto post_optimizations_begin()        { return post_optimizations_.begin(); }
    auto post_optimizations_end()          { return post_optimizations_.end(); }
    auto post_optimizations_begin()  const { return post_optimizations_.begin(); }
    auto post_optimizations_end()    const { return post_optimizations_.end(); }
    auto post_optimizations_cbegin() const { return post_optimizations_begin(); }
    auto post_optimizations_cend()   const { return post_optimizations_end(); }
};

}

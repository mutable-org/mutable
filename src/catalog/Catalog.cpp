#include <mutable/catalog/Catalog.hpp>

#include "backend/Interpreter.hpp"
#include "storage/ColumnStore.hpp"
#include "storage/PaxStore.hpp"
#include "storage/RowStore.hpp"
#include <mutable/catalog/CostFunctionCout.hpp>


using namespace m;


/*======================================================================================================================
 * Catalog
 *====================================================================================================================*/

Catalog * Catalog::the_catalog_(nullptr);

Catalog::Catalog()
    : allocator_(new memory::LinearAllocator())
    /* Initialize dummy cost function. */
    , cost_function_(std::make_unique<CostFunctionCout>())
    , default_backend_(backends_.end())
{
    /*----- Initialize standard functions. ---------------------------------------------------------------------------*/
#define M_FUNCTION(NAME, KIND) { \
    auto name = pool(#NAME); \
    auto res = standard_functions_.emplace(name, new Function(name, Function::FN_ ## NAME, Function::KIND)); \
    M_insist(res.second, "function already defined"); \
}
#include <mutable/tables/Functions.tbl>
#undef M_FUNCTION

    /*----- Initialize stores. ---------------------------------------------------------------------------------------*/
#define M_STORE(NAME, _) { \
    auto name = pool(#NAME); \
    auto res = store_factories_.emplace(name, new ConcreteStoreFactory<NAME>()); \
    M_insist(res.second, "store already defined"); \
}
#include <mutable/tables/Store.tbl>
#undef M_STORE
    M_insist(store_factories_.size() != 0);
    default_store(store_factories_.begin()->first); // set default store

    /*----- Initialize cardinality estimators. -----------------------------------------------------------------------*/
#define M_CARDINALITY_ESTIMATOR(NAME, _) { \
    auto name = pool(#NAME); \
    auto res = cardinality_estimator_factories_.emplace(name, new ConcreteCardinalityEstimatorFactory<NAME>()); \
    M_insist(res.second, "cardinality estimator already defined"); \
}
#include <mutable/tables/CardinalityEstimator.tbl>
#undef M_CARDINALITY_ESTIMATOR
    M_insist(cardinality_estimator_factories_.size() != 0);
    default_cardinality_estimator(cardinality_estimator_factories_.begin()->first); // set default cardinality estimator

    /*----- Initialize backends. -------------------------------------------------------------------------------------*/
#define M_BACKEND(NAME, _) { \
    auto name = pool(#NAME); \
    auto res = backends_.emplace(name, std::make_unique<NAME>()); \
}
#include <mutable/tables/Backend.tbl>
#undef M_BACKEND
    M_insist(backends_.size() != 0);
    default_backend(backends_.begin()->first);
}


Catalog::~Catalog()
{
    for (auto db : databases_)
        delete db.second;
    for (auto fn : standard_functions_)
        delete fn.second;
}

__attribute__((constructor(200)))
Catalog & Catalog::Get()
{
    if (not the_catalog_)
        the_catalog_ = new Catalog();
    return *the_catalog_;
}

__attribute__((destructor(200)))
void destroy_catalog()
{
    Catalog::Clear();
}


/*===== Stores =======================================================================================================*/

std::unique_ptr<Store> Catalog::create_store(const char *name, const Table &tbl) const
{
    name = pool(name);
    auto it = store_factories_.find(name);
    if (it == store_factories_.end()) throw std::invalid_argument("store not found");
    return it->second->make(tbl);
}

std::unique_ptr<Store> Catalog::create_store(const Table &tbl) const
{
    M_insist(default_store_, "there must always be a default store");
    return default_store_->make(tbl);
}


/*===== Cardinality Estimators =======================================================================================*/

std::unique_ptr<CardinalityEstimator> Catalog::create_cardinality_estimator(const char *name) const
{
    name = pool(name);
    auto it = cardinality_estimator_factories_.find(name);
    if (it == cardinality_estimator_factories_.end()) throw std::invalid_argument("estimator not found");
    return it->second->make();
}

std::unique_ptr<CardinalityEstimator> Catalog::create_cardinality_estimator() const
{
    M_insist(default_cardinality_estimator_, "there must always be a default cardinality estimator");
    return default_cardinality_estimator_->make();
}


/*===== Cost Functions ===============================================================================================*/

const CostFunction & Catalog::cost_function() const {
    return *cost_function_;
}

std::unique_ptr<CostFunction> Catalog::cost_function(std::unique_ptr<CostFunction> cost_function) {
    cost_function_.swap(cost_function);
    return cost_function;
}


/*===== Plan Enumerators =============================================================================================*/

PlanEnumerator & Catalog::plan_enumerator() const
{
    M_insist(default_plan_enumerator_ != plan_enumerators_.cend());
    return *default_plan_enumerator_->second;
}


/*===== Databases ====================================================================================================*/

Database & Catalog::add_database(const char *name)
{
    auto it = databases_.find(name);
    if (it != databases_.end()) throw std::invalid_argument("database with that name already exist");
    it = databases_.emplace_hint(it, name, new Database(name));
    return *it->second;
}

void Catalog::drop_database(const char *name)
{
    if (has_database_in_use() and get_database_in_use().name == name)
        throw std::invalid_argument("Cannot drop database; currently in use.");
    auto it = databases_.find(name);
    if (it == databases_.end())
        throw std::invalid_argument("Database of that name does not exist.");
    delete it->second;
    databases_.erase(it);
}

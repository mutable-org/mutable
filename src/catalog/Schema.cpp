#include "catalog/Schema.hpp"

#include <algorithm>
#include "globals.hpp"
#include <mutable/catalog/CardinalityEstimator.hpp>
#include <mutable/catalog/CostFunction.hpp>
#include <mutable/catalog/SimpleCostFunction.hpp>
#include <mutable/IR/PlanTable.hpp>
#include <mutable/util/fn.hpp>
#include "storage/ColumnStore.hpp"
#include "storage/PaxStore.hpp"
#include "storage/RowStore.hpp"
#include <algorithm>
#include <cmath>
#include <iterator>
#include <mutable/catalog/CardinalityEstimator.hpp>
#include <mutable/catalog/CostFunction.hpp>
#include <mutable/IR/Operator.hpp>
#include <mutable/IR/PlanTable.hpp>
#include <stdexcept>


using namespace m;


/*======================================================================================================================
 * Schema
 *====================================================================================================================*/

M_LCOV_EXCL_START
void Schema::dump(std::ostream &out) const { out << *this << std::endl; }
void Schema::dump() const { dump(std::cerr); }
M_LCOV_EXCL_STOP


/*======================================================================================================================
 * Attribute
 *====================================================================================================================*/

M_LCOV_EXCL_START
void Attribute::dump(std::ostream &out) const
{
    out << "Attribute `" << table.name << "`.`" << name << "`, "
        << "id " << id << ", "
        << "type " << *type
        << std::endl;
}

void Attribute::dump() const { dump(std::cerr); }
M_LCOV_EXCL_STOP


/*======================================================================================================================
 * Table
 *====================================================================================================================*/

Schema Table::schema() const
{
    Schema S;
    for (auto &attr : *this)
        S.add({this->name, attr.name}, attr.type);
    return S;
}

M_LCOV_EXCL_START
void Table::dump(std::ostream &out) const
{
    out << "Table `" << name << '`';
    for (const auto &attr : attrs_)
        out << "\n` " << attr.id << ": `" << attr.name << "` " << *attr.type;
    out << std::endl;
}

void Table::dump() const { dump(std::cerr); }
M_LCOV_EXCL_STOP


/*======================================================================================================================
 * Function
 *====================================================================================================================*/

constexpr const char * Function::FNID_TO_STR_[];
constexpr const char * Function::KIND_TO_STR_[];

M_LCOV_EXCL_START
void Function::dump(std::ostream &out) const
{
    out << "Function{ name = \"" << name << "\", fnid = " << FNID_TO_STR_[fnid] << ", kind = " << KIND_TO_STR_[kind]
        << "}" << std::endl;
}
M_LCOV_EXCL_STOP


/*======================================================================================================================
 * Database
 *====================================================================================================================*/

Database::Database(const char *name)
    : name(name)
{

    //TODO how to incorporate the injected file, pass as argument or do that inside the estimator?
    //TODO currently a workaround for the Integration Tests, maybe find a different solution?
    if (Options::Get().cardinality_estimator)
        cardinality_estimator_ = CardinalityEstimator::Create(Options::Get().cardinality_estimator, name);
    else
        cardinality_estimator_ = std::make_unique<CartesianProductEstimator>();
}

Database::~Database()
{
    for (auto &r : tables_)
        delete r.second;
    for (auto &f : functions_)
        delete f.second;
}

const Function * Database::get_function(const char *name) const
{
    try {
        return functions_.at(name);
    } catch (std::out_of_range) {
        /* not defined within the database; search the global catalog */
        return Catalog::Get().get_function(name);
    }
}


/*======================================================================================================================
 * Catalog
 *====================================================================================================================*/

Catalog::Catalog()
    : allocator_(new memory::LinearAllocator())
    , backend_(Backend::CreateInterpreter())
    /* Initialize dummy cost function. */
    , cost_function_(std::make_unique<SimpleCostFunction>())
{
    /* Initialize standard functions. */
#define M_FUNCTION(NAME, KIND) { \
    auto name = pool(#NAME); \
    auto res = standard_functions_.emplace(name, new Function(name, Function::FN_ ## NAME, Function::KIND)); \
    M_insist(res.second, "function already defined"); \
}
#include <mutable/tables/Functions.tbl>
#undef M_FUNCTION

    /* Initialize stores. */
#define M_STORE(NAME, _) { \
    auto name = pool(#NAME); \
    auto res = store_factories_.emplace(name, new ConcreteStoreFactory<NAME>()); \
    M_insist(res.second, "store already defined"); \
}
#include <mutable/tables/Store.tbl>
#undef M_STORE
    M_insist(store_factories_.size() != 0);
    default_store(store_factories_.begin()->first); // set default store

    /* Initialize cardinality estimators. */
#define M_CARDINALITY_ESTIMATOR(NAME, _) { \
    auto name = pool(#NAME); \
    auto res = cardinality_estimator_factories_.emplace(name, new ConcreteCardinalityEstimatorFactory<NAME>()); \
    M_insist(res.second, "cardinality estimator already defined"); \
}
#include <mutable/tables/CardinalityEstimator.tbl>
#undef M_CARDINALITY_ESTIMATOR
    M_insist(cardinality_estimator_factories_.size() != 0);
    default_cardinality_estimator(cardinality_estimator_factories_.begin()->first); // set default cardinality estimator
}


Catalog::~Catalog()
{
    for (auto db : databases_)
        delete db.second;
    for (auto fn : standard_functions_)
        delete fn.second;
}

Catalog Catalog::the_catalog_;

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

const CostFunction & Catalog::cost_function() const {
    return *cost_function_;
}

std::unique_ptr<CostFunction> Catalog::cost_function(std::unique_ptr<CostFunction> cost_function) {
    cost_function_.swap(cost_function);
    return cost_function;
}

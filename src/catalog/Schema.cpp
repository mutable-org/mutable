#include "catalog/Schema.hpp"

#include "globals.hpp"
#include "mutable/util/fn.hpp"
#include "storage/ColumnStore.hpp"
#include "storage/PaxStore.hpp"
#include "storage/RowStore.hpp"
#include <algorithm>
#include <cmath>
#include <iterator>
#include <stdexcept>


using namespace m;


/*======================================================================================================================
 * Schema
 *====================================================================================================================*/

void Schema::dump(std::ostream &out) const { out << *this << std::endl; }
void Schema::dump() const { dump(std::cerr); }


/*======================================================================================================================
 * Attribute
 *====================================================================================================================*/

void Attribute::dump(std::ostream &out) const
{
    out << "Attribute `" << table.name << "`.`" << name << "`, "
        << "id " << id << ", "
        << "type " << *type
        << std::endl;
}

void Attribute::dump() const { dump(std::cerr); }


/*======================================================================================================================
 * Table
 *====================================================================================================================*/

void Table::dump(std::ostream &out) const
{
    out << "Table `" << name << '`';
    for (const auto &attr : attrs_)
        out << "\n` " << attr.id << ": `" << attr.name << "` " << *attr.type;
    out << std::endl;
}

void Table::dump() const { dump(std::cerr); }


/*======================================================================================================================
 * Function
 *====================================================================================================================*/

constexpr const char * Function::FNID_TO_STR_[];
constexpr const char * Function::KIND_TO_STR_[];

void Function::dump(std::ostream &out) const
{
    out << "Function{ name = \"" << name << "\", fnid = " << FNID_TO_STR_[fnid] << ", kind = " << KIND_TO_STR_[kind]
        << "}" << std::endl;
}


/*======================================================================================================================
 * Database
 *====================================================================================================================*/

Database::Database(const char *name)
    : name(name)
{
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
    : allocator_(new rewire::LinearAllocator())
{
    /* Initialize standard functions. */
#define DB_FUNCTION(NAME, KIND) { \
    auto name = pool(#NAME); \
    auto res = standard_functions_.emplace(name, new Function(name, Function::FN_ ## NAME, Function::KIND)); \
    insist(res.second, "function already defined"); \
}
#include "mutable/tables/Functions.tbl"
#undef DB_FUNCTION

    /* Initialize stores. */
#define DB_STORE(NAME, _) { \
    auto name = pool(#NAME); \
    auto res = store_factories_.emplace(name, new ConcreteStoreFactory<NAME>()); \
    insist(res.second, "store already defined"); \
}
#include "mutable/tables/Store.tbl"
#undef DB_STORE
    insist(store_factories_.size() != 0);
    default_store(store_factories_.begin()->first); // set default store
}

Catalog::~Catalog()
{
    for (auto db : databases_)
        delete db.second;
    for (auto fn : standard_functions_)
        delete fn.second;
    for (auto sf : store_factories_)
        delete sf.second;
}

Catalog Catalog::the_catalog_;

Database & Catalog::add_database(const char *name) {
    auto it = databases_.find(name);
    if (it != databases_.end()) throw std::invalid_argument("database with that name already exist");
    it = databases_.emplace_hint(it, name, new Database(name));
    return *it->second;
}

void Catalog::drop_database(const char *name) {
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

std::unique_ptr<Store> Catalog::create_store(const Table &tbl) const {
    insist(default_store_, "there must always be a default store");
    return default_store_->make(tbl);
}

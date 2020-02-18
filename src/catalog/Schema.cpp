#include "catalog/Schema.hpp"

#include "storage/Store.hpp"
#include "util/fn.hpp"
#include <algorithm>
#include <cmath>
#include <iterator>
#include <stdexcept>


using namespace db;


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

Table::~Table()
{
    delete store_;
}

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
#include "tables/Functions.tbl"
#undef DB_FUNCTION
}

Catalog::~Catalog()
{
    for (auto s : databases_)
        delete s.second;
    for (auto fn : standard_functions_)
        delete fn.second;
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

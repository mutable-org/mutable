#include "storage/Store.hpp"

#include <cmath>


using namespace db;


/*======================================================================================================================
 * Store
 *====================================================================================================================*/

const std::unordered_map<std::string, Store::kind_t> Store::STR_TO_KIND = {
#define DB_STORE(NAME, _) { #NAME,  Store::S_ ## NAME },
#include "tables/Store.tbl"
#undef DB_STORE
};

std::unique_ptr<Store> Store::Create(Store::kind_t kind, const Table &table) {
    switch(kind) {
#define DB_STORE(NAME, _) case S_ ## NAME: return Create ## NAME(table);
#include "tables/Store.tbl"
#undef DB_STORE
    }
}

void Store::dump() const { dump(std::cerr); }

#include "mutable/storage/Store.hpp"

#include <cmath>


using namespace m;


/*======================================================================================================================
 * Store
 *====================================================================================================================*/

const std::unordered_map<std::string, Store::kind_t> Store::STR_TO_KIND = {
#define DB_STORE(NAME, _) { #NAME,  Store::S_ ## NAME },
#include "mutable/tables/Store.tbl"
#undef DB_STORE
};

std::unique_ptr<Store> Store::Create(Store::kind_t kind, const Table &table) {
    switch(kind) {
#define DB_STORE(NAME, _) case S_ ## NAME: return Create ## NAME(table);
#include "mutable/tables/Store.tbl"
#undef DB_STORE
    }
}

void Store::dump() const { dump(std::cerr); }

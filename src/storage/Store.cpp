#include "storage/Store.hpp"

#include <cmath>


using namespace m;


/*======================================================================================================================
 * Store
 *====================================================================================================================*/

const std::unordered_map<std::string, Store::kind_t> Store::STR_TO_KIND = {
#define M_STORE(NAME, _) { #NAME,  Store::S_ ## NAME },
#include <mutable/tables/Store.tbl>
#undef M_STORE
};

std::unique_ptr<Store> Store::Create(Store::kind_t kind, const Table &table) {
    switch(kind) {
#define M_STORE(NAME, _) case S_ ## NAME: return Create ## NAME(table);
#include <mutable/tables/Store.tbl>
#undef M_STORE
    } M_LCOV_EXCL_LINE /* exclude default case from coverage. */
}

M_LCOV_EXCL_START
void Store::dump() const { dump(std::cerr); }
M_LCOV_EXCL_STOP


/*======================================================================================================================
 * accept()
 *====================================================================================================================*/

#define ACCEPT(CLASS) \
    void CLASS::accept(StoreVisitor &v) { v(*this); } \
    void CLASS::accept(ConstStoreVisitor &v) const { v(*this); }
M_STORE_LIST(ACCEPT)
#undef ACCEPT

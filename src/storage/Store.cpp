#include "storage/Store.hpp"

#include <cmath>


using namespace m;


/*======================================================================================================================
 * Store
 *====================================================================================================================*/

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

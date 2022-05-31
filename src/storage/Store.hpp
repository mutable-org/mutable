#pragma once

#include <mutable/storage/Store.hpp>

#include "storage/ColumnStore.hpp"
#include "storage/PaxStore.hpp"
#include "storage/RowStore.hpp"


namespace m {

/* X macro for the internal `Store` implementations. */
#define M_STORE_LIST(X) \
    X(ColumnStore) \
    X(PaxStore) \
    X(RowStore)

}

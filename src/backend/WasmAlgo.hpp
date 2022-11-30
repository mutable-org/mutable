#pragma once

#include "backend/WasmUtil.hpp"
#include <mutable/parse/AST.hpp>


namespace m {

namespace wasm {

/*======================================================================================================================
 * sorting
 *====================================================================================================================*/

/** Sorts the buffer \p buffer using the quicksort algorithm and a branchless binary partition algorithm.  The
 * ordering is specified by \p order where the first element is the expression to order on and the second element is
 * `true` iff ordering should be performed ascending. */
template<bool IsGlobal>
void quicksort(const Buffer<IsGlobal> &buffer, const std::vector<SortingOperator::order_type> &order);


/*======================================================================================================================
 * explicit instantiation declarations
 *====================================================================================================================*/

extern template void quicksort(const GlobalBuffer&, const std::vector<SortingOperator::order_type>&);

}

}

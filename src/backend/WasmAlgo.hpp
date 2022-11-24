#pragma once

#include "backend/WasmUtil.hpp"
#include <mutable/parse/AST.hpp>


namespace m {

namespace wasm {

/*======================================================================================================================
 * sorting
 *====================================================================================================================*/

template<bool IsGlobal>
void quicksort(const Buffer<IsGlobal> &buffer, const std::vector<SortingOperator::order_type> &order);


/*======================================================================================================================
 * explicit instantiation declarations
 *====================================================================================================================*/

extern template void quicksort(const GlobalBuffer&, const std::vector<SortingOperator::order_type>&);

}

}

#pragma once

#include "backend/WasmUtil.hpp"


namespace m {

namespace wasm {

/*======================================================================================================================
 * sorting
 *====================================================================================================================*/

template<bool IsGlobal>
void quicksort(const Buffer<IsGlobal> &buffer, const std::vector<std::pair<const m::Expr*, bool>> &order);


/*======================================================================================================================
 * explicit instantiation declarations
 *====================================================================================================================*/

extern template void quicksort(const GlobalBuffer&, const std::vector<std::pair<const m::Expr*, bool>>&);

}

}

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
 * hashing
 *====================================================================================================================*/

/*----- bit mix functions --------------------------------------------------------------------------------------------*/

/** Mixes the bits of \p bits using the Murmur3 algorithm. */
U64 murmur3_bit_mix(U64 bits);


/*----- hash functions -----------------------------------------------------------------------------------------------*/

/** Hashes \p num_bytes bytes of \p bytes using the FNV-1a algorithm. */
U64 fnv_1a(Ptr<U8> bytes, U32 num_bytes);
/** Hashes the string \p str of type \p cs. */
U64 str_hash(const CharacterSequence &cs, Ptr<Char> str);
/** Hashes the elements of \p values where the first element is the type of the value to hash and the second element
 * is the value itself using the Murmur3-64a algorithm. */
U64 murmur3_64a_hash(std::vector<std::pair<const Type*, SQL_t>> values);


/*======================================================================================================================
 * explicit instantiation declarations
 *====================================================================================================================*/

extern template void quicksort(const GlobalBuffer&, const std::vector<SortingOperator::order_type>&);

}

}

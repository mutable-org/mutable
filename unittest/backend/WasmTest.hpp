#pragma once

#include "backend/WasmDSL.hpp"
#include "backend/WasmMacro.hpp"
#include <mutable/catalog/Catalog.hpp>


/*======================================================================================================================
 * Helper macros and functions
 *====================================================================================================================*/

namespace m {

namespace wasm {

inline void wasm_check(Boolx1 cond, const char *msg)
{
    IF (not cond) {
        Throw(exception::failed_unittest_check, Catalog::Get().pool(msg));
    };
}

inline void wasm_check(_Boolx1 cond, const char *msg)
{
    IF (not (cond.is_true_and_not_null())) {
        Throw(exception::failed_unittest_check, Catalog::Get().pool(msg));
    };
}

/** Similar to `CHECK` from `catch.hpp` but in wasm.  Checks `COND` and throws a `failed_unittest_check` exception
 * with error message `MSG` if `COND` was not fulfilled. */
#define WASM_CHECK(COND, MSG) wasm_check(COND, MSG)

/** Emits `WASM_CHECK`s to check equality of `expected` and `actual` for each of the first `length`-th characters. */
inline void check_string(const char *expected, Ptr<Charx1> actual, std::size_t length, std::string msg)
{
    for (std::size_t idx = 0; idx < length; ++idx)
        WASM_CHECK(expected[idx] == *(actual.clone() + idx), (msg + " at index " + std::to_string(idx)).c_str());
    actual.discard();
}

}

}

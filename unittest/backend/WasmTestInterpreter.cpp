#include "catch2/catch.hpp"
#include "backend/WasmTest.hpp"

#include <mutable/util/concepts.hpp>


using namespace m::wasm;


namespace m {

namespace wasm {

template<typename>
struct invoke_interpreter;

}

}

/** Extracts `L` values of type `T` from a `::wasm::Literal`. */
template<typename T, std::size_t L>
auto literal_value(::wasm::Literal literal)
{
    static_assert(not std::is_same_v<T, void>, "cannot access void literal");

    if constexpr (std::is_integral_v<T> and L == 1) { // signed and unsigned
        if constexpr (sizeof(T) <= 4)
            return literal.geti32();
        if constexpr (sizeof(T) == 8)
            return literal.geti64();
    }
    if constexpr (std::is_floating_point_v<T> and L == 1) {
        if constexpr (sizeof(T) <= 4)
            return literal.getf32();
        if constexpr (sizeof(T) == 8)
            return literal.getf64();
    }
    if constexpr (L > 1) {
        auto vec = literal.getv128();
        std::array<T, L> res;
        for (std::size_t idx = 0; idx < L; ++idx) {
            if constexpr (m::boolean<T>)
                res[idx] = bool(*(reinterpret_cast<uint8_t*>(vec.data()) + idx));
            else
                res[idx] = *(reinterpret_cast<T*>(vec.data()) + idx);
        }
        return res;
    }

    M_unreachable("unsupported type");
}

template<typename ReturnType, typename... ParamTypes, std::size_t ReturnL, std::size_t... ParamLs>
struct invoke_interpreter<PrimitiveExpr<ReturnType, ReturnL>(PrimitiveExpr<ParamTypes, ParamLs>...)>
{
    using fn_proxy_type = FunctionProxy<PrimitiveExpr<ReturnType, ReturnL>(PrimitiveExpr<ParamTypes, ParamLs>...)>;
    using return_type = std::conditional_t<ReturnL == 1, ReturnType, std::array<ReturnType, ReturnL>>;

    return_type operator()(fn_proxy_type &func, PrimitiveExpr<ParamTypes, ParamLs>... parameters) {
        Module::Allocator().perform_pre_allocations();

        M_insist(Module::Validate(), "invalid module"); // validate test code

        M_insist(((parameters.clone().expr()->_id == ::wasm::Expression::ConstId) and ... ),
                 "can only invoke function with constant parameters");
        auto results =
            Module::Get().instantiate().callFunction(func.c_name(),
                                                     { static_cast<const ::wasm::Const*>(parameters.expr())->value... });

        if constexpr (std::is_same_v<ReturnType, void>) {
            CHECK(results.isNone());
        } else {
            REQUIRE(results.isConcrete());
            REQUIRE(results.size() == 1);
            return literal_value<ReturnType, ReturnL>(results[0]);
        }
    };

    return_type operator()(fn_proxy_type &func, ParamTypes... parameters) requires (sizeof...(ParamTypes) > 0) {
        return operator()(func, PrimitiveExpr<ParamTypes, ParamLs>(parameters)...);
    }
};

template<typename ReturnType, typename... ParamTypes>
struct invoke_interpreter<ReturnType(ParamTypes...)>
    : invoke_interpreter<PrimitiveExpr<ReturnType, 1>(PrimitiveExpr<ParamTypes, 1>...)>
{
    using invoke_interpreter<PrimitiveExpr<ReturnType, 1>(PrimitiveExpr<ParamTypes, 1>...)>::invoke_interpreter;
};

template<typename... ParamTypes, std::size_t... ParamLs>
struct invoke_interpreter<void(PrimitiveExpr<ParamTypes, ParamLs>...)>
    : invoke_interpreter<PrimitiveExpr<void, 1>(PrimitiveExpr<ParamTypes, ParamLs>...)>
{
    using invoke_interpreter<PrimitiveExpr<void, 1>(PrimitiveExpr<ParamTypes, ParamLs>...)>::invoke_interpreter;
};

#define INVOKE(NAME, ...) [&]{ \
    invoke_interpreter<decltype(NAME)::type> invoker; \
    return invoker(NAME, ##__VA_ARGS__); \
}()

#define INVOKE_INLINE_(TYPE, BODY, ...) [&]{ \
    invoke_interpreter<TYPE> invoker; \
    FUNCTION(test, TYPE) BODY \
    return invoker(test, ##__VA_ARGS__); \
}()

#define INVOKE_INLINE(TYPE, BODY, ...) { \
    SECTION("") { \
        INVOKE_INLINE_(TYPE, M_UNPACK(BODY), ##__VA_ARGS__); \
    } \
}

#define CHECK_RESULT(RESULT, NAME, ...) { \
    auto result = INVOKE(NAME, ##__VA_ARGS__); \
    CHECK(RESULT == result); \
}

#define CHECK_RESULT_INLINE(RESULT, TYPE, BODY, ...) { \
    SECTION("") { \
        auto result = INVOKE_INLINE_(TYPE, M_UNPACK(BODY), ##__VA_ARGS__); \
        CHECK(RESULT == result); \
    } \
}

#define BACKEND_NAME "Interpreter"

#include "WasmDSLTest.tpp"
#include "WasmOperatorTest.tpp"
#include "WasmUtilTest.tpp"

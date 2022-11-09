#include "catch2/catch.hpp"
#include "backend/WasmTest.hpp"


using namespace m::wasm;


namespace m {

namespace wasm {

template<typename>
struct invoke_interpreter;

}

}

/** Extracts the value of type `T` from a `::wasm::Literal`. */
template<typename T>
T literal_value(::wasm::Literal literal)
{
    static_assert(not std::is_same_v<T, void>, "cannot access void literal");

    if constexpr (std::is_integral_v<T>) { // signed and unsigned
        if constexpr (sizeof(T) <= 4)
            return literal.geti32();
        if constexpr (sizeof(T) == 8)
            return literal.geti64();
    }
    if constexpr (std::is_floating_point_v<T>) {
        if constexpr (sizeof(T) <= 4)
            return literal.getf32();
        if constexpr (sizeof(T) == 8)
            return literal.getf64();
    }

    M_unreachable("unsupported type");
}

template<typename ReturnType, typename... ParamTypes>
struct invoke_interpreter<PrimitiveExpr<ReturnType>(PrimitiveExpr<ParamTypes>...)>
{
    using fn_proxy_type = FunctionProxy<PrimitiveExpr<ReturnType>(PrimitiveExpr<ParamTypes>...)>;

    ReturnType operator()(fn_proxy_type &func, ParamTypes... parameters) {
        Module::Allocator().perform_pre_allocations();

        M_insist(Module::Validate(), "invalid module"); // validate test code

        auto results = Module::Get().instantiate().callFunction(func.c_name(), { ::wasm::Literal(parameters)... });

        if constexpr (std::is_same_v<ReturnType, void>) {
            CHECK(results.isNone());
        } else {
            REQUIRE(results.isConcrete());
            REQUIRE(results.size() == 1);
            return literal_value<ReturnType>(results[0]);
        }
    };
};

template<typename ReturnType, typename... ParamTypes>
struct invoke_interpreter<ReturnType(ParamTypes...)>
    : invoke_interpreter<PrimitiveExpr<ReturnType>(PrimitiveExpr<ParamTypes>...)>
{
    using invoke_interpreter<PrimitiveExpr<ReturnType>(PrimitiveExpr<ParamTypes>...)>::invoke_interpreter;
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

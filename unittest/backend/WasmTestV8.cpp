#include "catch2/catch.hpp"
#include "backend/WasmTest.hpp"

#include "backend/V8Engine.hpp"
#include "backend/WebAssembly.hpp"
#include <mutable/util/concepts.hpp>
#include <string>
#include <v8.h>


using namespace m::memory;
using namespace m::wasm;
using namespace m::wasm::detail;


namespace m {

namespace wasm {

template<typename>
struct invoke_v8;

}

}

/** Extracts `L` values of type `T` from a `v8::Local<v8::Value>`. */
template<typename T, std::size_t L>
auto local_value(v8::Local<v8::Value> local)
{
    static_assert(not std::is_same_v<T, void>, "cannot access void value");

    if constexpr (std::is_integral_v<T> and L == 1) {
        if constexpr (std::is_signed_v<T>) {
            if constexpr (sizeof(T) <= 4)
                return local.As<v8::Int32>()->Value();
            if constexpr (sizeof(T) == 8)
                return local.As<v8::BigInt>()->Int64Value();
        } else {
            if constexpr (sizeof(T) <= 4)
                return local.As<v8::Uint32>()->Value();
            if constexpr (sizeof(T) == 8)
                return local.As<v8::BigInt>()->Uint64Value();
        }
    }
    if constexpr (std::is_floating_point_v<T> and L == 1)
        return local.As<v8::Number>()->Value();
    if constexpr (L > 1) {
        const ptrdiff_t vec_offset = local.As<v8::Uint32>()->Value();
        auto vec = m::WasmEngine::Get_Wasm_Context_By_ID(Module::ID()).vm.as<uint8_t*>() + vec_offset;
        std::array<T, L> res;
        for (std::size_t idx = 0; idx < L; ++idx) {
            if constexpr (m::boolean<T>)
                res[idx] = bool(*(vec + idx));
            else
                res[idx] = *(reinterpret_cast<T*>(vec) + idx);
        }
        return res;
    }

    M_unreachable("unsupported type");
}

template<typename ReturnType, typename... ParamTypes, std::size_t ReturnL, std::size_t... ParamLs>
struct invoke_v8<PrimitiveExpr<ReturnType, ReturnL>(PrimitiveExpr<ParamTypes, ParamLs>...)>
{
    using fn_proxy_type = FunctionProxy<PrimitiveExpr<ReturnType, ReturnL>(PrimitiveExpr<ParamTypes, ParamLs>...)>;
    using return_type = std::conditional_t<ReturnL == 1, ReturnType, std::array<ReturnType, ReturnL>>;

    private:
    v8::Isolate *isolate_ = nullptr;
    v8::ArrayBuffer::Allocator *allocator_ = nullptr;
    bool context_created_ = false;
    std::unique_ptr<V8InspectorClientImpl> inspector_;

    public:
    invoke_v8() {
        /* Ensure context for SIMD result vector in memory or CDT debugging. */
        if ((not std::same_as<ReturnType, void> and ReturnL > 1) or getenv("MUTABLE_CDT_PORT") != NULL)
            context_created_ = m::WasmEngine::Ensure_Wasm_Context_For_ID(Module::ID()).second;
    }

    ~invoke_v8() {
        isolate_->Exit();
        isolate_->Dispose();
        delete allocator_;
        inspector_.reset();
    }

    return_type operator()(fn_proxy_type &func, PrimitiveExpr<ParamTypes, ParamLs>... parameters) {
        /* Compile test code into `main` function. */
        using return_type_main =
            std::conditional_t<ReturnL == 1, PrimitiveExpr<ReturnType, ReturnL>, Ptr<PrimitiveExpr<ReturnType, ReturnL>>>;
        Function<return_type_main(void)> main("main");
        BLOCK_OPEN(main.body())
        {
            if constexpr (std::same_as<ReturnType, void>) {
                func(parameters...);
            } else {
                if constexpr (ReturnL == 1) {
                    main.emit_return(func(parameters...));
                } else {
                    /* Store SIMD vector in memory and return its address. */
                    auto ptr = Module::Allocator().pre_malloc<ReturnType, ReturnL>();
                    *ptr.clone() = func(parameters...);
                    main.emit_return(ptr);
                }
            }
        }
        Module::Get().emit_function_export("main");
        Module::Allocator().perform_pre_allocations();

        M_insist(Module::Validate(), "invalid module"); // validate test code

        /* Set flags and create isolate. */
        v8::V8::SetFlagsFromString(
            "--no-liftoff "
            "--wasm-bounds-checks "
            "--wasm-stack-checks "
        );
        v8::Isolate::CreateParams create_params;
        create_params.array_buffer_allocator = allocator_ = v8::ArrayBuffer::Allocator::NewDefaultAllocator();
        isolate_ = v8::Isolate::New(create_params);

        v8::Locker locker(isolate_);
        isolate_->Enter();

        /* Create required V8 scopes. */
        v8::Isolate::Scope isolate_scope(isolate_);
        v8::HandleScope handle_scope(isolate_); // tracks and disposes of all object handles

        /* Create context. */
        v8::Local<v8::ObjectTemplate> global = v8::ObjectTemplate::New(isolate_);
        global->Set(isolate_, "set_wasm_instance_raw_memory", v8::FunctionTemplate::New(isolate_, set_wasm_instance_raw_memory));
        v8::Local<v8::Context> context = v8::Context::New(isolate_, /* extensions= */ nullptr, global);
        v8::Context::Scope context_scope(context);

        /* Create the import object for instantiating the WebAssembly module. */
        auto imports = v8::Object::New(isolate_);
        auto env = v8::Object::New(isolate_);
        auto func_insist = v8::Function::New(context, insist).ToLocalChecked();
        env->Set(context, mkstr(*isolate_, "insist"), func_insist).Check();
        auto func_throw = v8::Function::New(context, _throw).ToLocalChecked();
        env->Set(context, mkstr(*isolate_, "throw"), func_throw).Check();
        Module::Get().emit_function_import<void(void*,uint32_t)>("read_result_set");
        auto func_read_result_set = v8::Function::New(context, read_result_set).ToLocalChecked();
        env->Set(context, mkstr(*isolate_, "read_result_set"), func_read_result_set).Check();
        M_DISCARD imports->Set(context, mkstr(*isolate_, "imports"), env);

        /* Create a WebAssembly instance object. */
        auto instance = instantiate(*isolate_, imports);

        /* If a wasm context was given by the caller, map its memory. */
        Memory mem; // memory object must not be destroyed during the execution
        if (m::WasmEngine::Has_Wasm_Context(Module::ID())) {
            /* Map the remaining address space to the output buffer. */
            auto &wasm_context = m::WasmEngine::Get_Wasm_Context_By_ID(Module::ID());
            const auto bytes_remaining = wasm_context.vm.size() - wasm_context.heap;
            mem = Catalog::Get().allocator().allocate(bytes_remaining);
            mem.map(bytes_remaining, 0, wasm_context.vm, wasm_context.heap);

            /* If wasm context exists, set the underlying memory for the instance. */
            v8::SetWasmInstanceRawMemory(instance, wasm_context.vm.as<uint8_t*>(), wasm_context.vm.size());
        }

        /* Get the exports of the created WebAssembly instance. */
        auto exports = instance->Get(context, mkstr(*isolate_, "exports")).ToLocalChecked().template As<v8::Object>();
        auto main_handle = exports->Get(context, mkstr(*isolate_, "main")).ToLocalChecked().template As<v8::Function>();

        /* If a debugging port is specified, set up the inspector and start it. */
        if (auto port_opt = getenv("MUTABLE_CDT_PORT"); port_opt != NULL) {
            try {
                std::string _port_opt(port_opt);
                auto port = std::stoi(_port_opt);
                if (port >= 1024 and not inspector_)
                    inspector_ = std::make_unique<V8InspectorClientImpl>(port, isolate_);
            } catch (std::exception &e) {
                throw m::invalid_argument(e.what());
            }
        }
        if (bool(inspector_)) {
            run_inspector(*inspector_, *isolate_, env);
            std::exit(EXIT_SUCCESS);
        }

        /* Invoke the exported function `main` of the module. */
        auto result = main_handle->Call(context, context->Global(), 0, nullptr).ToLocalChecked();
        REQUIRE(not result.IsEmpty());

        if constexpr (std::is_same_v<ReturnType, void>) {
            CHECK(result->IsUndefined());
            if (context_created_)
                m::WasmEngine::Dispose_Wasm_Context(Module::ID());
        } else {
            auto res = local_value<ReturnType, ReturnL>(result);
            if (context_created_)
                m::WasmEngine::Dispose_Wasm_Context(Module::ID());
            return res;
        }
    }

    return_type operator()(fn_proxy_type &func, ParamTypes... parameters) requires (sizeof...(ParamTypes) > 0) {
        return operator()(func, PrimitiveExpr<ParamTypes, ParamLs>(parameters)...);
    }
};

template<typename ReturnType, typename... ParamTypes>
struct invoke_v8<ReturnType(ParamTypes...)> : invoke_v8<PrimitiveExpr<ReturnType, 1>(PrimitiveExpr<ParamTypes, 1>...)>
{
    using invoke_v8<PrimitiveExpr<ReturnType, 1>(PrimitiveExpr<ParamTypes, 1>...)>::invoke_v8;
};

template<typename... ParamTypes, std::size_t... ParamLs>
struct invoke_v8<void(PrimitiveExpr<ParamTypes, ParamLs>...)>
    : invoke_v8<PrimitiveExpr<void, 1>(PrimitiveExpr<ParamTypes, ParamLs>...)>
{
    using invoke_v8<PrimitiveExpr<void, 1>(PrimitiveExpr<ParamTypes, ParamLs>...)>::invoke_v8;
};

#define INVOKE(NAME, ...) [&]{ \
    invoke_v8<decltype(NAME)::type> invoker; \
    return invoker(NAME, ##__VA_ARGS__); \
}()

#define INVOKE_INLINE_(TYPE, BODY, ...) [&]{ \
    invoke_v8<TYPE> invoker; \
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

#define BACKEND_NAME "V8"

#include "WasmDSLTest.tpp"
#include "WasmOperatorTest.tpp"
#include "WasmUtilTest.tpp"

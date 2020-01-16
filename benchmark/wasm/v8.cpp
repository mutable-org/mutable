#include "v8.hpp"

#include "util/macro.hpp"
#include <binaryen-c.h>
#include <chrono>
#include <iostream>
#include <libplatform/libplatform.h>
#include <v8.h>


#define V8STR(str) ( v8::String::NewFromUtf8(isolate, (str)).ToLocalChecked() )


namespace ch = std::chrono;


int main()
{
    constexpr std::size_t NUM_INTS = 50'000;

    std::cerr << "Expected sum: " << (NUM_INTS * (NUM_INTS - 1) / 2) << '\n';

    /* Initialize data. */
    int *data = new int[NUM_INTS];
    for (std::size_t i = 0; i != NUM_INTS; ++i)
        data[i] = i;

    {
        int sum = 0;
        auto begin = ch::high_resolution_clock::now();
        for (auto p = data; p != data + NUM_INTS; ++p)
            sum = inlined_sum(sum, *p);
        auto end = ch::high_resolution_clock::now();
        std::cout << "inlined_sum," << sum
                  << ',' << ch::duration_cast<ch::nanoseconds>(end - begin).count() / 1e3 << '\n';
    }

    {
        int sum = 0;
        auto begin = ch::high_resolution_clock::now();
        for (auto p = data; p != data + NUM_INTS; ++p)
            sum = c_sum(sum, *p);
        auto end = ch::high_resolution_clock::now();
        std::cout << "c_sum," << sum
                  << ',' << ch::duration_cast<ch::nanoseconds>(end - begin).count() / 1e3 << '\n';
    }

    {
        int sum = 0;
        auto vsum = virtual_sum::Create();
        auto begin = ch::high_resolution_clock::now();
        for (auto p = data; p != data + NUM_INTS; ++p)
            sum = (*vsum)(sum, *p);
        auto end = ch::high_resolution_clock::now();
        std::cout << "virtual_sum," << sum
                  << ',' << ch::duration_cast<ch::nanoseconds>(end - begin).count() / 1e3 << '\n';
        delete vsum;
    }

    {
        auto platform = v8::platform::NewDefaultPlatform();
        v8::V8::InitializePlatform(platform.get());
        v8::V8::Initialize();
        v8::Isolate::CreateParams create_params;
        create_params.array_buffer_allocator = v8::ArrayBuffer::Allocator::NewDefaultAllocator();
        auto isolate = v8::Isolate::New(create_params);

        {
            v8::Isolate::Scope isolate_scope(isolate);
            v8::HandleScope handle_scope(isolate); // tracks and disposes of all object handles
            v8::Local<v8::Context> Ctx = v8::Context::New(isolate);
            v8::Context::Scope context_scope(Ctx);

            auto module = v8::WasmModuleObject::DeserializeOrCompile(
                /* isolate=           */ isolate,
                /* serialized_module= */ { nullptr, 0 },
                /* wire_bytes=        */ v8::MemorySpan<const uint8_t>(wasm_module, wasm_module_size)
            ).ToLocalChecked();

            auto imports = v8::Object::New(isolate);

            v8::Local<v8::Value> instance_args[] = { module, imports };
            auto instance =
                Ctx->Global()->                                                             // get the `global` object
                Get(Ctx, V8STR("WebAssembly")).ToLocalChecked().As<v8::Object>()->          // get WebAssembly class
                Get(Ctx, V8STR("Instance")).ToLocalChecked().As<v8::Object>()->             // get WebAssembly.Instance class
                CallAsConstructor(Ctx, 2, instance_args).ToLocalChecked().As<v8::Object>(); // instantiate WebAssembly.Instance

            auto exports = instance->Get(Ctx, V8STR("exports")).ToLocalChecked().As<v8::Object>();
            auto fn = exports->Get(Ctx, V8STR("_Z3sumii")).ToLocalChecked().As<v8::Function>();
            insist(not fn.IsEmpty());

            v8::Local<v8::Value> args[] = {
                v8::Int32::New(isolate, 0),
                v8::Int32::New(isolate, 0),
            };

            auto begin = ch::high_resolution_clock::now();
            for (auto p = data; p != data + NUM_INTS; ++p) {
                args[1] = v8::Int32::New(isolate, *p);
                args[0] = fn->Call(Ctx, Ctx->Global(), 2, args).ToLocalChecked();
            }
            auto end = ch::high_resolution_clock::now();
            std::cout << "wasm_sum," << args[0].As<v8::Int32>()->Value()
                      << ',' << ch::duration_cast<ch::nanoseconds>(end - begin).count() / 1e3 << '\n';
        }

        isolate->Dispose();
    }

    delete[] data;
}

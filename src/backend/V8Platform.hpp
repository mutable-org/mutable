#pragma once

#include "util/memory.hpp"
#include "WebAssembly.hpp"
#include <libplatform/libplatform.h>
#include <v8.h>


namespace db {

/** The `V8Platform` is a `WasmPlatform` using [V8, Google's open source high-performance JavaScript and WebAssembly
 * engine] (https://v8.dev/). */
struct V8Platform : WasmPlatform
{
    private:
    static std::unique_ptr<v8::Platform> PLATFORM_;
    v8::ArrayBuffer::Allocator *allocator_ = nullptr;
    v8::Isolate *isolate_ = nullptr;
    rewire::Memory output_buffer_; ///< output buffer where the WASM modules write to

    public:
    V8Platform();
    ~V8Platform();

    void execute(const Operator &plan) override;

    private:
    /** Compile the `WasmModule` `module` and instantiate a `v8::WasmModuleObject` instance. */
    v8::Local<v8::WasmModuleObject> instantiate(const WasmModule &module, v8::Local<v8::Object> imports);
    /** Creates a V8 object that captures the entire environment.  TODO Only capture things relevant to the module. */
    v8::Local<v8::Object> create_env(const WasmContext &wasm_context, const Operator &plan) const;
    /** Converts any V8 value to JSON. */
    v8::Local<v8::String> to_json(v8::Local<v8::Value> val) const;
};

}

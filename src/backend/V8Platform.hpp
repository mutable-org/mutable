#pragma once

#include "backend/WasmUtil.hpp"
#include <libplatform/libplatform.h>
#include <v8.h>


namespace m::detail {

namespace v8_helper {

struct V8InspectorClientImpl;

}

struct V8PlatformDestructor;

void create_V8Platform();
void destroy_V8Platform();
void register_WasmV8();

/*======================================================================================================================
 * V8Platform
 *====================================================================================================================*/

/** The `V8Platform` is a `WasmPlatform` using [V8, Google's open source high-performance JavaScript and WebAssembly
 * engine] (https://v8.dev/). */
struct V8Platform : m::WasmPlatform
{
    friend void create_V8Platform();
    friend void destroy_V8Platform();
    friend void register_WasmV8();

    private:
    static v8::Platform *PLATFORM_;
    v8::ArrayBuffer::Allocator *allocator_ = nullptr;
    v8::Isolate *isolate_ = nullptr;

    /*----- Objects for remote debugging via CDT. --------------------------------------------------------------------*/
    std::unique_ptr<v8_helper::V8InspectorClientImpl> inspector_;

    public:
    V8Platform();

    V8Platform(const V8Platform &) = delete;

    V8Platform(V8Platform &&) = default;

    ~V8Platform();

    static v8::Platform * platform()
    {
        M_insist(bool(PLATFORM_));
        return PLATFORM_;
    }

    m::WasmModule compile(const m::Operator & plan) const override;

    void execute(const m::Operator & plan) override;

    private:
    /** Compile the `WasmModule` `module` and instantiate a `v8::WasmModuleObject` instance. */
    v8::Local<v8::WasmModuleObject> instantiate(const m::WasmModule & module, v8::Local<v8::Object> imports);

    /** Creates a V8 object that captures the entire environment.  TODO Only capture things relevant to the module. */
    v8::Local<v8::Object> create_env(WasmContext & wasm_context, const m::Operator & plan) const;

    /** Convert a `std::string` to a `v8::String`. */
    v8::Local<v8::String> mkstr(const std::string & str) const;

    /** Converts any V8 value to JSON. */
    v8::Local<v8::String> to_json(v8::Local<v8::Value> val) const;

    /** Create a JavaScript document for debugging via CDT. */
    std::string create_js_debug_script(const m::WasmModule & module, v8::Local<v8::Object> env,
                                       const WasmPlatform::WasmContext & wasm_context);
};

}

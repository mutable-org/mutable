#pragma once

#include "WebAssembly.hpp"
#include <libplatform/libplatform.h>
#include <v8.h>


namespace db {

struct V8Platform : WasmPlatform
{
    private:
    static std::unique_ptr<v8::Platform> PLATFORM_;
    v8::ArrayBuffer::Allocator *allocator_ = nullptr;
    v8::Isolate *isolate_ = nullptr;

    public:
    V8Platform();
    ~V8Platform();

    void execute(const WASMModule &module) override;
};

}

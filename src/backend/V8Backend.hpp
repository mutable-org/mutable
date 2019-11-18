#pragma once

#include "WebAssembly.hpp"
#include <libplatform/libplatform.h>
#include <v8.h>


namespace db {

struct V8Backend
{
    private:
    static std::unique_ptr<v8::Platform> PLATFORM_;
    v8::ArrayBuffer::Allocator *allocator_ = nullptr;
    v8::Isolate *isolate_ = nullptr;

    public:
    V8Backend();
    ~V8Backend();

    void execute(const WASMModule &module);
};

}

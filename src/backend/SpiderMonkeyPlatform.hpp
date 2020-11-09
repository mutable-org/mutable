#pragma once

#include "WebAssembly.hpp"
#include "jsapi.h"


namespace m {

struct SpiderMonkeyPlatform : WasmPlatform
{
    private:
    static bool is_init_;
    JSContext *ctx_ = nullptr;

    public:
    SpiderMonkeyPlatform();
    ~SpiderMonkeyPlatform();

    void execute(const WASMModule &module) override;
};

}

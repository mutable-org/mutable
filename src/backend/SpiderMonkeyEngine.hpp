#pragma once

#include "WebAssembly.hpp"
#include "jsapi.h"


namespace m {

struct SpiderMonkeyEngine : WasmEngine
{
    private:
    static bool is_init_;
    JSContext *ctx_ = nullptr;

    public:
    SpiderMonkeyEngine();
    ~SpiderMonkeyEngine();

    void execute(const WASMModule &module) override;
};

}

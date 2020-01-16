#pragma once

#include "WebAssembly.hpp"
#include "jsapi.h"


namespace db {

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

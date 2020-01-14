#pragma once

#include "WebAssembly.hpp"
#include "jsapi.h"


namespace db {

struct SpiderMonkeyPlatform : WasmPlatform
{
    private:

    public:
    SpiderMonkeyPlatform();
    ~SpiderMonkeyPlatform();

    void execute(const WASMModule &module) override;
};

}

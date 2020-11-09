#include "backend/SpiderMonkeyPlatform.hpp"

#include "js/ArrayBuffer.h"
#include "js/Initialization.h"


using namespace m;


bool SpiderMonkeyPlatform::is_init_ = false;

SpiderMonkeyPlatform::SpiderMonkeyPlatform()
{
    if (not is_init_) {
        if (not JS_Init())
            throw std::runtime_error("failed to initialize SpiderMonkey");
        is_init_ = true;
    }
    if (not ctx_) {
        ctx_ = JS_NewContext(/* maxbytes= */ 2048 * 1024 * 1024); // 2GiB
    }
}

SpiderMonkeyPlatform::~SpiderMonkeyPlatform()
{
    if (ctx_)
        JS_DestroyContext(ctx_);
    if (is_init_)
        JS_ShutDown();
}

void SpiderMonkeyPlatform::execute(const WASMModule &module)
{
    std::cerr << "Executing the WASM module on the SpiderMonkey platform.\n";
    // TODO
}

std::unique_ptr<Backend> Backend::CreateWasmSpiderMonkey()
{
    return std::make_unique<WasmBackend>(std::make_unique<SpiderMonkeyPlatform>());
}

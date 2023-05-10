#include "backend/SpiderMonkeyEngine.hpp"

#include "js/ArrayBuffer.h"
#include "js/Initialization.h"


using namespace m;


bool SpiderMonkeyEngine::is_init_ = false;

SpiderMonkeyEngine::SpiderMonkeyEngine()
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

SpiderMonkeyEngine::~SpiderMonkeyEngine()
{
    if (ctx_)
        JS_DestroyContext(ctx_);
    if (is_init_)
        JS_ShutDown();
}

void SpiderMonkeyEngine::execute(const WASMModule &module)
{
    std::cerr << "Executing the WASM module on the SpiderMonkey engine.\n";
    // TODO
}

std::unique_ptr<Backend> Backend::CreateWasmSpiderMonkey()
{
    return std::make_unique<WasmBackend>(std::make_unique<SpiderMonkeyEngine>());
}

#include "backend/Backend.hpp"

#include "backend/Interpreter.hpp"
#include "backend/SpiderMonkeyPlatform.hpp"
#include "backend/V8Platform.hpp"
#include "backend/WebAssembly.hpp"


using namespace db;


std::unique_ptr<Backend> Backend::CreateInterpreter() { return std::make_unique<Interpreter>(); }
std::unique_ptr<Backend> Backend::CreateWasmV8() {
    return std::make_unique<WasmBackend>(std::make_unique<V8Platform>());
}
std::unique_ptr<Backend> Backend::CreateWasmSpiderMonkey() {
    return std::make_unique<WasmBackend>(std::make_unique<SpiderMonkeyPlatform>());
}

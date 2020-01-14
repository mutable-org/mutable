#include "backend/Backend.hpp"

#include "backend/Interpreter.hpp"
#include "backend/WebAssembly.hpp"


using namespace db;


std::unique_ptr<Backend> Backend::CreateInterpreter() { return std::make_unique<Interpreter>(); }
std::unique_ptr<Backend> Backend::CreateWasmV8() { return std::make_unique<WasmV8Backend>(); }
std::unique_ptr<Backend> Backend::CreateWasmSpiderMonkey() { return std::make_unique<WasmSpiderMonkeyBackend>(); }

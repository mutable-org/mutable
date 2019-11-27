#include "backend/Backend.hpp"

#include "backend/Interpreter.hpp"
#include "backend/WebAssembly.hpp"


using namespace db;


std::unique_ptr<Backend> Backend::CreateInterpreter() { return std::make_unique<Interpreter>(); }

std::unique_ptr<Backend> Backend::CreateWASM() { return std::make_unique<WASMBackend>(); }

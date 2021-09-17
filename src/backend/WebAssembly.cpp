#include "backend/WebAssembly.hpp"

#include <binaryen-c.h>
#include <iostream>
#include <utility>


using namespace m;


/*======================================================================================================================
 * WasmModule
 *====================================================================================================================*/

WasmModule::WasmModule() : ref_(BinaryenModuleCreate()) { }

WasmModule::~WasmModule() { BinaryenModuleDispose(ref_); }

std::pair<uint8_t*, std::size_t> WasmModule::binary() const
{
    auto result = BinaryenModuleAllocateAndWrite(ref_, nullptr);
    return std::make_pair(reinterpret_cast<uint8_t*>(result.binary), result.binaryBytes);
}

std::ostream & m::operator<<(std::ostream &out, const WasmModule &module)
{
    auto result = BinaryenModuleAllocateAndWriteText(module.ref_);
    out << result;
    free(result);
    return out;
}

void WasmModule::dump(std::ostream &out) const {
    out << *this;
    auto [buffer, length] = binary();
    out << '[' << std::hex;
    for (auto ptr = buffer, end = buffer + length; ptr != end; ++ptr) {
        if (ptr != buffer) out << ", ";
        out << "0x" << uint32_t(*ptr);
    }
    out << std::dec;
    out << ']' << std::endl;
    free(buffer);
}
void WasmModule::dump() const { dump(std::cerr); }


/*======================================================================================================================
 * WasmPlatform
 *====================================================================================================================*/

uint32_t WasmPlatform::wasm_counter_ = 0;
std::unordered_map<uint32_t, std::unique_ptr<WasmPlatform::WasmContext>> WasmPlatform::contexts_;


/*======================================================================================================================
 * WasmBackend
 *====================================================================================================================*/

void WasmBackend::execute(const Operator &plan) const { platform_->execute(plan); }

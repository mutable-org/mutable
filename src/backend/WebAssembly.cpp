#include "backend/WebAssembly.hpp"

#include <binaryen-c.h>
#include <iostream>
#include <sys/mman.h>
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

M_LCOV_EXCL_START
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
M_LCOV_EXCL_STOP


/*======================================================================================================================
 * WasmPlatform
 *====================================================================================================================*/

WasmPlatform::WasmContext::WasmContext(uint32_t id, config_t config, const Operator &plan, std::size_t size)
    : config_(config)
    , id(id)
    , plan(plan)
    , vm(size)
{
    install_guard_page();
}

void WasmPlatform::WasmContext::install_guard_page()
{
    M_insist(Is_Page_Aligned(heap));
    if (not config(TRAP_GUARD_PAGES)) {
        /* Map the guard page to a fresh, zeroed page. */
        M_DISCARD mmap(vm.as<uint8_t*>() + heap, get_pagesize(), PROT_READ, MAP_FIXED|MAP_ANONYMOUS|MAP_PRIVATE, -1, 0);
    }
    heap += get_pagesize(); // install guard page
}


/*======================================================================================================================
 * WasmBackend
 *====================================================================================================================*/

void WasmBackend::execute(const Operator &plan) const { platform_->execute(plan); }

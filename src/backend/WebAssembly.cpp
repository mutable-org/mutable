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
 * WasmEngine
 *====================================================================================================================*/

WasmEngine::WasmContext::WasmContext(uint32_t id, config_t config, const Operator &plan, std::size_t size)
    : config_(config)
    , id(id)
    , plan(plan)
    , vm(size)
{
    install_guard_page(); // map nullptr page

    M_insist(size <= WASM_MAX_MEMORY);
}

uint32_t WasmEngine::WasmContext::map_table(const Table &table)
{
    M_insist(Is_Page_Aligned(heap));

    const auto num_rows_per_instance = table.layout().child().num_tuples();
    const auto instance_stride_in_bytes = table.layout().stride_in_bits() / 8U;
    const std::size_t num_instances = (table.store().num_rows() + num_rows_per_instance - 1) / num_rows_per_instance;
    const std::size_t bytes = instance_stride_in_bytes * num_instances;

    /* Map entry into WebAssembly linear memory. */
    const auto off = heap;
    const auto aligned_bytes = Ceil_To_Next_Page(bytes);
    const auto &mem = table.store().memory();
    if (aligned_bytes) {
        mem.map(aligned_bytes, 0, vm, off);
        heap += aligned_bytes;
        install_guard_page();
    }
    M_insist(Is_Page_Aligned(heap));

    return off;
}

void WasmEngine::WasmContext::install_guard_page()
{
    M_insist(Is_Page_Aligned(heap));
    if (not config(TRAP_GUARD_PAGES)) {
        /* Map the guard page to a fresh, zeroed page. */
        M_DISCARD mmap(vm.as<uint8_t*>() + heap, get_pagesize(), PROT_READ, MAP_FIXED|MAP_ANONYMOUS|MAP_PRIVATE, -1, 0);
    }
    heap += get_pagesize(); // install guard page
    M_insist(Is_Page_Aligned(heap));
}


/*======================================================================================================================
 * WasmBackend
 *====================================================================================================================*/

void WasmBackend::execute(const Operator &plan) const { engine_->execute(plan); }

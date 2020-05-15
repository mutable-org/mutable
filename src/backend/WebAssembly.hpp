#pragma once

#include "backend/Backend.hpp"
#include "catalog/Schema.hpp"
#include "IR/Operator.hpp"
#include "IR/OperatorVisitor.hpp"
#include "parse/ASTVisitor.hpp"
#include "util/macro.hpp"
#include <unordered_map>


namespace wasm {

struct Module;
struct Expression;

}

namespace db {

/** A `WasmModule` is a wrapper around a [**Binaryen**] (https://github.com/WebAssembly/binaryen) `wasm::Module`. */
struct WasmModule
{
    private:
    wasm::Module *ref_; ///< the underlying [**Binaryen**] (https://github.com/WebAssembly/binaryen) WASM module

    public:
    WasmModule();
    ~WasmModule();
    WasmModule(const WasmModule&) = delete;
    WasmModule(WasmModule&&) = default;

    /** Returns the underlying [**Binaryen**] (https://github.com/WebAssembly/binaryen) WASM module. */
    wasm::Module * ref() { return ref_; }
    /** Returns the underlying [**Binaryen**] (https://github.com/WebAssembly/binaryen) WASM module. */
    const wasm::Module * ref() const { return ref_; }

    /** Returns the binary representation of this module in a freshly allocated memory.  The caller must dispose of this
     * memory. */
    std::pair<uint8_t*, std::size_t> binary() const;

    /** Print module in human-readable text format. */
    friend std::ostream & operator<<(std::ostream &out, const WasmModule &module);

    void dump(std::ostream &out) const;
    void dump() const;
};

/** A `WasmPlatform` provides an environment to execute WebAssembly modules. */
struct WasmPlatform
{
    /** the size of a WebAssembly memory page, 64 KiB. */
    static constexpr std::size_t WASM_PAGE_SIZE = 1UL << 16;
    /** The maximum memory of a WebAssembly module:  2^32 - 2^16 bytes ≈ 2 GiB */
    static constexpr std::size_t WASM_MAX_MEMORY = (1UL << 31) - (1UL << 16);
    /** The alignment that is suitable for all built-in types. */
    static constexpr std::size_t WASM_ALIGNMENT = 8;

    /** The size of the module's output buffer in tuples. */
    static constexpr std::size_t NUM_TUPLES_OUTPUT_BUFFER = 32;

    /** A `WasmContext` holds associated information of a WebAssembly module instance. */
    struct WasmContext
    {
        uint32_t id; ///< a unique ID
        rewire::AddressSpace vm; ///< the WebAssembly module instance's virtual address space aka.\ *linear memory*
        uint32_t heap; ///< the beginning of the heap, encoded as offset from the beginning of the virtual address sapce

        WasmContext(uint32_t id, std::size_t size) : id(id), vm(size) { }
    };

    private:
    static uint32_t wasm_counter_; ///< a counter used to generate unique IDs
    static std::unordered_map<uint32_t, WasmContext> contexts_; ///< maps unique IDs to `WasmContext` instances

    protected:
    /** Creates a new `WasmContext` with `size` bytes of virtual address space. */
    static WasmContext & Create_Wasm_Context(std::size_t size) {
        auto res = contexts_.emplace(wasm_counter_, WasmContext(wasm_counter_, size));
        insist(res.second, "WasmContext with that ID already exists");
        ++wasm_counter_;
        return res.first->second;
    }

    /** Disposes of the `WasmContext` with ID `id`. */
    static void Dispose_Wasm_Context(uint32_t id) {
        auto res = contexts_.erase(id);
        (void) res;
        insist(res == 1, "There is no context with the given ID to erase");
    }

    public:
    /** Returns a reference to the `WasmContext` with ID `id`. */
    static WasmContext & Get_Wasm_Context_By_ID(uint32_t id) {
        auto it = contexts_.find(id);
        insist(it != contexts_.end(), "There is no context with the given ID");
        return it->second;
    }

    /** Compiles the `plan` to a `WasmModule`. */
    static WasmModule compile(const Operator &plan);

    virtual ~WasmPlatform() { }

    /** Compiles the given `plan` to a `WasmModule` and executes it on this `WasmPlatform`. */
    virtual void execute(const Operator &plan) = 0;
};

/** A `Backend` to execute `WasmModule`s on a specific `WasmPlatform`. */
struct WasmBackend : Backend
{
    private:
    std::unique_ptr<WasmPlatform> platform_;

    public:
    WasmBackend(std::unique_ptr<WasmPlatform> platform) : platform_(std::move(platform)) { }

    void execute(const Operator &plan) const override;
};

}

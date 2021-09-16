#pragma once

#include "catalog/Schema.hpp"
#include <mutable/backend/Backend.hpp>
#include <mutable/IR/Operator.hpp>
#include <mutable/parse/AST.hpp>
#include <mutable/util/macro.hpp>
#include <unordered_map>


/* Forward declaration of Binaryen classes. */
namespace wasm {

struct Module;
struct Expression;

}

namespace m {

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

/** A `WasmPlatform` provides an environment to compile and execute WebAssembly modules. */
struct WasmPlatform
{
    /** the size of a WebAssembly memory page, 64 KiB. */
    static constexpr std::size_t WASM_PAGE_SIZE = 1UL << 16;
    /** The maximum memory of a WebAssembly module:  2^32 - 2^16 bytes ≈ 4 GiB */
    static constexpr std::size_t WASM_MAX_MEMORY = (1UL << 32) - (1UL << 16);
    /** The alignment that is suitable for all built-in types. */
    static constexpr std::size_t WASM_ALIGNMENT = 8;

    /** A `WasmContext` holds associated information of a WebAssembly module instance. */
    struct WasmContext
    {
        uint32_t id; ///< a unique ID
        memory::AddressSpace vm; ///< the WebAssembly module instance's virtual address space aka.\ *linear memory*
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

    WasmPlatform() = default;
    virtual ~WasmPlatform() { }
    WasmPlatform(const WasmPlatform&) = delete;
    WasmPlatform(WasmPlatform&&) = default;

    /** Compiles the given `plan` for this `WasmPlatform`. */
    virtual WasmModule compile(const Operator &plan) const = 0;

    /** Compiles the given `plan` to a `WasmModule` and executes it on this `WasmPlatform`. */
    virtual void execute(const Operator &plan) = 0;
};

/** A `Backend` to execute a plan on a specific `WasmPlatform`. */
struct WasmBackend : Backend
{
    private:
    std::unique_ptr<WasmPlatform> platform_; ///< the `WasmPlatform` of this backend

    public:
    WasmBackend(std::unique_ptr<WasmPlatform> platform) : platform_(std::move(platform)) { }

    /** Returns this backend's `WasmPlatform`. */
    const WasmPlatform & platform() const { return *platform_; }

    /** Executes the given `plan` with this backend. */
    void execute(const Operator &plan) const override;
};

}

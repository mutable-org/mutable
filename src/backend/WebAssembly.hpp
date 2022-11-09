#pragma once

#include <memory>
#include <mutable/backend/Backend.hpp>
#include <mutable/IR/Operator.hpp>
#include <mutable/util/macro.hpp>
#include <mutable/util/memory.hpp>
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
    ::wasm::Module *ref_; ///< the underlying [**Binaryen**] (https://github.com/WebAssembly/binaryen) WASM module

    public:
    WasmModule();
    ~WasmModule();
    WasmModule(const WasmModule&) = delete;
    WasmModule(WasmModule&&) = default;

    /** Returns the underlying [**Binaryen**] (https://github.com/WebAssembly/binaryen) WASM module. */
    ::wasm::Module * ref() { return ref_; }
    /** Returns the underlying [**Binaryen**] (https://github.com/WebAssembly/binaryen) WASM module. */
    const ::wasm::Module * ref() const { return ref_; }

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
        enum config_t : uint64_t
        {
            TRAP_GUARD_PAGES = 0b1, ///< map guard pages with PROT_NONE to trap any accesses
        };

        private:
        config_t config_;

        public:
        unsigned id; ///< a unique ID
        const Operator &plan; ///< current plan
        memory::AddressSpace vm; ///<  WebAssembly module instance's virtual address space aka.\ *linear memory*
        uint32_t heap = 0; ///< beginning of the heap, encoded as offset from the beginning of the virtual address space

        WasmContext(uint32_t id, config_t configuration, const Operator &plan, std::size_t size);

        bool config(config_t cfg) const { return bool(cfg & config_); }

        /** Maps a table at the current start of `heap` and advances `heap` past the mapped region.  Returns the address
         * (in linear memory) of the mapped table.  Installs guard pages after each mapping.  Acknowledges
         * `TRAP_GUARD_PAGES`.  */
        uint32_t map_table(const Table &table);

        /** Installs a guard page at the current `heap` and increments `heap` to the next page.  Acknowledges
         * `TRAP_GUARD_PAGES`. */
        void install_guard_page();
    };

    private:
    ///> maps unique IDs to `WasmContext` instances
    static inline std::unordered_map<unsigned, std::unique_ptr<WasmContext>> contexts_;

    public:
    /** Creates a new `WasmContext` with `size` bytes of virtual address space. */
    static WasmContext & Create_Wasm_Context_For_ID(unsigned id,
                                                    WasmContext::config_t configuration = WasmContext::config_t(0x0),
                                                    const Operator &plan = NoOpOperator(std::cout),
                                                    std::size_t size = WASM_MAX_MEMORY)
    {
        auto wasm_context = std::make_unique<WasmContext>(id, configuration, plan, size);
        auto res = contexts_.emplace(id, std::move(wasm_context));
        M_insist(res.second, "WasmContext with that ID already exists");
        return *res.first->second;
    }

    /** Disposes the `WasmContext` with ID `id`. */
    static void Dispose_Wasm_Context(unsigned id) {
        auto res = contexts_.erase(id);
        (void) res;
        M_insist(res == 1, "There is no context with the given ID to erase");
    }

    /** Disposes the `WasmContext` `ctx`. */
    static void Dispose_Wasm_Context(const WasmContext &ctx) { Dispose_Wasm_Context(ctx.id); }

    /** Returns a reference to the `WasmContext` with ID `id`. */
    static WasmContext & Get_Wasm_Context_By_ID(unsigned id) {
        auto it = contexts_.find(id);
        M_insist(it != contexts_.end(), "There is no context with the given ID");
        return *it->second;
    }

    /** Tests if the `WasmContext` with ID `id` exists. */
    static bool Has_Wasm_Context(unsigned id) { return contexts_.find(id) != contexts_.end(); }

    WasmPlatform() = default;
    virtual ~WasmPlatform() { }
    WasmPlatform(const WasmPlatform&) = delete;
    WasmPlatform(WasmPlatform&&) = default;

    /** Compiles the given `plan` for this `WasmPlatform`. */
    virtual void compile(const Operator &plan) const = 0;

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

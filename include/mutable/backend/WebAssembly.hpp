#pragma once

#include <memory>
#include <mutable/backend/Backend.hpp>
#include <mutable/IR/Operator.hpp>
#include <mutable/storage/DataLayoutFactory.hpp>
#include <mutable/util/macro.hpp>
#include <mutable/util/memory.hpp>
#include <unordered_map>


namespace m {

/** A `WasmEngine` provides an environment to compile and execute WebAssembly modules. */
struct WasmEngine
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
        ///> factory used to create the result set data layout
        std::unique_ptr<const storage::DataLayoutFactory> result_set_factory;
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
    /** Creates a new `WasmContext` for ID `id` with `size` bytes of virtual address space. */
    static WasmContext & Create_Wasm_Context_For_ID(unsigned id,
                                                    WasmContext::config_t configuration = WasmContext::config_t(0x0),
                                                    const Operator &plan = NoOpOperator(std::cout),
                                                    std::size_t size = WASM_MAX_MEMORY)
    {
        auto wasm_context = std::make_unique<WasmContext>(id, configuration, plan, size);
        auto [it, inserted] = contexts_.emplace(id, std::move(wasm_context));
        M_insist(inserted, "WasmContext with that ID already exists");
        return *it->second;
    }

    /** If none exists, creates a new `WasmContext` for ID `id` with `size` bytes of virtual address space. */
    static std::pair<std::reference_wrapper<WasmContext>, bool>
    Ensure_Wasm_Context_For_ID(unsigned id,
                               WasmContext::config_t configuration = WasmContext::config_t(0x0),
                               const Operator &plan = NoOpOperator(std::cout),
                               std::size_t size = WASM_MAX_MEMORY)
    {
        auto [it, inserted] = contexts_.try_emplace(id, lazy_construct(
            [&](){ return std::make_unique<WasmContext>(id, configuration, plan, size); }
        ));
        return { std::ref(*it->second), inserted };
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

    WasmEngine() = default;
    virtual ~WasmEngine() { }
    WasmEngine(const WasmEngine&) = delete;
    WasmEngine(WasmEngine&&) = default;

    /** Compiles the given `plan` for this `WasmEngine`. */
    virtual void compile(const Operator &plan) const = 0;

    /** Executes the given `plan` on this `WasmEngine`. */
    virtual void execute(const Operator &plan) = 0;
};

/** A `Backend` to execute a plan on a specific `WasmEngine`. */
struct WasmBackend : Backend
{
    private:
    std::unique_ptr<WasmEngine> engine_; ///< the `WasmEngine` of this backend

    public:
    WasmBackend(std::unique_ptr<WasmEngine> engine) : engine_(std::move(engine)) { }

    /** Returns this backend's `WasmEngine`. */
    const WasmEngine & engine() const { return *engine_; }

    /** Executes the given `plan` with this backend. */
    void execute(const Operator &plan) const override;
};

}

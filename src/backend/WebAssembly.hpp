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

struct WasmModule
{
    private:
    wasm::Module *ref_;

    public:
    WasmModule();
    ~WasmModule();
    WasmModule(const WasmModule&) = delete;
    WasmModule(WasmModule&&) = default;

    wasm::Module * ref() { return ref_; }
    const wasm::Module * ref() const { return ref_; }

    /** Returns the binary representation of this module in a freshly allocated memory.  The caller must dispose of this
     * memory. */
    std::pair<uint8_t*, std::size_t> binary() const;

    /** Print module in human-readable text format. */
    friend std::ostream & operator<<(std::ostream &out, const WasmModule &module);

    void dump(std::ostream &out) const;
    void dump() const;
};

/** A platform to execute WASM modules. */
struct WasmPlatform
{
    static constexpr std::size_t WASM_PAGE_SIZE = 1UL << 16; // 64 KiB pages
    static constexpr std::size_t WASM_MAX_MEMORY = (1UL << 31) - (1UL << 16); // 2^32 - 2^16 bytes, approx 2 GiB

    struct WasmContext
    {
        uint32_t id;
        rewire::AddressSpace vm;

        WasmContext(uint32_t id, std::size_t size) : id(id), vm(size) { }
    };

    private:
    static uint32_t wasm_counter_;
    static std::unordered_map<uint32_t, WasmContext> contexts_;

    protected:
    static WasmContext & Create_Wasm_Context(std::size_t size) {
        auto res = contexts_.emplace(wasm_counter_, WasmContext(wasm_counter_, size));
        insist(res.second, "WasmContext with that ID already exists");
        ++wasm_counter_;
        return res.first->second;
    }

    static void Dispose_Wasm_Context(uint32_t id) {
        auto res = contexts_.erase(id);
        (void) res;
        insist(res == 1, "There is no context with the given ID to erase");
    }

    public:
    static WasmContext & Get_Wasm_Context_By_ID(uint32_t id) {
        auto it = contexts_.find(id);
        insist(it != contexts_.end(), "There is no context with the given ID");
        return it->second;
    }

    static WasmModule compile(const Operator &op);

    virtual ~WasmPlatform() { }
    virtual void execute(const WasmModule &module) = 0;
};

/** A backend to execute WASM modules on a specific platform. */
struct WasmBackend : Backend
{
    private:
    std::unique_ptr<WasmPlatform> platform_;

    public:
    WasmBackend(std::unique_ptr<WasmPlatform> platform) : platform_(std::move(platform)) { }

    void execute(const Operator &plan) const override;
};

}

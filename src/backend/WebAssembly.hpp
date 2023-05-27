#pragma once

#include <mutable/backend/WebAssembly.hpp>


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

}

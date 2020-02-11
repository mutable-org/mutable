#pragma once

#include <memory>


namespace db {

struct Operator;

/** Defines the interface of all execution `Backend`s.  Provides factory methods to create particular `Backend`
 * instances, e.g.\ an `db::Interpreter`.  */
struct Backend
{
    /** Creates a new `db::Interpreter` backend instance. */
    static std::unique_ptr<Backend> CreateInterpreter();

#ifdef WITH_V8
    /** Creates a new `db::WasmBackend` instance using the `db::V8Platform`. */
    static std::unique_ptr<Backend> CreateWasmV8();
#endif

#ifdef WITH_SPIDERMONKEY
    /** Creates a new `db::WasmBackend` instance using the `db::SpiderMonkeyPlatform`. */
    static std::unique_ptr<Backend> CreateWasmSpiderMonkey();
#endif

    virtual ~Backend() { }

    /** Executes the given `plan` using this `Backend`. */
    virtual void execute(const Operator &plan) const = 0;
};

}

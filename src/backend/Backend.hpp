#pragma once

#include <memory>


namespace db {

struct Operator;

/** Defines the backend interface. */
struct Backend
{
    static std::unique_ptr<Backend> CreateInterpreter();

#ifdef WITH_V8
    static std::unique_ptr<Backend> CreateWasmV8();
#endif

#ifdef WITH_SPIDERMONKEY
    static std::unique_ptr<Backend> CreateWasmSpiderMonkey();
#endif

    virtual ~Backend() { }

    virtual void execute(const Operator &plan) const = 0;
};

}

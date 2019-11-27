#pragma once

#include <memory>


namespace db {

struct Operator;

/** Defines the backend interface. */
struct Backend
{
    static std::unique_ptr<Backend> CreateInterpreter();
    static std::unique_ptr<Backend> CreateWASM();

    virtual ~Backend() { }

    virtual void execute(const Operator &plan) const = 0;
};

}

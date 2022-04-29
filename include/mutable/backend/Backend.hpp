#pragma once

#include <mutable/mutable-config.hpp>
#include <memory>
#include <string>
#include <unordered_map>


namespace m {

struct Operator;

/** Defines the interface of all execution `Backend`s.  Provides factory methods to create particular `Backend`
 * instances, e.g.\ an `Interpreter`.  */
struct M_EXPORT Backend
{
    virtual ~Backend() { }

    /** Executes the given `plan` using this `Backend`. */
    virtual void execute(const Operator &plan) const = 0;
};

}

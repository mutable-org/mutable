#pragma once

#include <mutable/IR/PhysicalOptimizer.hpp>
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

    /** Registers all physical operators of this `Backend` in \p phys_opt. */
    virtual void register_operators(PhysicalOptimizer &phys_opt) const = 0;

    /** Executes the already computed physical covering represented by \p plan using this `Backend`. */
    virtual void execute(const MatchBase &plan) const = 0;
};

}

#pragma once

#include <memory>
#include <string>
#include <unordered_map>


namespace m {

struct Operator;

/** Defines the interface of all execution `Backend`s.  Provides factory methods to create particular `Backend`
 * instances, e.g.\ an `Interpreter`.  */
struct Backend
{
    enum kind_t {
#define DB_BACKEND(NAME, _) B_ ## NAME,
#include "mutable/tables/Backend.tbl"
#undef DB_BACKEND
    };

    static const std::unordered_map<std::string, kind_t> STR_TO_KIND;

    /** Create a `Backend` instance given the kind of backend. */
    static std::unique_ptr<Backend> Create(kind_t kind);
    /** Create a `Backend` instance given the name of a backend. */
    static std::unique_ptr<Backend> Create(const char *kind) { return Create(STR_TO_KIND.at(kind)); }

#define DB_BACKEND(NAME, _) \
    static std::unique_ptr<Backend> Create ## NAME();
#include "mutable/tables/Backend.tbl"
#undef DB_BACKEND

    virtual ~Backend() { }

    /** Executes the given `plan` using this `Backend`. */
    virtual void execute(const Operator &plan) const = 0;
};

}

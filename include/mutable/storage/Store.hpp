#pragma once

#include <iostream>
#include <memory>
#include <mutable/mutable-config.hpp>
#include <mutable/util/macro.hpp>
#include <mutable/util/memory.hpp>
#include <string>
#include <unordered_map>


namespace m {

/*----- forward declarations -----------------------------------------------------------------------------------------*/
struct Attribute;
struct Schema;
struct StackMachine;
struct Table;

/** Defines a generic store interface. */
struct M_EXPORT Store
{
    private:
    const Table &table_; ///< the table defining this store's schema

    protected:
    Store(const Table &table) : table_(table) {}

    public:
    Store(const Store &) = delete;

    Store(Store &&) = default;

    virtual ~Store() {}

    const Table &table() const { return table_; }

    /** Returns the memory corresponding to the `Linearization`'s root node. */
    virtual const memory::Memory & memory() const = 0;

    /** Return the number of rows in this store. */
    virtual std::size_t num_rows() const = 0;

    /** Append a row to the store. */
    virtual void append() = 0;

    /** Drop the most recently appended row. */
    virtual void drop() = 0;

    virtual void dump(std::ostream &out) const = 0;
    void dump() const;
};

}

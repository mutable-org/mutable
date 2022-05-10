#pragma once

#include <iostream>
#include <memory>
#include <mutable/mutable-config.hpp>
#include <mutable/catalog/Type.hpp>
#include <mutable/storage/Linearization.hpp>
#include <mutable/util/macro.hpp>
#include <mutable/util/memory.hpp>
#include <mutable/util/Visitor.hpp>
#include <string>
#include <unordered_map>


namespace m {

/*----- forward declarations -----------------------------------------------------------------------------------------*/
struct Attribute;
struct Schema;
struct StackMachine;
struct Table;

struct StoreVisitor;
struct ConstStoreVisitor;

/** Defines a generic store interface. */
struct M_EXPORT Store
{
    private:
    const Table &table_; ///< the table defining this store's schema
    std::unique_ptr<Linearization> lin_; ///< the linearization describing the layout of this store

    protected:
    Store(const Table &table) : table_(table) {}

    public:
    Store(const Store &) = delete;

    Store(Store &&) = default;

    virtual ~Store() {}

    const Table &table() const { return table_; }

    const Linearization &linearization() const {
        if (not bool(lin_))
            throw runtime_error("no linearization provided");
        return *lin_;
    }

    protected:
    void linearization(std::unique_ptr<Linearization> lin) { lin_ = std::move(lin); }

    public:
    /** Returns the memory corresponding to the `idx`-th entry in the `Linearization`'s root node. */
    virtual const memory::Memory & memory(std::size_t idx) const = 0;

    /** Return the number of rows in this store. */
    virtual std::size_t num_rows() const = 0;

    /** Append a row to the store. */
    virtual void append() = 0;

    /** Drop the most recently appended row. */
    virtual void drop() = 0;

    /** Accept a store visitor. */
    virtual void accept(StoreVisitor &v) = 0;
    /** Accept a store visitor. */
    virtual void accept(ConstStoreVisitor &v) const = 0;

    virtual void dump(std::ostream &out) const = 0;
    void dump() const;
};

}

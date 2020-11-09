#pragma once

#include "mutable/catalog/Type.hpp"
#include "mutable/storage/Linearization.hpp"
#include "mutable/util/macro.hpp"
#include <algorithm>
#include <filesystem>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <variant>


namespace m {

struct Attribute;
struct Schema;
struct StackMachine;
struct Table;
struct RowStore;
struct ColumnStore;

template<bool C>
struct TheStoreVisitor;
using StoreVisitor = TheStoreVisitor<false>;
using ConstStoreVisitor = TheStoreVisitor<true>;

/** Reports an erroneous access to an attribute's value that is set to NULL. */
struct null_error : std::logic_error {
    null_error(const std::string &str) : logic_error(str) {}

    null_error(const char *str) : logic_error(str) {}
};

/** Defines a generic store interface. */
struct Store
{
    enum kind_t {
#define DB_STORE(NAME, _) S_ ## NAME,
#include "mutable/tables/Store.tbl"
#undef DB_STORE
    };

    static const std::unordered_map<std::string, kind_t> STR_TO_KIND;

    /** Create a `Store` instance given the kind of store. */
    static std::unique_ptr<Store> Create(kind_t kind, const Table &table);

    /** Create a `Store` instance given the name of a store. */
    static std::unique_ptr<Store> Create(const char *kind, const Table &table) {
        return Create(STR_TO_KIND.at(kind), table);
    }

#define DB_STORE(NAME, _) \
    static std::unique_ptr<Store> Create ## NAME(const Table &table);
#include "mutable/tables/Store.tbl"
#undef DB_STORE

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
        insist(bool(lin_));
        return *lin_;
    }

    protected:
    void linearization(std::unique_ptr<Linearization> lin) { lin_ = std::move(lin); }

    public:
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

template<bool C>
struct TheStoreVisitor
{
    static constexpr bool is_constant = C;

    template<typename T>
    using Const = std::conditional_t<is_constant, const T, T>;

    virtual ~TheStoreVisitor() { }

    void operator()(Const<Store> &s) { s.accept(*this); }
    virtual void operator()(Const<RowStore> &s) = 0;
    virtual void operator()(Const<ColumnStore> &s) = 0;
};

}

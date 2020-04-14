#pragma once

#include "catalog/Type.hpp"
#include "util/macro.hpp"
#include <algorithm>
#include <filesystem>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <variant>


namespace db {

struct Attribute;
struct Schema;
struct StackMachine;
struct Table;

template<bool C> struct TheStoreVisitor;
using StoreVisitor = TheStoreVisitor<false>;
using ConstStoreVisitor = TheStoreVisitor<true>;

/** Reports an erroneous access to an attribute's value that is set to NULL. */
struct null_error : std::logic_error
{
    null_error(const std::string &str) : logic_error(str) { }
    null_error(const char *str) : logic_error(str) { }
};

/** Defines a generic store interface. */
struct Store
{
    struct Row
    {
        virtual ~Row() { }

        virtual const Store & store() const = 0;

        /** Check whether the value of the attribute is NULL. */
        virtual bool isnull(const Attribute &attr) const = 0;

        /** Set the attribute to NULL. */
        virtual void setnull(const Attribute &attr) = 0;

        /** Return the value of attribue, converted to type `T`.
         * \exception null_error if the value of `attr` is not set, i.e. 'NULL'
         * \exception std::logic_error if the value of `attr` is not convertible to `T` */
        template<typename T>
        T get(const Attribute &attr) const { return get_(attr, T()); }

        /** Set the value of `attr` to `value`.  Converts `value` to the underlying type of `attr`.
         * \exception std::logic_error if `T` is not convertible to the type of `attr` */
        template<typename T>
        void set(const Attribute &attr, T value) { set_(attr, value); }

        protected:
        /*==============================================================================================================
         * Virtual Getters
         *
         * These protected member functions use tag dispatch to enable function overloading.  They must be implemented
         * by any store implementation.  To hide the tag dispatch parameter, a templated `get` method is exposed to the
         * interface.
         *============================================================================================================*/

        /** Retrieve the value of the attribute in this row. */
        virtual int64_t get_(const Attribute &attr, int64_t) const = 0;
        /** Retrieve the value of the attribute in this row. */
        virtual float get_(const Attribute &attr, float) const = 0;
        /** Retrieve the value of the attribute in this row. */
        virtual double get_(const Attribute &attr, double) const = 0;
        /** Retrieve the value of the attribute in this row. */
        virtual bool get_(const Attribute &attr, bool) const = 0;
        /** Retrieve the value of the attribute in this row. */
        virtual std::string get_(const Attribute &attr, std::string) const = 0;

        /** Retrieve the value of the attribute in this row. */
        int8_t get_(const Attribute &attr, int8_t) const { return int8_t(get_(attr, int64_t())); }
        /** Retrieve the value of the attribute in this row. */
        int16_t get_(const Attribute &attr, int16_t) const { return int16_t(get_(attr, int64_t())); }
        /** Retrieve the value of the attribute in this row. */
        int32_t get_(const Attribute &attr, int32_t) const { return int32_t(get_(attr, int64_t())); }

        /*==============================================================================================================
         * Virtual Setters
         *
         * These protected member functions  must be implemented by any store implementation.  To hide the cluttered
         * function name, a templated `set` method is exposed to the interface.
         *============================================================================================================*/

        /** Set the value of the attribute in this row. */
        virtual void set_(const Attribute &attr, int64_t value) = 0;
        /** Set the value of the attribute in this row. */
        virtual void set_(const Attribute &attr, float value) = 0;
        /** Set the value of the attribute in this row. */
        virtual void set_(const Attribute &attr, double value) = 0;
        /** Set the value of the attribute in this row. */
        virtual void set_(const Attribute &attr, bool value) = 0;
        /** Set the value of the attribute in this row. */
        virtual void set_(const Attribute &attr, std::string value) = 0;

        /** Set the value of the attribute in this row. */
        void set_(const Attribute &attr, int8_t value) { set_(attr, int64_t(value)); }
        /** Set the value of the attribute in this row. */
        void set_(const Attribute &attr, int16_t value) { set_(attr, int64_t(value)); }
        /** Set the value of the attribute in this row. */
        void set_(const Attribute &attr, int32_t value) { set_(attr, int64_t(value)); }
    };

    enum kind_t {
#define DB_STORE(NAME, _) S_ ## NAME,
#include "tables/Store.tbl"
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
#include "tables/Store.tbl"
#undef DB_STORE

    private:
    const Table &table_; ///< the table defining this store's schema

    protected:
    Store(const Table &table) : table_(table) { }

    public:
    Store(const Store&) = delete;
    Store(Store&&) = default;
    virtual ~Store() { }

    const Table & table() const { return table_; }

    /** Return the number of rows in this store. */
    virtual std::size_t num_rows() const = 0;

    /** Evaluate a function on each row of the store. */
    virtual void for_each(const std::function<void(Row &row)> &fn) = 0;
    virtual void for_each(const std::function<void(const Row &row)> &fn) const = 0;

    /** Append a row to the store. */
    virtual std::unique_ptr<Row> append() = 0;

    /** Drop the most recently appended row. */
    virtual void drop() = 0;

    /** Return a stack machine to load values row-wise directly from this store. */
    virtual StackMachine loader(const Schema &schema) const = 0;

    /** Return a `StackMachine` to update the specified attributes directly in this `Store`.  The `StackMachine` expects
     * one `Tuple` containing the `Value`s to write to the row.  The `Value`s must be in the same order as in the given
     * list of `Attribute`s `attrs`.  The row id can be set in the context at index `0`. */
    virtual StackMachine writer(const std::vector<const Attribute*> &attrs) const = 0;

    /** Accept a store visitor. */
    virtual void accept(StoreVisitor &v) = 0;
    /** Accept a store visitor. */
    virtual void accept(ConstStoreVisitor &v) const = 0;

    virtual void dump(std::ostream &out) const = 0;
    void dump() const;
};

struct RowStore;
struct ColumnStore;

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

using StoreVisitor = TheStoreVisitor<false>;
using ConstStoreVisitor = TheStoreVisitor<true>;

}

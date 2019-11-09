#pragma once

#include "catalog/Schema.hpp"
#include "catalog/Type.hpp"
#include "util/macro.hpp"
#include <algorithm>
#include <filesystem>
#include <functional>
#include <iostream>
#include <memory>
#include <type_traits>
#include <variant>


namespace db {

struct OperatorSchema;
struct StackMachine;

/** Reports an erroneous access to an attribute's value that is set to NULL. */
struct null_error : std::logic_error
{
    null_error(const std::string &str) : logic_error(str) { }
    null_error(const char *str) : logic_error(str) { }
};

/** The type of "NULL". */
struct null_type
{
    friend std::ostream & operator<<(std::ostream &out, null_type) { return out << "NULL"; }
    bool operator==(null_type) const { return true; }
};

/** A polymorphic type to hold a value of an attribute. */
using value_type = std::variant<
    null_type,
    int64_t,
    float,
    double,
    std::string,
    bool
>;

template<typename To>
To to(const value_type &value)
{
    return std::visit(
        [](auto value) -> To {
            if constexpr (std::is_convertible_v<decltype(value), To>) {
                return value;
            } else {
                unreachable("value cannot be converted to target type");
            }
        }, value);
}

}

namespace std {

template<>
struct hash<db::value_type>
{
    uint64_t operator()(const db::value_type &value) const {
        return std::visit(overloaded {
            [](auto v) -> uint64_t { return murmur3_64(std::hash<decltype(v)>()(v)); },
            [](db::null_type) -> uint64_t { return 0; },
            [](const std::string &v) -> uint64_t { return StrHash()(v.c_str()); },
        }, value);
    }
};

}

namespace db {

inline std::ostream & operator<<(std::ostream &out, const value_type &value)
{
    std::visit(overloaded {
        [&] (null_type) { out << "NULL"; },
        [&] (int64_t v) { out << v; },
        [&] (float v) { out << v << ".f"; },
        [&] (double v) { out << v << '.'; },
        [&] (std::string v) { out << '"' << v << '"'; },
        [&] (bool v) { out << (v ? "TRUE" : "FALSE"); },
    }, value);
    return out;
}

/** Prints an attribute's value to an output stream. */
inline void print(std::ostream &out, const Type *type, value_type value)
{
    std::visit(overloaded {
        [&] (null_type) { out << "NULL"; },
        [&] (float v) { out << v; },
        [&] (double v) { out << v; },
        [&] (std::string v) { out << '"' << escape(v) << '"'; },
        [&] (bool v) { out << (v ? "TRUE" : "FALSE"); },
        [&] (int64_t v) {
            if (auto n = as<const Numeric>(type); n->kind == Numeric::N_Decimal) {
                using std::setw, std::setfill;
                int64_t shift = pow(10, n->scale);
                int64_t pre = v / shift;
                int64_t post = std::abs(v) % shift;
                out << pre << '.' << setw(n->scale) << setfill('0') << post;
                return;
            } else {
                out << v;
            }
        },
    }, value);
}

/** Defines a generic store interface. */
struct Store
{
    struct Row
    {
        using callback_t = std::function<void(const Attribute&, value_type)>;

        virtual ~Row() { }

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

        /** Invokes a callback function for each attribute of the row, passing the attribute and its value. */
        virtual void dispatch(callback_t callback) const = 0;

        /** Output a human-readable representation of this row. */
        friend std::ostream & operator<<(std::ostream &out, const Row &row) {
            row.print(out);
            return out;
        }

        protected:
        /** Helper function to make operator<< virtual. */
        void print(std::ostream &out) const;

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

    private:
    const Table &table_; ///< the table defining this store's schema

    public:
    Store(const Table &table) : table_(table) { }
    virtual ~Store() { }

    const Table & table() const { return table_; }

    /** Return the number of rows in this store. */
    virtual std::size_t num_rows() const = 0;

    /** Saves the contents of the store to the file at `path`. */
    virtual void save(std::filesystem::path path) const = 0;

    /** Loads data for the store from a file at `path`.  Returns the number of rows loaded. */
    virtual std::size_t load(std::filesystem::path path) = 0;

    /** Evaluate a function on each row of the store. */
    virtual void for_each(const std::function<void(Row &row)> &fn) = 0;
    virtual void for_each(const std::function<void(const Row &row)> &fn) const = 0;

    /** Append a row to the store. */
    virtual std::unique_ptr<Row> append() = 0;

    /** Drop the most recently appended row. */
    virtual void drop() = 0;

    /** Return a stack machine to load values row-wise directly from this store. */
    virtual StackMachine loader(const OperatorSchema &schema) const = 0;

    /** Return a stack machine to update the specified attributes directly in this store.  The stack machine expects the
     * values to update the row with as input tuple, with the values in the same order as the given list of attributes.
     * Further, the row to update must be specified by the user of the stack machine by setting context at index 0 to
     * the respective row id. */
    virtual StackMachine writer(const std::vector<const Attribute*> &attrs) const = 0;

    virtual void dump(std::ostream &out) const = 0;
    void dump() const;
};

}

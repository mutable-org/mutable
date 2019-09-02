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

/** Prints an attribute's value to an output stream. */
void print(std::ostream &out, const Type *type, value_type value);

/** Defines a generic store interface. */
struct Store
{
    struct Row
    {
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
        virtual void dispatch(std::function<void(const Attribute &attr, value_type)> callback) const = 0;

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

        /** Set the value of the attribute in this row. */
        int8_t get_(const Attribute &attr, int8_t) const { return int8_t(get_(attr, int64_t())); }
        //* Set the value of the attribute in this row. 
        int16_t get_(const Attribute &attr, int16_t) const { return int16_t(get_(attr, int64_t())); }
        /** Set the value of the attribute in this row. */
        int32_t get_(const Attribute &attr, int32_t) const { return int32_t(get_(attr, int64_t())); }

        /*==============================================================================================================
         * Virtual Setters
         *
         * These protected member functions  must be implemented by any store implementation.  To hide the cluttered
         * function name, a templated `get` method is exposed to the interface.
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

    /** Saves the contents of the store to the file at `path`. */
    virtual void save(std::filesystem::path path) const = 0;

    /** Loads data for the store from a file at `path`.  Returns the number of rows loaded. */
    virtual std::size_t load(std::filesystem::path path) = 0;

    /** Evaluate a function on each row of the store. */
    virtual void for_each(const std::function<void(Row &row)> &fn) = 0;
    virtual void for_each(const std::function<void(const Row &row)> &fn) const = 0;

    /** Append a row to the store. */
    virtual std::unique_ptr<Row> append() = 0;

    virtual void dump(std::ostream &out) const = 0;
    void dump() const;
};

}

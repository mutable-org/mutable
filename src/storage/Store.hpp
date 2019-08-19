#pragma once

#include "catalog/Schema.hpp"
#include "catalog/Type.hpp"
#include "util/macro.hpp"
#include <algorithm>
#include <filesystem>
#include <iostream>
#include <type_traits>


namespace db {

/** Reports an erroneous access to an attribute's value that is set to NULL. */
struct null_error : std::logic_error
{
    null_error(const std::string &str) : logic_error(str) { }
    null_error(const char *str) : logic_error(str) { }
};

struct RowStore;

/** Defines a generic store interface. */
struct Store
{
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

    virtual void dump(std::ostream &out) const = 0;
    void dump() const;
};

struct RowStore : Store
{
    static constexpr std::size_t ALLOCATION_SIZE = 1UL << 40; ///< 1 TB

    public:
    /** This class acts as an interface to a row of a row store.  Physically, a row is just a byte array of a fixed
     * length.  To interpret this binary data one needs the table definition attached to the row store.
     */
    template<bool C>
    struct the_row
    {
        friend struct RowStore;

        static constexpr bool Is_Const = C;
        using store_t = std::conditional_t<Is_Const, const RowStore, RowStore>;

        public:
        store_t &store;
        private:
        uintptr_t addr_;

        private:
        the_row(store_t &store, uintptr_t addr) : store(store), addr_(addr) { }

        public:
        /** Check whether the value of the attribute is NULL. */
        bool isnull(const Attribute &attr) const {
            const std::size_t off = store.offset(store.table().size()) + attr.id;
            const std::size_t bytes = off / 8;
            const std::size_t bits = off % 8;
            auto p = reinterpret_cast<uint8_t*>(addr_ + bytes);
            return not bool((*p >> bits) & 0x1);
        }

        private:
        void null(const Attribute &attr, bool value) {
            const std::size_t off = store.offset(store.table().size()) + attr.id;
            const std::size_t bytes = off / 8;
            const std::size_t bits = off % 8;
            auto p = reinterpret_cast<uint8_t*>(addr_ + bytes);
            setbit(p, not value, bits);
        }

        public:
        /** Set the attribute to NULL. */
        void setnull(const Attribute &attr) { null(attr, true); }

        /** Retrieve the value of the attribute in this row. */
        template<typename T>
        T get(const Attribute &attr) const {
            if (isnull(attr))
                throw null_error("Attribute is NULL, cannot get value");

            const auto off = store.offset(attr);
            type_check<T>(attr);
            const auto bytes = off / 8;

            if constexpr (std::is_same_v<T, bool>) {
                const auto bits = off % 8;
                auto p = reinterpret_cast<uint8_t*>(addr_ + bytes);
                return bool((*p >> bits) & 0x1); // extract single bit
            } else if constexpr (std::is_same_v<T, std::string>) {
                auto cs = as<const CharacterSequence>(attr.type);
                if (cs->is_varying) {
                    unreachable("varying length character sequences are not supported by this store");
                } else {
                    insist(off % 8 == 0, "the attribute is not byte-aligned");
                    const auto len = attr.type->size() / 8;
                    auto p = reinterpret_cast<char*>(addr_ + bytes);
                    std::string str;
                    for (auto end = p + len; p != end; ++p)
                        str += *p;
                    return str;
                }
            } else {
                insist(off % 8 == 0, "the attribute is not byte-aligned");
                return *reinterpret_cast<T*>(addr_ + bytes);
            }
        }

        /** Retrieve the value of the attribute in this row. */
        template<typename T>
        void get(const Attribute &attr, T &value) const { value = get<T>(attr); }

        /** Set the value of the attribute in this row. */
        template<typename T>
        std::enable_if_t<not Is_Const, T> set(const Attribute &attr, T value) {
            null(attr, false);
            const auto off = store.offset(attr);
            type_check<T>(attr);
            const auto bytes = off / 8;

            if constexpr (std::is_same_v<T, bool>) {
                const auto bits = off % 8;
                auto p = reinterpret_cast<uint8_t*>(addr_ + bytes);
                setbit(p, value, bits);
            } else if constexpr (std::is_same_v<T, const char*>) {
                auto cs = as<const CharacterSequence>(attr.type);
                if (cs->is_varying) {
                    unreachable("varying length character sequences are not supported by this store");
                } else {
                    using std::min;
                    const auto len = attr.type->size() / 8;
                    auto p = reinterpret_cast<char*>(addr_ + bytes);
                    strncpy(p, value, len);
                }
            } else {
                insist(off % 8 == 0, "type is not boolean and not byte aligned");
                auto p = reinterpret_cast<T*>(addr_ + bytes);
                *p = value;
#ifndef NDEBUG
                T check;
                get(attr, check);
                insist(check == value, "failed to store value");
#endif
            }

            return value;
        }

        /** Invoke a callable with the value of `attr` in this row.  An example of how that callable should look like
         * can be found in the implementation of `operator<<`. */
        template<typename Callable>
        void dispatch(const Attribute &attr, Callable fn);

        /** Output a human-readable representation of this row. */
        template<bool Cx>
        friend std::ostream & operator<<(std::ostream &out, the_row<Cx> row);
    };

    using Row = the_row<false>;
    using ConstRow = the_row<true>;

    private:
    template<bool C>
    struct the_iterator
    {
        static constexpr bool Is_Const = C;
        using store_t = std::conditional_t<Is_Const, const RowStore, RowStore>;
        using row_t = std::conditional_t<Is_Const, ConstRow, Row>;

        public:
        store_t &store;
        private:
        uintptr_t addr_;

        public:
        the_iterator(store_t &store, uintptr_t addr) : store(store), addr_(addr) { }

        the_iterator & operator++() { addr_ += store.row_size()/8; return *this; }
        bool operator==(the_iterator other) const { return this->addr_ == other.addr_; }
        bool operator!=(the_iterator other) const { return not operator==(other); }

        row_t operator*() const { return row_t(store, addr_); }
    };

    public:
    using iterator = the_iterator<false>;
    using const_iterator = the_iterator<true>;

    private:
    void *data_; ///< the location of the data
    std::size_t num_rows_ = 0; ///< the number of rows in use
    std::size_t capacity_; ///< the number of available rows
    uint32_t *offsets_; ///< the offsets from the first column, in bits, of all columns
    uint32_t row_size_; ///< the size of a row, in bits; includes NULL bitmap and other meta data

    public:
    RowStore(const Table &table);
    ~RowStore();

    int offset(uint32_t idx) const {
        insist(idx <= table().size(), "index out of range");
        return offsets_[idx];
    }
    int offset(const Attribute &attr) const { return offset(attr.id); }

    Row operator[](std::size_t idx) { return Row(*this, at(idx)); }
    ConstRow operator[](std::size_t idx) const { return ConstRow(*this, at(idx)); }

    iterator begin() { return to(0); }
    iterator end()   { return to(num_rows_); }
    const_iterator cbegin() const { return to(0); }
    const_iterator cend()   const { return to(num_rows_); }
    const_iterator begin()  const { return cbegin(); }
    const_iterator end()    const { return cend(); }

    std::size_t num_rows() const { return num_rows_; }

    /** Returns the effective size of a row, in bits. */
    std::size_t row_size() const { return row_size_; }

    iterator append(std::size_t i = 1) {
        if (capacity_ - i < num_rows_)
            throw std::logic_error("storage capacity exceeded");
        auto it = to(num_rows_);
        num_rows_ += i;
        return it;
    }

    std::size_t load(std::filesystem::path path);
    void save(std::filesystem::path path) const;

    void dump(std::ostream &out) const;
    using Store::dump;

    private:
    /** Computes the offsets of the attributes within a row.  Tries to minimize the row size by storing the attributes
     * in descending order of their size, avoiding padding.  */
    void compute_offsets();

    /** Return a pointer to the `idx`th row. */
    uintptr_t at(std::size_t idx) const { return reinterpret_cast<uintptr_t>(data_) + row_size_/8 * idx; }
    /** Return an iterator at the `idx`th row. */
    iterator to(std::size_t idx) { return iterator(*this, at(idx)); }
    /** Return an iterator at the `idx`th row. */
    const_iterator to(std::size_t idx) const { return const_iterator(*this, at(idx)); }
};

template<bool C>
template<typename Callable>
void RowStore::the_row<C>::dispatch(const Attribute &attr, Callable fn)
{
    auto ty = attr.type;

    if (ty->is_boolean()) {
        bool value;
        get(attr, value);
        fn(value);
    } else if (auto cs = cast<const CharacterSequence>(ty)) {
        if (cs->is_varying) {
            unreachable("varying length character sequences are not supported by this store");
        } else {
            std::string str;
            get(attr, str);
            fn(str);
        }
    } else if (auto n = cast<const Numeric>(ty)) {
        switch (n->kind) {
            case Numeric::N_Int: {
                switch (n->precision) {
                    default:
                        unreachable("illegal integer type");
                    case 1: {
                        int8_t value;
                        get(attr, value);
                        fn(int64_t(value));
                        break;
                    }
                    case 2: {
                        int16_t value;
                        get(attr, value);
                        fn(int64_t(value));
                        break;
                    }
                    case 4: {
                        int32_t value;
                        get(attr, value);
                        fn(int64_t(value));
                        break;
                    }
                    case 8: {
                        int64_t value;
                        get(attr, value);
                        fn(value);
                        break;
                    }
                }
                break;
            }

            case Numeric::N_Float: {
                if (n->precision == 32) {
                    float value;
                    get(attr, value);
                    fn(value);
                } else {
                    double value;
                    get(attr, value);
                    fn(value);
                }
                break;
            }

            case Numeric::N_Decimal: {
                const auto p = ceil_to_pow_2(n->size());
                int64_t integral = 0;
                switch (p) {
                    default:
                        unreachable("illegal precision of decimal type");
                    case 8: {
                        int8_t value;
                        get(attr, value);
                        integral = value;
                        break;
                    }
                    case 16: {
                        int16_t value;
                        get(attr, value);
                        integral = value;
                        break;
                    }
                    case 32: {
                        int32_t value;
                        get(attr, value);
                        integral = value;
                        break;
                    }
                    case 64: {
                        int64_t value;
                        get(attr, value);
                        integral = value;
                        break;
                    }
                }
                int64_t pre, post;
                int64_t shift = pow(10, n->scale);
                pre = integral / shift;
                post = integral % shift;
                fn(pre, post);
                break;
            }
        }
    } else {
        unreachable("invalid type");
    }
}

}

#pragma once

#include "storage/Store.hpp"


namespace db {

struct RowStore : Store
{
#ifndef NDEBUG
    static constexpr std::size_t ALLOCATION_SIZE = 1UL << 30; ///< 1 GB
#else
    static constexpr std::size_t ALLOCATION_SIZE = 1UL << 40; ///< 1 TB
#endif

    public:
    /** This class acts as an interface to a row of a row store.  Physically, a row is just a byte array of a fixed
     * length.  To interpret this binary data one needs the table definition attached to the row store.
     */
    struct Row : Store::Row
    {
        friend struct RowStore;

        public:
        const RowStore &store;
        private:
        uintptr_t addr_;

        private:
        Row(const RowStore &store, uintptr_t addr) : store(store), addr_(addr) { }

        public:
        uintptr_t addr() const { return addr_; }

        /** Check whether the value of the attribute is NULL. */
        bool isnull(const Attribute &attr) const override {
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
        void setnull(const Attribute &attr) override { null(attr, true); }

        /** Invoke a callable for each attribute in the row and pass the attribute and its value. */
        void dispatch(callback_t callback) const override;

        private:
        /** Retrieve the value of the attribute in this row.  `T` is the exact type of the attribute as stored in the
         * row. */
        template<typename T>
        T get_exact(const Attribute &attr) const;

        /** Set the value of the attribute in this row.  `T` is the exact type of the attribute as stored in the row. */
        template<typename T>
        T set_exact(const Attribute &attr, T value);

        /** Retrieve the value of the attribute in this row.  This is a type-generic helper function. */
        template<typename T>
        T get_generic(const Attribute &attr) const;

        /** Set the value of the attribute in this row.  This is a type-generic helper function. */
        template<typename T>
        T set_generic(const Attribute &attr, T value);

        /*==============================================================================================================
         * Virtual Getters
         *============================================================================================================*/

        int64_t     get_(const Attribute &attr, int64_t) const override { return get_generic<int64_t>(attr); }
        float       get_(const Attribute &attr, float) const override { return get_generic<float>(attr); }
        double      get_(const Attribute &attr, double) const override { return get_generic<double>(attr); }
        bool        get_(const Attribute &attr, bool) const override { return get_generic<bool>(attr); }
        std::string get_(const Attribute &attr, std::string) const override { return get_generic<std::string>(attr); }

        /*==============================================================================================================
         * Virtual Setters
         *============================================================================================================*/

        void set_(const Attribute &attr, int64_t value) override { set_generic<int64_t>(attr, value); }
        void set_(const Attribute &attr, float value) override { set_generic<float>(attr, value); }
        void set_(const Attribute &attr, double value) override { set_generic<double>(attr, value); }
        void set_(const Attribute &attr, bool value) override { set_generic<bool>(attr, value); }
        void set_(const Attribute &attr, std::string value) override { set_generic<std::string>(attr, value); }
    };

    private:
    void *data_; ///< the location of the data
    std::size_t num_rows_ = 0; ///< the number of rows in use
    std::size_t capacity_; ///< the number of available rows
    uint32_t *offsets_; ///< the offsets from the first column, in bits, of all columns
    uint32_t row_size_; ///< the size of a row, in bits; includes NULL bitmap and other meta data

    public:
    RowStore(const Table &table);
    ~RowStore();

    virtual std::size_t num_rows() const override { return num_rows_; }

    int offset(uint32_t idx) const {
        insist(idx <= table().size(), "index out of range");
        return offsets_[idx];
    }
    int offset(const Attribute &attr) const { return offset(attr.id); }

    /** Returns the effective size of a row, in bits. */
    std::size_t row_size() const { return row_size_; }

    std::size_t load(std::filesystem::path path) override;
    void save(std::filesystem::path path) const override;

    void for_each(const std::function<void(Store::Row &row)> &fn) override {
        for (std::size_t i = 0, end = num_rows_; i != end; ++i) {
            Row r(*this, at(i));
            fn(r);
        }
    }

    void for_each(const std::function<void(const Store::Row &row)> &fn) const override {
        for (std::size_t i = 0, end = num_rows_; i != end; ++i) {
            Row r(*this, at(i));
            fn(r);
        }
    }

    std::unique_ptr<Store::Row> append() override {
        if (num_rows_ == capacity_)
            throw std::logic_error("row store exceeds capacity");
        auto row = std::unique_ptr<Store::Row>(new Row(*this, at(num_rows_)));
        ++num_rows_;
        return row;
    }

    StackMachine loader(const OperatorSchema &schema) const override;

    void dump(std::ostream &out) const override;
    using Store::dump;

    private:
    /** Computes the offsets of the attributes within a row.  Tries to minimize the row size by storing the attributes
     * in descending order of their size, avoiding padding.  */
    void compute_offsets();

    /** Return a pointer to the `idx`th row. */
    uintptr_t at(std::size_t idx) const { return reinterpret_cast<uintptr_t>(data_) + row_size_/8 * idx; }
};


/*======================================================================================================================
 * RowStore::Row
 *====================================================================================================================*/

template<typename T>
T RowStore::Row::get_exact(const Attribute &attr) const
{
    insist(not isnull(attr));

    const auto off = store.offset(attr);
    insist(type_check<T>(attr));
    const auto bytes = off / 8;

    if constexpr (std::is_same_v<T, bool>) {
        const auto bits = off % 8;
        auto p = reinterpret_cast<uint8_t*>(addr_ + bytes);
        return bool((*p >> bits) & 0x1); // extract single bit
    } else if constexpr (std::is_same_v<T, std::string>) {
        insist(off % 8 == 0, "the attribute is not byte-aligned");
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

template<typename T>
T RowStore::Row::set_exact(const Attribute &attr, T value)
{
    null(attr, false);
    const auto off = store.offset(attr);
    insist(type_check<T>(attr));
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
            const auto len = attr.type->size() / 8;
            auto p = reinterpret_cast<char*>(addr_ + bytes);
            strncpy(p, value, len);
        }
    } else if constexpr (std::is_same_v<T, std::string>) {
        auto cs = as<const CharacterSequence>(attr.type);
        if (cs->is_varying) {
            unreachable("varying length character sequences are not supported by this store");
        } else {
            const auto len = attr.type->size() / 8;
            auto p = reinterpret_cast<char*>(addr_ + bytes);
            strncpy(p, value.c_str(), len);
        }
    } else {
        insist(off % 8 == 0, "type is not boolean and not byte aligned");
        auto p = reinterpret_cast<T*>(addr_ + bytes);
        *p = value;
#ifndef NDEBUG
        T check = get_exact<T>(attr);
        insist(check == value, "failed to store value");
#endif
    }

    return value;
}

template<typename T>
T RowStore::Row::get_generic(const Attribute &attr) const
{
    auto ty = attr.type;

    insist(is_convertible<T>(ty), "Attribute not convertible to specified type");

    if constexpr (std::is_same_v<T, bool>)
    {
        insist(ty->is_boolean());
        return get_exact<bool>(attr);
    }
    else if constexpr (std::is_same_v<T, std::string>)
    {
        auto cs = as<const CharacterSequence>(ty);
        if (cs->is_varying)
            unreachable("varying length character sequences are not supported by this store");
        else
            return get_exact<std::string>(attr);
    }
    else if constexpr (std::is_arithmetic_v<T>)
    {
        auto n = as<const Numeric>(ty);
        switch (n->kind) {
            case Numeric::N_Int: {
                switch (n->precision) {
                    default: unreachable("illegal integer type");
                    case 1: return get_exact<int8_t>(attr);
                    case 2: return get_exact<int16_t>(attr);
                    case 4: return get_exact<int32_t>(attr);
                    case 8: return get_exact<int64_t>(attr);
                }
            }

            case Numeric::N_Float: {
                if (n->precision == 32)
                    return get_exact<float>(attr);
                else
                    return get_exact<double>(attr);
            }

            case Numeric::N_Decimal: {
                const auto p = ceil_to_pow_2(n->size());
                switch (p) {
                    default: unreachable("illegal precision of decimal type");
                    case 8: return get_exact<int8_t>(attr);
                    case 16: return get_exact<int16_t>(attr);
                    case 32: return get_exact<int32_t>(attr);
                    case 64: return get_exact<int64_t>(attr);
                }
            }
        }
    }

    unreachable("invalid type");
}

template<typename T>
T RowStore::Row::set_generic(const Attribute &attr, T value)
{
    auto ty = attr.type;

    insist(is_convertible<T>(ty), "Attribute not convertible to specified type");

    if constexpr (std::is_same_v<T, bool>)
    {
        insist(ty->is_boolean());
        return set_exact<bool>(attr, value);
    }
    else if constexpr (std::is_same_v<T, std::string>)
    {
        auto cs = as<const CharacterSequence>(ty);
        if (cs->is_varying)
            unreachable("varying length character sequences are not supported by this store");
        else
            return set_exact<std::string>(attr, value);
    }
    else if constexpr (std::is_arithmetic_v<T>)
    {
        auto n = as<const Numeric>(ty);
        switch (n->kind) {
            case Numeric::N_Int: {
                switch (n->precision) {
                    default: unreachable("illegal integer type");
                    case 1: return set_exact<int8_t>(attr, value);
                    case 2: return set_exact<int16_t>(attr, value);
                    case 4: return set_exact<int32_t>(attr, value);
                    case 8: return set_exact<int64_t>(attr, value);
                }
            }

            case Numeric::N_Float: {
                if (n->precision == 32)
                    return set_exact<float>(attr, value);
                else
                    return set_exact<double>(attr, value);
            }

            case Numeric::N_Decimal: {
                const auto p = ceil_to_pow_2(n->size());
                switch (p) {
                    default: unreachable("illegal precision of decimal type");
                    case 8: return set_exact<int8_t>(attr, value);
                    case 16: return set_exact<int16_t>(attr, value);
                    case 32: return set_exact<int32_t>(attr, value);
                    case 64: return set_exact<int64_t>(attr, value);
                }
            }
        }
    }

    unreachable("invalid type");
}

}

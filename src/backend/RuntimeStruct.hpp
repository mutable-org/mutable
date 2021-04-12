#pragma once

#include <cstdint>
#include <initializer_list>
#include <iostream>
#include <mutable/catalog/Type.hpp>


namespace m {

/* Forward declarations */
struct Schema;

/** A `RuntimeStruct` represents a mapping of entries of a `Schema` and optionally provided additional types to their
 * physical ordering, i.e. to their offsets. To order the entries efficiently, a row-wise data layout minimizing
 * padding is used. */
struct RuntimeStruct
{
    using index_type = uint32_t;
    using offset_type = uint32_t;

    friend void swap(RuntimeStruct &first, RuntimeStruct &second) {
        using std::swap;
        swap(first.offsets_, second.offsets_);
        swap(first.size_,    second.size_);
    }

    private:
    index_type num_fields_; ///< number of fields in the struct
    const Type **types_ = nullptr; ///< the `Type`s of the fields
    offset_type *offsets_ = nullptr; ///< the fields' offsets in bits
    offset_type size_; ///< the size of an instance in bits (including padding)

    RuntimeStruct() { }

    public:
    /** Construct a `RuntimeStruct` for the given type list `fields`.  */
    RuntimeStruct(std::initializer_list<const Type*> fields);

    /** Construct a `RuntimeStruct` for the given logical `Schema` `schema` and the given optional type list
     * `additional_fields`.  */
    RuntimeStruct(const Schema &schema, std::initializer_list<const Type*> additional_fields = {});

    RuntimeStruct(const RuntimeStruct&) = delete;
    RuntimeStruct(RuntimeStruct &&other) : RuntimeStruct() { swap(*this, other); }

    ~RuntimeStruct();

    RuntimeStruct & operator=(RuntimeStruct &&other) { swap(*this, other); return *this; }

    /** Returns the effective size of one instance, in bits. */
    offset_type size_in_bits() const { return size_; }

    /** Returns the effective size of one instance, in bytes. */
    offset_type size_in_bytes() const { return size_ / 8; }

    /** Returns the number of fields in this `RuntimeStruct`. */
    index_type num_entries() const { return num_fields_; }

    /** Returns the `Type` of the field at index `idx`. */
    const Type & type(index_type idx) const {
        insist(idx < num_entries());
        return *types_[idx];
    }

    /** Returns the offset (in bits) of the field at index `idx`. */
    offset_type offset(index_type idx) const {
        insist(idx < num_entries());
        return offsets_[idx];
    }
    /** Returns the `Type` and offset (in bits) of the field at index `idx`. */
    std::pair<const Type&, offset_type> operator[](index_type idx) const {
        return std::pair<const Type&, offset_type>(type(idx), offset(idx));
    }

    friend std::ostream & operator<<(std::ostream &out, const RuntimeStruct &schema);

    void dump(std::ostream &out) const;
    void dump() const;
};

}

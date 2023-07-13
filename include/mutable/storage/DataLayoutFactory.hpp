#pragma once

#include <mutable/catalog/Schema.hpp>
#include <mutable/storage/DataLayout.hpp>
#include <vector>


namespace m {

struct Type;

namespace storage {

/** This is an interface for factories that compute particular `DataLayout`s for a given sequence of `Type`s, e.g. row
 * layout or PAX. */
struct DataLayoutFactory
{
    virtual ~DataLayoutFactory() { }

    /** Creates and returns a *deep copy* of `this`. */
    virtual std::unique_ptr<DataLayoutFactory> clone() const = 0;

    /** Returns a `DataLayout` for the given `Type`s contained in \p schema and length \p num_tuples. */
    DataLayout make(const Schema &schema, std::size_t num_tuples = 0) const {
        view v(schema.cbegin(), schema.cend(), [](auto it) -> auto & { return it->type; });
        return make(v.begin(), v.end(), num_tuples);
    }

    /** Returns a `DataLayout` for the given `Type`s in the range from \p begin to \p end and length \p num_tuples. */
    template<typename It>
    DataLayout make(It begin, It end, std::size_t num_tuples = 0) const {
        return make(std::vector<const Type*>(begin, end), num_tuples);
    }

    /** Returns a `DataLayout` for the given \p types and length \p num_tuples (0 means infinite layout). */
    virtual DataLayout make(std::vector<const Type*> types, std::size_t num_tuples = 0) const = 0;
};

struct RowLayoutFactory : DataLayoutFactory
{
    std::unique_ptr<DataLayoutFactory> clone() const override { return std::make_unique<RowLayoutFactory>(); }

    using DataLayoutFactory::make;
    DataLayout make(std::vector<const Type*> types, std::size_t num_tuples = 0) const override;
};

struct PAXLayoutFactory : DataLayoutFactory
{
    static constexpr uint64_t DEFAULT_NUM_TUPLES = 16;
    static constexpr uint64_t DEFAULT_NUM_BYTES = 1UL << 12; ///< 4 KiB

    enum block_size_t { NTuples, NBytes }; ///< indicates whether the block size is given in number of tuples or bytes

    private:
    block_size_t option_;
    union {
        uint64_t num_tuples_;
        uint64_t num_bytes_;
    };

    public:
    PAXLayoutFactory(block_size_t option = NBytes)
        : option_(option)
    {
        if (NTuples == option_)
            num_tuples_ = DEFAULT_NUM_TUPLES;
        else
            num_bytes_ = DEFAULT_NUM_BYTES;
    }
    PAXLayoutFactory(block_size_t option, uint64_t num)
        : option_(option)
    {
        M_insist(num != 0, "number of tuples or rather number of bytes must at least be 1");
        if (NTuples == option_)
            num_tuples_ = num;
        else
            num_bytes_ = num;
    }

    std::unique_ptr<DataLayoutFactory> clone() const override {
        return std::make_unique<PAXLayoutFactory>(option_, NTuples == option_ ? num_tuples_ : num_bytes_);
    }

    using DataLayoutFactory::make;
    DataLayout make(std::vector<const Type*> types, std::size_t num_tuples = 0) const override;
};

}

}

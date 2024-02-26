#pragma once

#include <algorithm>
#include <cmath>
#include <cstring>
#include <iostream>
#include <mutable/util/concepts.hpp>
#include <mutable/util/exception.hpp>
#include <mutable/util/macro.hpp>
#include <utility>
#include <vector>


namespace m {

// Forward declarations
struct Table;
struct Schema;

namespace idx {

/** An enum class that lists all supported index methods. */
enum class IndexMethod { Array, Rmi };

/** The base class for indexes. */
struct IndexBase
{
    protected:
    IndexBase() = default;

    public:
    IndexBase(IndexBase&&) = default;
    virtual ~IndexBase() { }

    /** Bulkloads the index by executing a query on \p table using \p key_schema. */
    virtual void bulkload(const Table &table, const Schema &key_schema) = 0;
    /* Returns the number of entries in the index. */
    virtual std::size_t num_entries() const = 0;
    /** Returns the `IndexMethod` of the index. */
    virtual IndexMethod method() const = 0;

    virtual void dump(std::ostream &out) const = 0;
    virtual void dump() const = 0;

    protected:
    /** Constructs a query string to select all attributes in \p schema from \p table. */
    static std::string build_query(const Table &table, const Schema &schema);
};

/** A simple index based on a sorted array that maps keys to their `tuple_id`. */
template<typename Key>
struct ArrayIndex : IndexBase
{
    using key_type = Key;
    using value_type = std::size_t;
    using entry_type = std::pair<key_type, value_type>;
    using container_type = std::vector<entry_type>;
    using const_iterator = typename container_type::const_iterator;

    protected:
    container_type data_; ///< A vector holding the index entries consisting of pairs of key and value
    bool finalized_; ///< flag to signalize whether index is finalized, i.e. array is sorted

    /** Custom comparator class to handle the special case of \tparam key_type being `const char*`. */
    struct
    {
        template<typename T, typename U>
        requires (std::same_as<T, key_type> or std::same_as<T, entry_type>) and
                 (std::same_as<U, key_type> or std::same_as<U, entry_type>)
        bool operator()(const T &_lhs, const U &_rhs) const {
            key_type lhs = M_CONSTEXPR_COND((std::same_as<T, key_type>), _lhs, _lhs.first);
            key_type rhs = M_CONSTEXPR_COND((std::same_as<U, key_type>), _rhs, _rhs.first);
            if constexpr(std::same_as<key_type, const char*>)
                return std::strcmp(lhs, rhs) < 0;
            else
                return lhs < rhs;
        }
    } cmp;

    public:
    ArrayIndex() : finalized_(false) { }

    /** Bulkloads the index from \p table on the key contained in \p key_schema by executing a query and adding one
     * entry after another.  The index is finalized in the end.  Throws `m::invalid_arguent` if \p key_schema contains
     * more than one entry or `key_type` and the attribute type of the entry in \p key_schema do not match. */
    void bulkload(const Table &table, const Schema &key_schema) override;

    /** Returns the number of entries in the index. */
    std::size_t num_entries() const override { return data_.size(); }

    /** Returns the `IndexMethod` of the index. */
    IndexMethod method() const override { return IndexMethod::Array; }

    /** Adds a single pair of \p key and \p value to the index.  Note that `finalize()` has to be called afterwards for
     * the vector to be sorted and the index to be usable. */
    void add(const key_type key, const value_type value);

    /** Sorts the underlying vector and flags the index as finalized. */
    virtual void finalize() {
        std::sort(data_.begin(), data_.end(), cmp);
        finalized_ = true;
    }

    /** Returns `true` iff the index is currently finalized. */
    bool finalized() const { return finalized_; }

    /** Returns an iterator pointing to the first entry of the vector such that `entry.key` < \p key is `false`, i.e.
     * that is greater than or equal to \p key, or `end()` if no such element is found.  Throws `m::exception` if the
     * index is not finalized. */
    virtual const_iterator lower_bound(const key_type key) const {
        if (not finalized_) throw m::exception("Index is not finalized.");
        return std::lower_bound(data_.begin(), data_.end(), entry_type{key, value_type()}, cmp);
    }

    /** Returns an iterator pointing to the first entry of the vector such that `entry.key` < \p key is `true`, i.e.
     * that is strictly greater than \p key, or `end()` if no such element is found.  Throws `m::exception` if the index
     * is not finalized. */
    virtual const_iterator upper_bound(const key_type key) const {
        if (not finalized_) throw m::exception("Index is not finalized.");
        return std::upper_bound(data_.begin(), data_.end(), entry_type{key, value_type()}, cmp);
    }

    /** Returns an iterator pointing to the first entry of the index. */
    const_iterator begin()  const { return data_.cbegin(); }
    const_iterator cbegin() const { return data_.cbegin(); }
    /** Returns an interator pointing to the first element following the last entry of the index. */
    const_iterator end() const  { return data_.cend(); }
    const_iterator cend() const { return data_.cend(); }

    void dump(std::ostream &out) const override { out << "ArrayIndex<" << typeid(key_type).name() << '>' << std::endl; }
    void dump() const override { dump(std::cerr); }
};

/** A recursive model index with two layers consiting only of linear monels that maps keys to their `tuple_id`. */
template<arithmetic Key>
struct RecursiveModelIndex : ArrayIndex<Key>
{
    using base_type = ArrayIndex<Key>;
    using key_type = base_type::key_type;
    using value_type = base_type::value_type;
    using entry_type = base_type::entry_type;
    using container_type = base_type::container_type;
    using const_iterator = base_type::const_iterator;

    struct LinearModel
    {
        double slope;
        double intercept;

        LinearModel(double slope, double intercept) : slope(slope), intercept(intercept) { }

        double operator()(const key_type x) const { return std::fma(slope, static_cast<double>(x), intercept); }

        /** Builds a linear spline model between the \p first and \p last data point.  \p offset defines the first
         * y-value.  All y-values are scaled by \p compression_factor. */
        static LinearModel train_linear_spline(const_iterator first, const_iterator last,
                                               const std::size_t offset = 0, const double compression_factor = 1.0)
        {
            std::size_t n = std::distance(first, last);
            if (n == 0) return { 0.0, 0.0 };
            if (n == 1) return { 0.0, static_cast<double>(offset) * compression_factor };
            double numerator = static_cast<double>(n); // (offset + n) - offset
            double denominator = static_cast<double>((*(last - 1)).first - (*first).first);
            double slope = denominator != 0 ? numerator/denominator * compression_factor : 0.0;
            double intercept = offset * compression_factor - slope * (*first).first;
            return { slope, intercept };
        }

        /** Builds a linear regression model from all data points between the \p first and \p last .  \p offset defines
         * the first y-value.  All y-values are scaled by \p compression_factor. */
        static LinearModel train_linear_regression(const_iterator first, const_iterator last,
                                                   const std::size_t offset = 0, const double compression_factor = 1.0)
        {
            std::size_t n = std::distance(first, last);
            if (n == 0) return { 0.0, 0.0 };
            if (n == 1) return { 0.0, static_cast<double>(offset) * compression_factor };

            double mean_x = 0.0;
            double mean_y = 0.0;
            double c = 0.0;
            double m2 = 0.0;

            for (std::size_t i = 0; i != n; ++i) {
                auto x = (*(first + i)).first;
                std::size_t y = offset + i;

                double dx = x - mean_x;
                mean_x += dx /  (i + 1);
                mean_y += (y - mean_y) / (i + 1);
                c += dx * (y - mean_y);

                double dx2 = x - mean_x;
                m2 += dx * dx2;
            }

            double cov = c / (n - 1);
            double var = m2 / (n - 1);

            if (var == 0.f) return { 0.0, mean_y };

            double slope = cov / var * compression_factor;
            double intercept = mean_y * compression_factor - slope * mean_x;
            return { slope, intercept };
        }
    };

    protected:
    std::vector<LinearModel> models_; ///< A vector of linear models to index the underlying data.

    public:
    RecursiveModelIndex() : base_type() { }

    /** Returns the `IndexMethod` of the index. */
    IndexMethod method() const override { return IndexMethod::Rmi; }

    /** Sorts the underlying vector, builds the linear models, and flags the index as finalized. */
    void finalize() override;

    /** Returns an iterator pointing to the first entry of the vector such that `entry.key` < \p key is `false`, i.e.
     * that is greater than or equal to \p key, or `end()` if no such element is found.  Throws `m::exception` if the
     * index is not finalized. */
    const_iterator lower_bound(const key_type key) const override {
        if (not base_type::finalized()) throw m::exception("Index is not finalized.");
        return lower_bound_exponential_search(base_type::begin() + predict(key), key);
    }

    /** Returns an iterator pointing to the first entry of the vector such that `entry.key` < \p key is `true`, i.e.
     * that is strictly greater than \p key, or `end()` if no such element is found.  Throws `m::exception` if the index
     * is not finalized. */
    const_iterator upper_bound(const key_type key) const override {
        if (not base_type::finalized()) throw m::exception("Index is not finalized.");
        return upper_bound_exponential_search(base_type::begin() + predict(key), key);
    }

    void dump(std::ostream &out) const override { out << "RecursiveModelIndex<" << typeid(key_type).name() << '>' << std::endl; }
    void dump() const override { dump(std::cerr); }

    private:
    std::size_t predict(const key_type key) const {
        auto segment_id = std::clamp<double>(models_[0](key), 0, models_.size() - 2);
        auto pred = std::clamp<double>(models_[segment_id + 1](key), 0, base_type::data_.size());
        return static_cast<std::size_t>(pred);
    }
    const_iterator lower_bound_exponential_search(const_iterator pred, const key_type value) const {
        auto begin = base_type::begin();
        auto end = base_type::end();
        std::size_t bound = 1;
        if (base_type::cmp(*pred, value)) { // search right side
            auto prev = pred;
            auto curr = prev + bound;
            while (curr < end and base_type::cmp(*curr, value)) {
                bound *= 2;
                prev = curr;
                curr += bound;
            }
            return std::lower_bound(prev, std::min(curr + 1, end), value, base_type::cmp);
        } else { // search left side
            auto prev = pred;
            auto curr = prev - bound;
            while (curr > begin and not base_type::cmp(*curr, value)) {
                bound *= 2;
                prev = curr;
                curr -= bound;
            }
            return std::lower_bound(std::max(begin, curr), prev, value, base_type::cmp);
        }
    }
    const_iterator upper_bound_exponential_search(const_iterator pred, const key_type value) const {
        auto begin = base_type::begin();
        auto end = base_type::end();
        std::size_t bound = 1;
        if (not base_type::cmp(value, *pred)) { // search right side
            auto prev = pred;
            auto curr = prev + bound;
            while (curr < end and not base_type::cmp(value, *curr)) {
                bound *= 2;
                prev = curr;
                curr += bound;
            }
            return std::upper_bound(prev, std::min(curr + 1, end), value, base_type::cmp);
        } else { // search left side
            auto prev = pred;
            auto curr = prev - bound;
            while (curr > begin and base_type::cmp(value, *curr)) {
                bound *= 2;
                prev = curr;
                curr -= bound;
            }
            return std::upper_bound(std::max(begin, curr), prev, value, base_type::cmp);
        }
    }
};

#define M_INDEX_LIST_TEMPLATED(X) \
    X(m::idx::ArrayIndex<bool>) \
    X(m::idx::ArrayIndex<int8_t>) \
    X(m::idx::ArrayIndex<int16_t>) \
    X(m::idx::ArrayIndex<int32_t>) \
    X(m::idx::ArrayIndex<int64_t>) \
    X(m::idx::ArrayIndex<float>) \
    X(m::idx::ArrayIndex<double>) \
    X(m::idx::ArrayIndex<const char*>) \
    X(m::idx::RecursiveModelIndex<int8_t>) \
    X(m::idx::RecursiveModelIndex<int16_t>) \
    X(m::idx::RecursiveModelIndex<int32_t>) \
    X(m::idx::RecursiveModelIndex<int64_t>) \
    X(m::idx::RecursiveModelIndex<float>) \
    X(m::idx::RecursiveModelIndex<double>)

}

}

// explicit instantiation declarations
#define DECLARE(CLASS) \
    extern template struct CLASS;
M_INDEX_LIST_TEMPLATED(DECLARE)
#undef DECLARE

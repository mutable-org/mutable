#pragma once

#include <algorithm>


/** A sorted list of elements.  Allows duplicates. */
template<typename T>
struct sorted_list
{
    using vector_type = std::vector<T>;
    using value_type = T;
    using size_type = typename vector_type::size_type;

    private:
    vector_type v_;

    public:
    sorted_list() { }

    auto begin() { return v_.begin(); }
    auto end()   { return v_.end(); }
    auto begin() const { return v_.begin(); }
    auto end()   const { return v_.end(); }
    auto cbegin() const { return v_.cbegin(); }
    auto cend()   const { return v_.cend(); }

    auto empty() const { return v_.empty(); }
    auto size() const { return v_.size(); }
    auto reserve(size_type new_cap) { return v_.reserve(new_cap); }

    bool contains(const T &value) const {
        auto pos = std::lower_bound(begin(), end(), value);
        return pos != end() and *pos == value;
    }

    auto insert(T value) { return v_.insert(std::lower_bound(begin(), end(), value), value); }

    template<typename InsertIt>
    void insert(InsertIt first, InsertIt last) {
        while (first != last)
            insert(*first++);
    }
};

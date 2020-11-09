#pragma once

#include <functional>
#include <type_traits>
#include <unordered_set>


namespace m {

template<typename T, typename Hash>
struct dereference_hash
{
    auto operator()(const T *t) const {
        Hash h;
        return h(*t);
    }
};

template<typename T, typename KeyEqual>
struct dereference_equal_to
{
    auto operator()(const T *first, const T *second) const {
        KeyEqual eq;
        return eq(*first, *second);
    }
};

/** A pool implements an implicitly garbage-collected set of objects. */
template<typename T, typename Hash = std::hash<T>, typename KeyEqual = std::equal_to<T>>
struct Pool
{
    private:
    using table_type = std::unordered_set<T*, dereference_hash<T, Hash>, dereference_equal_to<T, KeyEqual>>;
    table_type table_;

    public:
    Pool() = default;
    Pool(std::size_t n) : table_(n) { }

    ~Pool() {
        for (auto p : table_)
            delete p;
    }

    template<typename U>
    const T * operator()(U t) {
        auto it = table_.find(&t);
        if (it == table_.end())
            it = table_.emplace_hint(it, new U(std::move(t)));
        return *it;
    }

    std::size_t size() const { return table_.size(); }
};

}

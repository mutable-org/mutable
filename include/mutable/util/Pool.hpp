#pragma once

#include <concepts>
#include <functional>
#include <mutable/util/fn.hpp>
#include <type_traits>
#include <unordered_set>


namespace m {

/** A data type representing a *pooled* (or *internalized*) object. */
template<typename T>
struct Pooled
{
    private:
    const T &ref_;

    public:
    Pooled(const T &ref) : ref_(ref) { }
    Pooled(const Pooled &other) : ref_(other.ref_) { }

    template<typename U>
    requires std::is_base_of_v<T, U>
    Pooled(const Pooled<U> &other) : ref_(other.ref_) { }

    operator const T & () const { return ref_; }
    operator const T * () const { return &ref_; }

    const T & operator*() const { return ref_; }
    const T * operator->() const { return &ref_; }

    template<typename U>
    requires requires (const T &ref) { m::as<const U>(ref); }
    Pooled<U> as() const {
        using m::as;
        return Pooled<U>(as<const U>(ref_));
    }

    template<typename U>
    bool operator==(Pooled<U> other) const { return &this->ref_ == &other.ref_; } // check referential equality
    template<typename U>
    bool operator!=(Pooled<U> other) const { return not operator==(other); }
    template<typename U>
    bool operator==(const U *other) const { return &this->ref_ == other; } // check referential equality
    template<typename U>
    bool operator!=(const U *other) const { return not operator==(other); }
};

/** The `PODPool` implements an implicitly garbage-collected set of *pooled* (or *internalized*) POD struct entities. */
template<typename T, typename Hash = std::hash<T>, typename KeyEqual = std::equal_to<T>, typename Copy = std::identity>
requires (not std::is_abstract_v<T>) // abstract types must be handled by separate pool
struct PODPool
{
    using pooled_type = T;
    using proxy_type = Pooled<T>;

    private:
    using table_type = std::unordered_set<T, Hash, KeyEqual>;
    table_type table_;

    public:
    using iterator = table_type::iterator;
    using const_iterator = table_type::const_iterator;

    public:
    PODPool() = default;
    PODPool(std::size_t initial_capacity) : table_(initial_capacity) { }
    virtual ~PODPool() { }

    std::size_t size() const { return table_.size(); }

    iterator begin() { return table_.begin(); }
    iterator end() { return table_.end(); }
    iterator begin() const { return table_.begin(); }
    iterator end() const { return table_.end(); }
    iterator cbegin() const { return table_.begin(); }
    iterator cend() const { return table_.end(); }

    template<typename U>
    proxy_type operator()(U &&u) {
        auto it = table_.find(u);
        if (it == table_.end())
            it = table_.emplace_hint(it, Copy{}(std::forward<U>(u))); // perfect forwarding: copy or move construct
        return proxy_type(*it);
    }
};

/** A pool implements an implicitly garbage-collected set of instances of a class hierarchy. */
template<typename T, typename Hash = std::hash<T>, typename KeyEqual = std::equal_to<T>>
struct Pool
{
    using pooled_type = T;
    using proxy_type = Pooled<T>;

    private:
    struct dereference_hash
    {
        ///> Mark this callable as *transparent*, allowing for computing the hash of various types that are
        ///interoperable. > See https://en.cppreference.com/w/cpp/container/unordered_map/find.
        using is_transparent = void;

        template<typename U>
        auto operator()(U &&u) const { return Hash{}(*u); }
    };

    struct dereference_equal_to
    {
        ///> Mark this callable as *transparent*, allowing for comparing various types that are interoperable. > See
        ///https://en.cppreference.com/w/cpp/container/unordered_map/find.
        using is_transparent = void;

        template<typename U, typename V>
        auto operator()(U &&first, V &&second) const { return KeyEqual{}(*first, *second); }
    };

    using table_type = std::unordered_set<std::unique_ptr<T>, dereference_hash, dereference_equal_to>;
    table_type table_;

    public:
    using iterator = table_type::iterator;
    using const_iterator = table_type::const_iterator;

    public:
    Pool() = default;
    Pool(std::size_t initial_capacity) : table_(initial_capacity) { }

    std::size_t size() const { return table_.size(); }

    iterator begin() { return table_.begin(); }
    iterator end() { return table_.end(); }
    iterator begin() const { return table_.begin(); }
    iterator end() const { return table_.end(); }
    iterator cbegin() const { return table_.begin(); }
    iterator cend() const { return table_.end(); }

    template<typename U>
    requires std::is_base_of_v<T, U>
    Pooled<U> operator()(U &&u) {
        auto it = table_.find(&u);
        if (it == table_.end())
            it = table_.emplace_hint(it, as<T>(std::make_unique<U>(std::forward<U>(u)))); // perfect forwarding: copy or move construct
        return Pooled<U>(as<U>(**it)); // double dereference to get address of instance
    }
};

struct StrClone
{
    const char * operator()(const char *str) const { return M_notnull(strdup(str)); }
    const char * operator()(std::string_view sv) const { return M_notnull(strndup(sv.data(), sv.size())); }
};

/** Explicit specialization of PODPool for strings (const char *). */
struct StringPool : PODPool<const char*, StrHash, StrEqual, StrClone>
{
    private:
    using super = PODPool<const char*, StrHash, StrEqual, StrClone>;

    public:
    StringPool() = default;
    StringPool(std::size_t initial_capacity) : super(initial_capacity) { }

    ~StringPool() {
        for (const char *str : *this)
            free((void*) str);
    }
};

using PooledString = StringPool::proxy_type;

}

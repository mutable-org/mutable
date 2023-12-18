#pragma once

#include <functional>
#include <mutable/util/fn.hpp>
#include <mutable/util/macro.hpp>
#include <type_traits>
#include <unordered_map>


namespace m {

// forward declaration
template<typename T, typename Pool, bool CanBeNone = false>
struct Pooled;

/** The `PODPool` implements an implicitly garbage-collected set of *pooled* (or *internalized*) POD struct entities. */
template<typename T, typename Hash = std::hash<T>, typename KeyEqual = std::equal_to<T>, typename Copy = std::identity>
struct PODPool
{
    using table_type = std::unordered_map<T, uint32_t, Hash, KeyEqual>;
    using pooled_type = T;
    using proxy_type = Pooled<T, PODPool, false>;
    using proxy_optional_type = Pooled<T, PODPool, true>;
    using hasher = Hash;
    using key_equal = KeyEqual;
    using copy = Copy;

    private:
    table_type table_;

    public:
    using const_iterator = table_type::const_iterator;

    public:
    PODPool() = default;
    PODPool(std::size_t initial_capacity) : table_(initial_capacity) { }
    virtual ~PODPool() {
#ifndef NDEBUG
        for (auto& [_, count] : table_)
            M_insist(count == 0, "deleting would create a dangling reference to pooled object");
#endif
    }

    /** Returns the number of elements in the pool. */
    std::size_t size() const { return table_.size(); }

    const_iterator begin() { return table_.cbegin(); }
    const_iterator end() { return table_.cend(); }
    const_iterator cbegin() const { return table_.begin(); }
    const_iterator cend() const { return table_.end(); }

    /** Returns the pooled \param u. */
    template<typename U>
    proxy_type operator()(U &&u);

    /** Returns a reference to the value referenced by \param pooled. */
    static const T & Get(const proxy_type &pooled);
};

/** A pool implements an implicitly garbage-collected set of instances of a class hierarchy. */
template<typename T, typename Hash = std::hash<T>, typename KeyEqual = std::equal_to<T>>
struct Pool
{
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

    using table_type = std::unordered_map<std::unique_ptr<T>, uint32_t, dereference_hash, dereference_equal_to>;
    using pooled_type = T;
    using proxy_type = Pooled<T, Pool, false>;
    using proxy_optional_type = Pooled<T, Pool, true>;
    using hasher = Hash;
    using key_equal = KeyEqual;

    private:
    table_type table_;

    public:
    using const_iterator = table_type::const_iterator;

    public:
    Pool() = default;
    Pool(std::size_t initial_capacity) : table_(initial_capacity) { }
    ~Pool() {
#ifndef NDEBUG
        /* Manually delete all entries and make sure we don't have any dangling references. */
        while (not table_.empty()) {
            typename table_type::node_type nh = table_.extract(table_.begin());
            M_insist(nh.mapped() == 0, "deleting would create a dangling reference to pooled object");
        }
#endif
    }

    /** Returns the number of elements in the pool. */
    std::size_t size() const { return table_.size(); }

    const_iterator begin() { return table_.cbegin(); }
    const_iterator end() { return table_.cend(); }
    const_iterator cbegin() const { return table_.begin(); }
    const_iterator cend() const { return table_.end(); }

    /** Returns the pooled \param u. */
    template<typename U>
    requires std::is_base_of_v<T, U>
    Pooled<U, Pool> operator()(U &&u);

    /** Returns a reference to the value referenced by \param pooled. */
    template<typename U>
    requires std::is_base_of_v<T, U>
    static const U & Get(const Pooled<U, Pool> &pooled);
};

/**
 * A data type representing a *pooled* (or *internalized*) object.
 *
 * If \tparam CanBeNone, an instance of `Pooled` can *not* point to any object, and `Pooled` provides additional methods
 * to query whether it is pointing to an actual object and to safely get the object, if any, via
 * `std::optional<std::reference_wrapper<const T>>`.
 */
template<typename T, typename Pool, bool CanBeNone>
struct Pooled
{
    ///> Can this `Pooled` *not* reference an object?
    static constexpr bool can_be_none = CanBeNone;

    using value_type = typename Pool::table_type::value_type;

    ///> \tparam Pool needs access to private c'tor
    friend Pool;

    friend void swap(Pooled &first, Pooled &second) {
        using std::swap;
        swap(first.pool_, second.pool_);
        swap(first.ref_,  second.ref_);
    }

    private:
    ///> The pool holding this value.  Can only be `nullptr` if `CanBeNone`
    Pool *pool_;
    ///> The pointer to the pooled object.  Can only become `nullptr` if the `Pooled` instance is moved.
    value_type *ref_ = nullptr;

    /** Constucts a fresh `Pooled` from a pooled \param value and its owning \param pool. */
    Pooled(Pool &pool, value_type &value) : pool_(&pool), ref_(&value) { ++ref_->second; }  // increase reference count

    public:
    Pooled() requires can_be_none : pool_(nullptr) { }
    Pooled(const Pooled &other) : pool_(other.pool_), ref_(M_notnull(other.ref_)) { ++ref_->second; }  // increase reference count
    Pooled(Pooled &&other) { swap(*this, other); }

    ~Pooled() {
        if (ref_) {
            M_insist(ref_->second > 0, "underflow reference count");
            if (--ref_->second == 0)
                /* TODO: free object in pool, as it is not referenced anymore */;
        }
    }

    Pooled & operator=(Pooled other) { swap(*this, other); return *this; }

    operator const T & () const { return Pool::Get(*this); }
    operator const T * () const { return &Pool::Get(*this); }

    const T & operator*() const { return Pool::Get(*this); }
    const T * operator->() const { return &Pool::Get(*this); }

    template<typename U>
    requires requires (const T &ref) { m::as<const U>(ref); }
    Pooled<U, Pool> as() const {
        M_insist(pool_);
        return Pooled<U, Pool>{*pool_, m::as<const U>(*ref_)};
    }

    template<typename U>
    bool operator==(Pooled<U, Pool> other) const { return this->ref_ == other.ref_; } // check referential equality
    template<typename U>
    bool operator!=(Pooled<U, Pool> other) const { return not operator==(other); }
    template<typename U>
    bool operator==(const U *other) const { return &Pool::Get(*this) == other; } // check referential equality
    template<typename U>
    bool operator!=(const U *other) const { return not operator==(other); }
};

template<typename T, typename Hash, typename KeyEqual, typename Copy>
template<typename U>
PODPool<T, Hash, KeyEqual, Copy>::proxy_type PODPool<T, Hash, KeyEqual, Copy>::operator()(U &&u)
{
    auto it = table_.find(u);
    if (it == table_.end())
        it = table_.emplace_hint(it, Copy{}(std::forward<U>(u)), 0); // perfect forwarding
    return proxy_type{*this, *it};
}

template<typename T, typename Hash, typename KeyEqual, typename Copy>
const T & PODPool<T, Hash, KeyEqual, Copy>::Get(const Pooled<T, PODPool> &pooled)
{
    M_insist(pooled.ref_);
    return pooled.ref_->first;
}

template<typename T, typename Hash, typename KeyEqual>
template<typename U>
requires std::is_base_of_v<T, U>
Pooled<U, Pool<T, Hash, KeyEqual>> Pool<T, Hash, KeyEqual>::operator()(U &&u)
{
    auto it = table_.find(&u);
    if (it == table_.end())
        it = table_.emplace_hint(it, as<T>(std::make_unique<U>(std::forward<U>(u))), 0); // perfect forwarding
    return Pooled<U, Pool>{*this, *it};
}

template<typename T, typename Hash, typename KeyEqual>
template<typename U>
requires std::is_base_of_v<T, U>
const U & Pool<T, Hash, KeyEqual>::Get(const Pooled<U, Pool> &pooled)
{
    M_insist(pooled.ref_);
    return as<U>(*pooled.ref_->first);  // additional dereference because of `std::unique_ptr` indirection
}

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
        for (auto [str, _] : *this)
            free((void*) str);
    }
};

using PooledString = StringPool::proxy_type;

}

namespace std {

template<typename T, typename Pool>
struct std::hash<m::Pooled<T, Pool>>
{
    uint64_t operator()(const m::Pooled<T, Pool> &pooled) const {
        return std::hash<const T*>{}(&*pooled); // hash of the address where the object is stored in the pool
    }
};

}

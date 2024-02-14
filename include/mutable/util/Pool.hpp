#pragma once

#include <functional>
#include <mutable/util/fn.hpp>
#include <mutable/util/macro.hpp>
#include <mutable/util/OptField.hpp>
#include <mutable/util/reader_writer_lock.hpp>
#include <type_traits>
#include <unordered_map>
#include <utility>


namespace m {

// forward declaration
template<typename T, typename Pool, bool CanBeNone = false>
struct Pooled;

/** The `PODPool` implements an implicitly garbage-collected set of *pooled* (or *internalized*) POD struct entities. */
template<typename T, typename Hash = std::hash<T>, typename KeyEqual = std::equal_to<T>, typename Copy = std::identity,
         bool ThreadSafe = false>
struct PODPool
{
    using counter_type = std::conditional_t<ThreadSafe, std::atomic<uint32_t>, uint32_t>;
    using table_type = std::unordered_map<T, counter_type, Hash, KeyEqual>;
    using pooled_type = T;
    using proxy_type = Pooled<T, PODPool, false>;
    using proxy_optional_type = Pooled<T, PODPool, true>;
    using hasher = Hash;
    using key_equal = KeyEqual;
    using copy = Copy;

    template<typename, typename, bool>
    friend struct Pooled;

    static constexpr bool is_thread_safe = ThreadSafe;

    private:
    table_type table_;

    alignas(64) mutable OptField<ThreadSafe, reader_writer_mutex> table_mutex_;

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

    private:
    /** Returns a reference to the value referenced by \param pooled. */
    template<bool CanBeNone>
    static const T & Get(const Pooled<T, PODPool, CanBeNone> &pooled);

    /** Erases the pooled entity from the pool.  Requires that the reference count is `0`. */
    template<bool CanBeNone>
    bool erase(const Pooled<T, PODPool, CanBeNone> &pooled);
};

/** A pool implements an implicitly garbage-collected set of instances of a class hierarchy. */
template<typename T, typename Hash = std::hash<T>, typename KeyEqual = std::equal_to<T>, bool ThreadSafe = false>
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

    using counter_type = std::conditional_t<ThreadSafe, std::atomic<uint32_t>, uint32_t>;
    using table_type = std::unordered_map<std::unique_ptr<T>, counter_type, dereference_hash, dereference_equal_to>;
    using pooled_type = T;
    template<typename U> using proxy_type = Pooled<U, Pool, false>;
    template<typename U> using proxy_optional_type = Pooled<U, Pool, true>;
    using hasher = Hash;
    using key_equal = KeyEqual;

    template<typename, typename, bool>
    friend struct Pooled;

    static constexpr bool is_thread_safe = ThreadSafe;

    private:
    table_type table_;

    alignas(64) mutable OptField<ThreadSafe, reader_writer_mutex> table_mutex_;

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
    requires std::derived_from<U, T>
    Pooled<U, Pool, false> operator()(U &&u);

    private:
    /** Returns a reference to the value referenced by \param pooled. */
    template<typename U, bool CanBeNone>
    requires std::is_base_of_v<T, U>
    static const U & Get(const Pooled<U, Pool, CanBeNone> &pooled);

    /** Erases the pooled entity from the pool.  Requires that the reference count is `0`. */
    template<typename U, bool CanBeNone>
    bool erase(const Pooled<U, Pool, CanBeNone> &pooled);
};

/**
 * A data type representing a *pooled* (or *internalized*) object.
 *
 * If \tparam CanBeNone, an instance of `Pooled` can *not* point to any object, and `Pooled` provides additional methods
 * to query whether it is pointing to an actual object and to safely get the object, if any.
 */
template<typename T, typename Pool, bool CanBeNone>
struct Pooled
{
    ///> Can this `Pooled` *not* reference an object?
    static constexpr bool can_be_none = CanBeNone;

    using value_type = typename Pool::table_type::value_type;

    ///> \tparam Pool needs access to private c'tor
    friend Pool;

    ///> Access privilege for conversion between optional and non-optional `Pooled` instances.
    template<typename, typename, bool>
    friend struct Pooled;

    friend void swap(Pooled &first, Pooled &second) {
        using std::swap;
        swap(first.pool_, second.pool_);
        swap(first.ref_,  second.ref_);
    }

    private:
    ///> The pool holding this value. Can only be `nullptr` if `CanBeNone` or if the `Pooled` instance is moved.
    Pool *pool_ = nullptr;
    ///> The pointer to the pooled object. Can only be `nullptr` if `CanBeNone` or if the `Pooled` instance is moved.
    value_type *ref_ = nullptr;

    /** Constucts a fresh `Pooled` from a pooled \param value and its owning \param pool. */
    Pooled(Pool *pool, value_type *value) : pool_(pool), ref_(value) {
        M_insist(bool(pool_) == bool(ref_), "inconsistent pooled state");
        if constexpr (CanBeNone) {
            if (ref_) ++ref_->second;  // increase reference count
        } else {
            ++M_notnull(ref_)->second;  // increase reference count
        }
    }

    public:
    Pooled() requires can_be_none = default;
    Pooled(const Pooled &other) : Pooled(other.pool_, other.ref_) { }
    Pooled(Pooled &&other) { swap(*this, other); }

    /**
     * Constructs an optional `Pooled` with a present value from a non-optional one. Example usecase: Assigning newly
     * created pooled object to an optional pooled member field.
     */
    Pooled(const Pooled<T, Pool, false> &other) requires can_be_none
        : Pooled(M_notnull(other.pool_), M_notnull(other.ref_))
    { }

    ///> Move-constructs an optional `Pooled` with a present value from a non-optional one.
    Pooled(Pooled<T, Pool, false> &&other) requires can_be_none
        : pool_(std::exchange(other.pool_, nullptr))
        , ref_(std::exchange(other.ref_, nullptr))
    { }

    ///> Constructs a non-optional `Pooled` from an optional one. Can only be used if value is present.
    explicit Pooled(const Pooled<T, Pool, true> &other) requires (not can_be_none)
        : Pooled(M_notnull(other.pool_), M_notnull(other.ref_))
    { }

    ///> Move-constructs a non-optional `Pooled` from an optional one. Can only be used if value is present.
    explicit Pooled(Pooled<T, Pool, true> &&other) requires (not can_be_none)
        : pool_(std::exchange(other.pool_, nullptr))
        , ref_(std::exchange(other.ref_, nullptr))
    { }

    bool has_value() const requires can_be_none { return ref_ != nullptr; }

    /**
     * Returns the number of references to the pooled object or 0 if
     * this `Pooled` CanBeNone and does *not* hold a reference to an object.
     */
    uint32_t count() const { return ref_ ? ref_->second : 0; }

    ~Pooled() {
        M_insist(bool(pool_) == bool(ref_), "inconsistent pooled state");
        if (ref_) {
            M_insist(ref_->second > 0, "underflow reference count");
            if (--ref_->second == 0)
                /* TODO: free object in pool, as it is not referenced anymore */
                // pool_->erase(*this);
                ;
        }
    }

    Pooled & operator=(Pooled other) { swap(*this, other); return *this; }

    operator const T & () const { return Pool::Get(*this); }
    operator const T * () const { return &Pool::Get(*this); }

    const T & operator*() const { return Pool::Get(*this); }
    const T * operator->() const { return &Pool::Get(*this); }

    /**
     * Explicitly casts this `Pooled<T>` to `Pooled<U>`. Supports both up-casting and down-casting.
     * Requires that the underlying type is polymorphic and `T` can be dynamically casted to \tparam U.
     */
    template<typename U>
    requires std::is_polymorphic_v<typename Pool::pooled_type> and (std::derived_from<U, T> or std::derived_from<T, U>)
    Pooled<U, Pool, false> as() const {
        M_insist(ref_, "cannot cast empty pooled object");
        M_insist(m::is<U>(ref_->first)); // check if the cast is valid
        return {pool_, ref_};
    }

    /**
     * Implicitly casts this `Pooled<T>` to `Pooled<U>`. Only supports up-casting.
     * Requires that the underlying type is polymorphic and `T` is derived from type \tparam U.
     */
    template<typename U>
    requires std::is_polymorphic_v<typename Pool::pooled_type> and std::derived_from<T, U>
    operator Pooled<U, Pool, false> () {
        return as<U>();
    }

    /**
     * Assigns a `Pooled<U>` to this `Pooled<T>`. Only supports up-casting.
     * Requires that the underlying type is polymorphic and rhs type( \tparam U ) is derived from lhs type (`T`).
     * The rhs can be optional or non-optional `Pooled`, in either case it can *not* be empty.
     */
    template<typename U, bool _CanBeNone>
    requires std::is_polymorphic_v<typename Pool::pooled_type> and std::derived_from<U, T>
    Pooled & operator=(Pooled<U, Pool, _CanBeNone> other) {
        M_insist(other.ref_, "rhs can not be empty");
        using std::swap;
        swap(this->pool_, other.pool_);
        swap(this->ref_ , other.ref_);
        return *this;
    }

    template<typename U, bool _CanBeNone>
    bool operator==(Pooled<U, Pool, _CanBeNone> other) const { return this->ref_ == other.ref_; } // check referential equality
    template<typename U, bool _CanBeNone>
    bool operator!=(Pooled<U, Pool, _CanBeNone> other) const { return not operator==(other); }
    template<typename U>
    bool operator==(const U *other) const { return &Pool::Get(*this) == other; } // check referential equality
    template<typename U>
    bool operator!=(const U *other) const { return not operator==(other); }

    friend std::ostream & operator<<(std::ostream &out, const Pooled &pooled) {
        if constexpr (streamable<std::ostream, T>)
            return out << *pooled;
        else
            return out << "Pooled<" << typeid(T).name() << ">";
    }

    void dump(std::ostream &out) const {
        out << *this
            << " (" << &Pool::Get(*this) << ")"
            << " count: " << this->count() << std::endl;
    }
    void dump() const { dump(std::cerr); }
};

template<typename T, typename Hash, typename KeyEqual, typename Copy, bool ThreadSafe>
template<typename U>
PODPool<T, Hash, KeyEqual, Copy, ThreadSafe>::proxy_type PODPool<T, Hash, KeyEqual, Copy, ThreadSafe>::operator()(U &&u)
{
    if constexpr (ThreadSafe) {
        reader_writer_lock lock{table_mutex_};
        typename table_type::iterator it;
        do {
            lock.lock_read();
            it = table_.find(u);
            if (it != table_.end())
                return proxy_type{this, &*it};
        } while (not lock.upgrade());
        M_insist(lock.owns_write_lock());
        it = table_.emplace_hint(it, Copy{}(std::forward<U>(u)), 0); // perfect forwarding
        return proxy_type{this, &*it};
    } else {
        auto it = table_.find(u);
        if (it == table_.end())
            it = table_.emplace_hint(it, Copy{}(std::forward<U>(u)), 0); // perfect forwarding
        return proxy_type{this, &*it};
    }
}

template<typename T, typename Hash, typename KeyEqual, typename Copy, bool ThreadSafe>
template<bool CanBeNone>
bool PODPool<T, Hash, KeyEqual, Copy, ThreadSafe>::erase(const Pooled<T, PODPool, CanBeNone> &pooled)
{
    M_insist(pooled.ref_, "cannot erase w/o valid reference");
    if constexpr (ThreadSafe) {
        write_lock lock{table_mutex_};
        if (pooled.ref_->second != 0) return false;  // entity was concurrently pooled
        table_.erase(pooled.ref_->first);
        return true;
    } else {
        M_insist(pooled.ref_->second == 0, "reference count must be 0 to erase");
        table_.erase(pooled.ref_->first);
        return true;
    }
}

template<typename T, typename Hash, typename KeyEqual, typename Copy, bool ThreadSafe>
template<bool CanBeNone>
const T & PODPool<T, Hash, KeyEqual, Copy, ThreadSafe>::Get(const Pooled<T, PODPool, CanBeNone> &pooled)
{
    M_insist(pooled.ref_);
    return pooled.ref_->first;
}

template<typename T, typename Hash, typename KeyEqual, bool ThreadSafe>
template<typename U>
requires std::derived_from<U, T>
Pool<T, Hash, KeyEqual, ThreadSafe>::proxy_type<U> Pool<T, Hash, KeyEqual, ThreadSafe>::operator()(U &&u)
{
    if constexpr (ThreadSafe) {
        reader_writer_lock lock{table_mutex_};
        typename table_type::iterator it;
        do {
            lock.lock_read();
            it = table_.find(&u);
            if (it != table_.end())
                return proxy_type<U>{this, &*it};
        } while (not lock.upgrade());
        M_insist(lock.owns_write_lock());
        it = table_.emplace_hint(it, as<T>(std::make_unique<U>(std::forward<U>(u))), 0); // perfect forwarding
        return proxy_type<U>{this, &*it};
    } else {
        auto it = table_.find(&u);
        if (it == table_.end())
            it = table_.emplace_hint(it, as<T>(std::make_unique<U>(std::forward<U>(u))), 0); // perfect forwarding
        return proxy_type<U>{this, &*it};
    }
}

template<typename T, typename Hash, typename KeyEqual, bool ThreadSafe>
template<typename U, bool CanBeNone>
bool Pool<T, Hash, KeyEqual, ThreadSafe>::erase(const Pooled<U, Pool, CanBeNone> &pooled)
{
    M_insist(pooled.ref_, "cannot erase w/o valid reference");
    if constexpr (ThreadSafe) {
        write_lock lock{table_mutex_};  // acquire write lock
        if (pooled.ref_->second != 0) return false;  // entity was concurrently pooled
        table_.erase(pooled.ref_->first);
        return true;
    } else {
        M_insist(pooled.ref_->second == 0, "reference count must be 0 to erase");
        table_.erase(pooled.ref_->first);
        return true;
    }
}

template<typename T, typename Hash, typename KeyEqual, bool ThreadSafe>
template<typename U, bool CanBeNone>
requires std::is_base_of_v<T, U>
const U & Pool<T, Hash, KeyEqual, ThreadSafe>::Get(const Pooled<U, Pool, CanBeNone> &pooled)
{
    M_insist(pooled.ref_);
    return as<U>(*pooled.ref_->first);  // additional dereference because of `std::unique_ptr` indirection
}

template<typename T, typename Hash = std::hash<T>, typename KeyEqual = std::equal_to<T>, typename Copy = std::identity>
using ThreadSafePODPool = PODPool<T, Hash, KeyEqual, Copy, true>;
template<typename T, typename Hash = std::hash<T>, typename KeyEqual = std::equal_to<T>>
using ThreadSafePool = Pool<T, Hash, KeyEqual, true>;

struct StrClone
{
    const char * operator()(const char *str) const { return M_notnull(strdup(str)); }
    const char * operator()(std::string_view sv) const { return M_notnull(strndup(sv.data(), sv.size())); }
};

namespace detail {

/** Explicit specialization of PODPool for strings (const char *). */
template<bool ThreadSafe = false>
struct _StringPool : PODPool<const char*, StrHash, StrEqual, StrClone, ThreadSafe>
{
    private:
    using super = PODPool<const char*, StrHash, StrEqual, StrClone, ThreadSafe>;

    public:
    _StringPool() = default;
    _StringPool(std::size_t initial_capacity) : super(initial_capacity) { }

    ~_StringPool() {
        for (auto [str, _] : *this)
            free((void*) str);
    }
};

}

using ThreadSafeStringPool = detail::_StringPool<true>;
using ThreadSafePooledString = ThreadSafeStringPool::proxy_type;

using StringPool = detail::_StringPool<false>;
using PooledString = StringPool::proxy_type;
using PooledOptionalString = StringPool::proxy_optional_type;

}

namespace std {

template<typename T, typename Pool, bool CanBeNone>
struct std::hash<m::Pooled<T, Pool, CanBeNone>>
{
    uint64_t operator()(const m::Pooled<T, Pool, CanBeNone> &pooled) const {
        return std::hash<const T*>{}(&*pooled); // hash of the address where the object is stored in the pool
    }
};

}

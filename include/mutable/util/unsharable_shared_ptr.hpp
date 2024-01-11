#pragma once

#include <memory>
#include <mutable/util/exception.hpp>
#include <mutable/util/fn.hpp>
#include <mutable/util/macro.hpp>


namespace m {

/**
 * This class extends `std::shared_ptr` to allow for *unsharing* an *exclusively* held object and thereby converting to
 * a `std::unique_ptr`.
 *
 * Beware, that this class relies on accurate
 * [`use_count()`](https://en.cppreference.com/w/cpp/memory/shared_ptr/use_count).  The `use_count()` is inaccurate if
 * the shared_ptr is shared across threads.  Therefore, this class must not be used in a shared environment.
 *
 * If support for multi-threading is needed, a *wrapper* around `unsharable_shared_ptr` may be used together with a
 * mutex to guard operations and synchronize on the `use_count()`.
 */
template<typename T>
struct unsharable_shared_ptr : public std::shared_ptr<T>
{
    using super = std::shared_ptr<T>;

    private:
    using deleter_func_type = void(*)(T*); // use function pointer instead of reference due to clang-17 issue
    static void default_deleter(T *ptr) { delete ptr; }
    static void noop_deleter(T*) { }

    public:
    template<typename Y>
    explicit unsharable_shared_ptr(Y *ptr) : super(ptr, &unsharable_shared_ptr::default_deleter) { }

    unsharable_shared_ptr() : super(nullptr) { }
    explicit unsharable_shared_ptr(std::nullptr_t) : super(nullptr) { }

    unsharable_shared_ptr(const unsharable_shared_ptr&) = default;
    unsharable_shared_ptr(unsharable_shared_ptr&&) = default;

    /**
     * Converts (and thereby moves) the exclusively held object from this `unsharable_shared_ptr` to a
     * `std::unique_ptr`.
     *
     * \return a `std::unique_ptr` owning the referenced object
     * \throw `std::logic_error` if this `unsharable_shared_ptr` does not hold the referenced object *exclusively*
     */
    std::unique_ptr<T> exclusive_shared_to_unique() {
        if (super::use_count() == 0) return nullptr;  // nothing to unshare
        if (super::use_count() > 1) throw m::invalid_state{"not exclusive"};
        *std::get_deleter<deleter_func_type>(as<super>(*this)) = &unsharable_shared_ptr::noop_deleter;
        std::unique_ptr<T> uptr{super::get()};
        super::reset();  // this will *not* delete the referenced object
        M_insist(super::get() == nullptr);
        return uptr;
    }
};

template<typename T, typename... Args>
unsharable_shared_ptr<T> make_unsharable_shared(Args&&... args)
{
    return unsharable_shared_ptr<T>{new T(std::forward<Args>(args)...)};
}

}

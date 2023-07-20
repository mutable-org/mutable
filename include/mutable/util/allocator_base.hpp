#pragma once

#include <cstdint>
#include <memory>
#include <type_traits>


namespace m {

template <typename T>
concept is_allocator = requires (T t, size_t size, size_t alignment, void *ptr) {
    { t.allocate(size, alignment) } -> std::same_as<void*>;
    { t.deallocate(ptr, size) } -> std::same_as<void>;
};

template<typename Actual>
struct allocator
{
    using size_type = std::size_t;

    /*------------------------------------------------------------------------------------------------------------------
     * Core routines
     *----------------------------------------------------------------------------------------------------------------*/

    /** Allocate `size` bytes aligned to `alignment`.  */
    void * allocate(size_type size, size_type alignment = 0) {
        return reinterpret_cast<Actual*>(this)->allocate(size, alignment);
    }

    /** Deallocate the allocation at `ptr` of size `size`. */
    void deallocate(void *ptr, size_type size) { reinterpret_cast<Actual*>(this)->deallocate(ptr, size); }

    /*------------------------------------------------------------------------------------------------------------------
     * Convenience overloads
     *----------------------------------------------------------------------------------------------------------------*/

    /** Allocate space for a single entity of type `T` that is aligned according to `T`s alignment requirement. */
    template<typename T>
    std::enable_if_t<not std::is_void_v<T>, T*>
    allocate() { return reinterpret_cast<T*>(allocate(sizeof(T), alignof(T))); }

    /** Allocate space for an array of `n` entities of type `T`.  The space is aligned according to `T`s alignment
     * requirement. */
    template<typename T>
    std::enable_if_t<not std::is_void_v<T>, T*>
    allocate(size_type n) { return reinterpret_cast<T*>(allocate(n * sizeof(T), alignof(T))); }

    /** Deallocate the space for an entity of type `T` at `ptr`. */
    template<typename T>
    std::enable_if_t<not std::is_void_v<T>, void>
    deallocate(T *ptr) { deallocate(reinterpret_cast<void*>(ptr), sizeof(T)); }

    /** Deallocate the space for an array of `n` entities of type `T`. */
    template<typename T>
    std::enable_if_t<not std::is_void_v<T>, void>
    deallocate(T *arr, size_type n) { deallocate(reinterpret_cast<void*>(arr), n * sizeof(T)); }

    /*------------------------------------------------------------------------------------------------------------------
     * make_unique
     *----------------------------------------------------------------------------------------------------------------*/

    template<typename T>
    std::enable_if_t<not std::is_array_v<T>, std::unique_ptr<T>>
    make_unique() { return std::unique_ptr<T>(allocate<T>()); }

    template<typename T>
    std::enable_if_t<std::is_array_v<T>, std::unique_ptr<T>>
    make_unique(size_type n) { return std::unique_ptr<T>(allocate<std::remove_extent_t<T>>(n)); }

    template<typename T>
    void dispose(std::unique_ptr<T> ptr) { deallocate<T>(ptr.release()); }

    template<typename T>
    void dispose(std::unique_ptr<T> ptr, size_type n) { deallocate<std::remove_extent_t<T>>(ptr.release(), n); }
};

}

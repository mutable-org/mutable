#pragma once

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <stdexcept>


namespace m {

/** This allocator serves allocations using malloc/free. */
struct malloc_allocator
{
    using size_type = std::size_t;

    /** Allocate `size` bytes aligned to `alignment`.  If `alignment` is `0`, the usual alignment of `malloc` applies.
     * */
    void * allocate(size_type size, size_type alignment = 0) {
        errno = 0;
        if (alignment) {
            alignment = std::max(alignment, sizeof(void*));
            void *ptr;
            const int err = posix_memalign(&ptr, alignment, size);
            if (err) [[unlikely]]
                throw std::runtime_error(strerror(err));
            return ptr;
        } else {
            void *ptr = malloc(size);
            if (ptr == nullptr) [[unlikely]] {
                const auto errsv = errno;
                throw std::runtime_error(strerror(errsv));
            }
            return ptr;
        }
    }

    /** Allocate space for a single entity of type `T` that is aligned according to `T`s alignment requirement. */
    template<typename T>
    std::enable_if_t<not std::is_void_v<T>, T*>
    allocate() { return reinterpret_cast<T*>(allocate(sizeof(T), alignof(T))); }

    /** Allocate space for an array of `n` entities of type `T`.  The space is aligned according to `T`s alignment
     * requirement. */
    template<typename T>
    std::enable_if_t<not std::is_void_v<T>, T*>
    allocate(size_type n) { return reinterpret_cast<T*>(allocate(n * sizeof(T), alignof(T))); }

    /** Deallocate the allocation at `ptr` of size `size`. */
    void deallocate(void *ptr, size_type size) { (void) size; free(ptr); }

    /** Deallocate the space for an entity of type `T` at `ptr`. */
    template<typename T>
    std::enable_if_t<not std::is_void_v<T>, void>
    deallocate(T *ptr) { deallocate(reinterpret_cast<void*>(ptr), sizeof(T)); }

    /** Deallocate the space for an array of `n` entities of type `T`. */
    template<typename T>
    std::enable_if_t<not std::is_void_v<T>, void>
    deallocate(T *arr, size_type n) { deallocate(reinterpret_cast<void*>(arr), n * sizeof(T)); }
};

}

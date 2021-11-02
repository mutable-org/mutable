#pragma once

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

    void * allocate(size_type size, size_type alignment = 0) {
        errno = 0;
        void *ptr = alignment ? aligned_alloc(alignment, size) : malloc(size);
        if (ptr == nullptr) {
            const auto errsv = errno;
            throw std::runtime_error(strerror(errsv));
        }
        return ptr;
    }

    template<typename T>
    T * allocate() { return reinterpret_cast<T*>(allocate(sizeof(T), alignof(T))); }

    template<typename T>
    T * allocate(size_type n) { return reinterpret_cast<T*>(allocate(n * sizeof(T), alignof(T))); }

    void deallocate(void *ptr, size_type) { free(ptr); }

    template<typename T>
    void deallocate(T *ptr) { deallocate(ptr, sizeof(T)); }
};

}

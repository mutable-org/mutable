#pragma once

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <mutable/util/allocator_base.hpp>
#include <stdexcept>


namespace m {

/** This allocator serves allocations using malloc/free. */
struct malloc_allocator : allocator<malloc_allocator>
{
    using base_type = allocator<malloc_allocator>;
    using base_type::allocate;
    using base_type::deallocate;

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

    /** Deallocate the allocation at `ptr` of size `size`. */
    void deallocate(void *ptr, size_type size) { (void) size; free(ptr); }
};

}

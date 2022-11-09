#pragma once

#include <algorithm>
#include <cerrno>
#include <climits>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <mutable/util/ADT.hpp>
#include <mutable/util/macro.hpp>
#include <stdexcept>

#if __linux
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#elif __APPLE__
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#endif


namespace m {

enum class AllocationStrategy
{
    Linear,
    Exponential,
};

// forward declarations
struct list_allocator;

namespace {

/** This allocator serves as a proxy to redirect allocations and deallocations back to `list_allocator`. */
struct list_allocator_proxy : allocator<list_allocator_proxy>
{
    friend struct ::m::list_allocator;

    using base_type = allocator<list_allocator_proxy>;
    using base_type::allocate;
    using base_type::deallocate;

    private:
    list_allocator *alloc_;

    public:
    list_allocator_proxy(list_allocator *alloc) : alloc_(M_notnull(alloc)) { }
    ~list_allocator_proxy() { }
    list_allocator_proxy(const list_allocator_proxy&) = delete;
    list_allocator_proxy(list_allocator_proxy&&) = default;
    list_allocator_proxy & operator=(list_allocator_proxy&&) = default;

    list_allocator * get_allocator() const { return alloc_; }

    void * allocate(size_type size, size_type alignment = 0);
    void deallocate(void *ptr, size_type size);
};

}

/** Implements a list allocator.
 *
 * Allocated (in use) chunk:
 *  +-----------+------+-------------+
 *  | used part | size | unused part |
 *  +-----------+------+-------------+
 *                ^^^^
 *               header
 *
 *  The unused part *may* be empty.
 *
 * Unused (in list) chunk:
 *  +-----------+--------------------+
 *  | ptr, size | unused space       |
 *  +-----------+--------------------+
 *    ^^^^^^^^^
 *     header
 *
 *  The crux is to move the header when a chunk is allocated.  This enables us to locate the header once the chunk is
 *  deallocated.
 *  The header stores the effective/actual size of the chunk.  (It does *not necessarily* store the size requested by
 *  the client's allocation.
 *  As long as a chunk is in use, we have no way of locating the header.  The client must properly deallocate the chunk
 *  by specifying the size that was specified when the chunk was allocated.
 *
 * */
struct list_allocator : allocator<list_allocator>
{
    friend struct list_allocator_proxy;

    using base_type = allocator<list_allocator>;
    using base_type::allocate;
    using base_type::deallocate;

    private:
    ///> the factor by which a chunk must be larger than the requested allocation to be considered for splitting
    static constexpr size_type CHUNK_SPLIT_FACTOR = 2;
    ///> mask of the high bit of a size_type (the type of the header of a used chunk)
    static constexpr size_type DEALLOCATION_BIT_MASK = size_type(1U) << (sizeof(size_type) * CHAR_BIT - 1U);

    ///> the size of the next actual memory allocation
    size_type min_allocation_size_;
    ///> the minimum alignment of an actual memory allocation
    size_type min_allocation_alignment_;
    ///> the allocation strategy; influences how `min_allocation_size_` is updated with each actual allocation
    AllocationStrategy strategy_;

    ///> the type of the internal list of unused chunks
    using chunk_list_type = doubly_linked_list<size_type, list_allocator_proxy>;
    ///> the list of unused (free) chunks
    chunk_list_type chunks_;
    ///> the type of a list node, which is the "header" of an unused chunk
    using header_type = chunk_list_type::node_type;
    ///> used to override the behavior of allocation requests by `chunks_` by routing through a proxy allocator
    void *override_addr_ = nullptr;

    public:
    friend void swap(list_allocator &first, list_allocator &second) {
        M_insist(first .override_addr_ == nullptr);
        M_insist(second.override_addr_ == nullptr);
        M_insist(first .chunks_.get_allocator().get_allocator() == &first);
        M_insist(second.chunks_.get_allocator().get_allocator() == &second);

        using std::swap;
        swap(first.min_allocation_size_,                   second.min_allocation_size_);
        swap(first.min_allocation_alignment_,              second.min_allocation_alignment_);
        swap(first.strategy_,                              second.strategy_);
        swap(first.chunks_,                                second.chunks_);

        /* Update allocator proxies. */
        first .chunks_.get_allocator() = list_allocator_proxy(&first);
        second.chunks_.get_allocator() = list_allocator_proxy(&second);

        M_insist(first .override_addr_ == nullptr);
        M_insist(second.override_addr_ == nullptr);
    }

    list_allocator(size_type initial_size = 0, AllocationStrategy strategy = AllocationStrategy::Linear)
        : strategy_(strategy)
        , chunks_(list_allocator_proxy(this))
    {
        min_allocation_size_ = ceil_to_multiple_of_pow_2(
            initial_size ? initial_size : sizeof(header_type),
            get_pagesize()
        );
        min_allocation_alignment_ = std::max(alignof(header_type), get_pagesize());

        if (initial_size) {
            add_fresh_chunk(1, 0);
            M_insist(chunks_.size() == 1);
        } else {
            M_insist(chunks_.empty());
        }
    }

    ~list_allocator() {
        while (not chunks_.empty()) {
            auto it = chunks_.begin();
            header_type *header = header_of_unused_chunk(&*it);

            const bool marked_for_deallocation = is_marked_for_deallocation(header);
            override_addr_ = header; // don't deallocate when unlinking node
            chunks_.erase(it); // unlink node
            if (marked_for_deallocation)
                munmap(header, extract_size(header));
        }
    }

    /** Copy c'tor.  Copies the current behavior (allocation size and strategy).  Does *not* copy the current
     * allocations. */
    explicit list_allocator(const list_allocator &other)
        : list_allocator(other.min_allocation_size_, other.strategy_)
    { }

    list_allocator(list_allocator &&other) : list_allocator() { swap(*this, other); }

    list_allocator & operator=(list_allocator other) { swap(*this, other); return *this; }

    void * allocate(const size_type size, const size_type alignment = 0) {
        if (size == 0) return nullptr;

        M_insist(alignment == 0 or is_pow_2(alignment), "alignment must be 0 or a power of 2");

        /* Search for a suitable chunk. */
        chunk_list_type::iterator it = chunks_.begin();
        for (; it != chunks_.end(); ++it) {
            /* Get the size of the chunk. */
            M_insist(*it > sizeof(size_type), "allocation of invalid size (too small)");
            header_type *header = header_of_unused_chunk(&*it);
            size_type usable_size = extract_size(header) - sizeof(size_type);

            /* Check chunk size and alignment. */
            if (usable_size < size or is_unaligned(header, alignment))
                continue;

            /* Found chunk. */
            goto found_chunk;
        }
        /* No chunk found, so add a fresh chunk. */
        M_insist(it == chunks_.end());
        it = add_fresh_chunk(size, alignment);

found_chunk:
        header_type *header = header_of_unused_chunk(&*it);
        size_type effective_size = extract_size(header);
        M_insist(effective_size >= size + sizeof(size_type));

#if 1
        /* See whether we can split the chunk. */
        if (effective_size >= sizeof(header_type) + size + sizeof(size_type) and
            size * CHUNK_SPLIT_FACTOR <= effective_size - sizeof(size_type))
        {
            it = split_unused_chunk(it, size);
            effective_size = extract_size(header);
            M_insist(effective_size >= size + sizeof(size_type));
        }
#endif

        unlink_chunk(it);
        size_of_used_chunk(header, size) = header->value_;
        return header;
    }

    void deallocate(void *ptr, const size_type size) {
        if (ptr == nullptr) return;
        const size_type masked_size = size_of_used_chunk(ptr, size);
        reclaim_chunk(ptr, masked_size);
    }

    size_type num_chunks_available() const { return chunks_.size(); }

M_LCOV_EXCL_START
    friend std::ostream & operator<<(std::ostream &out, const list_allocator &A) {
        out << "allocation size = " << A.min_allocation_size_
            << ", alignment = " << A.min_allocation_alignment_
            << ", available chunks: ";
        for (auto it = A.chunks_.begin(); it != A.chunks_.end(); ++it) {
            if (it != A.chunks_.begin())
                out << " -> ";
            const header_type *header = header_of_unused_chunk(&*it);
            out << '[';
            if (is_marked_for_deallocation(header))
                out << '*';
            out << ((void*) header) << ", " << extract_size(header) << "B]";
        }
        return out;
    }

    void dump(std::ostream &out) const { out << *this << std::endl; }
    void dump() const { dump(std::cerr); }
M_LCOV_EXCL_STOP

    private:
    /*==================================================================================================================
     * Helper functions
     *================================================================================================================*/

    static bool is_unaligned(void *ptr, size_type alignment) {
        M_insist(alignment == 0 or is_pow_2(alignment), "alignment must be a power of 2");
        return alignment == 0 ? false : reinterpret_cast<std::uintptr_t>(ptr) & (alignment - size_type(1));
    }

    static bool is_aligned(void *ptr, size_type alignment) { return not is_unaligned(ptr, alignment); }


    /*==================================================================================================================
     * Chunk operations
     *================================================================================================================*/

    /** Returns a reference to the effective size field of a chunk in use. */
    static size_type & size_of_used_chunk(void *chunk, const size_type size) {
        return *reinterpret_cast<size_type*>(
            ceil_to_multiple_of_pow_2(reinterpret_cast<std::uintptr_t>(chunk) + size, alignof(size_type))
        );
    }
    static size_type size_of_used_chunk(const void *chunk, const size_type size) {
        return *reinterpret_cast<const size_type*>(
            ceil_to_multiple_of_pow_2(reinterpret_cast<std::uintptr_t>(chunk) + size, alignof(size_type))
        );
    }

    /** Returns a pointer to the header of an unused chunk. */
    static header_type * header_of_unused_chunk(size_type *value_addr) {
        return reinterpret_cast<header_type*>(
            reinterpret_cast<uint8_t*>(value_addr) + sizeof(*value_addr) - sizeof(header_type)
        );
    }
    static const header_type * header_of_unused_chunk(const size_type *value_addr) {
        return reinterpret_cast<const header_type*>(
            reinterpret_cast<const uint8_t*>(value_addr) + sizeof(*value_addr) - sizeof(header_type)
        );
    }

    static bool is_marked_for_deallocation(const header_type *header) { return header->value_ & DEALLOCATION_BIT_MASK; }
    static size_type extract_size(const header_type *header) { return header->value_ & ~DEALLOCATION_BIT_MASK; }
    static size_type mark_for_deallocation(size_type n) { return n | DEALLOCATION_BIT_MASK; }
    static size_type preserve_deallocation(size_type n, const header_type *header) {
        return n | (header->value_ & DEALLOCATION_BIT_MASK);
    }


    /*==================================================================================================================
     * Allocation helpers
     *================================================================================================================*/

    /** Removes the referenced chunk from the list of available chunks without deallocating its memory. */
    chunk_list_type::iterator unlink_chunk(chunk_list_type::iterator it) {
        header_type *header = header_of_unused_chunk(&*it);
        override_addr_ = header; // unlink *without* deallocate
        return chunks_.erase(it);
    }

    /** Adds the given `chunk` to the list of unused chunks by emplacing the list node within the chunk's memory.
     * The list node is inserted at position `pos` within the linked list. */
    chunk_list_type::iterator link_chunk(chunk_list_type::iterator pos, void *chunk, size_type masked_size) {
        override_addr_ = chunk; // emplace in chunk
        pos = chunks_.insert(pos, masked_size);
        M_insist(pos != chunks_.end());
        M_insist(*pos == masked_size);
        return pos;
    }

    chunk_list_type::iterator insert_chunk_sorted(void *chunk, size_type masked_size) {
        auto it = chunks_.begin();
        for (; it != chunks_.end() and &*it < chunk; ++it);
        return link_chunk(it, chunk, masked_size);
    }


    /* Reclaims a chunk by adding it to the list of unused chunks and attempting to coalesce it with its immediate
     * neighbors.  The chunk may have been used or be an entirely new allocation. */
    void reclaim_chunk(void *chunk, size_type masked_size) {
        auto pos = insert_chunk_sorted(chunk, masked_size);
        M_insist(pos != chunks_.end());
        M_insist(header_of_unused_chunk(&*pos)->value_ == masked_size);
        pos = coalesce(pos);
        M_insist(pos != chunks_.end());
        if (pos != chunks_.begin())
            coalesce(std::prev(pos));
    }

    /** Allocates fresh memory that contains enough space to fit a chunk of size `size`, that is aligned according to
     * `alignment`, and that also fits a header.  The chunk is added to the list of chunks.
     * @return an iterator to the chunk */
    chunk_list_type::iterator add_fresh_chunk(const size_type size, const size_type alignment) {
        M_insist(alignment == 0 or is_pow_2(alignment), "alignment must be a power of 2");

        /* Ensure the alignment satisfies the requirements of `header_type`.  */
        const size_type effective_alignment = std::max(alignment, min_allocation_alignment_);
        M_insist(effective_alignment % min_allocation_alignment_ == 0, "size does not guarantee alignment");

        /* Ceil the size to a multiple of `header_type`'s alignment.  This avoids wasting space. */
        const size_type effective_size = ceil_to_multiple_of_pow_2(
            std::max(size + sizeof(size_type), min_allocation_size_),
            min_allocation_alignment_
        );

        /* Allocate the chunk with space for the header. */
        M_insist(override_addr_ == nullptr);
        void *chunk = aligned_mmap(effective_size, effective_alignment);
        M_insist(is_aligned(chunk, effective_alignment), "allocated a misaligned chunk");
        M_insist(is_aligned(chunk, alignment), "allocated a misaligned chunk");

        /* Insert chunk sorted. */
        auto pos = insert_chunk_sorted(chunk, mark_for_deallocation(effective_size));

        /* Update `min_allocation_size_` according to `strategy_`. */
        switch (strategy_) {
            case AllocationStrategy::Exponential:
                min_allocation_size_ *= 2;
                break;

            case AllocationStrategy::Linear: /* nothing to be done */;
        }

        /* Return chunk. */
        return pos;
    }

    /** Splits the given `chunk` into two chunks.  The first chunk will be located at `chunk` and have a usable size not
     * less than `required_size`.  The second chunk, containing the remaining space of the original chunk, is added to
     * the list of chunks.  Alignment is assumed and not checked.
     * @return iterator to the first chunk
     */
    chunk_list_type::iterator split_unused_chunk(chunk_list_type::iterator it, size_type required_size) {
        header_type *header_first_chunk = header_of_unused_chunk(&*it);
        const size_type effective_size_original_chunk = extract_size(header_first_chunk);
        M_insist(effective_size_original_chunk >= 2 * sizeof(header_type),
               "cannot split chunk that does not fit two headers");

        /* Calculate the effective size of the first chunk. */
        const size_type effective_size_first_chunk = ceil_to_multiple_of_pow_2(
            std::max(required_size + sizeof(size_type), sizeof(header_type)),
            alignof(header_type)
        );

        /* Update the effective size of the first chunk. */
#ifndef NDEBUG
        const bool was_marked_for_deallocation = is_marked_for_deallocation(header_first_chunk);
#endif
        header_first_chunk->value_ = preserve_deallocation(effective_size_first_chunk, header_first_chunk);
#ifndef NDEBUG
        M_insist(was_marked_for_deallocation == is_marked_for_deallocation(header_first_chunk));
#endif

        /* Add the second chunk to the list of unused chunks. */
        header_type *second_chunk = reinterpret_cast<header_type*>(
            reinterpret_cast<std::uintptr_t>(header_first_chunk) + effective_size_first_chunk
        );
        const size_type effective_size_second_chunk = effective_size_original_chunk - effective_size_first_chunk;
        M_insist(effective_size_second_chunk % alignof(header_type) == 0, "effective size violates alignment");
        M_insist(effective_size_second_chunk >= sizeof(header_type), "effective size is too small");
        link_chunk(std::next(it), second_chunk, effective_size_second_chunk);
        M_insist(not is_marked_for_deallocation(second_chunk),
               "the second split chunk must never be marked for deallocation");

        return it;
    }

    /** Attempt to coalesce the chunk pointed to by `it` and its successor.
     * @return an iterator to the coalesced chunk, if coalescing succeeded, `it` otherwise */
    chunk_list_type::iterator coalesce(chunk_list_type::iterator it) {
        header_type *header_first_chunk = header_of_unused_chunk(&*it);
        auto successor_it = std::next(it);

        /* No next chunk to coalesce with. */
        if (successor_it == chunks_.end())
            return it;

#ifndef NDEBUG
        const bool is_first_chunk_marked_for_deallocation = is_marked_for_deallocation(header_first_chunk);
#endif

        header_type *header_second_chunk = header_of_unused_chunk(&*successor_it);

        /* If the second chunk is marked for deallocation, it cannot be coalesced with. */
        if (is_marked_for_deallocation(header_second_chunk))
            return it;

        const size_type size_first_chunk = extract_size(header_first_chunk);
        void * const addr_end_first_chunk = reinterpret_cast<uint8_t*>(header_first_chunk) + size_first_chunk;

        if (addr_end_first_chunk != header_second_chunk)
            return it;

        /* Coalesce the second chunk into the first chunk. */
        M_insist(not is_marked_for_deallocation(header_second_chunk));
        const size_type size_second_chunk = extract_size(header_second_chunk);
        auto next = unlink_chunk(successor_it);
        header_first_chunk->value_ += size_second_chunk;

#ifndef NDEBUG
        M_insist(is_marked_for_deallocation(header_first_chunk) == is_first_chunk_marked_for_deallocation,
               "coalescing must not alter the deallocation mark");
#endif

        return std::prev(next);
    }

    void * proxy_allocate(size_type, size_type) {
        M_insist(override_addr_, "override address must be set before calling this function");
        void *ptr = override_addr_;
        override_addr_ = nullptr;
        return ptr;
    }

    void proxy_deallocate(void *ptr, size_type) {
        M_insist(ptr == override_addr_, "override address must be set before calling this function");
        override_addr_ = nullptr;
        /* nothing to be done */
    }

    void * aligned_mmap(size_type size, size_type alignment) {
        M_insist(alignment % get_pagesize() == 0, "alignment must be page aligned");
        M_insist(size % get_pagesize() == 0, "size must be a whole multiple of a page");

        if (alignment != get_pagesize()) { // over-aligned
            errno = 0;
            void *ptr = mmap(nullptr, size + alignment, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS|MAP_NORESERVE, /* fd= */ -1, /* offset= */ 0);
            if (ptr == MAP_FAILED) {
                const auto errsv = errno;
                throw std::runtime_error(strerror(errsv));
            }

            const std::uintptr_t unaligned_ptr = reinterpret_cast<std::uintptr_t>(ptr);
            const std::uintptr_t aligned_ptr = ceil_to_multiple_of_pow_2(unaligned_ptr, alignment);
            M_insist(aligned_ptr >= unaligned_ptr);
            M_insist(aligned_ptr < unaligned_ptr + alignment);
            void * const end_of_aligned_allocation = reinterpret_cast<void*>(aligned_ptr + size);

            munmap(ptr, aligned_ptr - unaligned_ptr); // unmap preceding memory
            munmap(end_of_aligned_allocation, alignment - (aligned_ptr - unaligned_ptr)); // unmap succeeding memory

            return reinterpret_cast<void*>(aligned_ptr);
        } else {
            errno = 0;
            void *ptr = mmap(nullptr, size, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS|MAP_NORESERVE, /* fd= */ -1, /* offset= */ 0);
            if (ptr == MAP_FAILED) {
                const auto errsv = errno;
                throw std::runtime_error(strerror(errsv));
            }
            M_insist(is_aligned(ptr, alignment), "misaligned allocation");
            return ptr;
        }

    }
};

inline void * list_allocator_proxy::allocate(size_type size, size_type alignment)
{
    return alloc_->proxy_allocate(size, alignment);
}

inline void list_allocator_proxy::deallocate(void *ptr, size_type size)
{
    return alloc_->proxy_deallocate(ptr, size);
}

}

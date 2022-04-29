#pragma once

#include <mutable/mutable-config.hpp>
#include <mutable/util/fn.hpp>
#include <cstdint>
#include <iostream>
#include <memory>


namespace m {

namespace memory {

struct Memory;

/** This is the common interface for all memory allocators that support *rewiring*.  */
struct M_EXPORT Allocator
{
    friend struct Memory;

    private:
    int fd_; ///< file descriptor of the underlying memory file

    public:
    Allocator();
    virtual ~Allocator();

    /** Return the file descriptor of the underlying memory file. */
    int fd() const { return fd_; }

    /** Creates a new memory object with `size` bytes of freshly allocated memory. */
    virtual Memory allocate(std::size_t size) = 0;

    protected:
    /** Deallocates a memory object. */
    virtual void deallocate(Memory &&mem) = 0;

    /** Helper method to inherit the friend ability to construct a `Memory` object. */
    Memory create_memory(void *addr, std::size_t size, std::size_t offset);
};

/** This class represents a reserved address space in virtual memory.  It can be used to map the contents of
 * `memory::Memory` instances into one contiguous virtual address range. */
struct M_EXPORT AddressSpace
{
    friend void swap(AddressSpace &first, AddressSpace &second) {
        using std::swap;
        swap(first.addr_, second.addr_);
        swap(first.size_, second.size_);
    }

    private:
    void *addr_; ///< pointer to the beginning of the virtual address space
    std::size_t size_; ///< size in bytes of the address space

    private:
    AddressSpace() : addr_(nullptr), size_(0) { }
    public:
    AddressSpace(std::size_t size);
    ~AddressSpace();
    AddressSpace(const AddressSpace&) = delete;
    AddressSpace(AddressSpace &&other) : AddressSpace() { swap(*this, other); }

    /** Returns a pointer to the beginning of the virtual address space. */
    void * addr() const { return addr_; }
    /** Returns the size in bytes of the virtual address space. */
    std::size_t size() const { return size_; }

    /** Get a pointer to the beginning of the virtual address space, converted to type `T`. */
    template<typename T>
    T as() const { return reinterpret_cast<T>(addr()); }
};

/** Represents a mapping created by a `memory::Allocator`.  The class holds all information on the mapping, i.e. a
 * reference to the allocator that created the mapping, a pointer to the beginning of the virtual address range that was
 * mapped to, the offset within the allocator, and the size of the allocation.  */
struct M_EXPORT Memory
{
    friend struct Allocator;

    private:
    Allocator *allocator_ = nullptr; ///< the allocator that created this memory allocation
    void *addr_ = nullptr; ///< pointer to the virtual address space where this allocation is mapped to
    std::size_t size_ = 0; ///< the size of this allocation
    std::size_t offset_ = 0; ///< the offset of this allocation within its allocator

    Memory(Allocator &allocator, void *addr, std::size_t size, std::size_t offset);

    public:
    friend void swap(Memory &first, Memory &second) {
        using std::swap;
        swap(first.allocator_, second.allocator_);
        swap(first.addr_,      second.addr_);
        swap(first.size_,      second.size_);
        swap(first.offset_,    second.offset_);
    }

    Memory() { }
    Memory(void *addr, std::size_t size) : addr_(addr), size_(size) { }
    ~Memory() { if (allocator_) allocator().deallocate(std::move(*this)); }
    Memory(const Memory&) = delete;
    Memory(Memory &&other) { swap(*this, other); }

    Memory & operator=(Memory &&other) { swap(*this, other); return *this; }

    /** Returns a reference to the allocator that created this allocation. */
    Allocator & allocator() const { M_insist(allocator_); return *allocator_; }
    /** Returns a pointer to the beginning of the virtual address space where this allocation is mapped to. */
    void * addr() const { return addr_; }
    /** Returns the size in bytes of this allocation. */
    std::size_t size() const { return size_; }
    /** Returns the offset in bytes of this allocation within its allocator. */
    std::size_t offset() const { return offset_; }

    /** Returns a pointer to the beginning of the virtual address space where this allocation is mapped to, converted to
     * type `T`. */
    template<typename T>
    T as() { return reinterpret_cast<T>(addr()); }
    /** Returns a pointer to the beginning of the virtual address space where this allocation is mapped to, converted to
     * type `T`. */
    template<typename T>
    const T as() const { return reinterpret_cast<T>(addr()); }

    /** Map `size` bytes starting at `offset_src` into the address space of `vm` at offset `offset_dst`.  */
    void map(std::size_t size, std::size_t offset_src, const AddressSpace &vm, std::size_t offset_dst) const;

    void dump(std::ostream &out) const;
    void dump() const;
};

/** This is the simplest kind of allocator. The idea is to keep a pointer at the first memory address of your memory
 * chunk and move it every time an allocation is done. In this allocator, the internal fragmentation is kept to a
 * minimum because all elements are sequentially inserted and the only fragmentation between them is the alignment.
 * Note that allocations are always page aligned and whole multiples of an entire page.  There is no overhead to
 * allocation and existing allocations are never modified.  Deallocation can only reclaim memory if all chronologically
 * later allocations have been deallocated before.  If possible, deallocate memory in the inverse order of allocation.
 */
struct M_EXPORT LinearAllocator : Allocator
{
    private:
    std::size_t offset_ = 0; ///< the offset from the start of the memory file of the next allocation

    ///> stack of allocations; allocations can be marked deallocated for later reclaiming
    std::vector<std::size_t> allocations_;

    public:
    LinearAllocator() { }
    ~LinearAllocator() { }

    Memory allocate(std::size_t size) override;

    /** Returns the offset in the underlying memory file where the next allocation is placed. */
    std::size_t offset() const { return offset_; }

    private:
    void deallocate(Memory &&mem) override;
};

}

}

#pragma once

#include "mutable/util/fn.hpp"
#include <cstdint>
#include <iostream>
#include <memory>


namespace rewire {

struct Memory;

/** This is the common interface for all memory allocators that support *rewiring*.  */
struct Allocator
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
    virtual void deallocate(Memory &mem) = 0;

    /** Helper method to inherit the friend ability to construct a `Memory` object. */
    Memory create_memory(void *addr, std::size_t size, std::size_t offset);
};

/** This class represents a reserved address space in virtual memory.  It can be used to map the contents of
 * `rewire::Memory` instances into one contiguous virtual address range. */
struct AddressSpace
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
    AddressSpace(AddressSpace &&other) { swap(*this, other); }

    /** Returns a pointer to the beginning of the virtual address space. */
    void * addr() const { return addr_; }
    /** Returns the size in bytes of the virtual address space. */
    std::size_t size() const { return size_; }

    /** Get a pointer to the beginning of the virtual address space, converted to type `T`. */
    template<typename T>
    T as() const { return reinterpret_cast<T>(addr()); }
};

/** Represents a mapping created by a `rewire::Allocator`.  THe class holds all information on the mapping, i.e. a
 * reference to the allocator that created the mapping, a pointer to the beginning of the virtual address range that was
 * mapped to, the offset within the allocator, and the size of the allocation.  */
struct Memory
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
    ~Memory() { if (allocator_) allocator().deallocate(*this); }
    Memory(const Memory&) = delete;
    Memory(Memory &&other) { swap(*this, other); }

    Memory & operator=(Memory &&other) { swap(*this, other); return *this; }

    /** Returns a reference to the allocator that created this allocation. */
    Allocator & allocator() const { return * allocator_; }
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
 * There is no overhead to allocation and existing allocations are never modified.  Deallocation does nothing and
 * reclaiming allocated memory is *not possible*.  */
struct LinearAllocator : Allocator
{
    private:
    std::size_t capacity_ = 0; ///< the currently allocated capacity of the memory file
    std::size_t offset_ = 0; ///< the offset from the start of the memory file of the next allocation

    public:
    LinearAllocator() { }
    ~LinearAllocator() { }

    Memory allocate(std::size_t size) override;

    private:
    void deallocate(Memory &mem) override;
};


}

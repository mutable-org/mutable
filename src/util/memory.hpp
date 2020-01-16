#pragma once

#include "util/fn.hpp"
#include <cstdint>
#include <iostream>
#include <memory>


namespace rewire {

struct Memory;

struct Allocator
{
    friend struct Memory;

    private:
    int fd_;

    public:
    Allocator();
    virtual ~Allocator();

    int fd() const { return fd_; }

    /** Create a new memory object with `size` bytes of fresh memory. */
    virtual Memory allocate(std::size_t size) = 0;

    protected:
    /** Deallocate a memory object. */
    virtual void deallocate(Memory &mem) = 0;

    /** Helper method to inherit the friend ability to construct a `Memory` object. */
    Memory create_memory(void *addr, std::size_t size, std::size_t offset);
};

struct AddressSpace
{
    friend void swap(AddressSpace &first, AddressSpace &second) {
        using std::swap;
        swap(first.addr_, second.addr_);
        swap(first.size_, second.size_);
    }

    private:
    void *addr_;
    std::size_t size_;

    private:
    AddressSpace() : addr_(nullptr), size_(0) { }
    public:
    AddressSpace(std::size_t size);
    ~AddressSpace();
    AddressSpace(const AddressSpace&) = delete;
    AddressSpace(AddressSpace &&other) { swap(*this, other); }

    void * addr() const { return addr_; }
    std::size_t size() const { return size_; }

    template<typename T>
    T as() const { return reinterpret_cast<T>(addr()); }
};

struct Memory
{
    friend struct Allocator;

    private:
    Allocator *allocator_ = nullptr;
    void *addr_ = nullptr;
    std::size_t size_ = 0;
    std::size_t offset_ = 0;

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

    Allocator & allocator() const { return * allocator_; }
    void * addr() const { return addr_; }
    std::size_t size() const { return size_; }
    std::size_t offset() const { return offset_; }

    template<typename T>
    T as() { return reinterpret_cast<T>(addr()); }
    template<typename T>
    const T as() const { return reinterpret_cast<T>(addr()); }

    /** Map `size` bytes starting at `offset_src` into the address space of `vm` at offset `offset_dst`.  */
    void map(std::size_t size, std::size_t offset_src, const AddressSpace &vm, std::size_t offset_dst);

    void dump(std::ostream &out) const;
    void dump() const;
};

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

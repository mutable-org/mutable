#include "util/memory.hpp"

#include "util/macro.hpp"
#include <cerrno>
#include <cstring>
#include <exception>
#include <stdexcept>

#if __linux
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#endif


#define IS_PAGE_ALIGNED(N) ( (N) == ( (N) & ~(PAGESIZE - 1UL) ) )
#define CEIL_TO_NEXT_PAGE(N) ( (N - 1UL) | (PAGESIZE - 1UL) ) + 1UL


using namespace rewire;


/*======================================================================================================================
 * Allocator
 *====================================================================================================================*/

Allocator::Allocator()
{
    errno = 0;
    fd_ = memfd_create("rewire_allocator", MFD_CLOEXEC);
    if (fd_ == -1)
        throw std::runtime_error(strerror(errno));
}

Allocator::~Allocator()
{
    close(fd_);
}

Memory Allocator::create_memory(void *addr, std::size_t size, std::size_t offset)
{
    return Memory(*this, addr, size, offset);
}


/*======================================================================================================================
 * AddressSpace
 *====================================================================================================================*/

AddressSpace::AddressSpace(std::size_t size)
{
    auto aligned_size = CEIL_TO_NEXT_PAGE(size);
    addr_ = mmap(nullptr, aligned_size, PROT_NONE, MAP_PRIVATE|MAP_ANONYMOUS, /* fd= */ -1, /* offset= */ 0);
    if (addr_ == MAP_FAILED)
        throw std::runtime_error(strerror(errno));
    size_ = aligned_size;
}

AddressSpace::~AddressSpace() { munmap(addr_, size_); }


/*======================================================================================================================
 * Memory
 *====================================================================================================================*/

Memory::Memory(Allocator &allocator, void *addr, std::size_t size, std::size_t offset)
    : allocator_(&allocator)
    , addr_(addr)
    , size_(size)
    , offset_(offset)
{ }

void Memory::map(std::size_t size, std::size_t offset_src, const AddressSpace &vm, std::size_t offset_dst)
{
    insist(size <= this->size(), "size exceeds memory size");
    insist(offset_src < this->size(), "source offset out of bounds");
    insist(IS_PAGE_ALIGNED(offset_src), "source offset is not page aligned");
    insist(offset_src + size <= this->size(), "source range out of bounds");

    insist(size <= vm.size(), "size exceeds address space");
    insist(offset_dst < vm.size(), "destination offset out of bounds");
    insist(IS_PAGE_ALIGNED(offset_dst), "destination offset is not page aligned");
    insist(offset_dst + size <= vm.size(), "destination range out of bounds");

    void *dst_addr = vm.as<uint8_t*>() + offset_dst;
    void *addr = mmap(dst_addr, size, PROT_READ|PROT_WRITE, MAP_SHARED|MAP_FIXED, allocator().fd(),
                      this->offset() + offset_src);
    if (addr == MAP_FAILED)
        throw std::runtime_error(strerror(errno));
    if (addr != dst_addr)
        throw std::runtime_error("MAP_FIXED failed");
}

void Memory::dump(std::ostream &out) const
{
    out << "Memory at virtual address " << addr() << " of size " << size() << " bytes mapped to offset " << offset()
        << " of file descriptor " << allocator().fd() << std::endl;
}
void Memory::dump() const { dump(std::cerr); }


/*======================================================================================================================
 * LinearAllocator
 *====================================================================================================================*/

Memory LinearAllocator::allocate(std::size_t size)
{
    std::size_t aligned_size = CEIL_TO_NEXT_PAGE(size);
    insist(aligned_size >= size, "size must be ceiled");
    insist((aligned_size & (PAGESIZE - 1UL)) == 0, "not page aligned");
    std::size_t min_cap = offset_ + aligned_size;
    if (min_cap > capacity_) {
        errno = 0;
        if (ftruncate(fd(), min_cap))
            throw std::runtime_error(strerror(errno));
    }

    errno = 0;
    void *addr = mmap(nullptr, aligned_size, PROT_READ|PROT_WRITE, MAP_SHARED, fd(), offset_);
    if (addr == MAP_FAILED)
        throw std::runtime_error(strerror(errno));

    auto mem = create_memory(addr, aligned_size, offset_);
    offset_ += aligned_size;
    return mem;
}

void LinearAllocator::deallocate(Memory &mem) { munmap(mem.addr(), mem.size()); }

#include <mutable/util/memory.hpp>

#include <mutable/util/macro.hpp>
#include <cerrno>
#include <climits>
#include <cstring>
#include <exception>
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


using namespace m;
using namespace m::memory;


/*======================================================================================================================
 * Allocator
 *====================================================================================================================*/

Allocator::Allocator()
{
#if __linux
    fd_ = memfd_create("rewire_allocator", MFD_CLOEXEC);
#elif __APPLE__
    auto name = std::to_string(getpid());
    fd_ = shm_open(name.c_str(), O_RDWR | O_TRUNC | O_CREAT, S_IRUSR | S_IWUSR);
    shm_unlink(name.c_str());
    ftruncate(fd_, 1UL << 44); // preallocate memory because resizing with `ftruncate()` is not supported on macOS
#endif
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
    if (size != 0) {
        auto aligned_size = Ceil_To_Next_Page(size);
        addr_ = mmap(nullptr, aligned_size, PROT_NONE, MAP_PRIVATE|MAP_ANONYMOUS, /* fd= */ -1, /* offset= */ 0);
        if (addr_ == MAP_FAILED)
            throw std::runtime_error(strerror(errno));
        size_ = aligned_size;
    }
}

AddressSpace::~AddressSpace() { if (addr_) munmap(addr_, size_); }


/*======================================================================================================================
 * Memory
 *====================================================================================================================*/

Memory::Memory(Allocator &allocator, void *addr, std::size_t size, std::size_t offset)
    : allocator_(&allocator)
    , addr_(addr)
    , size_(size)
    , offset_(offset)
{ }

void Memory::map(std::size_t size, std::size_t offset_src, const AddressSpace &vm, std::size_t offset_dst) const
{
    M_insist(size <= this->size(), "size exceeds memory size");
    M_insist(offset_src < this->size(), "source offset out of bounds");
    M_insist(Is_Page_Aligned(offset_src), "source offset is not page aligned");
    M_insist(offset_src + size <= this->size(), "source range out of bounds");

    M_insist(size <= vm.size(), "size exceeds address space");
    M_insist(offset_dst < vm.size(), "destination offset out of bounds");
    M_insist(Is_Page_Aligned(offset_dst), "destination offset is not page aligned");
    M_insist(offset_dst + size <= vm.size(), "destination range out of bounds");

    void *dst_addr = vm.as<uint8_t*>() + offset_dst;
    void *addr = mmap(dst_addr, size, PROT_READ|PROT_WRITE, MAP_SHARED|MAP_FIXED, allocator().fd(),
                      this->offset() + offset_src);
    if (addr == MAP_FAILED)
        throw std::runtime_error(strerror(errno));
    if (addr != dst_addr)
        throw std::runtime_error("MAP_FIXED failed");
}

M_LCOV_EXCL_START
void Memory::dump(std::ostream &out) const
{
    out << "Memory at virtual address " << addr() << " of size " << size() << " bytes mapped to offset " << offset()
        << " of file descriptor " << allocator().fd() << std::endl;
}
void Memory::dump() const { dump(std::cerr); }
M_LCOV_EXCL_STOP


/*======================================================================================================================
 * LinearAllocator
 *====================================================================================================================*/

Memory LinearAllocator::allocate(std::size_t size)
{
    if (size == 0) return Memory();

    const std::size_t aligned_size = Ceil_To_Next_Page(size);
    M_insist(aligned_size >= size, "size must be ceiled");
    M_insist(Is_Page_Aligned(aligned_size), "not page aligned");
#if __linux
    if (ftruncate(fd(), offset_ + aligned_size))
        throw std::runtime_error(strerror(errno));
#elif __APPLE__
    /* Nothing to be done.
     * Memory has been preallocated because resizing with `ftruncate()` is not supported on macOS.  */
#endif

    void *addr = mmap(nullptr, aligned_size, PROT_READ|PROT_WRITE, MAP_SHARED, fd(), offset_);
    if (addr == MAP_FAILED)
        throw std::runtime_error(strerror(errno));

    auto mem = create_memory(addr, aligned_size, offset_);
    allocations_.push_back(offset_);
    offset_ += aligned_size;
    return mem;
}

namespace {

/** Set MSB of a std::size_t. */
constexpr std::size_t MSB = std::size_t(1UL) << (sizeof(MSB) * CHAR_BIT - 1U);

/** Mark offset for deallocation by setting MSB.  This is safe as offsets with set MSB would exceed maximum file
 * size. */
std::size_t mark_for_deallocation(std::size_t offset) { return offset | MSB; }

/** Check whether MSB is set, i.e. offset marked for deallocation. */
bool is_marked_for_deallocation(uintptr_t offset) { return offset & MSB; }

/** Clear MSB. */
std::size_t unmarked(std::size_t offset) { return offset & ~MSB; }

}

void LinearAllocator::deallocate(Memory &&mem)
{
    if (&mem.allocator() != this)
        throw std::invalid_argument("memory has not been allocated by this allocator");

    /* Find the allocation. */
    auto it = std::find(allocations_.rbegin(), allocations_.rend(), mem.offset());
    if (it == allocations_.rend())
        throw std::invalid_argument("memory has not been allocated by this allocator or has already been deallocated");

    /* Unmap the mapped memory. */
    munmap(mem.addr(), mem.size());

#if __linux
    *it = mark_for_deallocation(mem.offset());

    /* Reclaim memory if this is the last allocation. */
    if (it == allocations_.rbegin()) {
        /* This is the last allocation.  Check how much memory we can reclaim. */
        std::size_t new_size_of_file;
        do {
            new_size_of_file = unmarked(*it);
            ++it;
        } while (it != allocations_.rend() and is_marked_for_deallocation(*it));
        M_insist(it == allocations_.rend() or not is_marked_for_deallocation(*it));

        /* Truncate file to reclaim memory. */
        if (ftruncate(fd(), new_size_of_file))
            throw std::runtime_error(strerror(errno));
        offset_ = new_size_of_file;

        /* Remove reclaimed allocations. */
        allocations_.resize(std::distance(it, allocations_.rend()));
    }
#elif __APPLE__
    /* Nothing to be done.
     * Memory has been preallocated because resizing with `ftruncate()` is not supported on macOS.  */
#endif
}

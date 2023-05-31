#include "catch2/catch.hpp"

#include <mutable/util/list_allocator.hpp>
#include <mutable/util/malloc_allocator.hpp>
#include <cstring>


using namespace m;

namespace {

struct S0 { int i; char c; };
struct S1 { double d; int i; };
struct alignas(8) S2 { char c; short s; };

template<typename Allocator>
void check_typed_allocation(Allocator &A)
{
#define CHECK_ALLOC(TYPE) { \
    auto p = A.template allocate<TYPE>(); \
    std::uintptr_t u = reinterpret_cast<std::uintptr_t>(p); \
    CHECK(u % sizeof(TYPE) == 0); \
    A.deallocate(p); \
}
    CHECK_ALLOC(int);
    CHECK_ALLOC(S0);
    CHECK_ALLOC(S1);
    CHECK_ALLOC(S2);
#undef CHECK_ALLOC
}

template<typename Allocator>
void check_array_allocation(Allocator &A)
{
#define CHECK_ALLOC(TYPE, COUNT) { \
    auto p = A.template allocate<TYPE>(COUNT); \
    std::uintptr_t u = reinterpret_cast<std::uintptr_t>(p); \
    CHECK(u % alignof(TYPE) == 0); \
    A.deallocate(p, COUNT); \
}
    CHECK_ALLOC(int, 42);
    CHECK_ALLOC(S0,  13);
    CHECK_ALLOC(S1,  73);
    CHECK_ALLOC(S2,   5);
#undef CHECK_ALLOC
}

}


TEST_CASE("malloc_allocator", "[core][util][allocator]")
{
    malloc_allocator A;

    SECTION("unaligned bytes")
    {
        char *p0 = (char*) A.allocate(3);
        strncpy(p0, "Test", 3);
        A.deallocate(p0, 3);

        char *p1 = (char*) A.allocate(5);
        char *p2 = (char*) A.allocate(2);

        strncpy(p1, "Hello", 5);
        A.deallocate(p1, 5);

        strncpy(p2, "World", 2);
        A.deallocate(p2, 2);
    }

    SECTION("aligned bytes")
    {
        char *p0 = (char*) A.allocate(1024, 64);
        std::uintptr_t u0 = reinterpret_cast<std::uintptr_t>(p0);
        CHECK(u0 % 64 == 0);
        strncpy(p0, "Test", 3);
        A.deallocate(p0, 3);

        char *p1 = (char*) A.allocate(1024, 128);
        std::uintptr_t u1 = reinterpret_cast<std::uintptr_t>(p1);
        CHECK(u1 % 128 == 0);

        char *p2 = (char*) A.allocate(256, 256);
        std::uintptr_t u2 = reinterpret_cast<std::uintptr_t>(p2);
        CHECK(u2 % 256 == 0);

        strncpy(p1, "Hello", 5);
        A.deallocate(p1, 5);

        strncpy(p2, "World", 2);
        A.deallocate(p2, 2);
    }

    SECTION("typed") { check_typed_allocation(A); }

    SECTION("array") { check_array_allocation(A); }
}

TEST_CASE("list_allocator", "[core][util][allocator]")
{
    list_allocator A;

#define CHECK_ALIGNED(PTR, ALN) \
    CHECK(reinterpret_cast<std::uintptr_t>(PTR) % ALN == 0);

    SECTION("in order")
    {
        void *p0 = A.allocate(42);
        void *p1 = A.allocate(13);
        void *p2 = A.allocate(73);

        CHECK(p0 != p1);
        CHECK(p1 != p2);
        CHECK(p0 != p2);

        A.deallocate(p0, 42);
        A.deallocate(p1, 13);
        A.deallocate(p2, 73);
    }

    SECTION("interleaved")
    {
        /* +-+ */
        void *p0 = A.allocate(42);
        A.deallocate(p0, 42);
        /* +-> 42 <-+ */
        void *p1 = A.allocate(13);
        CHECK(p0 == p1); // reuse memory of p0
        /* +-+ */
        void *p2 = A.allocate(73);
        CHECK(p1 != p2);
        /* +-+ */
        A.deallocate(p2, 73);
        /* +-> 73 <-+ */
        void *p3 = A.allocate(60);
        CHECK(p3 == p2); // reuse memory of p2
        /* +-+ */
        A.deallocate(p3, 60);
        /* +-> 73 <-+ */
        A.deallocate(p1, 13);
        /* +-> 13 <-> 73 <-+ */
    }

    SECTION("aligned")
    {
        void *pa_16 = A.allocate(42, 16);
        CHECK_ALIGNED(pa_16, 16);
        A.deallocate(pa_16, 42);

        void *pb_8 = A.allocate(13, 8);
        CHECK_ALIGNED(pb_8, 8);
        CHECK(pb_8 == pa_16);
        A.deallocate(pb_8, 13);

        void *pc_1024 = A.allocate(256, 1024);
        CHECK_ALIGNED(pc_1024, 1024);
        A.deallocate(pc_1024, 256);

        void *pd_4096 = A.allocate(512, 4096);
        CHECK_ALIGNED(pd_4096, 4096);
        A.deallocate(pd_4096, 512);

        void *pe_64k = A.allocate(128, 64 * 1024);
        CHECK_ALIGNED(pe_64k, 64 * 1024);
        A.deallocate(pe_64k, 128);
    }

    SECTION("typed") { check_typed_allocation(A); }

    SECTION("array") { check_array_allocation(A); }

    SECTION("swap")
    {
        list_allocator B;
        CHECK(A.num_chunks_available() == 0);
        CHECK(B.num_chunks_available() == 0);

        void *p0 = A.allocate(8);
        void *p1 = A.allocate(8);
        A.deallocate(p1, 8);
        CHECK(A.num_chunks_available() == 1);
        CHECK(B.num_chunks_available() == 0);

        swap(A, B);
        CHECK(A.num_chunks_available() == 0);
        CHECK(B.num_chunks_available() == 1);

        B.deallocate(p0, 8); // coalesce
        CHECK(A.num_chunks_available() == 0);
        CHECK(B.num_chunks_available() == 1);
    }

    SECTION("pre-allocation")
    {
        const std::size_t chunk_size = 1024;
        const std::size_t size = chunk_size - sizeof(list_allocator::size_type);
        list_allocator B(4 * chunk_size);

        void *pa = B.allocate(size); // 1024 in total
        void *pb = B.allocate(size); // 1024 in total
        CHECK(reinterpret_cast<uint8_t*>(pb) - reinterpret_cast<uint8_t*>(pa) == 1024);
        void *pc = B.allocate(size); // 1024 in total
        CHECK(reinterpret_cast<uint8_t*>(pc) - reinterpret_cast<uint8_t*>(pb) == 1024);
        void *pd = B.allocate(size); // 1024 in total
        CHECK(reinterpret_cast<uint8_t*>(pd) - reinterpret_cast<uint8_t*>(pc) == 1024);

        B.deallocate(pa, size);
        B.deallocate(pb, size);
        B.deallocate(pc, size);
        B.deallocate(pd, size);
    }

    SECTION("coalescing reverse order")
    {
        SECTION("within page")
        {
            list_allocator B(get_pagesize());
            CHECK(B.num_chunks_available() == 1); // pre-allocation

            auto *p0 = B.allocate(8);
            CHECK(B.num_chunks_available() == 1); // remainder of page
            auto *p1 = B.allocate(8);
            CHECK(B.num_chunks_available() == 1); // remainder of page
            auto *p2 = B.allocate(8);
            CHECK(B.num_chunks_available() == 1); // remainder of page

            B.deallocate(p2, 8); // should coalesce
            CHECK(B.num_chunks_available() == 1); // remainder of page
            B.deallocate(p1, 8); // should coalesce
            CHECK(B.num_chunks_available() == 1); // remainder of page

            auto *p3 = B.allocate(16);
            CHECK(B.num_chunks_available() == 1); // remainder of page
            CHECK(p3 == p1);

            B.deallocate(p3, 16); // should coalesce
            CHECK(B.num_chunks_available() == 1); // remainder of page
            B.deallocate(p0, 8); // should coalesce
            CHECK(B.num_chunks_available() == 1); // remainder of page

            auto *p4 = B.allocate(24);
            CHECK(B.num_chunks_available() == 1); // remainder of page
            CHECK(p4 == p0);

            B.deallocate(p4, 24); // should coalesce
            CHECK(B.num_chunks_available() == 1); // remainder of page
        }

        SECTION("across allocations")
        {
            const auto pagesize = get_pagesize();
            list_allocator B(pagesize);
            CHECK(B.num_chunks_available() == 1); // pre-allocation

            auto *p0 = B.allocate(2 * pagesize); // allocates new chunk
            CHECK(B.num_chunks_available() == 1); // original page
            auto *p1 = B.allocate(2 * pagesize); // allocates new chunk
            CHECK(B.num_chunks_available() == 1); // original page

            B.deallocate(p1, 2 * pagesize); // should not coalesce because of individual `mmap`s
            CHECK(B.num_chunks_available() == 2);
            B.deallocate(p0, 2 * pagesize); // should not coalesce because of individual `mmap`s
            CHECK(B.num_chunks_available() == 3);

            auto *p2 = B.allocate(4 * pagesize); // allocates new chunk
            CHECK(p2 != p0);

            B.deallocate(p2, 4 * pagesize); // should not coalesce because of individual `mmap`s
            CHECK(B.num_chunks_available() == 4);
        }

        SECTION("across pages, within allocations")
        {
            const auto pagesize = get_pagesize();
            list_allocator B(16 * pagesize);
            CHECK(B.num_chunks_available() == 1); // pre-allocation

            auto *p0 = B.allocate(2 * pagesize);
            CHECK(B.num_chunks_available() == 1); // remainder of 64 KiB
            auto *p1 = B.allocate(2 * pagesize);
            CHECK(B.num_chunks_available() == 1); // remainder of 64 KiB
            auto *p2 = B.allocate(2 * pagesize);
            CHECK(B.num_chunks_available() == 1); // remainder of 64 KiB

            B.deallocate(p2, 2 * pagesize); // should coalesce
            CHECK(B.num_chunks_available() == 1);
            B.deallocate(p1, 2 * pagesize); // should coalesce
            CHECK(B.num_chunks_available() == 1);

            auto *p3 = B.allocate(4 * pagesize);
            CHECK(B.num_chunks_available() == 1); // remainder of 64 KiB
            CHECK(p3 == p1);

            B.deallocate(p3, 4 * pagesize); // should coalesce
            CHECK(B.num_chunks_available() == 1);
            B.deallocate(p0, 2 * pagesize); // should coalesce
            CHECK(B.num_chunks_available() == 1);

            auto *p4 = B.allocate(6 * pagesize);
            CHECK(B.num_chunks_available() == 1); // remainder of 64 KiB
            CHECK(p4 == p0);

            B.deallocate(p4, 6 * pagesize);
            CHECK(B.num_chunks_available() == 1);
        }
    }

    SECTION("coalescing three chunks")
    {
        CHECK(A.num_chunks_available() == 0); // remainder of page
        void *p0 = A.allocate(8);
        void *p1 = A.allocate(8);
        void *p2 = A.allocate(8);
        CHECK(A.num_chunks_available() == 1); // remainder of page

        CHECK(p0 != p1);
        CHECK(p1 != p2);
        CHECK(p0 != p2);

        A.deallocate(p0, 8); // no coalescing yet
        CHECK(A.num_chunks_available() == 2);
        A.deallocate(p2, 8); // coalesce with remainder of page
        CHECK(A.num_chunks_available() == 2);

        void *p3 = A.allocate(16);
        CHECK(A.num_chunks_available() == 2); // p0 and remainder of page
        CHECK(p3 != p0);
        CHECK(p3 != p1);
        A.deallocate(p3, 16); // coalesce with remainder of page
        CHECK(A.num_chunks_available() == 2); // p0 and remainder of page

        A.deallocate(p1, 8); // coalesce with p0 *and* remainder of page
        CHECK(A.num_chunks_available() == 1); // p0 and remainder of page
    }
}

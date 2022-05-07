#include "catch2/catch.hpp"

#include <mutable/util/memory.hpp>
#include <memory>


using namespace m;
using namespace m::memory;


TEST_CASE("memory::AddressSpace", "[core][util][memory]")
{
    AddressSpace vm(10000);
    REQUIRE(vm.addr());
    REQUIRE(vm.size() >= 10000);
}

TEST_CASE("memory::Memory/c'tor", "[core][util][memory]")
{
    Memory mem;
    REQUIRE(mem.addr() == nullptr);
    REQUIRE(mem.size() == 0);
    REQUIRE(mem.offset() == 0);
}

TEST_CASE("memory::LinearAllocator", "[core][util][memory]")
{
    const std::size_t PAGE_SIZE = get_pagesize();
    const std::size_t INTS_PER_PAGE = PAGE_SIZE / sizeof(unsigned);

    LinearAllocator A;

    SECTION("c'tor/d'tor")
    {
        CHECK(A.offset() == 0);
    }

    SECTION("allocate/deallocate")
    {
        /* Allocate page of ints. */
        auto mem0 = A.allocate(PAGE_SIZE);
        CHECK(A.offset() == PAGE_SIZE);

        /* Write page of ints. */
        auto pi = mem0.as<unsigned*>();
        for (std::size_t i = 0; i != INTS_PER_PAGE; ++i)
            pi[i] = i;

        /* Allocate a single double. */
        auto mem1 = A.allocate(sizeof(double));
        CHECK(A.offset() == 2 * PAGE_SIZE);

        /* Write double. */
        auto pd = mem1.as<double*>();
        *pd = 3.14159;

        /* Check values have been written correctly. */
        for (std::size_t i = 0; i != 1024; ++i)
            REQUIRE(pi[i] == i);
        REQUIRE(*pd == 3.14159);
    }

#if __linux
    SECTION("reclaiming deallocations")
    {
        auto mem0 = std::make_unique<Memory>(A.allocate(PAGE_SIZE));
        CHECK(A.offset() == PAGE_SIZE);

        auto mem1 = std::make_unique<Memory>(A.allocate(PAGE_SIZE));
        CHECK(A.offset() == 2 * PAGE_SIZE);

        mem0.reset(); // deallocate first allocation
        CHECK(A.offset() == 2 * PAGE_SIZE);

        mem1.reset(); // deallocate last allocation -> reclaim memory
        CHECK(A.offset() == 0);

        auto mem2 = std::make_unique<Memory>(A.allocate(PAGE_SIZE));
        CHECK(A.offset() == PAGE_SIZE);

        auto mem3 = std::make_unique<Memory>(A.allocate(2 * PAGE_SIZE));
        CHECK(A.offset() == 3 * PAGE_SIZE);

        mem3.reset(); // deallocate last allocation -> reclaim memory
        CHECK(A.offset() == PAGE_SIZE);

        mem2.reset(); // deallocate last allocation -> reclaim memory
        CHECK(A.offset() == 0);
    }
#elif __APPLE__
#endif

    SECTION("map to address space")
    {
        auto mem = A.allocate(2 * PAGE_SIZE); // 2 pages
        auto p_mem = mem.as<unsigned*>();
        for (std::size_t i = 0; i != 2 * INTS_PER_PAGE; ++i)
            p_mem[i] = i;

        {
            AddressSpace vm(4 * PAGE_SIZE); // 4 pages
            mem.map(PAGE_SIZE, PAGE_SIZE, vm, 0); // map mem page 1 to vm page 0
            {
                auto p_vm = reinterpret_cast<unsigned*>(vm.addr());
                for (std::size_t i = 0; i != INTS_PER_PAGE; ++i)
                    REQUIRE(p_vm[i] == INTS_PER_PAGE + i);
            }
            mem.map(PAGE_SIZE, 0, vm, PAGE_SIZE); // map mem page 0 to vm page 1
            {
                auto p_vm = reinterpret_cast<unsigned*>(vm.addr());
                for (std::size_t i = 0; i != INTS_PER_PAGE; ++i)
                    REQUIRE(p_vm[i] == INTS_PER_PAGE + i);
                for (std::size_t i = 0; i != INTS_PER_PAGE; ++i)
                    REQUIRE(p_vm[INTS_PER_PAGE + i] == i);
            }
        }
    }
}

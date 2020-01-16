#include "catch.hpp"

#include "util/memory.hpp"


using namespace rewire;


TEST_CASE("rewire::AddressSpace", "[unit][util]")
{
    AddressSpace vm(10000);
    REQUIRE(vm.addr());
    REQUIRE(vm.size() >= 10000);
}

TEST_CASE("rewire::Memory/c'tor", "[unit][util]")
{
    Memory mem;
    REQUIRE(mem.addr() == nullptr);
    REQUIRE(mem.size() == 0);
    REQUIRE(mem.offset() == 0);
}

TEST_CASE("rewire::LinearAllocator", "[unit][util]")
{
    constexpr std::size_t INTS_PER_PAGE = PAGESIZE / sizeof(int);

    LinearAllocator A;

    SECTION("c'tor/d'tor") { }

    SECTION("allocate/deallocate")
    {
        auto mem0 = A.allocate(1024 * sizeof(int));
        auto pi = mem0.as<int*>();
        for (int i = 0; i != 1024; ++i)
            pi[i] = i;

        auto mem1 = A.allocate(sizeof(double));
        auto pd = mem1.as<double*>();
        *pd = 3.14159;

        for (int i = 0; i != 1024; ++i)
            REQUIRE(pi[i] == i);

        REQUIRE(*pd == 3.14159);
    }

    SECTION("map to address space")
    {
        auto mem = A.allocate(2 * INTS_PER_PAGE * sizeof(int)); // 2 pages
        auto p_mem = mem.as<int*>();
        for (int i = 0; i != 2 * INTS_PER_PAGE; ++i)
            p_mem[i] = i;

        {
            AddressSpace vm(4 * PAGESIZE); // 4 pages
            mem.map(PAGESIZE, PAGESIZE, vm, 0); // map mem page 1 to vm page 0
            {
                auto p_vm = reinterpret_cast<int*>(vm.addr());
                for (int i = 0; i != INTS_PER_PAGE; ++i)
                    REQUIRE(p_vm[i] == INTS_PER_PAGE + i);
            }
            mem.map(PAGESIZE, 0, vm, PAGESIZE); // map mem page 0 to vm page 1
            {
                auto p_vm = reinterpret_cast<int*>(vm.addr());
                for (int i = 0; i != INTS_PER_PAGE; ++i)
                    REQUIRE(p_vm[i] == INTS_PER_PAGE + i);
                for (int i = 0; i != INTS_PER_PAGE; ++i)
                    REQUIRE(p_vm[INTS_PER_PAGE + i] == i);
            }
        }
    }
}

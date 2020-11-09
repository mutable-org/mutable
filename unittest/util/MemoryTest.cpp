#include "catch.hpp"

#include "mutable/util/memory.hpp"


using namespace rewire;


TEST_CASE("rewire::AddressSpace", "[core][util][memory]")
{
    AddressSpace vm(10000);
    REQUIRE(vm.addr());
    REQUIRE(vm.size() >= 10000);
}

TEST_CASE("rewire::Memory/c'tor", "[core][util][memory]")
{
    Memory mem;
    REQUIRE(mem.addr() == nullptr);
    REQUIRE(mem.size() == 0);
    REQUIRE(mem.offset() == 0);
}

TEST_CASE("rewire::LinearAllocator", "[core][util][memory]")
{
    std::size_t INTS_PER_PAGE = get_pagesize() / sizeof(int);

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
            AddressSpace vm(4 * get_pagesize()); // 4 pages
            mem.map(get_pagesize(), get_pagesize(), vm, 0); // map mem page 1 to vm page 0
            {
                auto p_vm = reinterpret_cast<int*>(vm.addr());
                for (int i = 0; i != INTS_PER_PAGE; ++i)
                    REQUIRE(p_vm[i] == INTS_PER_PAGE + i);
            }
            mem.map(get_pagesize(), 0, vm, get_pagesize()); // map mem page 0 to vm page 1
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

#include <chrono>
#include <cstdint>
#include <iostream>
#include <mutable/util/list_allocator.hpp>
#include <mutable/util/malloc_allocator.hpp>
#include <type_traits>
#include <vector>


using namespace std::chrono;

#ifndef NDEBUG
static constexpr std::size_t NUM_ALLOCATIONS_START = 1UL<<10;
static constexpr std::size_t NUM_ALLOCATIONS_STOP  = 1UL<<13;
#else
static constexpr std::size_t NUM_ALLOCATIONS_START = 1UL<<10;
static constexpr std::size_t NUM_ALLOCATIONS_STOP  = 1UL<<16;
#endif


constexpr unsigned long long operator ""_Ki(unsigned long long n) { return n * 1024; }
constexpr unsigned long long operator ""_Mi(unsigned long long n) { return n * 1024 * 1024; }
constexpr unsigned long long operator ""_Gi(unsigned long long n) { return n * 1024 * 1024 * 1024; }

template<typename Allocator>
void run_benchmark_allocations_fixed_allocate(const std::string &name, const Allocator &proto,
                                     const float fraction_deallocate, const std::size_t size)
{
    for (std::size_t num_allocations = NUM_ALLOCATIONS_START;
         num_allocations <= NUM_ALLOCATIONS_STOP;
         num_allocations *= 2)
    {
        Allocator A(proto); // copy
        std::vector<void*> allocations(num_allocations);
        float p_dealloc = 0.f;
        std::size_t idx_dealloc = 0;

        auto begin = steady_clock::now();
        for (auto &allocation : allocations) {
            allocation = A.allocate(size);
            uint8_t *ptr = reinterpret_cast<uint8_t*>(allocation);
            *ptr = size; // enforce page fault

            p_dealloc += fraction_deallocate;
            if (p_dealloc >= 1.f) {
                p_dealloc -= 1.f;
                A.deallocate(allocations[idx_dealloc++], size);
            }
        }
        auto end = steady_clock::now();

        std::cout << "fixed_allocate," << name << ',' << size << ',' << fraction_deallocate << ','
                  << num_allocations << ',' << duration_cast<microseconds>(end - begin).count() / 1e3 << std::endl;
        for (; idx_dealloc != num_allocations; ++idx_dealloc)
            A.deallocate(allocations[idx_dealloc], size);
    }
}

template<typename Allocator>
void run_benchmark_allocations_fixed_allocate_then_deallocate(const std::string &name, const Allocator &proto,
                                                              const std::size_t size)
{
    for (std::size_t num_allocations = NUM_ALLOCATIONS_START;
         num_allocations <= NUM_ALLOCATIONS_STOP;
         num_allocations *= 2)
    {
        Allocator A(proto); // copy
        std::vector<void*> allocations(num_allocations);

        auto begin = steady_clock::now();
        for (auto &allocation : allocations) {
            allocation = A.allocate(size);
            uint8_t *ptr = reinterpret_cast<uint8_t*>(allocation);
            *ptr = size; // enforce page fault
        }
        for (auto allocation : allocations) {
            A.deallocate(allocation, size);
        }
        auto end = steady_clock::now();

        std::cout << "fixed_allocate_then_deallocate," << name << ',' << size << ',' << 1.f << ','
                  << num_allocations << ',' << duration_cast<microseconds>(end - begin).count() / 1e3 << std::endl;
    }
}

template<typename Allocator>
void run_benchmark_allocations_fixed_allocate_then_deallocate_reversed(const std::string &name, const Allocator &proto,
                                                                       const std::size_t size)
{
    for (std::size_t num_allocations = NUM_ALLOCATIONS_START;
         num_allocations <= NUM_ALLOCATIONS_STOP >> 2U;
         num_allocations *= 2)
    {
        Allocator A(proto); // copy
        std::vector<void*> allocations(num_allocations);

        auto begin = steady_clock::now();
        for (auto &allocation : allocations) {
            allocation = A.allocate(size);
            uint8_t *ptr = reinterpret_cast<uint8_t*>(allocation);
            *ptr = size; // enforce page fault
        }
        for (auto it = allocations.crbegin(); it != allocations.crend(); ++it) {
            A.deallocate(*it, size);
        }
        auto end = steady_clock::now();

        std::cout << "fixed_allocate_then_deallocate_reversed," << name << ',' << size << ',' << 1.f << ','
                  << num_allocations << ',' << duration_cast<microseconds>(end - begin).count() / 1e3 << std::endl;
    }
}

template<typename Allocator>
void run_benchmark_suite_for_allocator(const std::string &name, const Allocator &proto)
{
    for (float p : { 0.f, .5f, 1.f}) {
        run_benchmark_allocations_fixed_allocate(name, proto, p, 8);
        run_benchmark_allocations_fixed_allocate(name, proto, p, 4_Ki);
        run_benchmark_allocations_fixed_allocate(name, proto, p, 64_Ki);
#ifdef NDEBUG
        run_benchmark_allocations_fixed_allocate(name, proto, p, 8_Mi);
        run_benchmark_allocations_fixed_allocate(name, proto, p, 128_Mi);
#endif
    }

    run_benchmark_allocations_fixed_allocate_then_deallocate(name, proto, 8);
    run_benchmark_allocations_fixed_allocate_then_deallocate(name, proto, 4_Ki);
    run_benchmark_allocations_fixed_allocate_then_deallocate(name, proto, 64_Ki);
#ifdef NDEBUG
    run_benchmark_allocations_fixed_allocate_then_deallocate(name, proto, 8_Mi);
    run_benchmark_allocations_fixed_allocate_then_deallocate(name, proto, 128_Mi);
#endif

    run_benchmark_allocations_fixed_allocate_then_deallocate_reversed(name, proto, 8);
    run_benchmark_allocations_fixed_allocate_then_deallocate_reversed(name, proto, 4_Ki);
    run_benchmark_allocations_fixed_allocate_then_deallocate_reversed(name, proto, 64_Ki);
#ifdef NDEBUG
    run_benchmark_allocations_fixed_allocate_then_deallocate_reversed(name, proto, 8_Mi);
    run_benchmark_allocations_fixed_allocate_then_deallocate_reversed(name, proto, 128_Mi);
#endif
}


int main(void)
{
    std::cout << "type,allocator,size,p_dealloc,count,time" << std::endl;
    run_benchmark_suite_for_allocator("malloc", m::malloc_allocator{});
    run_benchmark_suite_for_allocator("list<Linear-4K>", m::list_allocator{4_Ki});
    run_benchmark_suite_for_allocator("list<Linear-64K>", m::list_allocator(64_Ki));
    run_benchmark_suite_for_allocator("list<Linear-4M>", m::list_allocator(4_Mi));
    run_benchmark_suite_for_allocator("list<Exponential-4K>", m::list_allocator{4_Ki, m::AllocationStrategy::Exponential});
    run_benchmark_suite_for_allocator("list<Exponential-64K>", m::list_allocator(64_Ki, m::AllocationStrategy::Exponential));
    run_benchmark_suite_for_allocator("list<Exponential-4M>", m::list_allocator(4_Mi, m::AllocationStrategy::Exponential));
}

#include "backend/WasmAlgo.hpp"

#include "backend/WasmMacro.hpp"

using namespace m;
using namespace m::wasm;


/*======================================================================================================================
 * sorting
 *====================================================================================================================*/

template<bool IsGlobal>
void m::wasm::quicksort(const Buffer<IsGlobal> &buffer, const std::vector<std::pair<const m::Expr*, bool>> &order)
{
    static_assert(IsGlobal, "quicksort on local buffers is not yet supported");

    /*----- Create load and swap proxies for buffer. -----*/
    auto load = buffer.create_load_proxy();
    auto swap = buffer.create_swap_proxy();

    /*---- Create branchless binary partition function. -----*/
    /* Receives the ID of the first tuple to partition, the past-the-end ID to partition, and the ID of the pivot
     * element as parameters. Returns ID of partition boundary s.t. all elements before this boundary are smaller
     * than or equal to the pivot element and all elements after or equal this boundary are greater than the pivot
     * element. */
    FUNCTION(partition, uint32_t(uint32_t, uint32_t, uint32_t))
    {
        auto S = CodeGenContext::Get().scoped_environment(); // create scoped environment

        auto begin = PARAMETER(0); // first ID to partition
        auto end = PARAMETER(1); // past-the-end ID to partition
        const auto &pivot = PARAMETER(2); // pivot element
        Wasm_insist(begin == pivot + 1U);
        Wasm_insist(begin < end);

        U32 last = end - 1U;

        DO_WHILE(begin < end) {
            /*----- Swap begin and last tuples. -----*/
            swap(begin, last.clone());

            /*----- Compare begin and last tuples to pivot element and advance cursors respectively. -----*/
            Bool begin_lt_pivot = compare(load, begin, pivot, order) < 0;
            Bool last_ge_pivot  = compare(load, last, pivot, order) >= 0;

            begin += begin_lt_pivot.to<uint32_t>();
            end -= last_ge_pivot.to<uint32_t>();
        }

        Wasm_insist(begin > pivot, "partition boundary must be located within the partitioned area");
        RETURN(begin);
    }

    /*---- Create quicksort function. -----*/
    /* Receives the ID of the first tuple to sort and the past-the-end ID to sort. */
    FUNCTION(quicksort, void(uint32_t, uint32_t))
    {
        auto S = CodeGenContext::Get().scoped_environment(); // create scoped environment

        const auto &begin = PARAMETER(0); // first ID to sort
        auto end = PARAMETER(1); // past-the-end ID to sort
        Wasm_insist(begin <= end);

        U32 last = end - 1U;

        WHILE(end - begin >= 2U) {
            Var<U32> mid((begin + end) >> 1U); // (begin + end) / 2

            /*----- Swap pivot (median of three) to begin. ----.*/
            Bool begin_le_mid  = compare(load, begin, mid, order) <= 0;
            Bool begin_le_last = compare(load, begin, last.clone(), order) <= 0;
            Bool mid_le_last   = compare(load, mid, last.clone(), order) <= 0;
            IF (begin_le_mid) {
                IF (begin_le_last.clone()) {
                    IF (mid_le_last.clone()) {
                        swap(begin, mid); // [begin, mid, last]
                    } ELSE {
                        swap(begin, last.clone()); // [begin, last, mid]
                    };
                }; // else [last, begin, mid]
            } ELSE {
                IF (mid_le_last) {
                    IF (not begin_le_last) {
                        swap(begin, last); // [mid, last, begin]
                    }; // else [mid, begin, last]
                } ELSE {
                    swap(begin, mid); // [last, mid, begin]
                };
            };

            /*----- Partition range [begin + 1, end[ using begin as pivot. -----*/
            mid = partition(begin + 1U, end, begin) - 1U;
            swap(begin, mid); // patch mid

            /*----- Recurse right partition, if necessary. -----*/
            IF (end - mid > 2U) {
                quicksort(mid + 1U, end);
            };

            /*----- Update end pointer. -----*/
            end = mid;
        }
    }
    quicksort(0, buffer.size());
}

// explicit instantiations to prevent linker errors
template void m::wasm::quicksort(const GlobalBuffer&, const std::vector<std::pair<const m::Expr*, bool>>&);

#pragma once

#include "backend/WasmUtil.hpp"
#include "parse/AST.hpp"
#include <binaryen-c.h>
#include <vector>


namespace db {

/*======================================================================================================================
 * WasmPartition
 *====================================================================================================================*/

struct WasmPartition
{
    using order_type = std::pair<const Expr*, bool>;

    virtual ~WasmPartition() { }

    /** Emits code to perform a binary partitioning of an array of tuples of type `schema`.
     *
     * \param module    the WebAssembly module
     * \param fn        the current function
     * \param schema    the `Schema` of the tuples
     * \param order     the ordering used for comparison
     * \param b_begin   the address of the first tuple
     * \param b_end     the address one after the last tuple
     * \param b_pivot   the address of the pivot element
     */
    virtual BinaryenExpressionRef emit(BinaryenModuleRef module, FunctionBuilder &fn, BlockBuilder &block,
                                       const Schema &schema, const std::vector<order_type> &order,
                                       BinaryenExpressionRef b_begin, BinaryenExpressionRef b_end,
                                       BinaryenExpressionRef b_pivot) const = 0;
};

/** Emits a function to perform partitioning of an array of comparable elements using conditional branches.
 *
 *      template<typename T>
 *      T * partition_branching(const T pivot, T *begin, T *end)
 *      {
 *          using std::swap;
 *          while (begin < end) {
 *              if (*begin < pivot) ++begin;
 *              else if (end[-1] >= pivot) --end;
 *              else swap(*begin, end[-1]);
 *          }
 *          return begin;
 *      }
 */
struct WasmPartitionBranching : WasmPartition
{
    BinaryenExpressionRef emit(BinaryenModuleRef module, FunctionBuilder &fn, BlockBuilder &block,
                               const Schema &schema, const std::vector<order_type> &order,
                               BinaryenExpressionRef b_begin, BinaryenExpressionRef b_end,
                               BinaryenExpressionRef b_pivot) const override;
};

/** Emits a function to perform partitioning of an array of comparable elements without conditional branches.
 *
 *      template<typename T>
 *      T * partition_branchless(const T pivot, T *begin, T *end)
 *      {
 *          while (begin < end) {
 *              const T left = *begin;
 *              const T right = end[-1];
 *              *begin = right;
 *              end[-1] = left;
 *              const ptrdiff_t adv_lo = right < pivot;
 *              const ptrdiff_t adv_hi = left >= pivot;
 *              begin += adv_lo;
 *              end   -= adv_hi;
 *          }
 *          return begin;
 *      }
 */
struct WasmPartitionBranchless : WasmPartition
{
    BinaryenExpressionRef emit(BinaryenModuleRef module, FunctionBuilder &fn, BlockBuilder &block,
                               const Schema &schema, const std::vector<order_type> &order,
                               BinaryenExpressionRef b_begin, BinaryenExpressionRef b_end,
                               BinaryenExpressionRef b_pivot) const override;
};


/*======================================================================================================================
 * WasmQuickSort
 *====================================================================================================================*/

struct WasmQuickSort
{
    using order_type = std::pair<const Expr*, bool>;

    const Schema &schema; ///< the schema of tuples to sort
    const std::vector<order_type> &order; ///< the attributes to sort by
    const WasmPartition &partitioning; ///< the partitioning function

    WasmQuickSort(const Schema &schema, const std::vector<order_type> &order, const WasmPartition &partitioning);

    /** Emits a function to sort a sequence of tuples using the Quicksort algorithm.
     *
     * @param module    the WebAssembly module
     * @param b_begin   the expression evaluating to the beginning of the sequence
     * @param b_end     the expression evaluating to the end of the sequence
     *
     *
     *      template<typename Partitioning, typename It>
     *      void qsort_singlerec(It begin, It end, Partitioning p)
     *      {
     *          using std::swap;
     *          using std::min;
     *          using std::max;
     *          assert(begin < end);
     *
     *          while (end - begin > 2) {
     *              // Compute median of three.
     *              auto pm = begin + (end - begin) / 2;
     *              bool left_le_mid   = *begin <= *pm;
     *              bool left_le_right = *begin <= *std::prev(end);
     *              bool mid_le_right  = *pm <= *std::prev(end);
     *              if (left_le_mid) {
     *                  if (left_le_right) {
     *                      if (mid_le_right)
     *                          swap(*begin, *pm);
     *                      else
     *                          swap(*begin, *std::prev(end));
     *                  } else {
     *                      // nothing to be done
     *                  }
     *              } else {
     *                  if (mid_le_right) {
     *                      if (left_le_right) {
     *                          // nothing to be done
     *                      } else {
     *                          swap(*begin, *std::prev(end));
     *                      }
     *                  } else {
     *                      swap(*begin, *pm);
     *                  }
     *              }
     *              It mid = p(*begin, begin + 1, end);
     *              assert(*(mid - 1) <= *begin);
     *              assert(*mid >= *begin);
     *              assert(verify_partition(begin + 1, mid, end));

     *              if (unlikely(mid != begin + 1)) {
     *                  swap(*begin, *(mid - 1));
     *                  --mid;
     *              }
     *              assert(verify_partition(begin, mid, end));

     *              if (end - mid >= 2) qsort_singlerec(mid, end, p); // recurse to the right
     *              end = mid;
     *          }
     *
     *          if (end - begin == 2) {
     *              if (*begin > *std::next(begin))
     *                  swap(*begin, *std::next(begin));
     *              return;
     *          }
     *      };
     */
    BinaryenFunctionRef emit(BinaryenModuleRef module) const;
};

}

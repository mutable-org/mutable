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
                                       const WasmStruct &struc, const std::vector<order_type> &order,
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
                               const WasmStruct &struc, const std::vector<order_type> &order,
                               BinaryenExpressionRef b_begin, BinaryenExpressionRef b_end,
                               BinaryenExpressionRef b_pivot) const override;
};

/** Emits a function to perform partitioning of an array of comparable elements without conditional branches.  This is
 * an implemenation in WebAssembly of our `partition_predicated_naive` algorithm in 'util/algorithms.hpp'.
 */
struct WasmPartitionBranchless : WasmPartition
{
    BinaryenExpressionRef emit(BinaryenModuleRef module, FunctionBuilder &fn, BlockBuilder &block,
                               const WasmStruct &struc, const std::vector<order_type> &order,
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

    /** Emits a function to sort a sequence of tuples using the Quicksort algorithm.  This is an implementation in
     * WebAssembly of our `qsort` algorithm in 'util/algorithms.hpp'.
     *
     * @param module    the WebAssembly module
     * @param b_begin   the expression evaluating to the beginning of the sequence
     * @param b_end     the expression evaluating to the end of the sequence
     */
    BinaryenFunctionRef emit(BinaryenModuleRef module) const;
};


/*======================================================================================================================
 * WasmBitMix
 *====================================================================================================================*/

struct WasmBitMix
{
    virtual ~WasmBitMix() { }

    virtual BinaryenExpressionRef emit(BinaryenModuleRef module, FunctionBuilder &fn, BlockBuilder &block,
                                       BinaryenExpressionRef bits) const = 0;
};

struct WasmBitMixMurmur3 : WasmBitMix
{
    BinaryenExpressionRef emit(BinaryenModuleRef module, FunctionBuilder &fn, BlockBuilder &block,
                               BinaryenExpressionRef bits) const override;
};


/*======================================================================================================================
 * WasmHash
 *====================================================================================================================*/

struct WasmHash
{
    virtual ~WasmHash() { }

    virtual BinaryenExpressionRef emit(BinaryenModuleRef module, FunctionBuilder &fn, BlockBuilder &block,
                                       const std::vector<BinaryenExpressionRef> &values) const = 0;
};

struct WasmHashMumur3_64A : WasmHash
{
    BinaryenExpressionRef emit(BinaryenModuleRef module, FunctionBuilder &fn, BlockBuilder &block,
                               const std::vector<BinaryenExpressionRef> &values) const override;
};

}

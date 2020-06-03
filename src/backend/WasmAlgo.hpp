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


/*======================================================================================================================
 * WasmHashTable
 *====================================================================================================================*/

struct WasmHashTable
{
    BinaryenModuleRef module;
    FunctionBuilder &fn;
    const WasmStruct &struc; ///< the structure of elements in the hash table

    public:
    WasmHashTable(BinaryenModuleRef module, FunctionBuilder &fn, const WasmStruct &struc)
        : module(module)
        , fn(fn)
        , struc(struc)
    { }

    virtual ~WasmHashTable() { }

    /** Create a fresh hash table at the address `begin` with `num_buckets` number of buckets.
     *
     * @param block         the block to emit code into
     * @param b_addr        the address where to allocate the hash table
     * @param num_buckets   the number of initial buckets to allocate
     * @return              the address immediately after the hash table
     * */
    virtual BinaryenExpressionRef create_table(BlockBuilder &block,
                                               BinaryenExpressionRef b_addr, std::size_t num_buckets) const = 0;

    /** Overwrite each slot in the hash table with `values`. */
    virtual void clear_table(BlockBuilder &block, BinaryenExpressionRef b_begin, BinaryenExpressionRef b_end) const = 0;

    /** Given the `hash` of an element, returns the location of its preferred bucket in the hash table. */
    virtual BinaryenExpressionRef hash_to_bucket(BinaryenExpressionRef b_hash) const = 0;

    /** Given a bucket address, locate a key inside the bucket.  Returns the address of the slot where the key is found
     * together with the number of probing steps performed.  If the key is not found, the address of the first slot that
     * is unoccupied is returned instead. */
    virtual std::pair<BinaryenExpressionRef, BinaryenExpressionRef>
    find_in_bucket(BlockBuilder &block, BinaryenExpressionRef b_bucket_addr,
                   const std::vector<BinaryenExpressionRef> &key) const = 0;

    /** Evaluates to `1` iff the slot is empty (i.e. not occupied). */
    virtual BinaryenExpressionRef is_slot_empty(BinaryenExpressionRef b_slot_addr) const = 0;

    /** Inserts a new entry into the bucket at `b_bucket_addr` by updating the bucket's probe length to `b_steps`,
     * marking the slot at `b_slot_addr` occupied, and placing the key in this slot. */
    virtual void emplace(BlockBuilder &block,
                         BinaryenExpressionRef b_bucket_addr, BinaryenExpressionRef b_steps,
                         BinaryenExpressionRef b_slot_addr, const std::vector<BinaryenExpressionRef> &key) const = 0;

    /** Creates a `WasmCGContext` to load values from the slot at `b_slot_addr`. */
    virtual WasmCGContext load_from_slot(BinaryenExpressionRef b_slot_addr) const = 0;

    /** Creates a `BinaryenExpressionRef` to store the value `b_value` as position `id` in the slot at `b_slot_addr`. */
    virtual BinaryenExpressionRef store_value_to_slot(BinaryenExpressionRef b_slot_addr, Schema::Identifier id,
                                              BinaryenExpressionRef b_value) const = 0;

    /** Given the address of a slot `b_slot_addr`, compute the address of the next slot.  That is, the address of the
     * slot immediately after `b_slot_addr`. */
    virtual BinaryenExpressionRef compute_next_slot(BinaryenExpressionRef b_slot_addr) const = 0;
};

struct WasmRefCountingHashTable : WasmHashTable
{
    static constexpr std::size_t REFERENCE_SIZE = 4; ///< 4 bytes for reference counting

    private:
    BinaryenExpressionRef b_addr_; ///< the address of the hash table
    BinaryenExpressionRef b_mask_; ///< the mask used to compute a slot address in the table, i.e. capacity * entry_size
    std::size_t entry_size_; ///< the size in bytes of a table entry, that is the key-value pair and meta data

    public:
    WasmRefCountingHashTable(BinaryenModuleRef module, FunctionBuilder &fn, const WasmStruct &struc)
        : WasmHashTable(module, fn, struc)
        , entry_size_(round_up_to_multiple<std::size_t>(REFERENCE_SIZE + struc.size(), 4))
    {
        b_addr_ = fn.add_local(BinaryenTypeInt32());
        b_mask_ = fn.add_local(BinaryenTypeInt32());
    }

    /** Create a WasmHashTable instance from an existing hash table. */
    WasmRefCountingHashTable(BinaryenModuleRef module, FunctionBuilder &fn, const WasmStruct &struc,
                             BinaryenExpressionRef b_addr, BinaryenExpressionRef b_mask)
        : WasmHashTable(module, fn, struc)
        , b_addr_(b_addr)
        , b_mask_(b_mask)
        , entry_size_(round_up_to_multiple<std::size_t>(REFERENCE_SIZE + struc.size(), 4))
    { }

    BinaryenExpressionRef create_table(BlockBuilder &block,
                                       BinaryenExpressionRef b_addr, std::size_t num_buckets) const override;

    void clear_table(BlockBuilder &block, BinaryenExpressionRef b_begin, BinaryenExpressionRef b_end) const override;

    BinaryenExpressionRef hash_to_bucket(BinaryenExpressionRef b_hash) const override;

    std::pair<BinaryenExpressionRef, BinaryenExpressionRef>
    find_in_bucket(BlockBuilder &block, BinaryenExpressionRef b_bucket_addr,
                   const std::vector<BinaryenExpressionRef> &key) const override;

    BinaryenExpressionRef is_slot_empty(BinaryenExpressionRef b_slot_addr) const override;

    void emplace(BlockBuilder &block,
                 BinaryenExpressionRef b_bucket_addr, BinaryenExpressionRef b_steps,
                 BinaryenExpressionRef b_slot_addr, const std::vector<BinaryenExpressionRef> &key) const override;

    WasmCGContext load_from_slot(BinaryenExpressionRef b_slot_addr) const override;

    BinaryenExpressionRef store_value_to_slot(BinaryenExpressionRef b_slot_addr, Schema::Identifier id,
                                              BinaryenExpressionRef b_value) const override;

    BinaryenExpressionRef compute_next_slot(BinaryenExpressionRef b_slot_addr) const override;

    BinaryenExpressionRef addr() const { return b_addr_; }
    BinaryenExpressionRef mask() const { return b_mask_; }

    std::size_t entry_size() const { return entry_size_; }
};

}

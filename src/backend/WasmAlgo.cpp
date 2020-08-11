#include "backend/WasmAlgo.hpp"

#include <sstream>
#include <string>
#include <unordered_map>


using namespace db;


/*======================================================================================================================
 * WasmPartitionBranching
 *====================================================================================================================*/

BinaryenExpressionRef WasmPartitionBranching::emit(BinaryenModuleRef module, FunctionBuilder &fn, BlockBuilder &block,
                                                   const WasmStruct &struc, const std::vector<order_type> &order,
                                                   BinaryenExpressionRef b_begin, BinaryenExpressionRef b_end,
                                                   BinaryenExpressionRef b_pivot) const
{
    (void) module;
    (void) fn;
    (void) block;
    (void) struc;
    (void) order;
    (void) b_begin;
    (void) b_end;
    (void) b_pivot;
    /*
(func $_Z19partition_branchingIiEPT_S0_S1_S1_ (; 0 ;) (param $0 i32) (param $1 i32) (param $2 i32) (result i32)
 (local $3 i32)
 (local $4 i32)
 (local $5 i32)
 (block $label$0
  (br_if $label$0
   (i32.ge_u
    (get_local $1)
    (get_local $2)
   )
  )
  (loop $label$1
   (block $label$2
    (block $label$3
     (br_if $label$3
      (i32.lt_s
       (tee_local $5
        (i32.load
         (get_local $1)
        )
       )
       (get_local $0)
      )
     )
     (set_local $3
      (i32.add
       (get_local $2)
       (i32.const -4)
      )
     )
     (loop $label$4
      (br_if $label$2
       (i32.ge_s
        (tee_local $4
         (i32.load
          (get_local $3)
         )
        )
        (get_local $0)
       )
      )
      (i32.store
       (get_local $1)
       (get_local $4)
      )
      (i32.store
       (get_local $3)
       (get_local $5)
      )
      (br_if $label$4
       (i32.ge_s
        (tee_local $5
         (i32.load
          (get_local $1)
         )
        )
        (get_local $0)
       )
      )
     )
    )
    (br_if $label$1
     (i32.lt_u
      (tee_local $1
       (i32.add
        (get_local $1)
        (i32.const 4)
       )
      )
      (get_local $2)
     )
    )
    (br $label$0)
   )
   (set_local $2
    (get_local $3)
   )
   (br_if $label$1
    (i32.lt_u
     (get_local $1)
     (get_local $3)
    )
   )
  )
 )
 (get_local $1)
)
     */
    unreachable("not implemented");
}


/*======================================================================================================================
 * WasmPartitionBranchless
 *====================================================================================================================*/

BinaryenExpressionRef WasmPartitionBranchless::emit(BinaryenModuleRef module, FunctionBuilder &fn, BlockBuilder &block,
                                                    const WasmStruct &struc, const std::vector<order_type> &order,
                                                    BinaryenExpressionRef b_begin, BinaryenExpressionRef b_end,
                                                    BinaryenExpressionRef b_pivot) const
{
    WasmSwap wasm_swap(module, fn);
    WasmCompare comparator(module, struc, order);

    /*----- Initialize left and right cursor. ------------------------------------------------------------------------*/
    WasmVariable left(fn, BinaryenTypeInt32());
    left.set(block, b_begin);

    WasmVariable right(fn, BinaryenTypeInt32());
    right.set(block, b_end);

    /*----- Create loop. ---------------------------------------------------------------------------------------------*/
    WasmDoWhile loop(module, "partition_branchless.loop", BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenLtUInt32(),
        /* left=   */ left,
        /* right=  */ right
    ));

    /*----- Offset right by one.  (This can be dropped when negative offsets are supported.) -------------------------*/
    auto b_last = BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ right,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(-struc.size()))
    );

    /*----- Create load context for left, right, and pivot. ----------------------------------------------------------*/
    auto load_context_left  = struc.create_load_context(left);
    auto load_context_right = struc.create_load_context(b_last);
    auto load_context_pivot = struc.create_load_context(b_pivot);

    /*----- Load values from pivot. ----------------------------------------------------------------------------------*/
    WasmCGContext value_context_pivot(module);
    for (auto &attr : struc.schema) {
        WasmVariable tmp(fn, get_binaryen_type(attr.type));
        tmp.set(block, load_context_pivot.get_value(attr.id));
        value_context_pivot.add(attr.id, tmp);
    }

    /*----- Swap left and right tuple. -------------------------------------------------------------------------------*/
    wasm_swap.emit(loop, struc, left, b_last);

    /*----- Compare left and right tuples to pivot. ------------------------------------------------------------------*/
    auto b_cmp_left  = comparator.emit(fn, loop, load_context_left,  value_context_pivot);
    auto b_left_is_ok = BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenLeSInt32(),
        /* left=   */ b_cmp_left,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(0))
    );
    auto b_cmp_right = comparator.emit(fn, loop, load_context_right, value_context_pivot);
    auto b_right_is_ok = BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenGeSInt32(),
        /* left=   */ b_cmp_right,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(0))
    );

    /* Advance left cursor. */
    {
        auto b_delta_begin = BinaryenSelect(
            /* module=    */ module,
            /* condition= */ b_left_is_ok,
            /* ifTrue=    */ BinaryenConst(module, BinaryenLiteralInt32(struc.size())),
            /* ifFalse=   */ BinaryenConst(module, BinaryenLiteralInt32(0)),
            /* type=      */ BinaryenTypeInt32()
        );
        left.set(loop, BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ left,
            /* right=  */ b_delta_begin
        ));
    }

    /* Advance right cursor. */
    {
        auto b_delta_end = BinaryenSelect(
            /* module=    */ module,
            /* condition= */ b_right_is_ok,
            /* ifTrue=    */ BinaryenConst(module, BinaryenLiteralInt32(-struc.size())),
            /* ifFalse=   */ BinaryenConst(module, BinaryenLiteralInt32(0)),
            /* type=      */ BinaryenTypeInt32()
        );
        right.set(loop, BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ right,
            /* right=  */ b_delta_end
        ));
    }

    block += loop.finalize();
    return left();
}


/*======================================================================================================================
 * WasmQuickSort
 *====================================================================================================================*/

WasmQuickSort::WasmQuickSort(const Schema &schema, const std::vector<order_type> &order,
                             const WasmPartition &partitioning)
    : schema(schema)
    , order(order)
    , partitioning(partitioning)
{ }

BinaryenFunctionRef WasmQuickSort::emit(BinaryenModuleRef module) const
{
    WasmStruct tuple(module, schema);
    WasmCompare comparator(module, tuple, order);

    std::ostringstream oss;
    oss << "qsort";
    for (auto &o : order)
        oss << '_' << *o.first << '_' << ( o.second ? "ASC" : "DESC" );
    const std::string fn_name = oss.str();

    std::vector<BinaryenType> param_types = { /* begin= */ BinaryenTypeInt32(), /* end= */ BinaryenTypeInt32() };
    FunctionBuilder fn(module, fn_name.c_str(), BinaryenTypeNone(), param_types);

    WasmSwap wasm_swap(module, fn);

    const auto b_begin = BinaryenLocalGet(
        /* module= */ module,
        /* index=  */ 0,
        /* type=   */ BinaryenTypeInt32()
    );
    const auto b_end = BinaryenLocalGet(
        /* module= */ module,
        /* index=  */ 1,
        /* type=   */ BinaryenTypeInt32()
    );
    const auto b_delta = BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenSubInt32(),
        /* left=   */ b_end,
        /* right=  */ b_begin
    );
    const auto b_last = BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ b_end,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(-tuple.size()))
    );

    WasmWhile loop(module, (fn_name + ".loop").c_str(), BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenGtSInt32(),
        /* left=   */ b_delta,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(2 * tuple.size()))
    ));

    /*----- Compute middle of data. ----------------------------------------------------------------------------------*/
    const auto b_size = BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenDivUInt32(),
        /* left=   */ b_delta,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(tuple.size()))
    );
    const auto b_half = BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenDivUInt32(),
        /* left=   */ b_size,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(2))
    );
    const auto b_offset_mid = BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenMulInt32(),
        /* left=   */ b_half,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(tuple.size()))
    );
    WasmVariable mid(fn, BinaryenTypeInt32());
    mid.set(loop, BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ b_begin,
        /* right=  */ b_offset_mid
    ));

    auto load_context_left  = tuple.create_load_context(b_begin);
    auto load_context_mid   = tuple.create_load_context(mid);
    auto load_context_right = tuple.create_load_context(b_last);

    /*----- Compare three elements pairwise. -------------------------------------------------------------------------*/
    auto b_cmp_left_mid   = comparator.emit(fn, loop, load_context_left, load_context_mid);
    auto b_cmp_left_right = comparator.emit(fn, loop, load_context_left, load_context_right);
    auto b_cmp_mid_right  = comparator.emit(fn, loop, load_context_mid,  load_context_right);

    auto b_left_le_mid = BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenLeSInt32(),
        /* left=   */ b_cmp_left_mid,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(0))
    );
    auto b_left_le_right = BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenLeSInt32(),
        /* left=   */ b_cmp_left_right,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(0))
    );
    auto b_mid_le_right = BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenLeSInt32(),
        /* left=   */ b_cmp_mid_right,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(0))
    );

    /*----- Swap pivot to front. -------------------------------------------------------------------------------------*/
    BlockBuilder block_swap_left_mid(module);
    wasm_swap.emit(block_swap_left_mid, tuple, b_begin, mid);
    BlockBuilder block_swap_left_right(module);
    wasm_swap.emit(block_swap_left_right, tuple, b_begin, b_last);
    block_swap_left_mid.name("if_0_true-swap_left_mid");
    block_swap_left_right.name("if_0_false-swap_left_right");
    auto b_if_0 = BinaryenIf(
        /* module=    */ module,
        /* condition= */ b_mid_le_right,
        /* ifTrue=    */ block_swap_left_mid.finalize(),
        /* ifFalse=   */ block_swap_left_right.finalize()
    );
    auto b_if_1 = BinaryenIf(
        /* module=    */ module,
        /* condition= */ b_left_le_right,
        /* ifTrue=    */ b_if_0,
        /* ifFalse=   */ BlockBuilder(module, "if_1_false-noop").finalize()
    );
    block_swap_left_right.name("if_2_false-swap_left_right");
    auto b_if_2 = BinaryenIf(
        /* module=    */ module,
        /* condition= */ b_left_le_right,
        /* ifTrue=    */ BlockBuilder(module, "if_2_true-noop").finalize(),
        /* ifFalse=   */ block_swap_left_right.finalize()
    );
    block_swap_left_mid.name("if_3_false-swap_left_mid");
    auto b_if_3 = BinaryenIf(
        /* module=    */ module,
        /* condition= */ b_mid_le_right,
        /* ifTrue=    */ b_if_2,
        /* ifFalse=   */ block_swap_left_mid.finalize()
    );
    auto b_if_4 = BinaryenIf(
        /* module=    */ module,
        /* condition= */ b_left_le_mid,
        /* ifTrue=    */ b_if_1,
        /* ifFalse=   */ b_if_3
    );
    loop += b_if_4;

    /*----- Partition range begin + 1 to end using begin as pivot. ---------------------------------------------------*/
    auto b_begin_plus_one = BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ b_begin,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(tuple.size()))
    );
    WasmPartitionBranchless partition;
    auto b_partition = partition.emit(
        /* module=      */ module,
        /* fn=          */ fn,
        /* block=       */ loop,
        /* struc=       */ tuple,
        /* order=       */ order,
        /* b_begin=     */ b_begin_plus_one,
        /* b_end=       */ b_end,
        /* b_pivot=     */ b_begin
    );
    mid.set(loop, b_partition);

    /*----- Patch mid pointer, if necessary. -------------------------------------------------------------------------*/
    auto b_mid_minus_one = BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ mid,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(-tuple.size()))
    );
    wasm_swap.emit(loop, tuple, b_begin, b_mid_minus_one);

    /*----- Recurse right, if necessary. -----------------------------------------------------------------------------*/
    {
        auto b_delta_right = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenSubInt32(),
            /* left=   */ b_end,
            /* right=  */ mid
        );
        auto b_recurse_right_cond = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenGeSInt32(),
            /* left=   */ b_delta_right,
            /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(2 * tuple.size()))
        );
        BinaryenExpressionRef args[] = { mid, b_end };
        auto b_recurse_right = BinaryenCall(
            /* module=      */ module,
            /* target=      */ fn_name.c_str(),
            /* operands=    */ args,
            /* numOperands= */ 2,
            /* returnType=  */ BinaryenTypeNone()
        );
        loop += BinaryenIf(
            /* module=    */ module,
            /* condition= */ b_recurse_right_cond,
            /* ifTrue=    */ b_recurse_right,
            /* ifFalse=   */ nullptr
        );
    }

    /*----- Update end pointer. --------------------------------------------------------------------------------------*/
    loop += BinaryenLocalSet(
        /* module= */ module,
        /* index=  */ BinaryenLocalGetGetIndex(b_end),
        /* value=  */ b_mid_minus_one
    );

    /*----- Emit loop. -----------------------------------------------------------------------------------------------*/
    fn.block() += loop.finalize();

    /*----- Handle the case where end - begin == 2. ------------------------------------------------------------------*/
    {
        BlockBuilder block_swap(module);
        wasm_swap.emit(block_swap, tuple, b_begin, b_last);

        BlockBuilder block_compare(module);
        auto load_context_first  = tuple.create_load_context(b_begin);
        auto load_context_second = tuple.create_load_context(b_last);
        auto b_compare = comparator.emit(fn, block_compare, load_context_first, load_context_second);
        auto b_cond_swap = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenGtSInt32(),
            /* left=   */ b_compare,
            /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(0))
        );
        block_compare += BinaryenIf(
            /* module=    */ module,
            /* condition= */ b_cond_swap,
            /* ifTrue=    */ block_swap.finalize(),
            /* ifFalse=   */ nullptr
        );

        auto b_cond = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenEqInt32(),
            /* left=   */ b_delta,
            /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(2 * tuple.size()))
        );
        fn.block() += BinaryenIf(
            /* module=    */ module,
            /* condition= */ b_cond,
            /* ifTrue=    */ block_compare.finalize(),
            /* ifFalse=   */ nullptr
        );
    }

    /*----- Add function definition to module. -----------------------------------------------------------------------*/
    return fn.finalize();
}


/*======================================================================================================================
 * WasmBitMixMurmur3
 *====================================================================================================================*/

BinaryenExpressionRef WasmBitMixMurmur3::emit(BinaryenModuleRef module, FunctionBuilder &fn, BlockBuilder &block,
                                              BinaryenExpressionRef bits) const
{
    /* Taken from https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp by Austin Appleby.  We use the
     * optimized constants found by David Stafford, in particular the values for `Mix01`, as reported at
     * http://zimbry.blogspot.com/2011/09/better-bit-mixing-improving-on.html. */
    insist(BinaryenExpressionGetType(bits) == BinaryenTypeInt64(), "WasmBitMix expects a 64-bit integer");

    WasmVariable v(fn, BinaryenTypeInt64());

    /* v = v ^ (v >> 31) */
    {
        auto Shr = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenShrUInt64(),
            /* left=   */ bits,
            /* right=  */ BinaryenConst(module, BinaryenLiteralInt64(31))
        );
        v.set(block, BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenXorInt64(),
            /* left=   */ bits,
            /* right=  */ Shr
        ));
    }

    /* v = v * 0x7fb5d329728ea185ULL */
    v.set(block, BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenMulInt64(),
        /* left=   */ v,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt64(0x7fb5d329728ea185ULL))
    ));

    /* v = v ^ (v >> 27) */
    {
        auto Shr = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenShrUInt64(),
            /* left=   */ v,
            /* right=  */ BinaryenConst(module, BinaryenLiteralInt64(27))
        );
        v.set(block, BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenXorInt64(),
            /* left=   */ v,
            /* right=  */ Shr
        ));
    }

    /* v = v * 0x81dadef4bc2dd44dULL */
    v.set(block, BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenMulInt64(),
        /* left=   */ v,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt64(0x81dadef4bc2dd44dULL))
    ));

    /* v = v ^ (v >> 33) */
    {
        auto Shr = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenShrUInt64(),
            /* left=   */ v,
            /* right=  */ BinaryenConst(module, BinaryenLiteralInt64(33))
        );
        v.set(block, BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenXorInt64(),
            /* left=   */ v,
            /* right=  */ Shr
        ));
    }

    return v;
}


/*======================================================================================================================
 * WasmHashMumur64A
 *====================================================================================================================*/

BinaryenExpressionRef WasmHashMumur3_64A::emit(BinaryenModuleRef module, FunctionBuilder &fn, BlockBuilder &block,
                                               const std::vector<BinaryenExpressionRef> &values) const
{
    /* Inspired by https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp by Austin Appleby.  We use
     * constants from MurmurHash2_64 as reported on https://sites.google.com/site/murmurhash/. */
    insist(values.size() != 0, "cannot compute the hash of an empty sequence of values");
    const auto m = BinaryenConst(module, BinaryenLiteralInt64(0xc6a4a7935bd1e995LLU));

    WasmVariable h(fn, BinaryenTypeInt64());
    if (values.size() == 1) {
        /* In case of a single 64-bit value, we just run the bit mixer. */
        h.set(block, reinterpret(module, values[0], BinaryenTypeInt64()));
    } else {
        /*  uint64_t h = seed ^ (len * m) */
        h.set(block, BinaryenConst(module, BinaryenLiteralInt64(0xc6a4a7935bd1e995LLU * values.size())));

        WasmVariable k(fn, BinaryenTypeInt64());
        for (auto val : values) {
            k.set(block, reinterpret(module, val, BinaryenTypeInt64()));

            /* k = k * m */
            k.set(block, BinaryenBinary(
                /* module= */ module,
                /* op=     */ BinaryenMulInt64(),
                /* left=   */ k,
                /* right=  */ m
            ));

            /* k = ROTL32(k, 47); */
            k.set(block, BinaryenBinary(
                /* module= */ module,
                /* op=     */ BinaryenRotLInt64(),
                /* left=   */ k,
                /* right=  */ BinaryenConst(module, BinaryenLiteralInt64(47))
            ));

            /* k = k * m */
            k.set(block, BinaryenBinary(
                /* module= */ module,
                /* op=     */ BinaryenMulInt64(),
                /* left=   */ k,
                /* right=  */ m
            ));

            /* h = h ^ k; */
            h.set(block, BinaryenBinary(
                /* module= */ module,
                /* op=     */ BinaryenXorInt64(),
                /* left=   */ h,
                /* right=  */ k
            ));

            /* h = ROTL32(h, 45); */
            h.set(block, BinaryenBinary(
                /* module= */ module,
                /* op=     */ BinaryenRotLInt64(),
                /* left=   */ h,
                /* right=  */ BinaryenConst(module, BinaryenLiteralInt64(45))
            ));

            /* h = h * 5 + 0xe6546b64 */
            {
                auto Mul = BinaryenBinary(
                    /* module= */ module,
                    /* op=     */ BinaryenMulInt64(),
                    /* left=   */ h,
                    /* right=  */ BinaryenConst(module, BinaryenLiteralInt64(5))
                );
                h.set(block, BinaryenBinary(
                    /* module= */ module,
                    /* op=     */ BinaryenAddInt64(),
                    /* left=   */ Mul,
                    /* right=  */ BinaryenConst(module, BinaryenLiteralInt64(0xe6546b64ULL))
                ));
            }
        }

        /* h = h ^ len */
        h.set(block, BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenXorInt64(),
            /* left=   */ h,
            /* right=  */ BinaryenConst(module, BinaryenLiteralInt64(values.size()))
        ));
    }

    return WasmBitMixMurmur3{}.emit(module, fn, block, h);
}


/*======================================================================================================================
 * WasmRefCountingHashTable
 *====================================================================================================================*/

BinaryenExpressionRef WasmRefCountingHashTable::create_table(BlockBuilder &block, BinaryenExpressionRef b_addr,
                                                             std::size_t num_buckets) const
{
    num_buckets = ceil_to_pow_2(num_buckets);
    addr_.set(block, b_addr);
    mask_.set(block, BinaryenConst(module, BinaryenLiteralInt32(num_buckets - 1)));

    /*----- Return end of hash table. --------------------------------------------------------------------------------*/
    return BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ addr_,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(num_buckets * entry_size()))
    );
}

void WasmRefCountingHashTable::clear_table(BlockBuilder &block,
                                           BinaryenExpressionRef b_begin, BinaryenExpressionRef b_end) const
{
    WasmVariable induction(fn, BinaryenTypeInt32());
    induction.set(block, b_begin);

    WasmWhile loop(module, "clear_table.loop", BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenLtUInt32(),
        /* left=   */ induction,
        /* right=  */ b_end
    ));

    /*----- Clear entry. ---------------------------------------------------------------------------------------------*/
    loop += BinaryenStore(
        /* module= */ module,
        /* bytes=  */ REFERENCE_SIZE,
        /* offset= */ 0,
        /* align=  */ 0,
        /* ptr=    */ induction,
        /* value=  */ BinaryenConst(module, BinaryenLiteralInt32(0)),
        /* type=   */ BinaryenTypeInt32()
    );

    /*----- Advance induction variable. ------------------------------------------------------------------------------*/
    induction.set(loop, BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ induction,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(entry_size_))
    ));

    block += loop.finalize();
}

BinaryenExpressionRef WasmRefCountingHashTable::hash_to_bucket(BinaryenExpressionRef b_hash) const
{
    auto b_bucket_index = BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenAndInt32(),
        /* left=   */ b_hash,
        /* right=  */ mask_
    );
    auto b_bucket_offset = BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenMulInt32(),
        /* left=   */ b_bucket_index,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(entry_size()))
    );
    return BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ addr_,
        /* right=  */ b_bucket_offset
    );
}

std::pair<BinaryenExpressionRef, BinaryenExpressionRef>
WasmRefCountingHashTable::find_in_bucket(BlockBuilder &block, BinaryenExpressionRef b_bucket_addr,
                                         const std::vector<BinaryenExpressionRef> &key) const
{
    insist(key.size() <= struc.schema.num_entries(), "incorrect number of key values");
    WasmLoop loop(module, "find_in_bucket.loop");

    /*----- Create local runner . ------------------------------------------------------------------------------------*/
    WasmVariable runner(fn, BinaryenTypeInt32());
    runner.set(block, b_bucket_addr);

    auto b_table_size = BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ mask_,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(1))
    );

    WasmVariable b_table_size_in_bytes(fn, BinaryenTypeInt32());
    b_table_size_in_bytes.set(block, BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenMulInt32(),
        /* left=   */ b_table_size,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(entry_size_))
    ));

    /*----- Initialize step counter. ---------------------------------------------------------------------------------*/
    WasmVariable step(fn, BinaryenTypeInt32());
    step.set(block, BinaryenConst(module, BinaryenLiteralInt32(entry_size_)));

    /*----- Advance to next slot. ------------------------------------------------------------------------------------*/
    BlockBuilder advance(module, "find_in_bucket.loop.body.step");
    {
        auto b_runner_inc = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ runner,
            /* right=  */ step
        );
        auto b_runner_wrapped = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenSubInt32(),
            /* left=   */ b_runner_inc,
            /* right=  */ b_table_size_in_bytes
        );
        auto b_table_end = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ addr_,
            /* right=  */ b_table_size_in_bytes
        );
        auto b_is_overflow = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenGeUInt32(),
            /* left=   */ b_runner_inc,
            /* right=  */ b_table_end
        );
        auto b_runner_upd = BinaryenSelect(
            /* module=    */ module,
            /* condition= */ b_is_overflow,
            /* ifTrue=    */ b_runner_wrapped,
            /* ifFalse=   */ b_runner_inc,
            /* type=      */ BinaryenTypeInt32()
        );
        runner.set(advance, b_runner_upd);

        step.set(advance, BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ step,
            /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(entry_size_))
        ));
        advance += loop.continu();
    }

    /*----- Get reference count from bucket. -------------------------------------------------------------------------*/
    auto b_ref_count = BinaryenLoad(
        /* module= */ module,
        /* bytes=  */ REFERENCE_SIZE,
        /* signed= */ false,
        /* offset= */ 0,
        /* align=  */ 0,
        /* type=   */ BinaryenTypeInt32(),
        /* ptr=    */ runner
    );

    /*----- Check whether bucket is occupied. ------------------------------------------------------------------------*/
    auto b_is_occupied = BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenNeInt32(),
        /* left=   */ b_ref_count,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(0))
    );

    /*----- Compare keys. --------------------------------------------------------------------------------------------*/
    BinaryenExpressionRef b_keys_not_equal = nullptr;
    {
        auto ld_key = struc.create_load_context(runner, /* offset= */ REFERENCE_SIZE);
        std::size_t idx = 0;
        for (auto b_key_find : key) {
            auto &e = struc.schema[idx++];
            auto b_key_bucket = ld_key[e.id];
            auto b_cmp = WasmCompare::Ne(module, *e.type, b_key_bucket, b_key_find);

            if (b_keys_not_equal) {
                b_keys_not_equal = BinaryenBinary(
                    /* module= */ module,
                    /* op=     */ BinaryenOrInt32(),
                    /* left=   */ b_keys_not_equal,
                    /* right=  */ b_cmp
                );
            } else {
                b_keys_not_equal = b_cmp;
            }
        }
    }

    auto b_if_key_equal = BinaryenIf(
        /* module=    */ module,
        /* condition= */ b_keys_not_equal,
        /* ifTrue=    */ advance.finalize(), // key not equal, continue search
        /* ifFalse=   */ nullptr // hit, break loop
    );
    loop += BinaryenIf(
        /* module=    */ module,
        /* condition= */ b_is_occupied,
        /* ifTrue=    */ b_if_key_equal, // slot occupied, compare key
        /* ifFalse=   */ nullptr // miss, break loop
    );
    block += loop.finalize();

    return std::make_pair(runner(), step());
}

BinaryenExpressionRef WasmRefCountingHashTable::is_slot_empty(BinaryenExpressionRef b_slot_addr) const
{
    return BinaryenUnary(
        /* module= */ module,
        /* op=     */ BinaryenEqZInt32(),
        /* value=  */ get_bucket_ref_count(b_slot_addr)
    );
}

BinaryenExpressionRef WasmRefCountingHashTable::compare_key(BinaryenExpressionRef b_slot_addr,
                                                            const std::vector<BinaryenExpressionRef> &key) const
{
    BinaryenExpressionRef b_keys_equal = nullptr;
    {
        auto ld_key = struc.create_load_context(b_slot_addr, /* offset= */ REFERENCE_SIZE);
        std::size_t idx = 0;
        for (auto b_key_find : key) {
            auto &e = struc.schema[idx++];
            auto b_key_bucket = ld_key[e.id];
            auto b_cmp = WasmCompare::Eq(module, *e.type, b_key_bucket, b_key_find);

            if (b_keys_equal) {
                b_keys_equal = BinaryenBinary(
                    /* module= */ module,
                    /* op=     */ BinaryenAndInt32(),
                    /* left=   */ b_keys_equal,
                    /* right=  */ b_cmp
                );
            } else {
                b_keys_equal = b_cmp;
            }
        }
    }
    return b_keys_equal;
}

void WasmRefCountingHashTable::emplace(BlockBuilder &block,
                                       BinaryenExpressionRef b_bucket_addr, BinaryenExpressionRef b_steps,
                                       BinaryenExpressionRef b_slot_addr,
                                       const std::vector<BinaryenExpressionRef> &key) const
{
    insist(key.size() <= struc.schema.num_entries(), "incorrect number of key values");

    /*----- Update bucket probe length. ------------------------------------------------------------------------------*/
    block += BinaryenStore(
        /* module= */ module,
        /* bytes=  */ REFERENCE_SIZE,
        /* offset= */ 0,
        /* align=  */ 0,
        /* ptr=    */ b_bucket_addr,
        /* value=  */ b_steps,
        /* type=   */ BinaryenTypeInt32()
    );

    /*----- Set slot as occupied. ------------------------------------------------------------------------------------*/
    block += BinaryenStore(
        /* module= */ module,
        /* bytes=  */ REFERENCE_SIZE,
        /* offset= */ 0,
        /* align=  */ 0,
        /* ptr=    */ b_slot_addr,
        /* value=  */ BinaryenConst(module, BinaryenLiteralInt32(entry_size_)),
        /* type=   */ BinaryenTypeInt32()
    );

    /*----- Write key to slot. ---------------------------------------------------------------------------------------*/
    std::size_t idx = 0;
    for (auto b_key : key)
        block += struc.store(b_slot_addr, struc.schema[idx++].id, b_key, /* offset= */ REFERENCE_SIZE);
}

WasmCGContext WasmRefCountingHashTable::load_from_slot(BinaryenExpressionRef b_slot_addr) const
{
    return struc.create_load_context(b_slot_addr, /* offset= */ REFERENCE_SIZE);
}

BinaryenExpressionRef WasmRefCountingHashTable::store_value_to_slot(BinaryenExpressionRef b_slot_addr,
                                                                    Schema::Identifier id,
                                                                    BinaryenExpressionRef value) const
{
    return struc.store(b_slot_addr, id, value, /* offset= */ REFERENCE_SIZE);
}

BinaryenExpressionRef WasmRefCountingHashTable::compute_next_slot(BinaryenExpressionRef b_slot_addr) const
{
    return BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ b_slot_addr,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(entry_size_))
    );
}

BinaryenExpressionRef WasmRefCountingHashTable::get_bucket_ref_count(BinaryenExpressionRef b_bucket_addr) const
{
    return BinaryenLoad(
        /* module= */ module,
        /* bytes=  */ REFERENCE_SIZE,
        /* signed= */ false,
        /* offset= */ 0,
        /* align=  */ 0,
        /* type=   */ BinaryenTypeInt32(),
        /* ptr=    */ b_bucket_addr
    );
}

BinaryenExpressionRef WasmRefCountingHashTable::insert_with_duplicates(BlockBuilder &block,
                                                                       BinaryenExpressionRef b_hash,
                                                                       const std::vector<BinaryenExpressionRef> &key)
    const
{
    /*----- Compute address of bucket. -------------------------------------------------------------------------------*/
    WasmVariable bucket_addr(fn, BinaryenTypeInt32());
    bucket_addr.set(block, hash_to_bucket(b_hash));

    /*----- Load steps in bucket. ------------------------------------------------------------------------------------*/
    WasmVariable steps(fn, BinaryenTypeInt32());
    steps.set(block, get_bucket_ref_count(bucket_addr));

    WasmVariable slot_addr(fn, BinaryenTypeInt32());
    BlockBuilder bucket_create(module, "insert.create_bucket");
    BlockBuilder bucket_insert(module, "insert.append_to_bucket");

    /*----- If the bucket is empty, initialize it. -------------------------------------------------------------------*/
    // TODO remove this by moving after bucket_create/bucket_insert
    emplace(bucket_create, bucket_addr, BinaryenConst(module, BinaryenLiteralInt32(entry_size())), bucket_addr, {key});
    slot_addr.set(bucket_create, bucket_addr);

    /*----- Compute end of hash table. -------------------------------------------------------------------------------*/
    auto b_size = BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ mask(),
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(1))
    );
    WasmVariable table_size_in_bytes(fn, BinaryenTypeInt32());
    table_size_in_bytes.set(bucket_insert, BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenMulInt32(),
        /* left=   */ b_size,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(entry_size()))
    ));
    auto b_table_end = BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ addr(),
        /* right=  */ table_size_in_bytes
    );

    {
        /*----- Evaluate formula for Gaussian sum to compute the end of the bucket -----------------------------------*/
        WasmVariable probe_len(fn, BinaryenTypeInt32());
        probe_len.set(bucket_insert, BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenDivUInt32(),
            /* left=   */ steps,
            /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(entry_size()))
        ));
        auto b_square = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenMulInt32(),
            /* left=   */ probe_len,
            /* right=  */ probe_len
        );
        auto b_sum = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ b_square,
            /* right=  */ probe_len
        );
        auto b_gauss = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenShrUInt32(),
            /* left=   */ b_sum,
            /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(1))
        );

        /*----- Compute the end of the bucket. -----------------------------------------------------------------------*/
        auto b_dist_in_bytes = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenMulInt32(),
            /* left=   */ b_gauss,
            /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(entry_size()))
        );
        slot_addr.set(bucket_insert, BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ bucket_addr,
            /* right=  */ b_dist_in_bytes
        ));
        auto b_exceeds_table = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenGeUInt32(),
            /* left=   */ slot_addr,
            /* right=  */ b_table_end
        );
        auto b_slot_addr_wrapped = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenSubInt32(),
            /* left=   */ slot_addr,
            /* right=  */ table_size_in_bytes
        );
        slot_addr.set(bucket_insert, BinaryenSelect(
            /* module=    */ module,
            /* condition= */ b_exceeds_table,
            /* ifTrue=    */ b_slot_addr_wrapped,
            /* ifFalse=   */ slot_addr,
            /* type=      */ BinaryenTypeInt32()
        ));
    }

    /*----- Find next free slot in bucket. ---------------------------------------------------------------------------*/
    WasmWhile find_slot(module, "insert.append_to_bucket.find_slot", get_bucket_ref_count(slot_addr));
    {
        steps.set(find_slot, BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ steps,
            /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(entry_size()))
        ));
        slot_addr.set(find_slot, BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ slot_addr,
            /* right=  */ steps
        ));
        auto b_exceeds_table = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenGeUInt32(),
            /* left=   */ slot_addr,
            /* right=  */ b_table_end
        );
        auto b_slot_addr_wrapped = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenSubInt32(),
            /* left=   */ slot_addr,
            /* right=  */ table_size_in_bytes
        );
        slot_addr.set(find_slot, BinaryenSelect(
            /* module=    */ module,
            /* condition= */ b_exceeds_table,
            /* ifTrue=    */ b_slot_addr_wrapped,
            /* ifFalse=   */ slot_addr,
            /* type=      */ BinaryenTypeInt32()
        ));
    }
    bucket_insert += find_slot.finalize();

    /*----- Place tuple in slot. -------------------------------------------------------------------------------------*/
    steps.set(bucket_insert, BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ steps,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(entry_size()))
    ));
    emplace(bucket_insert, bucket_addr, steps, slot_addr, { key });

    /*----- Create conditional jump based on whether the bucket is empty. --------------------------------------------*/
    block += BinaryenIf(
        /* module=    */ module,
        /* condition= */ get_bucket_ref_count(bucket_addr),
        /* ifTrue=    */ bucket_insert.finalize(),
        /* ifFalse=   */ bucket_create.finalize()
    );

    return slot_addr;
}

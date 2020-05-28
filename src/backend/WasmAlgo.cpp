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
    const char *loop_name = "partition_branchless";
    const char *body_name = "partition_branchless.body";
    BlockBuilder loop_body(module, body_name);

    /*----- Copy begin and end. --------------------------------------------------------------------------------------*/
    {
        auto b_begin_local = fn.add_local(BinaryenTypeInt32());
        block += BinaryenLocalSet(
            /* module= */ module,
            /* index=  */ BinaryenLocalGetGetIndex(b_begin_local),
            /* value=  */ b_begin
        );
        b_begin = b_begin_local;

        auto b_end_local = fn.add_local(BinaryenTypeInt32());
        block += BinaryenLocalSet(
            /* module= */ module,
            /* index=  */ BinaryenLocalGetGetIndex(b_end_local),
            /* value=  */ b_end
        );
        b_end = b_end_local;
    }

    /*----- Offset end by one.  (This can be dropped when negative offsets are supported.) ---------------------------*/
    auto b_last = BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ b_end,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(-struc.size()))
    );

    /*----- Create load context for left, right, and pivot. ----------------------------------------------------------*/
    auto load_context_left  = struc.create_load_context(b_begin);
    auto load_context_right = struc.create_load_context(b_last);
    auto load_context_pivot = struc.create_load_context(b_pivot);

    /*----- Load values from pivot. ----------------------------------------------------------------------------------*/
    WasmCGContext value_context_pivot(module);
    for (auto &attr : struc.schema) {
        BinaryenType b_attr_type = get_binaryen_type(attr.type);

        auto b_tmp_pivot = fn.add_local(b_attr_type);
        block += BinaryenLocalSet(
            /* module= */ module,
            /* index=  */ BinaryenLocalGetGetIndex(b_tmp_pivot),
            /* value=  */ load_context_pivot.get_value(attr.id)
        );
        value_context_pivot.add(attr.id, b_tmp_pivot);
    }

    /*----- Swap left and right tuple. -------------------------------------------------------------------------------*/
    wasm_swap.emit(loop_body, struc, b_begin, b_last);

    /*----- Compare left and right tuples to pivot. ------------------------------------------------------------------*/
    auto b_cmp_left  = comparator.emit(fn, loop_body, load_context_left,  value_context_pivot);
    auto b_left_is_ok = BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenLeSInt32(),
        /* left=   */ b_cmp_left,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(0))
    );
    auto b_cmp_right = comparator.emit(fn, loop_body, load_context_right, value_context_pivot);
    auto b_right_is_ok = BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenGeSInt32(),
        /* left=   */ b_cmp_right,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(0))
    );

    /* Advance begin cursor. */
    {
        auto b_delta_begin = BinaryenSelect(
            /* module=    */ module,
            /* condition= */ b_left_is_ok,
            /* ifTrue=    */ BinaryenConst(module, BinaryenLiteralInt32(struc.size())),
            /* ifFalse=   */ BinaryenConst(module, BinaryenLiteralInt32(0)),
            /* type=      */ BinaryenTypeInt32()
        );
        auto b_begin_upd = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ b_begin,
            /* right=  */ b_delta_begin
        );
        loop_body += BinaryenLocalSet(
            /* module= */ module,
            /* index=  */ BinaryenLocalGetGetIndex(b_begin),
            /* value=  */ b_begin_upd
        );
    }

    /* Advance end cursor. */
    {
        auto b_delta_end = BinaryenSelect(
            /* module=    */ module,
            /* condition= */ b_right_is_ok,
            /* ifTrue=    */ BinaryenConst(module, BinaryenLiteralInt32(-struc.size())),
            /* ifFalse=   */ BinaryenConst(module, BinaryenLiteralInt32(0)),
            /* type=      */ BinaryenTypeInt32()
        );
        auto b_end_upd = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ b_end,
            /* right=  */ b_delta_end
        );
        loop_body += BinaryenLocalSet(
            /* module= */ module,
            /* index=  */ BinaryenLocalGetGetIndex(b_end),
            /* value=  */ b_end_upd
        );
    }

    /*----- Create loop header. --------------------------------------------------------------------------------------*/
    auto b_loop_cond = BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenLtUInt32(),
        /* left=   */ b_begin,
        /* right=  */ b_end
    );
    loop_body += BinaryenBreak(
        /* module=    */ module,
        /* name=      */ loop_name,
        /* condition= */ b_loop_cond,
        /* value=     */ nullptr
    );

    block += BinaryenLoop(
        /* module= */ module,
        /* in=     */ loop_name,
        /* body=   */ loop_body.finalize()
    );

    return b_begin;
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

    BlockBuilder loop_body(module, "qsort_loop.body");

    const auto b_last = BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ b_end,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(-tuple.size()))
    );

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
    const auto b_mid_addr = BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ b_begin,
        /* right=  */ b_offset_mid
    );
    const auto b_mid = fn.add_local(BinaryenTypeInt32());
    loop_body += BinaryenLocalSet(
        /* module= */ module,
        /* index=  */ BinaryenLocalGetGetIndex(b_mid),
        /* value=  */ b_mid_addr
    );

    auto load_context_left  = tuple.create_load_context(b_begin);
    auto load_context_mid   = tuple.create_load_context(b_mid);
    auto load_context_right = tuple.create_load_context(b_last);

    /*----- Compare three elements pairwise. -------------------------------------------------------------------------*/
    auto b_cmp_left_mid   = comparator.emit(fn, loop_body, load_context_left, load_context_mid);
    auto b_cmp_left_right = comparator.emit(fn, loop_body, load_context_left, load_context_right);
    auto b_cmp_mid_right  = comparator.emit(fn, loop_body, load_context_mid,  load_context_right);

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
    wasm_swap.emit(block_swap_left_mid, tuple, b_begin, b_mid);
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
    loop_body += b_if_4;

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
        /* block=       */ loop_body,
        /* struc=       */ tuple,
        /* order=       */ order,
        /* b_begin=     */ b_begin_plus_one,
        /* b_end=       */ b_end,
        /* b_pivot=     */ b_begin
    );
    loop_body += BinaryenLocalSet(
        /* module= */ module,
        /* index=  */ BinaryenLocalGetGetIndex(b_mid),
        /* value=  */ b_partition
    );

    /*----- Patch mid pointer, if necessary. -------------------------------------------------------------------------*/
    auto b_mid_minus_one = BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ b_mid,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(-tuple.size()))
    );
    wasm_swap.emit(loop_body, tuple, b_begin, b_mid_minus_one);

    /*----- Recurse right, if necessary. -----------------------------------------------------------------------------*/
    {
        auto b_delta_right = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenSubInt32(),
            /* left=   */ b_end,
            /* right=  */ b_mid
        );
        auto b_recurse_right_cond = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenGeSInt32(),
            /* left=   */ b_delta_right,
            /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(2 * tuple.size()))
        );
        BinaryenExpressionRef args[] = { b_mid, b_end };
        auto b_recurse_right = BinaryenCall(
            /* module=      */ module,
            /* target=      */ fn_name.c_str(),
            /* operands=    */ args,
            /* numOperands= */ 2,
            /* returnType=  */ BinaryenTypeNone()
        );
        loop_body += BinaryenIf(
            /* module=    */ module,
            /* condition= */ b_recurse_right_cond,
            /* ifTrue=    */ b_recurse_right,
            /* ifFalse=   */ nullptr
        );
    }

    /*----- Update end pointer. --------------------------------------------------------------------------------------*/
    loop_body += BinaryenLocalSet(
        /* module= */ module,
        /* index=  */ BinaryenLocalGetGetIndex(b_end),
        /* value=  */ b_mid_minus_one
    );

    /*----- Emit loop header. ----------------------------------------------------------------------------------------*/
    auto b_loop_cond = BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenGtSInt32(),
        /* left=   */ b_delta,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(2 * tuple.size()))
    );
    loop_body += BinaryenBreak(
        /* module=    */ module,
        /* name=      */ "qsort_loop",
        /* condition= */ b_loop_cond,
        /* value=     */ nullptr
    );

    auto b_loop = BinaryenLoop(
        /* module= */ module,
        /* in=     */ "qsort_loop",
        /* body=   */ loop_body.finalize()
    );

    /*----- Emit loop entry. -----------------------------------------------------------------------------------------*/
    fn.block() += BinaryenIf(
        /* module=    */ module,
        /* condition= */ b_loop_cond,
        /* ifTrue=    */ b_loop,
        /* ifFalse=   */ nullptr
    );

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

    auto v = fn.add_local(BinaryenTypeInt64());

    // v = v ^ (v >> 31)
    {
        auto Shr = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenShrUInt64(),
            /* left=   */ bits,
            /* right=  */ BinaryenConst(module, BinaryenLiteralInt64(31))
        );
        auto Xor = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenXorInt64(),
            /* left=   */ bits,
            /* right=  */ Shr
        );
        block += BinaryenLocalSet(
            /* module= */ module,
            /* index=  */ BinaryenLocalGetGetIndex(v),
            /* value=  */ Xor
        );
    }

    // v = v * 0x7fb5d329728ea185ULL
    {
        auto Mul = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenMulInt64(),
            /* left=   */ v,
            /* right=  */ BinaryenConst(module, BinaryenLiteralInt64(0x7fb5d329728ea185ULL))
        );
        block += BinaryenLocalSet(
            /* module= */ module,
            /* index=  */ BinaryenLocalGetGetIndex(v),
            /* value=  */ Mul
        );
    }

    // v = v ^ (v >> 27)
    {
        auto Shr = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenShrUInt64(),
            /* left=   */ v,
            /* right=  */ BinaryenConst(module, BinaryenLiteralInt64(27))
        );
        auto Xor = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenXorInt64(),
            /* left=   */ v,
            /* right=  */ Shr
        );
        block += BinaryenLocalSet(
            /* module= */ module,
            /* index=  */ BinaryenLocalGetGetIndex(v),
            /* value=  */ Xor
        );
    }

    // v = v * 0x81dadef4bc2dd44dULL
    {
        auto Mul = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenMulInt64(),
            /* left=   */ v,
            /* right=  */ BinaryenConst(module, BinaryenLiteralInt64(0x81dadef4bc2dd44dULL))
        );
        block += BinaryenLocalSet(
            /* module= */ module,
            /* index=  */ BinaryenLocalGetGetIndex(v),
            /* value=  */ Mul
        );
    }

    // v = v ^ (v >> 33)
    {
        auto Shr = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenShrUInt64(),
            /* left=   */ v,
            /* right=  */ BinaryenConst(module, BinaryenLiteralInt64(33))
        );
        auto Xor = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenXorInt64(),
            /* left=   */ v,
            /* right=  */ Shr
        );
        block += BinaryenLocalSet(
            /* module= */ module,
            /* index=  */ BinaryenLocalGetGetIndex(v),
            /* value=  */ Xor
        );
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
    BinaryenExpressionRef h = fn.add_local(BinaryenTypeInt64());

    if (values.size() == 1) {
        /* In case of a single 64-bit value, we just run the bit mixer. */
        block += BinaryenLocalSet(
            /* module= */ module,
            /* index=  */ BinaryenLocalGetGetIndex(h),
            /* value=  */ reinterpret(module, values[0], BinaryenTypeInt64())
        );
    } else {
        //  uint64_t h = seed ^ (len * m)
        block += BinaryenLocalSet(
            /* module= */ module,
            /* index=  */ BinaryenLocalGetGetIndex(h),
            /* value=  */ BinaryenConst(module, BinaryenLiteralInt64(0xc6a4a7935bd1e995LLU * values.size()))
        );
        BinaryenExpressionRef k = fn.add_local(BinaryenTypeInt64());
        for (auto val : values) {
            block += BinaryenLocalSet(
                /* module= */ module,
                /* index=  */ BinaryenLocalGetGetIndex(k),
                /* value=  */ reinterpret(module, val, BinaryenTypeInt64())
            );

            // k = k * m
            {
                auto Mul = BinaryenBinary(
                    /* module= */ module,
                    /* op=     */ BinaryenMulInt64(),
                    /* left=   */ k,
                    /* right=  */ m
                );
                block += BinaryenLocalSet(
                    /* module= */ module,
                    /* index=  */ BinaryenLocalGetGetIndex(k),
                    /* value=  */ Mul
                );
            }

            // k = ROTL32(k, 47);
            {
                auto Rot = BinaryenBinary(
                    /* module= */ module,
                    /* op=     */ BinaryenRotLInt64(),
                    /* left=   */ k,
                    /* right=  */ BinaryenConst(module, BinaryenLiteralInt64(47))
                );
                block += BinaryenLocalSet(
                    /* module= */ module,
                    /* index=  */ BinaryenLocalGetGetIndex(k),
                    /* value=  */ Rot
                );
            }

            // k = k * m
            {
                auto Mul = BinaryenBinary(
                    /* module= */ module,
                    /* op=     */ BinaryenMulInt64(),
                    /* left=   */ k,
                    /* right=  */ m
                );
                block += BinaryenLocalSet(
                    /* module= */ module,
                    /* index=  */ BinaryenLocalGetGetIndex(k),
                    /* value=  */ Mul
                );
            }

            // h = h ^ k;
            {
                auto Xor = BinaryenBinary(
                    /* module= */ module,
                    /* op=     */ BinaryenXorInt64(),
                    /* left=   */ h,
                    /* right=  */ k
                );
                block += BinaryenLocalSet(
                    /* module= */ module,
                    /* index=  */ BinaryenLocalGetGetIndex(h),
                    /* value=  */ Xor
                );
            }

            // h = ROTL32(h, 45);
            {
                auto Rot = BinaryenBinary(
                    /* module= */ module,
                    /* op=     */ BinaryenRotLInt64(),
                    /* left=   */ h,
                    /* right=  */ BinaryenConst(module, BinaryenLiteralInt64(45))
                );
                block += BinaryenLocalSet(
                    /* module= */ module,
                    /* index=  */ BinaryenLocalGetGetIndex(h),
                    /* value=  */ Rot
                );
            }

            // h = h * 5
            {
                auto Mul = BinaryenBinary(
                    /* module= */ module,
                    /* op=     */ BinaryenMulInt64(),
                    /* left=   */ h,
                    /* right=  */ BinaryenConst(module, BinaryenLiteralInt64(5))
                );
                block += BinaryenLocalSet(
                    /* module= */ module,
                    /* index=  */ BinaryenLocalGetGetIndex(h),
                    /* value=  */ Mul
                );
            }
        }
    }

    return WasmBitMixMurmur3{}.emit(module, fn, block, h);
}

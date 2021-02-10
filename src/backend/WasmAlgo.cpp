#include "backend/WasmAlgo.hpp"

#include <sstream>
#include <string>
#include <unordered_map>


using namespace m;


/*======================================================================================================================
 * WasmPartitionBranching
 *====================================================================================================================*/

WasmTemporary WasmPartitionBranching::emit(BinaryenModuleRef module, FunctionBuilder &fn, BlockBuilder &block,
                                           const WasmStruct &struc, const std::vector<order_type> &order,
                                           WasmTemporary begin, WasmTemporary end, WasmTemporary pivot) const
{
    (void) module;
    (void) fn;
    (void) block;
    (void) struc;
    (void) order;
    (void) begin;
    (void) end;
    (void) pivot;
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

WasmTemporary WasmPartitionBranchless::emit(BinaryenModuleRef module, FunctionBuilder &fn, BlockBuilder &block,
                                                    const WasmStruct &struc, const std::vector<order_type> &order,
                                                    WasmTemporary begin, WasmTemporary end,
                                                    WasmTemporary pivot) const
{
    WasmSwap wasm_swap(module, fn);
    WasmCompare comparator(module, struc, order);

    /*----- Initialize left and right cursor. ------------------------------------------------------------------------*/
    WasmVariable left(fn, BinaryenTypeInt32());
    block += left.set(std::move(begin));

    WasmVariable right(fn, BinaryenTypeInt32());
    block += right.set(std::move(end));

    /*----- Create loop. ---------------------------------------------------------------------------------------------*/
    WasmDoWhile loop(module, "partition_branchless.loop", BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenLtUInt32(),
        /* left=   */ left,
        /* right=  */ right
    ));

    /*----- Offset right by one.  (This can be dropped when negative offsets are supported.) -------------------------*/
    WasmTemporary last = BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ right,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(-struc.size()))
    );

    /*----- Create load context for left, right, and pivot. ----------------------------------------------------------*/
    auto load_context_left  = struc.create_load_context(left);
    auto load_context_right = struc.create_load_context(last.clone(module));
    auto load_context_pivot = struc.create_load_context(std::move(pivot));

    /*----- Load values from pivot. ----------------------------------------------------------------------------------*/
    WasmCGContext value_context_pivot(module);
    for (auto &attr : struc.schema) {
        WasmVariable tmp(fn, get_binaryen_type(attr.type));
        block += tmp.set(load_context_pivot.get_value(attr.id));
        value_context_pivot.add(attr.id, tmp);
    }

    /*----- Swap left and right tuple. -------------------------------------------------------------------------------*/
    wasm_swap.emit(loop, struc, left, std::move(last));

    /*----- Compare left and right tuples to pivot. ------------------------------------------------------------------*/
    WasmTemporary cmp_left  = comparator.emit(fn, loop, load_context_left,  value_context_pivot);
    WasmTemporary left_is_ok = BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenLeSInt32(),
        /* left=   */ cmp_left,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(0))
    );
    WasmTemporary cmp_right = comparator.emit(fn, loop, load_context_right, value_context_pivot);
    WasmTemporary right_is_ok = BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenGeSInt32(),
        /* left=   */ cmp_right,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(0))
    );

    /* Advance left cursor. */
    {
        WasmTemporary delta_begin = BinaryenSelect(
            /* module=    */ module,
            /* condition= */ left_is_ok,
            /* ifTrue=    */ BinaryenConst(module, BinaryenLiteralInt32(struc.size())),
            /* ifFalse=   */ BinaryenConst(module, BinaryenLiteralInt32(0)),
            /* type=      */ BinaryenTypeInt32()
        );
        loop += left.set(BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ left,
            /* right=  */ delta_begin
        ));
    }

    /* Advance right cursor. */
    {
        WasmTemporary delta_end = BinaryenSelect(
            /* module=    */ module,
            /* condition= */ right_is_ok,
            /* ifTrue=    */ BinaryenConst(module, BinaryenLiteralInt32(-struc.size())),
            /* ifFalse=   */ BinaryenConst(module, BinaryenLiteralInt32(0)),
            /* type=      */ BinaryenTypeInt32()
        );
        loop += right.set(BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ right,
            /* right=  */ delta_end
        ));
    }

    block += loop.finalize();
    return left;
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
    std::ostringstream oss;
    oss << "qsort";
    for (auto &o : order)
        oss << '_' << *o.first << '_' << ( o.second ? "ASC" : "DESC" );
    const std::string fn_name = oss.str();

    std::vector<BinaryenType> param_types = { /* begin= */ BinaryenTypeInt32(), /* end= */ BinaryenTypeInt32() };
    FunctionBuilder fn(module, fn_name.c_str(), BinaryenTypeNone(), param_types);

    WasmStruct tuple(module, schema);
    WasmCompare comparator(module, tuple, order);
    WasmSwap wasm_swap(module, fn);

    /* Get parameters. */
    WasmVariable begin(module, BinaryenTypeInt32(), 0);
    WasmVariable end(module, BinaryenTypeInt32(), 1);

    WasmTemporary delta = BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenSubInt32(),
        /* left=   */ end,
        /* right=  */ begin
    );
    WasmTemporary last = BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ end,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(-tuple.size()))
    );

    WasmWhile loop(module, (fn_name + ".loop").c_str(), BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenGtSInt32(),
        /* left=   */ delta.clone(module),
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(2 * tuple.size()))
    ));

    /*----- Compute middle of data. ----------------------------------------------------------------------------------*/
    WasmVariable mid(fn, BinaryenTypeInt32());
    {
        WasmTemporary count = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenDivUInt32(),
            /* left=   */ delta.clone(module),
            /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(tuple.size()))
        );
        WasmTemporary half_the_count = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenDivUInt32(),
            /* left=   */ count,
            /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(2))
        );
        WasmTemporary offset_mid = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenMulInt32(),
            /* left=   */ half_the_count,
            /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(tuple.size()))
        );
        loop += mid.set(BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ begin,
            /* right=  */ offset_mid
        ));
    }

    /* Create load contexts for the first/last/middle element. */
    auto load_context_left  = tuple.create_load_context(begin);
    auto load_context_mid   = tuple.create_load_context(mid);
    auto load_context_right = tuple.create_load_context(last.clone(module));

    /*----- Compare three elements pairwise. -------------------------------------------------------------------------*/
    WasmTemporary cmp_left_mid   = comparator.emit(fn, loop, load_context_left, load_context_mid);
    WasmTemporary cmp_left_right = comparator.emit(fn, loop, load_context_left, load_context_right);
    WasmTemporary cmp_mid_right  = comparator.emit(fn, loop, load_context_mid,  load_context_right);

    WasmTemporary left_le_mid = BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenLeSInt32(),
        /* left=   */ cmp_left_mid,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(0))
    );
    WasmTemporary left_le_right = BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenLeSInt32(),
        /* left=   */ cmp_left_right,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(0))
    );
    WasmTemporary mid_le_right = BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenLeSInt32(),
        /* left=   */ cmp_mid_right,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(0))
    );

    /*----- Swap pivot to front. -------------------------------------------------------------------------------------*/
    BlockBuilder swap_left_mid(module, "swap_left_mid");
    wasm_swap.emit(swap_left_mid, tuple, begin, mid);
    BlockBuilder swap_left_right(module, "swap_left_right");
    wasm_swap.emit(swap_left_right, tuple, begin, last.clone(module));
    /*
     *  if (left <= mid) {                  // if_4
     *      if (left <= right) {            // if_1
     *          if (mid <= right)           // if_0
     *              swap(left, mid);        // left <= mid && left <= right && mid <= right -> [left, mid, right]
     *          else
     *              swap(left, right);      // left <= mid && left <= right && mid > right -> [left, right, mid]
     *      } else {
     *          nop;                        // left <= mid && left > right -> [right, left, mid]
     *      }
     *  } else {
     *      if (mid <= right) {             // if_3
     *          if (left <= right)          // if_2
     *              nop;                    // left > mid && mid <= right && left <= right -> [mid, left, right]
     *          else
     *              swap(left, right);      // left > mid && mid <= right && left > right -> [mid, right, left]
     *      } else {
     *          swap(left, mid)             // left > mid && mid > right -> [right, mid, left]
     *      }
     *  }
     *
     */
    WasmTemporary if_0 = BinaryenIf(
        /* module=    */ module,
        /* condition= */ mid_le_right.clone(module),
        /* ifTrue=    */ swap_left_mid.clone().finalize(),
        /* ifFalse=   */ swap_left_right.clone().finalize()
    );
    WasmTemporary if_1 = BinaryenIf(
        /* module=    */ module,
        /* condition= */ left_le_right.clone(module),
        /* ifTrue=    */ if_0,
        /* ifFalse=   */ nullptr // no-op
    );
    WasmTemporary if_2 = BinaryenIf(
        /* module=    */ module,
        /* condition= */ left_le_right.clone(module),
        /* ifTrue=    */ BinaryenNop(module), // no-op
        /* ifFalse=   */ swap_left_right.finalize()
    );
    WasmTemporary if_3 = BinaryenIf(
        /* module=    */ module,
        /* condition= */ mid_le_right.clone(module),
        /* ifTrue=    */ if_2,
        /* ifFalse=   */ swap_left_mid.finalize()
    );
    WasmTemporary if_4 = BinaryenIf(
        /* module=    */ module,
        /* condition= */ left_le_mid,
        /* ifTrue=    */ if_1,
        /* ifFalse=   */ if_3
    );
    loop += std::move(if_4);

    /*----- Partition range begin + 1 to end using begin as pivot. ---------------------------------------------------*/
    WasmTemporary begin_plus_one = BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ begin,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(tuple.size()))
    );
    WasmPartitionBranchless partition;
    WasmTemporary pos_partition = partition.emit(
        /* module=      */ module,
        /* fn=          */ fn,
        /* block=       */ loop,
        /* struc=       */ tuple,
        /* order=       */ order,
        /* b_begin=     */ std::move(begin_plus_one),
        /* b_end=       */ end,
        /* b_pivot=     */ begin
    );
    loop += mid.set(std::move(pos_partition));

    /*----- Patch mid pointer, if necessary. -------------------------------------------------------------------------*/
    WasmTemporary mid_minus_one = BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ mid,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(-tuple.size()))
    );
    wasm_swap.emit(loop, tuple, begin, mid_minus_one.clone(module));

    /*----- Recurse right, if necessary. -----------------------------------------------------------------------------*/
    {
        WasmTemporary delta_right = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenSubInt32(),
            /* left=   */ end,
            /* right=  */ mid
        );
        WasmTemporary recurse_right_cond = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenGeSInt32(),
            /* left=   */ delta_right,
            /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(2 * tuple.size()))
        );
        BinaryenExpressionRef args[] = { mid, end };
        WasmTemporary recurse_right = BinaryenCall(
            /* module=      */ module,
            /* target=      */ fn_name.c_str(),
            /* operands=    */ args,
            /* numOperands= */ 2,
            /* returnType=  */ BinaryenTypeNone()
        );
        loop += BinaryenIf(
            /* module=    */ module,
            /* condition= */ recurse_right_cond,
            /* ifTrue=    */ recurse_right,
            /* ifFalse=   */ nullptr
        );
    }

    /*----- Update end pointer. --------------------------------------------------------------------------------------*/
    loop += end.set(std::move(mid_minus_one));

    /*----- Emit loop. -----------------------------------------------------------------------------------------------*/
    fn.block() += loop.finalize();

    /*----- Handle the case where end - begin == 2. ------------------------------------------------------------------*/
    {
        BlockBuilder block_swap(module);
        wasm_swap.emit(block_swap, tuple, begin, last.clone(module));

        BlockBuilder block_compare(module);
        auto load_context_first  = tuple.create_load_context(begin);
        auto load_context_second = tuple.create_load_context(last.clone(module));
        WasmTemporary compare = comparator.emit(fn, block_compare, load_context_first, load_context_second);
        WasmTemporary cond_swap = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenGtSInt32(),
            /* left=   */ compare,
            /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(0))
        );
        block_compare += BinaryenIf(
            /* module=    */ module,
            /* condition= */ cond_swap,
            /* ifTrue=    */ block_swap.finalize(),
            /* ifFalse=   */ nullptr
        );

        WasmTemporary cond = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenEqInt32(),
            /* left=   */ delta.clone(module),
            /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(2 * tuple.size()))
        );
        fn.block() += BinaryenIf(
            /* module=    */ module,
            /* condition= */ cond,
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

WasmTemporary WasmBitMixMurmur3::emit(BinaryenModuleRef module, FunctionBuilder &fn, BlockBuilder &block,
                                      WasmTemporary the_bits) const
{
    /* Taken from https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp by Austin Appleby.  We use the
     * optimized constants found by David Stafford, in particular the values for `Mix01`, as reported at
     * http://zimbry.blogspot.com/2011/09/better-bit-mixing-improving-on.html. */
    insist(the_bits.type() == BinaryenTypeInt64(), "WasmBitMix expects a 64-bit integer");

    WasmVariable v(fn, BinaryenTypeInt64());
    WasmVariable bits(fn, BinaryenTypeInt64());
    block += bits.set(std::move(the_bits));

    /* v = v ^ (v >> 31) */
    {
        WasmTemporary Shr = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenShrUInt64(),
            /* left=   */ bits,
            /* right=  */ BinaryenConst(module, BinaryenLiteralInt64(31))
        );
        block += v.set(BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenXorInt64(),
            /* left=   */ bits,
            /* right=  */ Shr
        ));
    }

    /* v = v * 0x7fb5d329728ea185ULL */
    block += v.set(BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenMulInt64(),
        /* left=   */ v,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt64(0x7fb5d329728ea185ULL))
    ));

    /* v = v ^ (v >> 27) */
    {
        WasmTemporary Shr = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenShrUInt64(),
            /* left=   */ v,
            /* right=  */ BinaryenConst(module, BinaryenLiteralInt64(27))
        );
        block += v.set(BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenXorInt64(),
            /* left=   */ v,
            /* right=  */ Shr
        ));
    }

    /* v = v * 0x81dadef4bc2dd44dULL */
    block += v.set(BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenMulInt64(),
        /* left=   */ v,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt64(0x81dadef4bc2dd44dULL))
    ));

    /* v = v ^ (v >> 33) */
    {
        WasmTemporary Shr = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenShrUInt64(),
            /* left=   */ v,
            /* right=  */ BinaryenConst(module, BinaryenLiteralInt64(33))
        );
        block += v.set(BinaryenBinary(
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

WasmTemporary WasmHashMumur3_64A::emit(BinaryenModuleRef module, FunctionBuilder &fn, BlockBuilder &block,
                                       const std::vector<WasmTemporary> &values) const
{
    /* Inspired by https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp by Austin Appleby.  We use
     * constants from MurmurHash2_64 as reported on https://sites.google.com/site/murmurhash/. */
    insist(values.size() != 0, "cannot compute the hash of an empty sequence of values");

    if (values.size() == 1) {
        /* In case of a single 64-bit value, we just run the bit mixer. */
        return WasmBitMixMurmur3{}.emit(module, fn, block, reinterpret(module, values[0].clone(module), BinaryenTypeInt64()));
    }

    if (values.size() == 2) {
        WasmTemporary left  = values[0].clone(module);
        WasmTemporary right = values[1].clone(module);

        std::size_t combined_size = 0;
        combined_size += (left.type()  == BinaryenTypeInt32() or left.type()  == BinaryenTypeFloat32()) ? 32 : 64;
        combined_size += (right.type() == BinaryenTypeInt32() or right.type() == BinaryenTypeFloat32()) ? 32 : 64;

        if (combined_size <= 64) {
            WasmTemporary shl = BinaryenBinary(
                module,
                BinaryenShlInt64(),
                reinterpret(module, std::move(left), BinaryenTypeInt64()),
                BinaryenConst(module, BinaryenLiteralInt64(32))
            );
            WasmTemporary combined = BinaryenBinary(
                module,
                BinaryenOrInt64(),
                shl,
                reinterpret(module, std::move(right), BinaryenTypeInt64())
            );
            return WasmBitMixMurmur3{}.emit(module, fn, block, std::move(combined));
        }
    }

#if 0
    /* Compute combined size of values. */
    std::size_t combined_size = 0;
    for (auto v : values) {
        auto ty = BinaryenExpressionGetType(v);
        if (ty == BinaryenTypeInt32() or ty == BinaryenTypeFloat32())
            combined_size += 4;
        else
            combined_size += 8;
    }
#endif

    /* TODO: When the combined values fit into a qword, combine them first before hashing. */

    /* General Murmur3. */
    WasmVariable h(fn, BinaryenTypeInt64());
    const auto m = BinaryenConst(module, BinaryenLiteralInt64(0xc6a4a7935bd1e995LLU));

    /*  uint64_t h = seed ^ (len * m) */
    block += h.set(BinaryenConst(module, BinaryenLiteralInt64(0xc6a4a7935bd1e995LLU * values.size())));

    WasmVariable k(fn, BinaryenTypeInt64());
    for (auto &val : values) {
        block += k.set(reinterpret(module, val.clone(module), BinaryenTypeInt64()));

        /* k = k * m */
        block += k.set(BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenMulInt64(),
            /* left=   */ k,
            /* right=  */ m
        ));

        /* k = ROTL32(k, 47); */
        block += k.set(BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenRotLInt64(),
            /* left=   */ k,
            /* right=  */ BinaryenConst(module, BinaryenLiteralInt64(47))
        ));

        /* k = k * m */
        block += k.set(BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenMulInt64(),
            /* left=   */ k,
            /* right=  */ m
        ));

        /* h = h ^ k; */
        block += h.set(BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenXorInt64(),
            /* left=   */ h,
            /* right=  */ k
        ));

        /* h = ROTL32(h, 45); */
        block += h.set(BinaryenBinary(
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
            block += h.set(BinaryenBinary(
                /* module= */ module,
                /* op=     */ BinaryenAddInt64(),
                /* left=   */ Mul,
                /* right=  */ BinaryenConst(module, BinaryenLiteralInt64(0xe6546b64ULL))
            ));
        }
    }

    /* h = h ^ len */
    block += h.set(BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenXorInt64(),
        /* left=   */ h,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt64(values.size()))
    ));

    return WasmBitMixMurmur3{}.emit(module, fn, block, h);
}


/*======================================================================================================================
 * WasmRefCountingHashTable
 *====================================================================================================================*/

WasmTemporary WasmRefCountingHashTable::create_table(BlockBuilder &block, WasmTemporary addr,
                                                     std::size_t num_buckets) const
{
    num_buckets = ceil_to_pow_2(num_buckets);
    insist(num_buckets <= 1UL << 31, "initial capacity exceeds uint32 type");
    block += addr_.set(std::move(addr));
    block += mask_.set(BinaryenConst(module, BinaryenLiteralInt32(num_buckets - 1)));

    /*----- Return end of hash table. --------------------------------------------------------------------------------*/
    return BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ addr_,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(num_buckets * entry_size()))
    );
}

void WasmRefCountingHashTable::clear_table(BlockBuilder &block,
                                           WasmTemporary begin, WasmTemporary end) const
{
    WasmVariable induction(fn, BinaryenTypeInt32());
    block += induction.set(std::move(begin));

    WasmWhile loop(module, "clear_table.loop", BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenLtUInt32(),
        /* left=   */ induction,
        /* right=  */ std::move(end)
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
    loop += induction.set(BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ induction,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(entry_size_))
    ));

    block += loop.finalize();
}

WasmTemporary WasmRefCountingHashTable::hash_to_bucket(WasmTemporary hash) const
{
    WasmTemporary bucket_index = BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenAndInt32(),
        /* left=   */ hash,
        /* right=  */ mask_
    );
    WasmTemporary bucket_offset = BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenMulInt32(),
        /* left=   */ bucket_index,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(entry_size()))
    );
    return BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ addr_,
        /* right=  */ bucket_offset
    );
}

std::pair<WasmTemporary, WasmTemporary>
WasmRefCountingHashTable::find_in_bucket(BlockBuilder &block, WasmTemporary bucket_addr,
                                         const std::vector<WasmTemporary> &key) const
{
    insist(key.size() <= struc.schema.num_entries(), "incorrect number of key values");
    WasmLoop loop(module, "find_in_bucket.loop");

    /*----- Create local runner . ------------------------------------------------------------------------------------*/
    WasmVariable runner(fn, BinaryenTypeInt32());
    block += runner.set(std::move(bucket_addr));

    WasmTemporary table_size = BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ mask_,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(1))
    );

    WasmVariable table_size_in_bytes(fn, BinaryenTypeInt32());
    block += table_size_in_bytes.set(BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenMulInt32(),
        /* left=   */ table_size,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(entry_size_))
    ));

    /*----- Initialize step counter. ---------------------------------------------------------------------------------*/
    WasmVariable step(fn, BinaryenTypeInt32());
    block += step.set(BinaryenConst(module, BinaryenLiteralInt32(entry_size_)));

    /*----- Advance to next slot. ------------------------------------------------------------------------------------*/
    BlockBuilder advance(module, "find_in_bucket.loop.body.step");
    {
        WasmVariable runner_inc(fn, BinaryenTypeInt32());
        advance += runner_inc.set(BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ runner,
            /* right=  */ step
        ));
        WasmTemporary runner_wrapped = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenSubInt32(),
            /* left=   */ runner_inc,
            /* right=  */ table_size_in_bytes
        );
        WasmTemporary table_end = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ addr_,
            /* right=  */ table_size_in_bytes
        );
        WasmTemporary is_overflow = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenGeUInt32(),
            /* left=   */ runner_inc,
            /* right=  */ table_end
        );
        advance += runner.set(BinaryenSelect(
            /* module=    */ module,
            /* condition= */ is_overflow,
            /* ifTrue=    */ runner_wrapped,
            /* ifFalse=   */ runner_inc,
            /* type=      */ BinaryenTypeInt32()
        ));

        advance += step.set(BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ step,
            /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(entry_size_))
        ));
        advance += loop.continu();
    }

    /*----- Get reference count from bucket. -------------------------------------------------------------------------*/
    WasmTemporary ref_count = BinaryenLoad(
        /* module= */ module,
        /* bytes=  */ REFERENCE_SIZE,
        /* signed= */ false,
        /* offset= */ 0,
        /* align=  */ 0,
        /* type=   */ BinaryenTypeInt32(),
        /* ptr=    */ runner
    );

    /*----- Check whether bucket is occupied. ------------------------------------------------------------------------*/
    WasmTemporary is_occupied = BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenNeInt32(),
        /* left=   */ ref_count,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(0))
    );

    /*----- Compare keys. --------------------------------------------------------------------------------------------*/
    WasmTemporary keys_not_equal;
    {
        auto ld_key = struc.create_load_context(runner, /* offset= */ REFERENCE_SIZE);
        std::size_t idx = 0;
        for (auto &part_of_key : key) {
            auto &e = struc.schema[idx++];
            WasmTemporary key_in_bucket = ld_key[e.id];
            WasmTemporary cmp = WasmCompare::Ne(module, *e.type, std::move(key_in_bucket), part_of_key.clone(module));

            if (keys_not_equal.is()) {
                keys_not_equal = BinaryenBinary(
                    /* module= */ module,
                    /* op=     */ BinaryenOrInt32(),
                    /* left=   */ keys_not_equal,
                    /* right=  */ cmp
                );
            } else {
                keys_not_equal = std::move(cmp);
            }
        }
    }
    insist(keys_not_equal.is());

    WasmTemporary if_key_equal = BinaryenIf(
        /* module=    */ module,
        /* condition= */ keys_not_equal,
        /* ifTrue=    */ advance.finalize(), // key not equal, continue search
        /* ifFalse=   */ nullptr // hit, break loop
    );
    loop += BinaryenIf(
        /* module=    */ module,
        /* condition= */ is_occupied,
        /* ifTrue=    */ if_key_equal, // slot occupied, compare key
        /* ifFalse=   */ nullptr // miss, break loop
    );
    block += loop.finalize();

    return std::make_pair(runner.get(), step.get());
}

WasmTemporary WasmRefCountingHashTable::is_slot_empty(WasmTemporary slot_addr) const
{
    return BinaryenUnary(
        /* module= */ module,
        /* op=     */ BinaryenEqZInt32(),
        /* value=  */ get_bucket_ref_count(std::move(slot_addr))
    );
}

WasmTemporary WasmRefCountingHashTable::compare_key(WasmTemporary slot_addr,
                                                    const std::vector<Schema::Identifier> &IDs,
                                                    const std::vector<WasmTemporary> &key) const
{
    insist(not IDs.empty());
    insist(IDs.size() == key.size());

    WasmTemporary keys_equal;
    {
        auto ld_key = struc.create_load_context(std::move(slot_addr), /* offset= */ REFERENCE_SIZE);
        auto key_it = key.begin();
        for (auto id : IDs) {
            auto ty = struc.schema[id].second.type;
            WasmTemporary key_in_bucket = ld_key[id];
            WasmTemporary cmp = WasmCompare::Eq(module, *ty, std::move(key_in_bucket), key_it++->clone(module));

            if (keys_equal.is()) {
                keys_equal = BinaryenBinary(
                    /* module= */ module,
                    /* op=     */ BinaryenAndInt32(),
                    /* left=   */ keys_equal,
                    /* right=  */ cmp
                );
            } else {
                keys_equal = std::move(cmp);
            }
        }
    }
    return keys_equal;
}

void WasmRefCountingHashTable::emplace(BlockBuilder &block, WasmTemporary bucket_addr, WasmTemporary steps,
                                       WasmTemporary slot_addr, const std::vector<Schema::Identifier> &IDs,
                                       const std::vector<WasmTemporary> &key) const
{
    insist(key.size() <= struc.schema.num_entries(), "incorrect number of key values");
    insist(IDs.size() == key.size(), "number of identifiers and length of key must be equal");

    /*----- Update bucket probe length. ------------------------------------------------------------------------------*/
    block += BinaryenStore(
        /* module= */ module,
        /* bytes=  */ REFERENCE_SIZE,
        /* offset= */ 0,
        /* align=  */ 0,
        /* ptr=    */ std::move(bucket_addr),
        /* value=  */ std::move(steps),
        /* type=   */ BinaryenTypeInt32()
    );

    /*----- Set slot as occupied. ------------------------------------------------------------------------------------*/
    block += BinaryenStore(
        /* module= */ module,
        /* bytes=  */ REFERENCE_SIZE,
        /* offset= */ 0,
        /* align=  */ 0,
        /* ptr=    */ slot_addr.clone(module),
        /* value=  */ BinaryenConst(module, BinaryenLiteralInt32(entry_size_)),
        /* type=   */ BinaryenTypeInt32()
    );

    /*----- Write key to slot. ---------------------------------------------------------------------------------------*/
    auto id = IDs.begin();
    auto key_it = key.begin();
    while (id != IDs.end())
        block += struc.store(slot_addr.clone(module), *id++, key_it++->clone(module), /* offset= */ REFERENCE_SIZE);
}

WasmCGContext WasmRefCountingHashTable::load_from_slot(WasmTemporary slot_addr) const
{
    return struc.create_load_context(std::move(slot_addr), /* offset= */ REFERENCE_SIZE);
}

WasmTemporary WasmRefCountingHashTable::store_value_to_slot(WasmTemporary slot_addr,
                                                            Schema::Identifier id,
                                                            WasmTemporary value) const
{
    return struc.store(std::move(slot_addr), id, std::move(value), /* offset= */ REFERENCE_SIZE);
}

WasmTemporary WasmRefCountingHashTable::compute_next_slot(WasmTemporary slot_addr) const
{
    return BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ slot_addr,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(entry_size_))
    );
}

WasmTemporary WasmRefCountingHashTable::get_bucket_ref_count(WasmTemporary bucket_addr) const
{
    return BinaryenLoad(
        /* module= */ module,
        /* bytes=  */ REFERENCE_SIZE,
        /* signed= */ false,
        /* offset= */ 0,
        /* align=  */ 0,
        /* type=   */ BinaryenTypeInt32(),
        /* ptr=    */ bucket_addr
    );
}

WasmTemporary WasmRefCountingHashTable::insert_with_duplicates(BlockBuilder &block,
                                                               WasmTemporary hash,
                                                               const std::vector<Schema::Identifier> &IDs,
                                                               const std::vector<WasmTemporary> &key)
    const
{
#if 0
    {
        BinaryenExpressionRef args[] = { BinaryenConst(module, BinaryenLiteralInt32(0xABCD)) };
        block += BinaryenCall(module, "print", args, 1, BinaryenTypeNone());
    }
#endif

    /*----- Compute index and address of bucket. ---------------------------------------------------------------------*/
    WasmVariable bucket_index(fn, BinaryenTypeInt32());
    block += bucket_index.set(BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenAndInt32(),
        /* left=   */ std::move(hash),
        /* right=  */ mask()
    ));
    WasmTemporary bucket_offset = BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenMulInt32(),
        /* left=   */ bucket_index,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(entry_size()))
    );
    WasmVariable bucket_addr(fn, BinaryenTypeInt32());
    block += bucket_addr.set(BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ addr_,
        /* right=  */ bucket_offset
    ));

    /*----- Load steps in bucket. ------------------------------------------------------------------------------------*/
    WasmVariable steps(fn, BinaryenTypeInt32());
    block += steps.set(get_bucket_ref_count(bucket_addr));

    WasmVariable slot_addr(fn, BinaryenTypeInt32());
    BlockBuilder bucket_create(module, "insert.create_bucket");
    BlockBuilder bucket_insert(module, "insert.append_to_bucket");

    /*----- If the bucket is empty, initialize it. -------------------------------------------------------------------*/
    bucket_create += slot_addr.set(bucket_addr);

    /*----- Compute end of hash table. -------------------------------------------------------------------------------*/
    WasmVariable table_size_in_bytes(fn, BinaryenTypeInt32());
    {
        WasmTemporary size = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ mask(),
            /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(1))
        );
        bucket_insert += table_size_in_bytes.set(BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenMulInt32(),
            /* left=   */ size,
            /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(entry_size()))
        ));
    }

    {
        /*----- Evaluate formula for Gaussian sum to compute the end of the bucket -----------------------------------*/
        WasmVariable probe_len(fn, BinaryenTypeInt32());
        bucket_insert += probe_len.set(BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenDivUInt32(),
            /* left=   */ steps,
            /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(entry_size()))
        ));
        WasmTemporary square = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenMulInt32(),
            /* left=   */ probe_len,
            /* right=  */ probe_len
        );
        WasmTemporary sum = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ square,
            /* right=  */ probe_len
        );
        WasmTemporary distance = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenShrUInt32(),
            /* left=   */ sum,
            /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(1))
        );
        WasmTemporary slot_index = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ bucket_index,
            /* right=  */ distance
        );
        WasmTemporary slot_index_wrapped = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenAndInt32(),
            /* left=   */ slot_index,
            /* right=  */ mask()
        );
        WasmTemporary slot_offset_in_bytes = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenMulInt32(),
            /* left=   */ slot_index_wrapped,
            /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(entry_size()))
        );
        bucket_insert += slot_addr.set(BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ addr(),
            /* right=  */ slot_offset_in_bytes
        ));
    }

    /*----- Find next free slot in bucket. ---------------------------------------------------------------------------*/
    WasmWhile find_slot(module, "insert.append_to_bucket.find_slot", get_bucket_ref_count(slot_addr));
    {
        find_slot += steps.set(BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ steps,
            /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(entry_size()))
        ));
        WasmVariable slot_addr_inc(fn, BinaryenTypeInt32());
        find_slot += slot_addr_inc.set(BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ slot_addr,
            /* right=  */ steps
        ));
        WasmTemporary table_end = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ addr(),
            /* right=  */ table_size_in_bytes
        );
        WasmTemporary exceeds_table = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenGeUInt32(),
            /* left=   */ slot_addr_inc,
            /* right=  */ table_end
        );
        WasmTemporary slot_addr_wrapped = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenSubInt32(),
            /* left=   */ slot_addr_inc,
            /* right=  */ table_size_in_bytes
        );
        find_slot += slot_addr.set(BinaryenSelect(
            /* module=    */ module,
            /* condition= */ exceeds_table,
            /* ifTrue=    */ slot_addr_wrapped,
            /* ifFalse=   */ slot_addr_inc,
            /* type=      */ BinaryenTypeInt32()
        ));
    }
    bucket_insert += find_slot.finalize();

    bucket_insert += steps.set(BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ steps,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(entry_size()))
    ));

    /*----- Create conditional jump based on whether the bucket is empty. --------------------------------------------*/
    block += BinaryenIf(
        /* module=    */ module,
        /* condition= */ get_bucket_ref_count(bucket_addr),
        /* ifTrue=    */ bucket_insert.finalize(),
        /* ifFalse=   */ bucket_create.finalize()
    );

#if 0
    {
        BinaryenExpressionRef args[] = { slot_addr };
        block += BinaryenCall(module, "print", args, 1, BinaryenTypeNone());
    }
    {
        BinaryenExpressionRef args[] = { BinaryenConst(module, BinaryenLiteralInt32(0xDCBA)) };
        block += BinaryenCall(module, "print", args, 1, BinaryenTypeNone());
    }
#endif

    /*----- After finding the next free slot in the bucket, place the key in that slot. ------------------------------*/
    emplace(block, bucket_addr, steps, slot_addr, IDs, key);
    return slot_addr;
}

WasmTemporary WasmRefCountingHashTable::insert_without_duplicates(BlockBuilder &block,
                                                                  WasmTemporary hash,
                                                                  const std::vector<Schema::Identifier> &IDs,
                                                                  const std::vector<WasmTemporary> &key)
    const
{
    /*----- Compute address of bucket. -------------------------------------------------------------------------------*/
    WasmVariable bucket_addr(fn, BinaryenTypeInt32());
    block += bucket_addr.set(hash_to_bucket(std::move(hash)));

    /*----- Locate entry with key `key` in the bucket, or end of bucket if no such key exists. -----------------------*/
    auto [ slot_addr_found, steps_found ] = find_in_bucket(block, bucket_addr, key);

    BlockBuilder insert(module, "insert.new_entry");

    /*----- Create new entry in new hash table. ----------------------------------------------------------------------*/
    emplace(insert, bucket_addr, std::move(steps_found), slot_addr_found.clone(module), IDs, key);

    /*----- Create conditional branch depending on whether `key` already exists in the bucket. -----------------------*/
    block += BinaryenIf(
        /* module=    */ module,
        /* condition= */ is_slot_empty(slot_addr_found.clone(module)),
        /* ifTrue=    */ insert.finalize(),
        /* ifFalse=   */ nullptr
    );

    return std::move(slot_addr_found);
}

BinaryenFunctionRef WasmRefCountingHashTable::rehash(WasmHash &hasher,
                                                     const std::vector<Schema::Identifier> &key_ids,
                                                     const std::vector<Schema::Identifier> &payload_ids) const
{
    if (fn_rehash_) return fn_rehash_;

    std::ostringstream oss;
    oss << "WasmRefCountingHashTable::rehash_keys";
    for (auto k : key_ids)
        oss << '_' << k;
    if (not payload_ids.empty()) {
        oss << "_payload";
        for (auto p : payload_ids)
            oss << '_' << p;
    }
    const std::string name = oss.str();

    /*----- Create rehashing function. -------------------------------------------------------------------------------*/
    std::vector<BinaryenType> fn_rehash_params;
    fn_rehash_params.reserve(4);
    fn_rehash_params.push_back(BinaryenTypeInt32()); // 0: old addr
    fn_rehash_params.push_back(BinaryenTypeInt32()); // 1: old mask
    fn_rehash_params.push_back(BinaryenTypeInt32()); // 2: new addr
    fn_rehash_params.push_back(BinaryenTypeInt32()); // 3: new mask
    FunctionBuilder fn_rehash(module, name.c_str(), BinaryenTypeNone(), fn_rehash_params);
    WasmRefCountingHashTable HT_old(
        /* module= */ module,
        /* fn=     */ fn_rehash,
        /* block=  */ fn_rehash.block(),
        /* struc=  */ struc,
        /* b_addr= */ BinaryenLocalGet(module, 0, BinaryenTypeInt32()),
        /* b_mask= */ BinaryenLocalGet(module, 1, BinaryenTypeInt32())
    );

    WasmRefCountingHashTable HT_new(
        /* module= */ module,
        /* fn=     */ fn_rehash,
        /* block=  */ fn_rehash.block(),
        /* struc=  */ struc,
        /* b_addr= */ BinaryenLocalGet(module, 2, BinaryenTypeInt32()),
        /* b_mask= */ BinaryenLocalGet(module, 3, BinaryenTypeInt32())
    );

    /*----- Compute properties of old hash table. --------------------------------------------------------------------*/
    WasmVariable table_end_old(fn_rehash, BinaryenTypeInt32());
    {
        WasmTemporary mask_old = BinaryenLocalGet(
            /* module= */ module,
            /* index=  */ 1,
            /* type=   */ BinaryenTypeInt32()
        );
        WasmTemporary size_old = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ mask_old,
            /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(1))
        );
        WasmTemporary size_in_bytes_old = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenMulInt32(),
            /* left=   */ size_old,
            /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(HT_old.entry_size()))
        );
        fn_rehash.block() += table_end_old.set(BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ HT_old.addr(),
            /* right=  */ size_in_bytes_old
        ));
    }

    /*----- Initialize a runner to traverse the old hash table. ------------------------------------------------------*/
    WasmVariable runner(fn_rehash, BinaryenTypeInt32());
    fn_rehash.block() += runner.set(HT_old.addr());

    /*----- Advance to first occupied slot. --------------------------------------------------------------------------*/
    {
        WasmLoop next(module, "rehash.advance_to_first");
        next += runner.set(HT_old.compute_next_slot(runner));

        WasmTemporary in_bounds = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenLtUInt32(),
            /* left=   */ runner,
            /* right=  */ table_end_old
        );
        next += BinaryenIf(
            /* module=    */ module,
            /* condition= */ in_bounds,
            /* ifTrue=    */ next.continu(HT_old.is_slot_empty(runner)),
            /* ifFalse=   */ nullptr
        );

        fn_rehash.block() += BinaryenIf(
            /* module=    */ module,
            /* condition= */ HT_old.is_slot_empty(runner),
            /* ifTrue=    */ next.finalize(),
            /* ifFalse=   */ nullptr
        );
    }

    /*----- Iterate over all entries in the old hash table. ----------------------------------------------------------*/
    WasmDoWhile for_each(module, "rehash.for_each", BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenLtUInt32(),
        /* left=   */ runner,
        /* right=  */ table_end_old
    ));

    /*----- Re-insert the current element in the new hash table. -----------------------------------------------------*/
    {
        /* NOTE: Invariant is that on entry to the loop body, runner points to an occupied slot. */

        /*----- Get key. ---------------------------------------------------------------------------------------------*/
        auto ld = HT_old.load_from_slot(runner);
        std::vector<WasmTemporary> key;
        for (auto kid : key_ids)
            key.emplace_back(ld.get_value(kid));

        /*----- Compute hash. ----------------------------------------------------------------------------------------*/
        WasmTemporary hash = hasher.emit(module, fn_rehash, for_each, key);
        WasmTemporary hash_i32 = BinaryenUnary(
            /* module= */ module,
            /* op=     */ BinaryenWrapInt64(),
            /* value=  */ hash
        );

        /*----- Re-insert the element. -------------------------------------------------------------------------------*/
        WasmTemporary slot_addr = HT_new.insert_with_duplicates(for_each, std::move(hash_i32), key_ids, key);

        /*---- Write payload. ----------------------------------------------------------------------------------------*/
        for (auto pid : payload_ids)
            for_each += HT_new.store_value_to_slot(slot_addr.clone(module), pid, ld.get_value(pid));
    }

    /*----- Advance to next occupied slot. ---------------------------------------------------------------------------*/
    {
        WasmLoop next(module, "rehash.for_each.advance");
        next += runner.set(HT_old.compute_next_slot(runner));

        WasmTemporary in_bounds = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenLtUInt32(),
            /* left=   */ runner,
            /* right=  */ table_end_old
        );
        WasmTemporary slot_is_empty = HT_old.is_slot_empty(runner);

        next += BinaryenIf(
            /* module=    */ module,
            /* condition= */ in_bounds,
            /* ifTrue=    */ next.continu(std::move(slot_is_empty)),
            /* ifFalse=   */ nullptr
        );
        for_each += next.finalize();
    }

    fn_rehash.block() += for_each.finalize();
    return fn_rehash_ = fn_rehash.finalize();
}

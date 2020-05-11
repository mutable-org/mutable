#include "backend/WasmAlgo.hpp"

#include <sstream>
#include <string>
#include <unordered_map>


using namespace db;


/*======================================================================================================================
 * WasmPartitionBranching
 *====================================================================================================================*/

BinaryenExpressionRef WasmPartitionBranching::emit(BinaryenModuleRef module, FunctionBuilder &fn, BlockBuilder &block,
                                                   const Schema &schema, const std::vector<order_type> &order,
                                                   BinaryenExpressionRef b_begin, BinaryenExpressionRef b_end,
                                                   BinaryenExpressionRef b_pivot) const
{
    (void) module;
    (void) fn;
    (void) block;
    (void) schema;
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
                                                    const Schema &schema, const std::vector<order_type> &order,
                                                    BinaryenExpressionRef b_begin, BinaryenExpressionRef b_end,
                                                    BinaryenExpressionRef b_pivot) const
{
    const char *loop_name = "partition_branchless";
    const char *body_name = "partition_branchless.body";
    BlockBuilder loop_body(module, body_name);
    std::unordered_map<BinaryenType, BinaryenExpressionRef> swap_temp;

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

    WasmStruct tuple(module, schema);

    /*----- Offset end by one tuple.  (This can be dropped when negative offsets are supported.) ---------------------*/
    auto b_last = BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ b_end,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(-tuple.size()))
    );

    /*----- Create load context for left, right, and pivot. ----------------------------------------------------------*/
    auto load_context_left  = tuple.create_load_context(b_begin);
    auto load_context_right = tuple.create_load_context(b_last);
    auto load_context_pivot = tuple.create_load_context(b_pivot);
    WasmCGContext value_context_pivot(module);

    for (auto &attr : schema) {
        BinaryenType b_attr_type = get_binaryen_type(attr.type);

        /*----- Load value from pivot. -------------------------------------------------------------------------------*/
        auto b_tmp_pivot = fn.add_local(b_attr_type);
        block += BinaryenLocalSet(
            /* module= */ module,
            /* index=  */ BinaryenLocalGetGetIndex(b_tmp_pivot),
            /* value=  */ load_context_pivot.get_value(attr.id)
        );
        value_context_pivot.add(attr.id, b_tmp_pivot);

        /*----- Swap attribute of left and right tuple. --------------------------------------------------------------*/
        /* Introduce temporary for the swap. */
        auto it = swap_temp.find(b_attr_type);
        if (it == swap_temp.end())
            it = swap_temp.emplace_hint(it, b_attr_type, fn.add_local(b_attr_type));
        auto b_swap = it->second;

        /* tmp = *(end-1) */
        loop_body += BinaryenLocalSet(
            /* module= */ module,
            /* index=  */ BinaryenLocalGetGetIndex(b_swap),
            /* value=  */ load_context_right.get_value(attr.id)
        );
        /* *(end-1) = *begin */
        loop_body += tuple.store(b_last, attr.id, load_context_left.get_value(attr.id));
        /* *begin = tmp */
        loop_body += tuple.store(b_begin, attr.id, b_swap);
    }

    /* Compare left and right tuples to pivot. */
    BinaryenExpressionRef b_left_is_ok = nullptr;
    BinaryenExpressionRef b_right_is_ok = nullptr;
    for (auto &o : order) {
        auto b_expr_pivot = value_context_pivot.compile(*o.first);

        /*----- Compare left to pivot. -------------------------------------------------------------------------------*/
        {
            BinaryenExpressionRef b_cmp_left;
            auto b_expr_left = load_context_left.compile(*o.first);
            auto n = as<const Numeric>(o.first->type());
            switch (n->kind) {
                case Numeric::N_Int:
                case Numeric::N_Decimal:
                    if (n->size() <= 32)
                        b_cmp_left = BinaryenBinary(
                            /* module= */ module,
                            /* op=     */ o.second ? BinaryenLtSInt32() : BinaryenGtSInt32(),
                            /* left=   */ b_expr_left,
                            /* right=  */ b_expr_pivot
                        );
                    else
                        b_cmp_left = BinaryenBinary(
                            /* module= */ module,
                            /* op=     */ o.second ? BinaryenLtSInt64() : BinaryenGtSInt64(),
                            /* left=   */ b_expr_left,
                            /* right=  */ b_expr_pivot
                        );
                    break;

                case Numeric::N_Float:
                    if (n->size() == 32)
                        b_cmp_left = BinaryenBinary(
                            /* module= */ module,
                            /* op=     */ o.second ? BinaryenLtFloat32() : BinaryenGtFloat32(),
                            /* left=   */ b_expr_left,
                            /* right=  */ b_expr_pivot
                        );
                    else
                        b_cmp_left = BinaryenBinary(
                            /* module= */ module,
                            /* op=     */ o.second ? BinaryenLtFloat64() : BinaryenGtFloat64(),
                            /* left=   */ b_expr_left,
                            /* right=  */ b_expr_pivot
                        );
                    break;
            }
            if (b_left_is_ok) {
                auto b_left_is_ok_upd = BinaryenBinary(
                    /* module= */ module,
                    /* op=     */ BinaryenAndInt32(),
                    /* left=   */ b_left_is_ok,
                    /* right=  */ b_cmp_left
                );
                loop_body += BinaryenLocalSet(
                    /* module= */ module,
                    /* index=  */ BinaryenLocalGetGetIndex(b_left_is_ok),
                    /* value=  */ b_left_is_ok_upd
                );
            } else {
                b_left_is_ok = fn.add_local(BinaryenTypeInt32());
                loop_body += BinaryenLocalSet(
                    /* module= */ module,
                    /* index=  */ BinaryenLocalGetGetIndex(b_left_is_ok),
                    /* value=  */ b_cmp_left
                );
            }
        }

        /*----- Compare right to pivot. ------------------------------------------------------------------------------*/
        {
            BinaryenExpressionRef b_cmp_right;
            auto b_expr_right = load_context_right.compile(*o.first);
            auto n = as<const Numeric>(o.first->type());
            switch (n->kind) {
                case Numeric::N_Int:
                case Numeric::N_Decimal:
                    if (n->size() <= 32)
                        b_cmp_right = BinaryenBinary(
                            /* module= */ module,
                            /* op=     */ o.second ? BinaryenLeSInt32() : BinaryenGeSInt32(),
                            /* left=   */ b_expr_pivot,
                            /* right=  */ b_expr_right
                        );
                    else
                        b_cmp_right = BinaryenBinary(
                            /* module= */ module,
                            /* op=     */ o.second ? BinaryenLeSInt64() : BinaryenGeSInt64(),
                            /* left=   */ b_expr_pivot,
                            /* right=  */ b_expr_right
                        );
                    break;

                case Numeric::N_Float:
                    if (n->size() == 32)
                        b_cmp_right = BinaryenBinary(
                            /* module= */ module,
                            /* op=     */ o.second ? BinaryenLeFloat32() : BinaryenGeFloat32(),
                            /* left=   */ b_expr_pivot,
                            /* right=  */ b_expr_right
                        );
                    else
                        b_cmp_right = BinaryenBinary(
                            /* module= */ module,
                            /* op=     */ o.second ? BinaryenLeFloat64() : BinaryenGeFloat64(),
                            /* left=   */ b_expr_pivot,
                            /* right=  */ b_expr_right
                        );
                    break;
            }
            if (b_right_is_ok) {
                auto b_right_is_ok_upd = BinaryenBinary(
                    /* module= */ module,
                    /* op=     */ BinaryenAndInt32(),
                    /* left=   */ b_right_is_ok,
                    /* right=  */ b_cmp_right
                );
                loop_body += BinaryenLocalSet(
                    /* module= */ module,
                    /* index=  */ BinaryenLocalGetGetIndex(b_right_is_ok),
                    /* value=  */ b_right_is_ok_upd
                );
            } else {
                b_right_is_ok = fn.add_local(BinaryenTypeInt32());
                loop_body += BinaryenLocalSet(
                    /* module= */ module,
                    /* index=  */ BinaryenLocalGetGetIndex(b_right_is_ok),
                    /* value=  */ b_cmp_right
                );
            }
        }
    }
    insist(b_left_is_ok);
    insist(b_right_is_ok);

    /* Advance begin cursor. */
    {
        auto b_delta_begin = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenMulInt32(),
            /* left=   */ b_left_is_ok,
            /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(tuple.size()))
        );
        auto b_begin_inc = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ b_begin,
            /* right=  */ b_delta_begin
        );
        loop_body += BinaryenLocalSet(
            /* module= */ module,
            /* index=  */ BinaryenLocalGetGetIndex(b_begin),
            /* value=  */ b_begin_inc
        );
    }

    /* Advance end cursor. */
    {
        auto b_delta_end = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenMulInt32(),
            /* left=   */ b_right_is_ok,
            /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(tuple.size()))
        );
        auto b_end_inc = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenSubInt32(),
            /* left=   */ b_end,
            /* right=  */ b_delta_end
        );
        loop_body += BinaryenLocalSet(
            /* module= */ module,
            /* index=  */ BinaryenLocalGetGetIndex(b_end),
            /* value=  */ b_end_inc
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

    auto b_last = BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ b_end,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(-tuple.size()))
    );

    /*----- Compute pivot element as median of three. ----------------------------------------------------------------*/
    auto b_half = BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenDivSInt32(),
        /* left=   */ b_delta,
        /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(2))
    );
    auto b_mid_addr = BinaryenBinary(
        /* module= */ module,
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ b_begin,
        /* right=  */ b_half
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
    auto b_block_noop = BlockBuilder(module).finalize();
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
        /* schema=      */ schema,
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
    {
        auto b_is_left_not_empty = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenNeInt32(),
            /* left=   */ b_mid,
            /* right=  */ b_begin_plus_one
        );
        BlockBuilder block_left_not_empty(module, "left_not_empty");
        auto b_mid_minus_one = BinaryenBinary(
            /* module= */ module,
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ b_mid,
            /* right=  */ BinaryenConst(module, BinaryenLiteralInt32(-tuple.size()))
        );
        wasm_swap.emit(block_left_not_empty, tuple, b_begin, b_mid_minus_one);
        block_left_not_empty += BinaryenLocalSet(
            /* module= */ module,
            /* index=  */ BinaryenLocalGetGetIndex(b_mid),
            /* value=  */ b_mid_minus_one
        );
        loop_body += BinaryenIf(
            /* module=    */ module,
            /* condition= */ b_is_left_not_empty,
            /* ifTrue=    */ block_left_not_empty.finalize(),
            /* ifFalse=   */ nullptr
        );
    }

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
        /* value=  */ b_mid
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

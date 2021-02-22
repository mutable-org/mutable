#include "backend/WebAssembly.hpp"

#include "backend/Interpreter.hpp"
#include "backend/WasmAlgo.hpp"
#include "backend/WasmUtil.hpp"
#include "mutable/IR/CNF.hpp"
#include "storage/ColumnStore.hpp"
#include "storage/RowStore.hpp"
#include "mutable/storage/Store.hpp"
#include <binaryen-c.h>
#include <exception>
#include <regex>
#include <sstream>
#include <unordered_map>


using namespace m;


/*======================================================================================================================
 * Code generation helper classes
 *====================================================================================================================*/

namespace {

struct GroupingData : OperatorData
{
    WasmStruct *struc = nullptr;
    WasmHashTable *HT = nullptr;
    WasmVariable watermark_high; ///< stores the maximum number of entries before growing the table is required
    bool needs_running_count; ///< whether we must count the elements per group during grouping

    GroupingData(FunctionBuilder &fn)
        : watermark_high(fn, BinaryenTypeInt32())
    { }

    ~GroupingData() {
        delete HT;
        delete struc;
    }
};

struct SortingData : OperatorData
{
    WasmStruct *struc = nullptr;
    WasmVariable begin;

    SortingData(FunctionBuilder &fn)
        : begin(fn, BinaryenTypeInt32())
    { }

    ~SortingData() {
        delete struc;
    }
};

struct SimpleHashJoinData : OperatorData
{
    WasmStruct *struc = nullptr;
    WasmHashTable *HT = nullptr;
    WasmVariable watermark_high; ///< stores the maximum number of entries before growing the table is required
    bool is_build_phase = true;

    SimpleHashJoinData(FunctionBuilder &fn)
        : watermark_high(fn, BinaryenTypeInt32())
    { }

    ~SimpleHashJoinData() {
        delete HT;
        delete struc;
    }
};

}

/** Compiles a physical plan to WebAssembly. */
struct WasmCodeGen : ConstOperatorVisitor
{
    private:
    WasmModule &module_; ///< the WASM module
    FunctionBuilder main_; ///< the main function (or entry)
    WasmVariable head_of_heap_; ///< address to the head of the heap
    WasmVariable num_tuples_; ///< number of result tuples produced

    WasmCodeGen(WasmModule &module)
        : module_(module)
        , main_(module.ref(), "run", BinaryenTypeInt32(), { /* module ID */ BinaryenTypeInt32() })
        , head_of_heap_(main_, BinaryenTypeInt32())
        , num_tuples_(main_, BinaryenTypeInt32())
    { }

    public:
    static WasmModule compile(const Operator &plan);

    /** Returns the current WASM module. */
    BinaryenModuleRef module() { return module_.ref(); }

    /** Returns the current `FunctionBuilder`. */
    FunctionBuilder & fn() { return main_; }

    /** Returns the current `FunctionBuilder`. */
    const FunctionBuilder & fn() const { return main_; }

    /** Returns the local variable holding the number of result tuples produced. */
    const WasmVariable & num_tuples() const { return num_tuples_; }

    /** Returns the local variable holding the address to the head of the heap. */
    const WasmVariable & head_of_heap() const { return head_of_heap_; }

    /** Creates a new local and returns it as a `WasmVariable`. */
    WasmVariable add_local(BinaryenType ty) { return WasmVariable(main_, ty); }

    /** Adds a global import to the module.  */
    void import(std::string name, BinaryenType ty) {
        if (not BinaryenGetGlobal(module(), name.c_str())) {
            BinaryenAddGlobalImport(
                /* module=             */ module(),
                /* internalName=       */ name.c_str(),
                /* externalModuleName= */ "env",
                /* externalBaseName=   */ name.c_str(),
                /* type=               */ ty,
                /* mutable=            */ false
            );
        }
    }

    /** Returns the value of the global with the given `name`. */
    WasmTemporary get_imported(const std::string &name, BinaryenType ty) const {
        return BinaryenGlobalGet(module_.ref(), name.c_str(), ty);
    }

    WasmTemporary inc_num_tuples(int32_t n = 1) {
        WasmTemporary inc = BinaryenBinary(
            /* module= */ module(),
            /* op=     */ BinaryenAddInt32(),
            /* lhs=    */ num_tuples_,
            /* rhs=    */ BinaryenConst(module(), BinaryenLiteralInt32(n))
        );
        return num_tuples_.set(std::move(inc));
    }

    void align_head_of_heap(BlockBuilder &block) {
        auto b_head_inc = BinaryenBinary(
            /* module= */ module(),
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ head_of_heap(),
            /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(WasmPlatform::WASM_ALIGNMENT - 1))
        );
        auto b_head_aligned = BinaryenBinary(
            /* module= */ module(),
            /* op=     */ BinaryenAndInt32(),
            /* left=   */ b_head_inc,
            /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(~(int32_t(WasmPlatform::WASM_ALIGNMENT) - 1)))
        );
        block += head_of_heap().set(b_head_aligned);
    }
    void align_head_of_heap() { align_head_of_heap(main_.block()); }

    private:
    /*----- OperatorVisitor ------------------------------------------------------------------------------------------*/
    using ConstOperatorVisitor::operator();
#define DECLARE(CLASS) void operator()(const CLASS &op) override;
    DB_OPERATOR_LIST(DECLARE)
#undef DECLARE
};

/** Compiles a single pipeline.  Pipelines begin at producer nodes in the operator tree. */
struct WasmPipelineCG : ConstOperatorVisitor
{
    friend struct WasmStoreCG;
    friend struct WasmCodeGen;

    WasmCodeGen &CG; ///< the current codegen context

    private:
    WasmCGContext context_; ///< wasm context for compilation of expressions
    BlockBuilder block_; ///< used to construct the current block
    const char *name_; ///< name of this pipeline

    public:
    WasmPipelineCG(WasmCodeGen &CG, const char *name)
        : CG(CG)
        , context_(CG.module())
        , block_(CG.module(), (std::string("pipeline.") + name).c_str())
        , name_(name)
    { }

    WasmPipelineCG(WasmCodeGen &CG)
        : CG(CG)
        , context_(CG.module())
        , block_(CG.module())
        , name_(nullptr)
    { }

    /** Return the current WASM module. */
    BinaryenModuleRef module() { return CG.module(); }

    /** Compiles the pipeline of the given producer to a WASM block. */
    static WasmTemporary compile(const Producer &prod, WasmCodeGen &CG, const char *name) {
        WasmPipelineCG P(CG, name);
        P(prod);
        return P.block_.finalize();
    }

    WasmCGContext & context() { return context_; }
    const WasmCGContext & context() const { return context_; }

    const char * name() const { return name_; }

    private:
    void emit_write_results(const Schema &schema);

    /* Operators */
    using ConstOperatorVisitor::operator();
#define DECLARE(CLASS) void operator()(const CLASS &op) override;
    DB_OPERATOR_LIST(DECLARE)
#undef DECLARE
};

struct WasmStoreCG : ConstStoreVisitor
{
    WasmPipelineCG &pipeline;
    const Producer &op;

    WasmStoreCG(WasmPipelineCG &pipeline, const Producer &op)
        : pipeline(pipeline)
        , op(op)
    { }

    ~WasmStoreCG() { }

    using ConstStoreVisitor::operator();
    void operator()(const RowStore &store) override;
    void operator()(const ColumnStore &store) override;
};


/*======================================================================================================================
 * WasmCodeGen
 *====================================================================================================================*/

WasmModule WasmCodeGen::compile(const Operator &plan)
{
    WasmModule module; // fresh module
    WasmCodeGen codegen(module);

    /*----- Add memory. ----------------------------------------------------------------------------------------------*/
    BinaryenSetMemory(
        /* module=         */ module.ref(),
        /* initial=        */ 1,
        /* maximum=        */ WasmPlatform::WASM_MAX_MEMORY / WasmPlatform::WASM_PAGE_SIZE, // allowed maximum
        /* exportName=     */ "memory",
        /* segments=       */ nullptr,
        /* segmentPassive= */ nullptr,
        /* segmentOffsets= */ nullptr,
        /* segmentSizes=   */ nullptr,
        /* numSegments=    */ 0,
        /* shared=         */ 0
    );

#if 1
    /*----- Add print function. --------------------------------------------------------------------------------------*/
    std::vector<BinaryenType> print_param_types;
    print_param_types.push_back(BinaryenTypeInt32());

    BinaryenAddFunctionImport(
        /* module=             */ module.ref(),
        /* internalName=       */ "print",
        /* externalModuleName= */ "env",
        /* externalBaseName=   */ "print",
        /* params=             */ BinaryenTypeCreate(&print_param_types[0], print_param_types.size()),
        /* results=            */ BinaryenTypeNone()
    );
#endif

    /*----- Set up head of heap. -------------------------------------------------------------------------------------*/
    codegen.import("head_of_heap", BinaryenTypeInt32());
    codegen.main_.block() += codegen.head_of_heap_.set(codegen.get_imported("head_of_heap", BinaryenTypeInt32()));

    /*----- Compile plan. --------------------------------------------------------------------------------------------*/
    codegen(plan); // emit code

    /*----- Write number of results. ---------------------------------------------------------------------------------*/
    codegen.main_.block() += BinaryenStore(
        /* module= */ module.ref(),
        /* bytes=  */ 4,
        /* offset= */ 0,
        /* align=  */ 0,
        /* ptr=    */ codegen.head_of_heap(),
        /* value=  */ codegen.num_tuples(),
        /* type=   */ BinaryenTypeInt32()
    );

#if 0
    {
        BinaryenExpressionRef args[] = { codegen.num_tuples() };
        codegen.main_.block() += BinaryenCall(
            /* module=      */ module.ref(),
            /* target=      */ "print",
            /* operands=    */ args,
            /* numOperands= */ 1,
            /* returnType=  */ BinaryenTypeNone()
        );
    }
#endif

    /*----- Return the new head of heap . ----------------------------------------------------------------------------*/
    codegen.main_.block() += codegen.head_of_heap();

    /*----- Add function. --------------------------------------------------------------------------------------------*/
    codegen.main_.finalize();
    BinaryenAddFunctionExport(module.ref(), "run", "run");

    /*----- Validate module before optimization. ---------------------------------------------------------------------*/
    if (not BinaryenModuleValidate(module.ref())) {
        module.dump();
        throw std::logic_error("invalid module");
    }

#if 1
    /*----- Optimize module. -----------------------------------------------------------------------------------------*/
#ifndef NDEBUG
    std::ostringstream dump_before_opt;
    module.dump(dump_before_opt);
#endif
    BinaryenSetOptimizeLevel(2); // O2
    BinaryenSetShrinkLevel(0); // shrinking not required
    BinaryenModuleOptimize(module.ref());

    /*----- Validate module after optimization. ----------------------------------------------------------------------*/
    if (not BinaryenModuleValidate(module.ref())) {
#ifndef NDEBUG
        std::cerr << "Module invalid after optimization!" << std::endl;
        std::cerr << "WebAssembly before optimization:\n" << dump_before_opt.str() << std::endl;
        std::cerr << "WebAssembly after optimization:\n";
#endif
        module.dump(std::cerr);
    }
#endif

    return module;
}

void WasmCodeGen::operator()(const ScanOperator &op)
{
    std::ostringstream oss;
    oss << "scan_" << op.alias();
    const std::string name = oss.str();
    main_.block() += WasmPipelineCG::compile(op, *this, name.c_str());
}

void WasmCodeGen::operator()(const CallbackOperator &op) { (*this)(*op.child(0)); }

void WasmCodeGen::operator()(const PrintOperator &op) { (*this)(*op.child(0)); }

void WasmCodeGen::operator()(const NoOpOperator &op) { (*this)(*op.child(0)); }

void WasmCodeGen::operator()(const FilterOperator &op) { (*this)(*op.child(0)); }

void WasmCodeGen::operator()(const JoinOperator &op)
{
    switch (op.algo()) {
        default:
            unreachable("not implemented");

        case JoinOperator::J_SimpleHashJoin: {
            insist(op.children().size() == 2, "SimpleHashJoin is a binary operation and expects exactly two children");
            auto &build = *op.child(0);
            auto &probe = *op.child(1);

            auto data = new SimpleHashJoinData(fn());
            op.data(data);
            data->struc = new WasmStruct(module(), build.schema());

            (*this)(build);

#if 0
            /*----- Verify the number of entries in the hash table. --------------------------------------------------*/
            /* Compute end of HT. */
            auto HT = as<WasmRefCountingHashTable>(data->HT);
            auto b_table_size = BinaryenBinary(module(), BinaryenAddInt32(), HT->mask(),
                                               BinaryenConst(module(), BinaryenLiteralInt32(1)));
            auto b_table_size_in_bytes = BinaryenBinary(module(), BinaryenMulInt32(), b_table_size,
                                                        BinaryenConst(module(), BinaryenLiteralInt32(HT->entry_size())));
            WasmVariable table_end(fn(), BinaryenTypeInt32());
            table_end.set(fn().block(), BinaryenBinary(module(), BinaryenAddInt32(), HT->addr(), b_table_size_in_bytes));

            /* Count occupied slots in HT. */
            WasmVariable counter(fn(), BinaryenTypeInt32());
            WasmVariable runner(fn(), BinaryenTypeInt32());
            runner.set(fn().block(), HT->addr());
            WasmWhile for_each(module(), "SHJ.for_each", BinaryenBinary(module(), BinaryenLtUInt32(), runner, table_end));
            auto b_is_occupied = BinaryenBinary(module(), BinaryenNeInt32(), HT->get_bucket_ref_count(runner), BinaryenConst(module(), BinaryenLiteralInt32(0)));
            counter.set(for_each, BinaryenBinary(module(), BinaryenAddInt32(), counter, b_is_occupied));
            runner.set(for_each, BinaryenBinary(module(), BinaryenAddInt32(), runner, BinaryenConst(module(), BinaryenLiteralInt32(HT->entry_size()))));
            fn().block() += for_each.finalize();

            /* Print number of occupied slots. */
            {
                BinaryenExpressionRef args[] = { counter() };
                fn().block() += BinaryenCall(module(), "print", args, 1, BinaryenTypeNone());
            }
#endif

            data->is_build_phase = false;
            (*this)(probe);
        }
    }
}

void WasmCodeGen::operator()(const ProjectionOperator &op)
{
    if (op.children().size())
        (*this)(*op.child(0));
    else
        main_.block() += WasmPipelineCG::compile(op, *this, "projection");
}

void WasmCodeGen::operator()(const LimitOperator &op) { (*this)(*op.child(0)); }

void WasmCodeGen::operator()(const GroupingOperator &op)
{
    auto data = new GroupingData(fn());
    op.data(data);

    data->needs_running_count = false;
    for (auto agg : op.aggregates()) {
        auto fe = as<const FnApplicationExpr>(agg);
        switch (fe->get_function().fnid) {
            default:
                continue;

            case Function::FN_AVG:
                data->needs_running_count = true;
                goto exit;
        }
    }
exit:

    if (data->needs_running_count)
        data->struc = new WasmStruct(module(), op.schema(), { Type::Get_Integer(Type::TY_Scalar, 8) }); // with counter
    else
        data->struc = new WasmStruct(module(), op.schema()); // without counter

    (*this)(*op.child(0));

    auto HT = as<WasmRefCountingHashTable>(data->HT);
    WasmPipelineCG pipeline(*this);

    /*----- Initialize runner at start of hash table. ----------------------------------------------------------------*/
    WasmVariable induction(fn(), BinaryenTypeInt32());
    main_.block() += induction.set(HT->addr());

    /*----- Compute end of hash table. -------------------------------------------------------------------------------*/
    WasmVariable end(fn(), BinaryenTypeInt32());
    WasmTemporary size = BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ HT->mask(),
        /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(1))
    );
    WasmTemporary size_in_bytes = BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenMulInt32(),
        /* left=   */ size,
        /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(HT->entry_size()))
    );
    main_.block() += end.set(BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ HT->addr(),
        /* right=  */ size_in_bytes
    ));

    /*----- Advance to first occupied slot. --------------------------------------------------------------------------*/
    {
        WasmLoop advance_to_first(module(), "group_by.advance_to_first");
        advance_to_first += induction.set(data->HT->compute_next_slot(induction));
        WasmTemporary is_in_bounds = BinaryenBinary(
            /* module= */ module(),
            /* op=     */ BinaryenLtUInt32(),
            /* left=   */ induction,
            /* right=  */ end
        );
        WasmTemporary is_slot_empty = data->HT->is_slot_empty(induction);
        advance_to_first += BinaryenIf(
            /* module=    */ module(),
            /* condition= */ is_in_bounds,
            /* ifTrue=    */ advance_to_first.continu(is_slot_empty.clone(module())),
            /* ifFalse=   */ nullptr
        );
        main_.block() += BinaryenIf(
            /* module=    */ module(),
            /* condition= */ is_slot_empty.clone(module()),
            /* ifTrue=    */ advance_to_first.finalize(),
            /* ifFalse=   */ nullptr
        );
    }

    /*----- Create new pipeline starting at `GroupingOperator`. ------------------------------------------------------*/
    WasmWhile loop(module(), "group_by.foreach", BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenLtUInt32(),
        /* left=   */ induction,
        /* right=  */ end
    ));

    /*----- Emit code for data accesses. -----------------------------------------------------------------------------*/
    auto ld = data->HT->load_from_slot(induction);
    for (auto e : op.schema())
        pipeline.context().add(e.id, ld.get_value(e.id));

    /*----- Emit code for rest of the pipeline. ----------------------------------------------------------------------*/
    swap(pipeline.block_, loop);
    pipeline(*op.parent());
    swap(pipeline.block_, loop);

    /*----- Increment induction variable, then advance to next occupied slot. ----------------------------------------*/
    {
        WasmLoop advance(module(), "group_by.foreach.advace");
        advance += induction.set(data->HT->compute_next_slot(induction));
        WasmTemporary is_in_bounds = BinaryenBinary(
            /* module= */ module(),
            /* op=     */ BinaryenLtUInt32(),
            /* left=   */ induction,
            /* right=  */ end
        );
        WasmTemporary is_slot_empty = data->HT->is_slot_empty(induction);
        advance += BinaryenIf(
            /* module=    */ module(),
            /* condition= */ is_in_bounds,
            /* ifTrue=    */ advance.continu(std::move(is_slot_empty)),
            /* ifFalse=   */ nullptr
        );
        loop += advance.finalize();
    }

    pipeline.block_ += loop.finalize();
    main_.block() += pipeline.block_.finalize();
}

void WasmCodeGen::operator()(const SortingOperator &op)
{
    auto data = new SortingData(fn());
    data->struc = new WasmStruct(module(), op.child(0)->schema());
    op.data(data);

    (*this)(*op.child(0));

    /* Save the current head of heap as the end of the data to sort. */
    WasmVariable data_end(fn(), BinaryenTypeInt32());
    main_.block() += data_end.set(head_of_heap());
    align_head_of_heap();

    /*----- Generate sorting algorithm and invoke with start and end of data segment. --------------------------------*/
    WasmQuickSort qsort(op.child(0)->schema(), op.order_by(), WasmPartitionBranchless{});
    BinaryenExpressionRef qsort_args[] = { data->begin, data_end };
    BinaryenFunctionRef b_qsort = qsort.emit(module());
    main_.block() += BinaryenCall(
        /* module=      */ module(),
        /* target=      */ BinaryenFunctionGetName(b_qsort),
        /* operands=    */ qsort_args,
        /* numOperands= */ 2,
        /* returnType=  */ BinaryenTypeNone()
    );

    /*----- Create new pipeline starting at `SortingOperator`. -------------------------------------------------------*/
    WasmPipelineCG pipeline(*this);
    WasmWhile loop(module(), "order_by.foreach", BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenLtUInt32(),
        /* left=   */ data->begin,
        /* right=  */ data_end
    ));

    /*----- Emit code for data accesses. -----------------------------------------------------------------------------*/
    auto ld = data->struc->create_load_context(data->begin);
    for (auto &attr : op.child(0)->schema())
        pipeline.context().add(attr.id, ld.get_value(attr.id));

    /*----- Emit code for rest of the pipeline. ----------------------------------------------------------------------*/
    swap(pipeline.block_, loop);
    pipeline(*op.parent());
    swap(pipeline.block_, loop);

    /*----- Increment induction variable. ----------------------------------------------------------------------------*/
    loop += data->begin.set(BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ data->begin,
        /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(data->struc->size()))
    ));

    main_.block() += loop.finalize();
}


/*======================================================================================================================
 * WasmPipelineCG
 *====================================================================================================================*/

void WasmPipelineCG::emit_write_results(const Schema &schema)
{
    std::size_t offset = 0;
    const WasmVariable &out = CG.head_of_heap();
    for (auto &attr : schema) {
        WasmTemporary value = context()[attr.id];
        const uint32_t bytes = attr.type->size() == 64 ? 8 : 4;
        block_ += BinaryenStore(
            /* module= */ module(),
            /* bytes=  */ bytes,
            /* offset= */ offset,
            /* align=  */ 0,
            /* ptr=    */ out,
            /* value=  */ value,
            /* type=   */ get_binaryen_type(attr.type)
        );
        offset += 8;
    }

    block_ += CG.head_of_heap().set(BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ out,
        /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(offset))
    ));
}

void WasmPipelineCG::operator()(const ScanOperator &op)
{
    std::ostringstream oss;

    /*----- Get the number of rows in the scanned table. -------------------------------------------------------------*/
    auto & table = op.store().table();
    oss << table.name << "_num_rows";
    CG.import(oss.str(), BinaryenTypeInt32());
    WasmTemporary num_rows = CG.get_imported(oss.str(), BinaryenTypeInt32());

    WasmVariable induction(CG.fn(), BinaryenTypeInt32()); // initialized to 0

    oss.str("");
    oss << "scan_" << op.alias();
    WasmWhile loop(module(), oss.str().c_str(), BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenLtUInt32(),
        /* left=   */ induction,
        /* right=  */ num_rows
    ));

    /*----- Generate code to access attributes and emit code for the rest of the pipeline. ---------------------------*/
    swap(block_, loop);
    WasmStoreCG store(*this, op);
    store(op.store());
    swap(block_, loop);

    /*----- Increment induction variable. ----------------------------------------------------------------------------*/
    loop += induction.set(BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ induction,
        /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(1))
    ));

    block_ += loop.finalize();
}

void WasmPipelineCG::operator()(const CallbackOperator &op)
{
    emit_write_results(op.schema());
}

void WasmPipelineCG::operator()(const PrintOperator &op)
{
#if 0
    /* Callback per result tuple. */
    std::vector<BinaryenExpressionRef> args;
    for (auto &e : op.schema()) {
        auto it = intermediates_.find(e.id);
        insist(it != intermediates_.end(), "unknown identifier");
        args.push_back(it->second);
    }
    block_ += BinaryenCall(
        /* module=      */ module(),
        /* target=      */ "print",
        /* operands=    */ &args[0],
        /* numOperands= */ args.size(),
        /* returnType=  */ BinaryenTypeNone()
    );
#else
    emit_write_results(op.schema());
#endif
    block_ += CG.inc_num_tuples();
    // TODO issue a callback whenever NUM_TUPLES_OUTPUT_BUFFER tuples have been written, and reset counter
}

void WasmPipelineCG::operator()(const NoOpOperator&) { block_ += CG.inc_num_tuples(); }

void WasmPipelineCG::operator()(const FilterOperator &op)
{
    BlockBuilder then_block(module(), "filter.accept");
    swap(this->block_, then_block);
    (*this)(*op.parent());
    swap(this->block_, then_block);

    block_ += BinaryenIf(
        /* module=  */ module(),
        /* cond=    */ context().compile(op.filter()),
        /* ifTrue=  */ then_block.finalize(),
        /* ifFalse= */ nullptr
    );
}

void WasmPipelineCG::operator()(const JoinOperator &op)
{
    switch (op.algo()) {
        default:
            unreachable("not implemented");

        case JoinOperator::J_SimpleHashJoin: {
            WasmHashMumur3_64A hasher;
            auto data = as<SimpleHashJoinData>(op.data());

            /*----- Decompose the join predicate of the form `A.x = B.y` into parts `A.x` and `B.y`. -----------------*/
            auto &pred = op.predicate();
            insist(pred.size() == 1, "invalid predicate for simple hash join");
            auto &clause = pred[0];
            insist(clause.size() == 1, "invalid predicate for simple hash join");
            auto &literal = clause[0];
            insist(not literal.negative(), "invalid predicate for simple hash join");
            auto binary = as<const BinaryExpr>(literal.expr());
            insist(binary->tok == TK_EQUAL, "invalid predicate for simple hash join");
            auto first = as<const Designator>(binary->lhs);
            auto second = as<const Designator>(binary->rhs);
            auto [build, probe] = op.child(0)->schema().has({first->get_table_name(), first->attr_name.text}) ?
                                  std::make_pair(first, second) : std::make_pair(second, first);

            Schema::Identifier build_key_id(build->table_name.text, build->attr_name.text);
            Schema::Identifier probe_key_id(probe->table_name.text, probe->attr_name.text);

            if (data->is_build_phase) {
                std::vector<WasmTemporary> key;
                key.emplace_back(context().get_value(build_key_id));

                /*----- Compute payload ids. -------------------------------------------------------------------------*/
                std::vector<Schema::Identifier> payload_ids;
                for (auto attr : op.child(0)->schema()) {
                    if (attr.id != build_key_id)
                        payload_ids.push_back(attr.id);
                }

                /*----- Allocate hash table. -------------------------------------------------------------------------*/
                uint32_t initial_capacity = 32;
                if (auto scan = cast<ScanOperator>(op.child(0))) /// XXX: hack for pre-allocation
                    initial_capacity = ceil_to_pow_2<decltype(initial_capacity)>(scan->store().num_rows() / .8);

                data->HT = new WasmRefCountingHashTable(module(), CG.fn(), *data->struc);
                auto HT = as<WasmRefCountingHashTable>(data->HT);

                WasmVariable end(CG.fn(), BinaryenTypeInt32());
                CG.fn().block() += end.set(HT->create_table(CG.fn().block(), CG.head_of_heap(), initial_capacity));
                CG.fn().block() += data->watermark_high.set(BinaryenConst(module(), BinaryenLiteralInt32(8 * initial_capacity / 10)));

                /*----- Update head of heap. -------------------------------------------------------------------------*/
                CG.fn().block() += CG.head_of_heap().set(end);
                CG.align_head_of_heap();

                /*----- Initialize hash table. -----------------------------------------------------------------------*/
                HT->clear_table(CG.fn().block(), HT->addr(), end);

                /*----- Create a counter for the number of entries. --------------------------------------------------*/
                WasmVariable num_entries(CG.fn(), BinaryenTypeInt32());
                CG.fn().block() += num_entries.set(BinaryenConst(module(), BinaryenLiteralInt32(0)));

                /*----- Create code block to resize the hash table when the high watermark is reached. ---------------*/
                BlockBuilder perform_rehash(module(), "join.build.rehash");

                /*----- Compute the mask for the new size. -----------------------------------------------------------*/
                WasmTemporary mask_x2 = BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenShlInt32(),
                    /* left=   */ HT->mask(),
                    /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(1))
                );
                WasmVariable mask_new(CG.fn(), BinaryenTypeInt32());
                perform_rehash += mask_new.set(BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenAddInt32(),
                    /* left=   */ mask_x2,
                    /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(1))
                ));

                /*----- Compute new hash table size. -----------------------------------------------------------------*/
                WasmTemporary size_new = BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenAddInt32(),
                    /* left=   */ mask_new,
                    /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(1))

                );
                WasmTemporary size_in_bytes_new = BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenMulInt32(),
                    /* left=   */ size_new.clone(module()),
                    /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(HT->entry_size()))
                );

                /*----- Allocate new hash table. ---------------------------------------------------------------------*/
                WasmVariable begin_new(CG.fn(), BinaryenTypeInt32());
                perform_rehash += begin_new.set(CG.head_of_heap());
                perform_rehash += CG.head_of_heap().set(BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenAddInt32(),
                    /* left=   */ begin_new,
                    /* right=  */ size_in_bytes_new
                ));
                CG.align_head_of_heap(perform_rehash);

                /*----- Rehash to double size. -----------------------------------------------------------------------*/
                BinaryenFunctionRef b_fn_rehash = HT->rehash(hasher, { build_key_id }, payload_ids);
                BinaryenExpressionRef rehash_args[4] = {
                    /* addr_old= */ HT->addr(),
                    /* mask_old= */ HT->mask(),
                    /* addr_new= */ begin_new,
                    /* mask_new= */ mask_new
                };
                perform_rehash += BinaryenCall(
                    /* module=      */ module(),
                    /* target=      */ BinaryenFunctionGetName(b_fn_rehash),
                    /* operands=    */ rehash_args,
                    /* numOperands= */ ARR_SIZE(rehash_args),
                    /* returnType=  */ BinaryenTypeNone()
                );

                /*----- Update hash table. ---------------------------------------------------------------------------*/
                perform_rehash += HT->addr().set(begin_new);
                perform_rehash += HT->mask().set(mask_new);

                /*----- Update high watermark. -----------------------------------------------------------------------*/
                perform_rehash += data->watermark_high.set(BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenDivUInt32(),
                    /* left=   */ BinaryenBinary(
                                      /* module= */ module(),
                                      /* op=     */ BinaryenMulInt32(),
                                      /* left=   */ size_new,
                                      /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(8))
                    ),
                    /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(10))
                ));

                /*----- Perform resizing to twice the capacity when the high watermark is reached. -------------------*/
                WasmTemporary has_reached_watermark = BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenGeUInt32(),
                    /* left=   */ num_entries,
                    /* right=  */ data->watermark_high
                );
                block_ += BinaryenIf(
                    /* module=    */ module(),
                    /* condition= */ has_reached_watermark,
                    /* ifTrue=    */ perform_rehash.finalize(),
                    /* ifFalse=   */ nullptr
                );

                /*----- Compute hash. --------------------------------------------------------------------------------*/
                WasmTemporary hash = hasher.emit(module(), CG.fn(), block_, key);
                WasmTemporary hash_i32 = BinaryenUnary(
                    /* module= */ module(),
                    /* op=     */ BinaryenWrapInt64(),
                    /* value=  */ hash
                );

                /*----- Insert the element. --------------------------------------------------------------------------*/
                WasmVariable slot_addr(CG.fn(), BinaryenTypeInt32());
                block_ += slot_addr.set(HT->insert_with_duplicates(block_, std::move(hash_i32), { build_key_id }, key));

                /*---- Write payload. --------------------------------------------------------------------------------*/
                for (auto attr : op.child(0)->schema())
                    block_ += HT->store_value_to_slot(slot_addr, attr.id, context().get_value(attr.id));

                block_ += num_entries.set(BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenAddInt32(),
                    /* left=   */ num_entries,
                    /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(1))
                ));
            } else {
                auto HT = as<WasmRefCountingHashTable>(data->HT);
                std::vector<WasmTemporary> key;
                key.emplace_back(context().get_value(probe_key_id));

                /*----- Compute hash. --------------------------------------------------------------------------------*/
                WasmTemporary hash = hasher.emit(module(), CG.fn(), block_, key);
                WasmTemporary hash_i32 = BinaryenUnary(
                    /* module= */ module(),
                    /* op=     */ BinaryenWrapInt64(),
                    /* value=  */ hash
                );

                /*----- Compute address of bucket. -------------------------------------------------------------------*/
                WasmVariable bucket_addr(CG.fn(), BinaryenTypeInt32());
                block_ += bucket_addr.set(HT->hash_to_bucket(std::move(hash_i32)));

                /*----- Iterate over all entries in the bucket and compare the key. ----------------------------------*/
                WasmVariable slot_addr(CG.fn(), BinaryenTypeInt32());
                block_ += slot_addr.set(bucket_addr);
                WasmVariable step(CG.fn(), BinaryenTypeInt32());
                block_ += step.set(BinaryenConst(module(), BinaryenLiteralInt32(0)));
                WasmTemporary ref_count = HT->get_bucket_ref_count(bucket_addr);
                WasmWhile loop(module(), "join.probe", BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenLtUInt32(),
                    /* left=   */ step,
                    /* right=  */ ref_count
                ));

                /*----- Load values from HT. -------------------------------------------------------------------------*/
                auto ld = HT->load_from_slot(slot_addr);
                for (auto &attr : op.child(0)->schema())
                    context().add(attr.id, ld.get_value(attr.id));

                /*----- Check whether the key of the entry in the bucket -- identified by `build_key_id` -- equals the
                 * probe key. ----------------------------------------------------------------------------------------*/
                WasmTemporary is_key_equal = HT->compare_key(slot_addr, { build_key_id }, key);
                BlockBuilder keys_equal(module(), "join.probe.match");

                swap(block_, keys_equal);
                (*this)(*op.parent());
                swap(block_, keys_equal);

                loop += BinaryenIf(
                    /* module=    */ module(),
                    /* condition= */ is_key_equal,
                    /* ifTrue=    */ keys_equal.finalize(),
                    /* ifFalse=   */ nullptr
                );

                /*----- Compute size and end of table to handle wrap around. -----------------------------------------*/
                WasmTemporary size = BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenAddInt32(),
                    /* left=   */ HT->mask(),
                    /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(1))
                );
                WasmVariable table_size_in_bytes(CG.fn(), BinaryenTypeInt32());
                block_ += table_size_in_bytes.set(BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenMulInt32(),
                    /* left=   */ size,
                    /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(HT->entry_size()))
                ));
                WasmTemporary table_end = BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenAddInt32(),
                    /* left=   */ HT->addr(),
                    /* right=  */ table_size_in_bytes
                );

                loop += step.set(BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenAddInt32(),
                    /* left=   */ step,
                    /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(HT->entry_size()))
                ));
                WasmVariable slot_addr_inc(CG.fn(), BinaryenTypeInt32());
                loop += slot_addr_inc.set(BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenAddInt32(),
                    /* left=   */ slot_addr,
                    /* right=  */ step
                ));
                WasmTemporary exceeds_table = BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenGeUInt32(),
                    /* left=   */ slot_addr_inc,
                    /* right=  */ table_end
                );
                WasmTemporary slot_addr_wrapped = BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenSubInt32(),
                    /* left=   */ slot_addr_inc,
                    /* right=  */ table_size_in_bytes
                );
                loop += slot_addr.set(BinaryenSelect(
                    /* module=    */ module(),
                    /* condition= */ exceeds_table,
                    /* ifTrue=    */ slot_addr_wrapped,
                    /* ifFalse=   */ slot_addr_inc,
                    /* type=      */ BinaryenTypeInt32()
                ));
                block_ += loop.finalize();
            }
        }
    }
}

void WasmPipelineCG::operator()(const ProjectionOperator &op)
{
    auto p = op.projections().begin();
    for (auto &e : op.schema()) {
        if (not context().has(e.id))
            context().add(e.id, context().compile(*p->first));
        ++p;
    }
    (*this)(*op.parent());
}

void WasmPipelineCG::operator()(const LimitOperator &op)
{
    /* Emit code for the rest of the pipeline. */
    BlockBuilder within_limits_block(module(), "limit.within");
    swap(this->block_, within_limits_block);
    (*this)(*op.parent());
    swap(this->block_, within_limits_block);

    /* Declare a new counter.  Will be initialized to 0. */
    WasmVariable count(CG.fn(), BinaryenTypeInt32());

    /* Check whether the pipeline has exceeded the limit. */
    WasmTemporary cond_exceeds_limits = BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenGeSInt32(),
        /* lhs=    */ count,
        /* rhs=    */ BinaryenConst(module(), BinaryenLiteralInt32(op.limit() + op.offset()))
    );

    /* Abort the pipeline once the limit is exceeded. */
    block_ += BinaryenBreak(
        /* module= */ module(),
        /* name=   */ block_.name(),
        /* cond=   */ cond_exceeds_limits,
        /* value=  */ nullptr
    );

    /* Check whether the pipeline is within the limits. */
    WasmTemporary cond_within_limits = BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenGeSInt32(),
        /* lhs=    */ count,
        /* rhs=    */ BinaryenConst(module(), BinaryenLiteralInt32(op.offset()))
    );
    block_ += BinaryenIf(
        /* module=  */ module(),
        /* cond=    */ cond_within_limits,
        /* ifTrue=  */ within_limits_block.finalize(),
        /* ifFalse= */ nullptr
    );

    block_ += count.set(BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenAddInt32(),
        /* lhs=    */ count,
        /* rhs=    */ BinaryenConst(module(), BinaryenLiteralInt32(1))
    ));
}

void WasmPipelineCG::operator()(const GroupingOperator &op)
{
    WasmHashMumur3_64A hasher;
    auto data = as<GroupingData>(op.data());

    std::vector<Schema::Identifier> key_IDs;
    for (std::size_t i = 0; i != op.group_by().size(); ++i)
        key_IDs.push_back(op.schema()[i].id);

    /*----- Allocate hash table. -------------------------------------------------------------------------------------*/
    std::size_t initial_capacity = 0;

    /*--- XXX: Crude hack to do pre-allocation in benchmarks. --------------------------------------------------------*/
    if (not op.group_by().empty()) {
        std::regex reg_is_distinct("n\\d+");
        std::size_t num_groups_est = 1;
        for (auto expr : op.group_by()) {
            if (auto d = cast<const Designator>(expr)) {
                if (std::regex_match(d->attr_name.text, reg_is_distinct)) {
                    errno = 0;
                    unsigned num_distinct_values = strtol(d->attr_name.text + 1, nullptr, 10);
                    if (not errno) {
                        num_groups_est = mul_wo_overflow(num_groups_est, num_distinct_values);
                        continue;
                    }
                }
            }
            goto estimation_failed;
        }

        if (num_groups_est > 1e9)
            initial_capacity = 1e9 / .7;
        else
            initial_capacity = ceil_to_pow_2<decltype(initial_capacity)>(num_groups_est / .7);
estimation_failed:;
    }

    if (auto scan = cast<ScanOperator>(op.child(0))) { // XXX: hack for pre-allocation
        if (initial_capacity == 0)
            initial_capacity = ceil_to_pow_2<decltype(initial_capacity)>(scan->store().num_rows() / .8);
        else
            initial_capacity = std::min(initial_capacity, ceil_to_pow_2<decltype(initial_capacity)>(scan->store().num_rows() / .8));
    }

    if (initial_capacity == 0)
        initial_capacity = 32;

    data->HT = new WasmRefCountingHashTable(module(), CG.fn(), *data->struc);
    auto HT = as<WasmRefCountingHashTable>(data->HT);

    WasmVariable end(CG.fn(), BinaryenTypeInt32());
    CG.fn().block() += end.set(HT->create_table(CG.fn().block(), CG.head_of_heap(), initial_capacity));
    CG.fn().block() += data->watermark_high.set(BinaryenConst(module(), BinaryenLiteralInt32(8 * initial_capacity / 10)));

    /*----- Update head of heap. -------------------------------------------------------------------------------------*/
    CG.fn().block() += CG.head_of_heap().set(end);
    CG.align_head_of_heap();

    /*----- Initialize hash table. -----------------------------------------------------------------------------------*/
    HT->clear_table(CG.fn().block(), HT->addr(), end);

    /*----- Create a counter for the number of groups. ---------------------------------------------------------------*/
    WasmVariable num_groups(CG.fn(), BinaryenTypeInt32());
    CG.fn().block() += num_groups.set(BinaryenConst(module(), BinaryenLiteralInt32(0)));

    /*----- Compute group and aggregate ids. -------------------------------------------------------------------------*/
    std::vector<Schema::Identifier> grp_ids;
    std::vector<Schema::Identifier> agg_ids;
    {
        std::size_t i = 0;
        for (; i != op.group_by().size(); ++i) {
            auto grp = op.schema()[i];
            grp_ids.push_back(grp.id);
        }
        for (std::size_t end = i + op.aggregates().size(); i != end; ++i) {
            auto agg = op.schema()[i];
            agg_ids.push_back(agg.id);
        }
    }

    /*----- Create code block to resize the hash table. --------------------------------------------------------------*/
    BlockBuilder perform_rehash(module(), "group_by.rehash");

    /*----- Compute the mask for the new size. -----------------------------------------------------------------------*/
    WasmTemporary mask_x2 = BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenShlInt32(),
        /* left=   */ HT->mask(),
        /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(1))
    );
    WasmVariable mask_new(CG.fn(), BinaryenTypeInt32());
    perform_rehash += mask_new.set(BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ mask_x2,
        /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(1))
    ));

    /*----- Compute new hash table size. -----------------------------------------------------------------------------*/
    WasmVariable size_new(CG.fn(), BinaryenTypeInt32());
    perform_rehash += size_new.set(BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ mask_new,
        /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(1))

    ));
    WasmTemporary size_in_bytes_new = BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenMulInt32(),
        /* left=   */ size_new,
        /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(HT->entry_size()))
    );

    /*----- Allocate new hash table. ---------------------------------------------------------------------------------*/
    WasmVariable begin_new(CG.fn(), BinaryenTypeInt32());
    perform_rehash += begin_new.set(CG.head_of_heap());
    perform_rehash += CG.head_of_heap().set(BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ begin_new,
        /* right=  */ size_in_bytes_new
    ));
    CG.align_head_of_heap(perform_rehash);

    /*----- Rehash to double size. -----------------------------------------------------------------------------------*/
    BinaryenFunctionRef b_fn_rehash = HT->rehash(hasher, grp_ids, agg_ids);
    BinaryenExpressionRef rehash_args[4] = {
        /* addr_old= */ HT->addr(),
        /* mask_old= */ HT->mask(),
        /* addr_new= */ begin_new,
        /* mask_new= */ mask_new
    };
    perform_rehash += BinaryenCall(
        /* module=      */ module(),
        /* target=      */ BinaryenFunctionGetName(b_fn_rehash),
        /* operands=    */ rehash_args,
        /* numOperands= */ ARR_SIZE(rehash_args),
        /* returnType=  */ BinaryenTypeNone()
    );

    /*----- Update hash table and grouping data. ---------------------------------------------------------------------*/
    perform_rehash += HT->addr().set(begin_new);
    perform_rehash += HT->mask().set(mask_new);

    /*----- Update high watermark. -----------------------------------------------------------------------------------*/
    perform_rehash += data->watermark_high.set(BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenDivUInt32(),
        /* left=   */ BinaryenBinary(
                          /* module= */ module(),
                          /* op=     */ BinaryenMulInt32(),
                          /* left=   */ size_new,
                          /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(8))
        ),
        /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(10))
    ));

    /*----- Perform resizing to twice the capacity when the high watermark is reached. -------------------------------*/
    WasmTemporary has_reached_watermark = BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenGeUInt32(),
        /* left=   */ num_groups,
        /* right=  */ data->watermark_high
    );
    block_ += BinaryenIf(
        /* module=    */ module(),
        /* condition= */ has_reached_watermark,
        /* ifTrue=    */ perform_rehash.finalize(),
        /* ifFalse=   */ nullptr
    );

    /*----- Compute grouping key. ------------------------------------------------------------------------------------*/
    std::vector<WasmTemporary> key;
    for (auto grp : op.group_by())
        key.emplace_back(context().compile(*grp));

    /*----- Compute hash. --------------------------------------------------------------------------------------------*/
    WasmTemporary hash = hasher.emit(module(), CG.fn(), block_, key);
    WasmTemporary hash_i32 = BinaryenUnary(
        /* module= */ module(),
        /* op=     */ BinaryenWrapInt64(),
        /* value=  */ hash
    );

    /*----- Compute address of bucket. -------------------------------------------------------------------------------*/
    WasmVariable bucket_addr(CG.fn(), BinaryenTypeInt32());
    block_ += bucket_addr.set(data->HT->hash_to_bucket(std::move(hash_i32)));

    /*----- Locate entry with key `key` in the bucket, or end of bucket if no such key exists. -----------------------*/
    auto [ slot_addr_found, steps_found ] = data->HT->find_in_bucket(block_, bucket_addr, key);
    WasmVariable slot_addr(CG.fn(), BinaryenTypeInt32());
    block_ += slot_addr.set(std::move(slot_addr_found));

    /*----- Create or update the group. ------------------------------------------------------------------------------*/
    BlockBuilder create_group(module(), "group_by.create_group");
    BlockBuilder update_group(module(), "group_by.update");

    create_group += num_groups.set(BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ num_groups,
        /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(1))
    ));

    auto ld_slot = data->HT->load_from_slot(slot_addr);
    data->HT->emplace(create_group, bucket_addr, std::move(steps_found), slot_addr, key_IDs, key);

    if (data->needs_running_count) {
        create_group += BinaryenStore(
            /* module= */ module(),
            /* bytes=  */ 4,
            /* offset= */ WasmRefCountingHashTable::REFERENCE_SIZE + data->struc->offset(op.schema().num_entries()),
            /* align=  */ 0,
            /* ptr=    */ slot_addr,
            /* value=  */ BinaryenConst(module(), BinaryenLiteralInt32(1)),
            /* type=   */ BinaryenTypeInt32()
        );

        WasmTemporary old_count = BinaryenLoad(
            /* module= */ module(),
            /* bytes=  */ 4,
            /* signed= */ false,
            /* offset= */ WasmRefCountingHashTable::REFERENCE_SIZE + data->struc->offset(op.schema().num_entries()),
            /* align=  */ 0,
            /* type=   */ BinaryenTypeInt32(),
            /* ptr=    */ slot_addr
        );
        WasmTemporary new_count = BinaryenBinary(
            /* module= */ module(),
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ old_count,
            /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(1))
        );
        update_group += BinaryenStore(
            /* module= */ module(),
            /* bytes=  */ 4,
            /* offset= */ WasmRefCountingHashTable::REFERENCE_SIZE + data->struc->offset(op.schema().num_entries()),
            /* align=  */ 0,
            /* ptr=    */ slot_addr,
            /* value=  */ new_count,
            /* type=   */ BinaryenTypeInt32()
        );
    }

    for (std::size_t i = 0; i != op.aggregates().size(); ++i) {
        auto e = op.schema()[op.group_by().size() + i];
        auto agg = op.aggregates()[i];

        auto fn_expr = as<const FnApplicationExpr>(agg);
        auto &fn = fn_expr->get_function();
        insist(fn.kind == Function::FN_Aggregate, "not an aggregation function");

        /*----- Emit code to evaluate arguments. ---------------------------------------------------------------------*/
        insist(fn_expr->args.size() <= 1, "unsupported aggregate with more than one argument");
        std::vector<WasmTemporary> args;
        for (auto arg : fn_expr->args)
            args.emplace_back(context().compile(*arg));

        switch (fn.fnid) {
            default:
                unreachable("unsupported aggregate function");

            case Function::FN_MIN: {
                insist(args.size() == 1, "aggregate function expects exactly one argument");
                create_group += data->HT->store_value_to_slot(slot_addr, e.id, args[0].clone(module()));
                WasmVariable val(CG.fn(), get_binaryen_type(e.type));
                update_group += val.set(args[0].clone(module()));
                WasmVariable old_val(CG.fn(), get_binaryen_type(e.type));
                update_group += old_val.set(ld_slot.get_value(e.id));
                auto n = as<const Numeric>(e.type);
                switch (n->kind) {
                    case Numeric::N_Int: {
                        WasmTemporary less = BinaryenBinary(
                            /* module= */ module(),
                            /* op=     */ n->size() <= 32 ? BinaryenLtSInt32() : BinaryenLtSInt64(),
                            /* left=   */ old_val,
                            /* right=  */ val
                        );
#if 1
                        /* Compute MIN via select. */
                        WasmTemporary new_val = BinaryenSelect(
                            /* module=    */ module(),
                            /* condition= */ less,
                            /* ifTrue=    */ old_val,
                            /* ifFalse=   */ val,
                            /* type=      */ n->size() <= 32 ? BinaryenTypeInt32() : BinaryenTypeInt64()
                        );
                        update_group += data->HT->store_value_to_slot(slot_addr, e.id, std::move(new_val));
#else
                        /* Compute MIN via conditional branch. */
                        WasmTemporary upd = data->HT->store_value_to_slot(slot_addr, e.id, std::move(val));
                        update_group += BinaryenIf(
                            /* module=    */ module(),
                            /* condition= */ less,
                            /* ifTrue=    */ upd,
                            /* ifFalse=   */ nullptr
                        );
#endif
                        break;
                    }

                    case Numeric::N_Decimal:
                        unreachable("not implemented");

                    case Numeric::N_Float: {
                        WasmTemporary new_val = BinaryenBinary(
                            /* module= */ module(),
                            /* op=     */ n->size() == 32 ? BinaryenMinFloat32() : BinaryenMinFloat64(),
                            /* left=   */ old_val,
                            /* right=  */ val
                        );
                        update_group += data->HT->store_value_to_slot(slot_addr, e.id, std::move(new_val));
                        break;
                    }
                }
                break;
            }

            case Function::FN_MAX: {
                insist(args.size() == 1, "aggregate function expects exactly one argument");
                create_group += data->HT->store_value_to_slot(slot_addr, e.id, args[0].clone(module()));
                WasmVariable val(CG.fn(), get_binaryen_type(e.type));
                update_group += val.set(args[0].clone(module()));
                WasmVariable old_val(CG.fn(), get_binaryen_type(e.type));
                update_group += old_val.set(ld_slot.get_value(e.id));
                auto n = as<const Numeric>(e.type);
                switch (n->kind) {
                    case Numeric::N_Int: {
                        WasmTemporary greater = BinaryenBinary(
                            /* module= */ module(),
                            /* op=     */ n->size() <= 32 ? BinaryenGtSInt32() : BinaryenGtSInt64(),
                            /* left=   */ old_val,
                            /* right=  */ val
                        );
#if 1
                        /* Compute MAX via select. */
                        WasmTemporary new_val = BinaryenSelect(
                            /* module=    */ module(),
                            /* condition= */ greater,
                            /* ifTrue=    */ old_val,
                            /* ifFalse=   */ val,
                            /* type=      */ n->size() <= 32 ? BinaryenTypeInt32() : BinaryenTypeInt64()
                        );
                        update_group += data->HT->store_value_to_slot(slot_addr, e.id, std::move(new_val));
#else
                        /* Compute MAX via conditional branch. */
                        WasmTemporary upd = data->HT->store_value_to_slot(slot_addr, e.id, std::move(val));
                        update_group += BinaryenIf(
                            /* module=    */ module(),
                            /* condition= */ greater,
                            /* ifTrue=    */ upd,
                            /* ifFalse=   */ nullptr
                        );
#endif
                        break;
                    }

                    case Numeric::N_Decimal:
                        unreachable("not implemented");

                    case Numeric::N_Float: {
                        WasmTemporary new_val = BinaryenBinary(
                            /* module= */ module(),
                            /* op=     */ n->size() == 32 ? BinaryenMaxFloat32() : BinaryenMaxFloat64(),
                            /* left=   */ old_val,
                            /* right=  */ val
                        );
                        update_group += data->HT->store_value_to_slot(slot_addr, e.id, std::move(new_val));
                        break;
                    }
                }
                break;
            }

            case Function::FN_SUM: {
                insist(args.size() == 1, "aggregate function expects exactly one argument");
                auto b_old_val = ld_slot.get_value(e.id);
                auto n = as<const Numeric>(fn_expr->args[0]->type());
                switch (n->kind) {
                    case Numeric::N_Int: {
                        WasmTemporary extd = convert(module(), std::move(args[0]), n, Type::Get_Integer(Type::TY_Vector, 8));
                        create_group += data->HT->store_value_to_slot(slot_addr, e.id, extd.clone(module()));
                        WasmTemporary new_val = BinaryenBinary(
                            /* module= */ module(),
                            /* op=     */ BinaryenAddInt64(),
                            /* left=   */ b_old_val,
                            /* right=  */ extd
                        );
                        update_group += data->HT->store_value_to_slot(slot_addr, e.id, std::move(new_val));
                        break;
                    }

                    case Numeric::N_Decimal:
                        unreachable("not implemented");

                    case Numeric::N_Float:
                        WasmTemporary extd = convert(module(), std::move(args[0]), n, Type::Get_Double(Type::TY_Vector));
                        create_group += data->HT->store_value_to_slot(slot_addr, e.id, extd.clone(module()));
                        WasmTemporary new_val = BinaryenBinary(
                            /* module= */ module(),
                            /* op=     */ BinaryenAddFloat64(),
                            /* left=   */ b_old_val,
                            /* right=  */ extd
                        );
                        update_group += data->HT->store_value_to_slot(slot_addr, e.id, std::move(new_val));
                        break;
                }
                break;
            }

            case Function::FN_AVG: {
                insist(args.size() == 1, "aggregate function expects exactly one argument");
                /* Compute AVG as iterative mean as described in Knuth, The Art of Computer Programming Vol 2, section
                 * 4.2.2. */
                WasmTemporary argument = convert(module(), std::move(args[0]), fn_expr->args[0]->type(), Type::Get_Double(Type::TY_Scalar));
                create_group += data->HT->store_value_to_slot(slot_addr, e.id, argument.clone(module()));
                WasmTemporary old_avg = ld_slot.get_value(e.id);
                WasmTemporary delta_absolute = BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenSubFloat64(),
                    /* left=   */ argument,
                    /* right=  */ old_avg.clone(module())
                );
                WasmTemporary running_count = BinaryenLoad(
                    /* module= */ module(),
                    /* bytes=  */ 4,
                    /* signed= */ false,
                    /* offset= */ WasmRefCountingHashTable::REFERENCE_SIZE + data->struc->offset(op.schema().num_entries()),
                    /* align=  */ 0,
                    /* type=   */ BinaryenTypeInt32(),
                    /* ptr=    */ slot_addr
                );
                WasmTemporary delta_relative = BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenDivFloat64(),
                    /* left=   */ delta_absolute,
                    /* right=  */ BinaryenUnary(module(), BinaryenConvertSInt32ToFloat64(), running_count)
                );
                WasmTemporary new_avg = BinaryenBinary(
                    /* module= */ module(),
                    /* op=     */ BinaryenAddFloat64(),
                    /* left=   */ old_avg,
                    /* right=  */ delta_relative
                );
                update_group += data->HT->store_value_to_slot(slot_addr, e.id, std::move(new_avg));
                break;
            }

            case Function::FN_COUNT: {
                if (args.size() == 0) {
                    create_group += data->HT->store_value_to_slot(slot_addr, e.id,
                                                                  BinaryenConst(module(), BinaryenLiteralInt64(1)));
                    WasmTemporary old_val = ld_slot.get_value(e.id);
                    WasmTemporary new_val = BinaryenBinary(
                        /* module= */ module(),
                        /* op=     */ BinaryenAddInt64(),
                        /* left=   */ old_val,
                        /* right=  */ BinaryenConst(module(), BinaryenLiteralInt64(1))
                    );
                    update_group += data->HT->store_value_to_slot(slot_addr, e.id, std::move(new_val));
                } else {
                    // TODO verify if NULL
                    unreachable("not yet supported");
                }
                break;
            }
        }
    }

    block_ += BinaryenIf(
        /* module=    */ module(),
        /* condition= */ data->HT->is_slot_empty(slot_addr),
        /* ifTrue=    */ create_group.finalize(),
        /* ifFalse=   */ update_group.finalize()
    );
}

void WasmPipelineCG::operator()(const SortingOperator &op)
{
    auto data = as<SortingData>(op.data());

    /* Save the current head of heap as the beginning of the data to sort. */
    CG.fn().block() += data->begin.set(CG.head_of_heap());

    for (auto &attr : op.child(0)->schema())
        block_ += data->struc->store(CG.head_of_heap(), attr.id, context()[attr.id]);

    /* Advance head of heap. */
    block_ += CG.head_of_heap().set(BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ CG.head_of_heap(),
        /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(data->struc->size()))
    ));
}


/*======================================================================================================================
 * WasmStoreCG
 *====================================================================================================================*/

void WasmStoreCG::operator()(const RowStore &store)
{
    std::ostringstream oss;
    auto &table = store.table();

    /*----- Import table address. ------------------------------------------------------------------------------------*/
    WasmVariable row_addr(pipeline.CG.fn(), BinaryenTypeInt32());
    pipeline.CG.import(table.name, BinaryenTypeInt32());
    pipeline.CG.fn().block() += row_addr.set(pipeline.CG.get_imported(table.name, BinaryenTypeInt32()));

    /*----- Generate code to access null bitmap and value of all required attributes. --------------------------------*/
    const auto null_bitmap_offset = store.offset(table.size());
    for (auto &e : op.schema()) {
        auto &attr = table[e.id.name];

        /*----- Generate code for null bit check. --------------------------------------------------------------------*/
        {
            const auto null_bit_offset = null_bitmap_offset + attr.id;
            const auto qwords = null_bit_offset / 32;
            const auto bits = null_bit_offset % 32;

            WasmTemporary null_bitmap_addr = BinaryenBinary(
                /* module= */ pipeline.module(),
                /* op=     */ BinaryenAddInt32(),
                /* left=   */ row_addr,
                /* right=  */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(qwords))
            );
            WasmTemporary qword = BinaryenLoad(
                /* module= */ pipeline.module(),
                /* bytes=  */ 4,
                /* signed= */ false,
                /* offset= */ 0,
                /* align=  */ 0,
                /* type=   */ BinaryenTypeInt32(),
                /* ptr=    */ null_bitmap_addr
            );
            WasmTemporary shr = BinaryenBinary(
                /* module= */ pipeline.module(),
                /* op=     */ BinaryenShrUInt32(),
                /* left=   */ qword,
                /* right=  */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(bits))
            );
            WasmTemporary is_null = BinaryenBinary(
                /* module= */ pipeline.module(),
                /* op=     */ BinaryenAndInt32(),
                /* left=   */ shr,
                /* right=  */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(1))
            );
            // TODO add to context nulls
            (void) is_null;
        }

        /*----- Generate code for value access. ----------------------------------------------------------------------*/
        if (attr.type->size() < 8) {
            WasmTemporary byte = BinaryenLoad(
                /* module= */ pipeline.module(),
                /* bytes=  */ 1,
                /* signed= */ false,
                /* offset= */ store.offset(attr.id) / 8,
                /* align=  */ 0,
                /* type=   */ BinaryenTypeInt32(),
                /* ptr=    */ row_addr
            );
            WasmTemporary shifted = BinaryenBinary(
                /* module= */ pipeline.module(),
                /* op=     */ BinaryenShrUInt32(),
                /* lhs=    */ byte,
                /* rhs=    */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(store.offset(attr.id) % 8))
            );
            WasmTemporary value = BinaryenBinary(
                /* module= */ pipeline.module(),
                /* op=     */ BinaryenAndInt32(),
                /* lhs=    */ shifted,
                /* rhs=    */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32((1 << attr.type->size()) - 1))
            );
            pipeline.context().add(e.id, std::move(value));
        } else {
            WasmTemporary value = BinaryenLoad(
                /* module= */ pipeline.module(),
                /* bytes=  */ attr.type->size() / 8,
                /* signed= */ true,
                /* offset= */ store.offset(attr.id) / 8,
                /* align=  */ 0,
                /* type=   */ get_binaryen_type(attr.type),
                /* ptr=    */ row_addr
            );
            pipeline.context().add(e.id, std::move(value));
        }
    }

    /*----- Generate code for rest of the pipeline. ------------------------------------------------------------------*/
    pipeline(*op.parent());

    /*----- Emit code to advance to next row. ------------------------------------------------------------------------*/
    pipeline.block_ += row_addr.set(BinaryenBinary(
        /* module= */ pipeline.module(),
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ row_addr,
        /* right=  */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(store.row_size() / 8))
    ));
}

void WasmStoreCG::operator()(const ColumnStore &store)
{
    std::ostringstream oss;
    auto &table = store.table();

    /* Import null bitmap column address. */
    // TODO

    /*----- Generate code to access null bitmap and value of all required attributes. --------------------------------*/
    std::vector<WasmVariable> col_addrs;
    for (auto &e : op.schema()) {
        auto &attr = table[e.id.name];

        oss.str("");
        oss << table.name << '.' << attr.name;
        auto name = oss.str();

        /*----- Import column address. -------------------------------------------------------------------------------*/
        auto &col_addr = col_addrs.emplace_back(pipeline.CG.fn(), BinaryenTypeInt32());
        pipeline.CG.import(name, BinaryenTypeInt32());
        pipeline.CG.fn().block() += col_addr.set(pipeline.CG.get_imported(name, BinaryenTypeInt32()));

        if (attr.type->size() < 8) {
            unreachable("not implemented");
        } else {
            WasmTemporary value = BinaryenLoad(
                /* module=  */ pipeline.module(),
                /* bytes=   */ attr.type->size() / 8,
                /* signed=  */ true,
                /* offset=  */ 0,
                /* align=   */ 0,
                /* type=    */ get_binaryen_type(attr.type),
                /* ptr=     */ col_addr
            );
            pipeline.context().add(e.id, std::move(value));
        }
    }

    /*----- Generate code for rest of the pipeline. ------------------------------------------------------------------*/
    pipeline(*op.parent());

    /*----- Emit code to advance each column address to next row. ----------------------------------------------------*/
    auto col_addr_it = col_addrs.cbegin();
    for (auto &e : op.schema()) {
        auto &attr = table[e.id.name];
        auto &col_addr = *col_addr_it++;
        const auto attr_size = std::max<std::size_t>(1, attr.type->size() / 8);

        pipeline.block_ += col_addr.set(BinaryenBinary(
            /* module= */ pipeline.module(),
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ col_addr,
            /* right=  */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(attr_size))
        ));
    }
}


/*======================================================================================================================
 * WasmModule
 *====================================================================================================================*/

WasmModule::WasmModule() : ref_(BinaryenModuleCreate()) { }

WasmModule::~WasmModule() { BinaryenModuleDispose(ref_); }

std::pair<uint8_t*, std::size_t> WasmModule::binary() const
{
    auto result = BinaryenModuleAllocateAndWrite(ref_, nullptr);
    return std::make_pair(reinterpret_cast<uint8_t*>(result.binary), result.binaryBytes);
}

std::ostream & m::operator<<(std::ostream &out, const WasmModule &module)
{
    auto result = BinaryenModuleAllocateAndWriteText(module.ref_);
    out << result;
    free(result);
    return out;
}

void WasmModule::dump(std::ostream &out) const {
    out << *this;
    auto [buffer, length] = binary();
    out << '[' << std::hex;
    for (auto ptr = buffer, end = buffer + length; ptr != end; ++ptr) {
        if (ptr != buffer) out << ", ";
        out << "0x" << uint32_t(*ptr);
    }
    out << std::dec;
    out << ']' << std::endl;
    free(buffer);
}
void WasmModule::dump() const { dump(std::cerr); }


/*======================================================================================================================
 * WasmPlatform
 *====================================================================================================================*/

uint32_t WasmPlatform::wasm_counter_ = 0;
std::unordered_map<uint32_t, WasmPlatform::WasmContext> WasmPlatform::contexts_;

WasmModule WasmPlatform::compile(const Operator &op)
{
    return WasmCodeGen::compile(op);
}


/*======================================================================================================================
 * WasmBackend
 *====================================================================================================================*/

void WasmBackend::execute(const Operator &plan) const { platform_->execute(plan); }

#include "backend/WebAssembly.hpp"

#include "backend/Interpreter.hpp"
#include "backend/WasmAlgo.hpp"
#include "backend/WasmUtil.hpp"
#include "IR/CNF.hpp"
#include "storage/ColumnStore.hpp"
#include "storage/RowStore.hpp"
#include "storage/Store.hpp"
#include <binaryen-c.h>
#include <exception>
#include <sstream>
#include <unordered_map>


using namespace db;


/*======================================================================================================================
 * Code generation helper classes
 *====================================================================================================================*/

namespace {

struct GroupingData : OperatorData
{
    WasmStruct *struc = nullptr;
    WasmHashTable *HT = nullptr;
    BinaryenExpressionRef b_begin;
    BinaryenExpressionRef b_end;
    BinaryenExpressionRef b_watermark_high;

    ~GroupingData() {
        delete HT;
        delete struc;
    }
};

struct SortingData : OperatorData
{
    BinaryenExpressionRef b_data_begin;
};

}

/** Compiles a physical plan to WebAssembly. */
struct WasmCodeGen : ConstOperatorVisitor
{
    private:
    WasmModule &module_; ///< the WASM module
    FunctionBuilder main_; ///< the main function (or entry)
    std::unordered_map<std::string, BinaryenExpressionRef> imports_; ///< output locations of columns
    BinaryenExpressionRef b_num_tuples_; ///< number of result tuples produced
    BinaryenExpressionRef b_head_of_heap_; ///< address to the head of the heap

    WasmCodeGen(WasmModule &module)
        : module_(module)
        , main_(module.ref(), "run", BinaryenTypeInt32(), { /* module ID */ BinaryenTypeInt32() })
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
    BinaryenExpressionRef num_tuples() const { return b_num_tuples_; }

    /** Returns the local variable holding the address to the head of the heap. */
    BinaryenExpressionRef head_of_heap() const { return b_head_of_heap_; }

    /** Creates a new local and returns an expression accessing this fresh local. */
    BinaryenExpressionRef add_local(BinaryenType ty) { return main_.add_local(ty); }

    /** Adds a global import to the module.  If the import is mutable, the value of the global is copied over into a
     * fresh local variable.  (This is a work-around, since global imports cannot be mutable.)
     * \param name          the name of the imported value
     * \param ty            the type of the imported value
     * \param is_mutable    whether the imported value is mutable
     * \returns a BinaryenExpressionRef with the value of the global
     */
    BinaryenExpressionRef add_import(std::string name, BinaryenType ty, bool is_mutable = false) {
        auto it = imports_.find(name);
        if (it == imports_.end()) {
            BinaryenAddGlobalImport(
                /* module=             */ module(),
                /* internalName=       */ name.c_str(),
                /* externalModuleName= */ "env",
                /* externalBaseName=   */ name.c_str(),
                /* type=               */ ty,
                /* mutable=            */ false
            );
            auto b_global = BinaryenGlobalGet(
                /* module= */ module(),
                /* name=   */ name.c_str(),
                /* type=   */ ty
            );
            if (is_mutable) {
                auto b_local = add_local(ty);
                main_.block() += BinaryenLocalSet(
                    /* module= */ module(),
                    /* index=  */ BinaryenLocalGetGetIndex(b_local),
                    /* value=  */ b_global
                );
                it = imports_.emplace_hint(it, name, b_local);
            } else {
                it = imports_.emplace_hint(it, name, b_global);
            }
        }
        return it->second;
    }

    /** Returns the local variable that is initialized with the imported global of the given `name`. */
    BinaryenExpressionRef get_import(const std::string &name) const {
        auto it = imports_.find(name);
        insist(it != imports_.end(), "no import with the given name");
        return it->second;
    }

    BinaryenExpressionRef inc_num_tuples(int32_t n = 1) {
        auto b_inc = BinaryenBinary(
            /* module= */ module(),
            /* op=     */ BinaryenAddInt32(),
            /* lhs=    */ b_num_tuples_,
            /* rhs=    */ BinaryenConst(module(), BinaryenLiteralInt32(n))
        );
        return BinaryenLocalSet(
            /* module= */ module(),
            /* index=  */ BinaryenLocalGetGetIndex(b_num_tuples_),
            /* value=  */ b_inc
        );
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
        block += BinaryenLocalSet(
            /* module= */ module(),
            /* index=  */ BinaryenLocalGetGetIndex(head_of_heap()),
            /* value=  */ b_head_aligned
        );
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

    /** Return the current WASM module. */
    BinaryenModuleRef module() { return CG.module(); }

    /** Compiles the pipeline of the given producer to a WASM block. */
    static BinaryenExpressionRef compile(const Producer &prod, WasmCodeGen &CG, const char *name) {
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

    /*----- Count number of result tuples. ---------------------------------------------------------------------------*/
    codegen.b_num_tuples_ = codegen.add_local(BinaryenTypeInt32());

    /*----- Set up head of heap. -------------------------------------------------------------------------------------*/
    codegen.b_head_of_heap_ = codegen.add_import("head_of_heap", BinaryenTypeInt32(), /* is_mutable= */ true);

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

    /*----- Return the new head of heap . ----------------------------------------------------------------------------*/
    codegen.main_.block() += codegen.head_of_heap();

    /*----- Add function. --------------------------------------------------------------------------------------------*/
    codegen.main_.finalize();
    BinaryenAddFunctionExport(module.ref(), "run", "run");

    /*----- Validate module. -----------------------------------------------------------------------------------------*/
    if (not BinaryenModuleValidate(module.ref())) {
        module.dump();
        throw std::logic_error("invalid module");
    }

#if 0
    /*----- Optimize module. -----------------------------------------------------------------------------------------*/
#ifdef NDEBUG
    std::cerr << "WebAssembly before optimization:\n";
    module.dump();
    BinaryenSetOptimizeLevel(2); // O2
    BinaryenSetShrinkLevel(0); // shrinking not required
    BinaryenModuleOptimize(module.ref());
    std::cerr << "WebAssembly after optimization:\n";
    module.dump();
#endif
#endif

    return module;
}

void WasmCodeGen::operator()(const ScanOperator &op)
{
    std::ostringstream oss;
    oss << "scan_" << op.store().table().name;
    const std::string name = oss.str();
    main_.block() += WasmPipelineCG::compile(op, *this, name.c_str());
}

void WasmCodeGen::operator()(const CallbackOperator &op) { (*this)(*op.child(0)); }

void WasmCodeGen::operator()(const PrintOperator &op) { (*this)(*op.child(0)); }

void WasmCodeGen::operator()(const NoOpOperator &op) { (*this)(*op.child(0)); }

void WasmCodeGen::operator()(const FilterOperator &op) { (*this)(*op.child(0)); }

void WasmCodeGen::operator()(const JoinOperator &op)
{
    // TODO implement
    for (auto c : op.children())
        (*this)(*c);
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
    auto data = new GroupingData();
    op.data(data);

    (*this)(*op.child(0));

    /*----- Create new pipeline starting at `GroupingOperator`. ------------------------------------------------------*/
    auto loop_name = "groupby";
    WasmPipelineCG pipeline(*this, loop_name);
    BlockBuilder loop_body(module(), "groupby.body");
    auto b_induction_var = fn().add_local(BinaryenTypeInt32());
    main_.block() += BinaryenLocalSet(
        /* module= */ module(),
        /* index=  */ BinaryenLocalGetGetIndex(b_induction_var),
        /* value=  */ data->b_begin                                 // XXX: should happen immediately before pipeline
    );

    /*----- Advance to first occupied slot. --------------------------------------------------------------------------*/
    {
        BlockBuilder next(module());

        next += BinaryenLocalSet(
            /* module= */ module(),
            /* index=  */ BinaryenLocalGetGetIndex(b_induction_var),
            /* value=  */ data->HT->compute_next_slot(b_induction_var)
        );

        auto b_in_bounds = BinaryenBinary(
            /* module= */ module(),
            /* op=     */ BinaryenLtUInt32(),
            /* left=   */ b_induction_var,
            /* right=  */ data->b_end
        );
        auto b_slot_is_empty = data->HT->is_slot_empty(b_induction_var);

        auto b_if_slot_empty = BinaryenBreak(
            /* module=    */ module(),
            /* name=      */ "groupby.advance_to_first",
            /* condition= */ b_slot_is_empty,
            /* value=     */ nullptr
        );
        next += BinaryenIf(
            /* module=    */ module(),
            /* condition= */ b_in_bounds,
            /* ifTrue=    */ b_if_slot_empty,
            /* ifFalse=   */ nullptr
        );

        auto b_loop_advance = BinaryenLoop(
            /* module= */ module(),
            /* in=     */ "groupby.advance_to_first",
            /* body=   */ next.finalize()
        );
        main_.block() += BinaryenIf(
            /* module=    */ module(),
            /* condition= */ b_slot_is_empty,
            /* ifTrue=    */ b_loop_advance,
            /* ifFalse=   */ nullptr
        );
    }

    /*----- Emit code for data accesses. -----------------------------------------------------------------------------*/
    auto ld = data->HT->load_from_slot(b_induction_var);
    for (auto e : op.schema())
        pipeline.context().add(e.id, ld.get_value(e.id));

    /*----- Emit code for rest of the pipeline. ----------------------------------------------------------------------*/
    swap(pipeline.block_, loop_body);
    pipeline(*op.parent());
    swap(pipeline.block_, loop_body);

    /*----- Increment induction variable, then advance to next occupied slot. ----------------------------------------*/
    {
        BlockBuilder next(module());

        next += BinaryenLocalSet(
            /* module= */ module(),
            /* index=  */ BinaryenLocalGetGetIndex(b_induction_var),
            /* value=  */ data->HT->compute_next_slot(b_induction_var)
        );

        auto b_in_bounds = BinaryenBinary(
            /* module= */ module(),
            /* op=     */ BinaryenLtUInt32(),
            /* left=   */ b_induction_var,
            /* right=  */ data->b_end
        );
        auto b_slot_is_empty = data->HT->is_slot_empty(b_induction_var);

        auto b_if_slot_empty = BinaryenBreak(
            /* module=    */ module(),
            /* name=      */ "groupby.advance",
            /* condition= */ b_slot_is_empty,
            /* value=     */ nullptr
        );
        next += BinaryenIf(
            /* module=    */ module(),
            /* condition= */ b_in_bounds,
            /* ifTrue=    */ b_if_slot_empty,
            /* ifFalse=   */ nullptr
        );

        auto b_loop_advance = BinaryenLoop(
            /* module= */ module(),
            /* in=     */ "groupby.advance",
            /* body=   */ next.finalize()
        );
        loop_body += b_loop_advance;
    }

    /*----- Create loop header.  -------------------------------------------------------------------------------------*/
    /* if (induction_var < data_end) continue; else break; */
    auto b_loop_cond = BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenLtUInt32(),
        /* left=   */ b_induction_var,
        /* right=  */ data->b_end
    );
    loop_body += BinaryenBreak(
        /* module=    */ module(),
        /* name=      */ loop_name,
        /* condition= */ b_loop_cond,
        /* value=     */ nullptr
    );

    /*----- Create loop. ---------------------------------------------------------------------------------------------*/
    auto b_loop = BinaryenLoop(
        /* module= */ module(),
        /* in=     */ loop_name,
        /* body=   */ loop_body.finalize()
    );
    pipeline.block_ += BinaryenIf(
        /* module=    */ module(),
        /* condition= */ b_loop_cond,
        /* ifTrue=    */ b_loop,
        /* ifFalse=   */ nullptr
    );
    main_.block() += pipeline.block_.finalize();
}

void WasmCodeGen::operator()(const SortingOperator &op)
{
    auto data = new SortingData();
    op.data(data);

    (*this)(*op.child(0));

    /* Save the current head of heap as the end of the data to sort. */
    auto b_data_end = add_local(BinaryenTypeInt32());
    main_.block() += BinaryenLocalSet(
        /* module= */ module(),
        /* index=  */ BinaryenLocalGetGetIndex(b_data_end),
        /* value=  */ head_of_heap()
    );
    align_head_of_heap();

    /*----- Generate sorting algorithm and invoke with start and end of data segment. --------------------------------*/
    WasmQuickSort qsort(op.child(0)->schema(), op.order_by(), WasmPartitionBranchless{});
    BinaryenExpressionRef qsort_args[] = { data->b_data_begin, b_data_end };
    auto b_qsort = qsort.emit(module());
    main_.block() += BinaryenCall(
        /* module=      */ module(),
        /* target=      */ BinaryenFunctionGetName(b_qsort),
        /* operands=    */ qsort_args,
        /* numOperands= */ 2,
        /* returnType=  */ BinaryenTypeNone()
    );

    /*----- Create new pipeline starting at `SortingOperator`. -------------------------------------------------------*/
    auto loop_name = "orderby";
    WasmPipelineCG pipeline(*this, loop_name);
    BlockBuilder loop_body(module(), "orderby.body");

    /*----- Emit code for data accesses. -----------------------------------------------------------------------------*/
    std::size_t offset = 0;
    std::size_t alignment = 0;
    for (auto &attr : op.child(0)->schema()) {
        const std::size_t size_in_bytes = attr.type->size() < 8 ? 1 : attr.type->size() / 8;
        alignment = std::max(alignment, size_in_bytes);
        if (offset % size_in_bytes)
            offset += size_in_bytes - (offset % size_in_bytes); // self-align


        auto b_val = BinaryenLoad(
            /* module= */ module(),
            /* bytes=  */ size_in_bytes,
            /* signed= */ true,
            /* offset= */ offset,
            /* align=  */ 0,
            /* type=   */ get_binaryen_type(attr.type),
            /* ptr=    */ data->b_data_begin
        );
        pipeline.context().add(attr.id, b_val);

        offset += size_in_bytes;
    }
    if (offset % alignment)
        offset += alignment - (offset % alignment); // align tuple

    /*----- Emit code for rest of the pipeline. ----------------------------------------------------------------------*/
    swap(pipeline.block_, loop_body);
    pipeline(*op.parent());
    swap(pipeline.block_, loop_body);

    /*----- Increment induction variable. ----------------------------------------------------------------------------*/
    auto b_inc = BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ data->b_data_begin,
        /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(offset))
    );
    loop_body += BinaryenLocalSet(
        /* module= */ module(),
        /* index=  */ BinaryenLocalGetGetIndex(data->b_data_begin),
        /* value=  */ b_inc
    );

    /*----- Create loop header.  -------------------------------------------------------------------------------------*/
    /* if (data_begin < data_end) continue; else break; */
    auto b_loop_cond = BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenLtUInt32(),
        /* left=   */ data->b_data_begin,
        /* right=  */ b_data_end
    );
    loop_body += BinaryenBreak(
        /* module=    */ module(),
        /* name=      */ loop_name,
        /* condition= */ b_loop_cond,
        /* value=     */ nullptr
    );

    /*----- Create loop. ---------------------------------------------------------------------------------------------*/
    auto b_loop = BinaryenLoop(
        /* module= */ module(),
        /* in=     */ loop_name,
        /* body=   */ loop_body.finalize()
    );
    pipeline.block_ += BinaryenIf(
        /* module=    */ module(),
        /* condition= */ b_loop_cond,
        /* ifTrue=    */ b_loop,
        /* ifFalse=   */ nullptr
    );
    main_.block() += pipeline.block_.finalize();
}


/*======================================================================================================================
 * WasmPipelineCG
 *====================================================================================================================*/

void WasmPipelineCG::emit_write_results(const Schema &schema)
{
    std::size_t offset = 0;
    auto b_out = CG.head_of_heap();
    for (auto &attr : schema) {
        auto value = context()[attr.id];
        auto bytes = attr.type->size() == 64 ? 8 : 4;
        block_ += BinaryenStore(
            /* module= */ module(),
            /* bytes=  */ bytes,
            /* offset= */ offset,
            /* align=  */ 0,
            /* ptr=    */ b_out,
            /* value=  */ value,
            /* type=   */ get_binaryen_type(attr.type)
        );
        offset += 8;
    }

    auto inc = BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ b_out,
        /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(offset))
    );
    block_ += BinaryenLocalSet(
        /* module = */ module(),
        /* index=   */ BinaryenLocalGetGetIndex(CG.head_of_heap()),
        /* value=   */ inc
    );
}

void WasmPipelineCG::operator()(const ScanOperator &op)
{
    auto &table = op.store().table();
    std::ostringstream oss;

    oss.str("");
    oss << "scan_" << table.name;
    auto loop_name = oss.str();
    oss << ".body";
    auto body_name = oss.str();
    BlockBuilder loop_body(module(), body_name.c_str());

    /*----- Create induction variable. -------------------------------------------------------------------------------*/
    auto b_induction_var = CG.add_local(BinaryenTypeInt32());

    /*----- Import table size. ---------------------------------------------------------------------------------------*/
    oss.str("");
    oss << table.name << "_num_rows";
    auto table_size = oss.str();
    BinaryenAddGlobalImport(
        /* module=             */ module(),
        /* internalName=       */ table_size.c_str(),
        /* externalModuleName= */ "env",
        /* externalBaseName=   */ table_size.c_str(),
        /* type=               */ BinaryenTypeInt32(),
        /* mutable=            */ false
    );
    auto b_table_size = BinaryenGlobalGet(
        /* module= */ module(),
        /* name=   */ table_size.c_str(),
        /* type=   */ BinaryenTypeInt32()
    );

    swap(block_, loop_body);
    /*----- Generate code to access attributes and emit code for the rest of the pipeline. ---------------------------*/
    WasmStoreCG store(*this, op);
    store(op.store());
    swap(block_, loop_body);

    /*----- Increment induction variable. ----------------------------------------------------------------------------*/
    auto b_inc_induction_var = BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ b_induction_var,
        /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(1))
    );
    loop_body += BinaryenLocalSet(
        /* module= */ module(),
        /* index=  */ BinaryenLocalGetGetIndex(b_induction_var),
        /* value=  */ b_inc_induction_var
    );

    /*----- Create loop header. --------------------------------------------------------------------------------------*/
    /* if (induction_var < table_size) continue; else break; */
    auto b_loop_cond = BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenLtUInt32(),
        /* left=   */ b_induction_var,
        /* right=  */ b_table_size
    );
    loop_body += BinaryenBreak(
        /* module=    */ module(),
        /* name=      */ loop_name.c_str(),
        /* condition= */ b_loop_cond,
        /* value=     */ nullptr
    );

    /*----- Create loop. ---------------------------------------------------------------------------------------------*/
    auto b_loop = BinaryenLoop(
        /* module= */ module(),
        /* in=     */ loop_name.c_str(),
        /* body=   */ loop_body.finalize()
    );
    block_ += BinaryenIf(
        /* module=    */ module(),
        /* condition= */ b_loop_cond,
        /* ifTrue=    */ b_loop,
        /* ifFalse=   */ nullptr
    );
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
    (*this)(*op.parent());
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
    auto b_count = CG.add_local(BinaryenTypeInt32());

    /* Check whether the pipeline has exceeded the limit. */
    auto b_cond_exceeds_limits = BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenGeSInt32(),
        /* lhs=    */ b_count,
        /* rhs=    */ BinaryenConst(module(), BinaryenLiteralInt32(op.limit() + op.offset()))
    );

    /* Abort the pipeline once the limit is exceeded. */
    block_ += BinaryenBreak(
        /* module= */ module(),
        /* name=   */ block_.name(),
        /* cond=   */ b_cond_exceeds_limits,
        /* value=  */ nullptr
    );

    /* Check whether the pipeline is within the limits. */
    auto b_cond_within_limits = BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenGeSInt32(),
        /* lhs=    */ b_count,
        /* rhs=    */ BinaryenConst(module(), BinaryenLiteralInt32(op.offset()))
    );
    block_ += BinaryenIf(
        /* module=  */ module(),
        /* cond=    */ b_cond_within_limits,
        /* ifTrue=  */ within_limits_block.finalize(),
        /* ifFalse= */ nullptr
    );

    auto b_inc = BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenAddInt32(),
        /* lhs=    */ b_count,
        /* rhs=    */ BinaryenConst(module(), BinaryenLiteralInt32(1))
    );
    block_ += BinaryenLocalSet(
        /* module= */ module(),
        /* index=  */ BinaryenLocalGetGetIndex(b_count),
        /* value=  */ b_inc
    );
}

void WasmPipelineCG::operator()(const GroupingOperator &op)
{
    auto data = as<GroupingData>(op.data());
    WasmHashMumur3_64A hasher;

    /*----- Allocate hash table. -------------------------------------------------------------------------------------*/
    constexpr uint32_t INITIAL_CAPACITY = 32;
    data->b_begin = CG.fn().add_local(BinaryenTypeInt32());
    data->b_end = CG.fn().add_local(BinaryenTypeInt32());
    data->b_watermark_high = CG.fn().add_local(BinaryenTypeInt32());
    CG.fn().block() += BinaryenLocalSet(
        /* module= */ module(),
        /* index=  */ BinaryenLocalGetGetIndex(data->b_begin),
        /* value=  */ CG.head_of_heap()
    );
    data->struc = new WasmStruct(module(), op.schema());
    data->HT = new WasmRefCountingHashTable(module(), CG.fn(), *data->struc);
    auto HT = as<WasmRefCountingHashTable>(data->HT);

    auto b_HT_end = HT->create_table(CG.fn().block(), CG.head_of_heap(), INITIAL_CAPACITY);
    CG.fn().block() += BinaryenLocalSet(
        /* module= */ module(),
        /* index=  */ BinaryenLocalGetGetIndex(data->b_end),
        /* value=  */ b_HT_end
    );
    CG.fn().block() += BinaryenLocalSet(
        /* module= */ module(),
        /* index=  */ BinaryenLocalGetGetIndex(data->b_watermark_high),
        /* value=  */ BinaryenConst(module(), BinaryenLiteralInt32(8 * INITIAL_CAPACITY / 10))
    );

    /*----- Update head of heap. -------------------------------------------------------------------------------------*/
    CG.fn().block() += BinaryenLocalSet(
        /* module= */ module(),
        /* index=  */ BinaryenLocalGetGetIndex(CG.head_of_heap()),
        /* value=  */ data->b_end
    );
    CG.align_head_of_heap();

    /*----- Initialize hash table. -----------------------------------------------------------------------------------*/
    HT->clear_table(CG.fn().block(), data->b_begin, data->b_end);

    /*----- Create a counter for the number of groups. ---------------------------------------------------------------*/
    auto b_num_groups = CG.fn().add_local(BinaryenTypeInt32());
    CG.fn().block() += BinaryenLocalSet(
        /* module= */ module(),
        /* index=  */ BinaryenLocalGetGetIndex(b_num_groups),
        /* value=  */ BinaryenConst(module(), BinaryenLiteralInt32(0))
    );

    /*----- Emit a function to perform the rehashing. ----------------------------------------------------------------*/
    std::vector<BinaryenType> rehash_params;
    rehash_params.reserve(4);
    rehash_params.push_back(BinaryenTypeInt32()); // 0: addr
    rehash_params.push_back(BinaryenTypeInt32()); // 1: mask
    rehash_params.push_back(BinaryenTypeInt32()); // 2: new addr
    rehash_params.push_back(BinaryenTypeInt32()); // 3: new mask
    FunctionBuilder fn_rehash(module(), "WasmRefCountingHashTable.rehash", BinaryenTypeNone(), rehash_params);
    {
        WasmRefCountingHashTable HT_old(
            /* module= */ module(),
            /* fn=     */ fn_rehash,
            /* struc=  */ HT->struc,
            /* b_addr= */ BinaryenLocalGet(module(), 0, BinaryenTypeInt32()),
            /* b_mask= */ BinaryenLocalGet(module(), 1, BinaryenTypeInt32())
        );

        WasmRefCountingHashTable HT_new(
            /* module= */ module(),
            /* fn=     */ fn_rehash,
            /* struc=  */ HT->struc,
            /* b_addr= */ BinaryenLocalGet(module(), 2, BinaryenTypeInt32()),
            /* b_mask= */ BinaryenLocalGet(module(), 3, BinaryenTypeInt32())
        );

        /*----- Compute properties of old hash table. ----------------------------------------------------------------*/
        auto b_addr_old = BinaryenLocalGet(
            /* module= */ module(),
            /* index=  */ 0,
            /* type=   */ BinaryenTypeInt32()
        );
        auto b_mask_old = BinaryenLocalGet(
            /* module= */ module(),
            /* index=  */ 1,
            /* type=   */ BinaryenTypeInt32()
        );
        auto b_size_old = BinaryenBinary(
            /* module= */ module(),
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ b_mask_old,
            /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(1))
        );
        auto b_size_in_bytes_old = BinaryenBinary(
            /* module= */ module(),
            /* op=     */ BinaryenMulInt32(),
            /* left=   */ b_size_old,
            /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(HT_old.entry_size()))
        );
        auto b_table_end_old = fn_rehash.add_local(BinaryenTypeInt32());
        fn_rehash.block() += BinaryenLocalSet(
            /* module= */ module(),
            /* index=  */ BinaryenLocalGetGetIndex(b_table_end_old),
            /* value=  */ BinaryenBinary(
                              /* module= */ module(),
                              /* op=     */ BinaryenAddInt32(),
                              /* left=   */ b_addr_old,
                              /* right=  */ b_size_in_bytes_old
                          )
        );

        /*----- Initialize a runner to traverse the old hash table. --------------------------------------------------*/
        auto b_runner = fn_rehash.add_local(BinaryenTypeInt32());
        fn_rehash.block() += BinaryenLocalSet(
            /* module= */ module(),
            /* index=  */ BinaryenLocalGetGetIndex(b_runner),
            /* value=  */ b_addr_old
        );

        /*----- Advance to first occupied slot. ----------------------------------------------------------------------*/
        {
            BlockBuilder next(module());

            next += BinaryenLocalSet(
                /* module= */ module(),
                /* index=  */ BinaryenLocalGetGetIndex(b_runner),
                /* value=  */ HT_old.compute_next_slot(b_runner)
            );

            auto b_in_bounds = BinaryenBinary(
                /* module= */ module(),
                /* op=     */ BinaryenLtUInt32(),
                /* left=   */ b_runner,
                /* right=  */ b_table_end_old
            );
            auto b_slot_is_empty = HT_old.is_slot_empty(b_runner);

            auto b_if_slot_empty = BinaryenBreak(
                /* module=    */ module(),
                /* name=      */ "groupby.advance_to_first",
                /* condition= */ b_slot_is_empty,
                /* value=     */ nullptr
            );
            next += BinaryenIf(
                /* module=    */ module(),
                /* condition= */ b_in_bounds,
                /* ifTrue=    */ b_if_slot_empty,
                /* ifFalse=   */ nullptr
            );

            auto b_loop_advance = BinaryenLoop(
                /* module= */ module(),
                /* in=     */ "groupby.advance_to_first",
                /* body=   */ next.finalize()
            );
            fn_rehash.block() += BinaryenIf(
                /* module=    */ module(),
                /* condition= */ b_slot_is_empty,
                /* ifTrue=    */ b_loop_advance,
                /* ifFalse=   */ nullptr
            );
        }

        /*----- Iterate over all entries in the old hash table. ------------------------------------------------------*/
        const char *loop_name = "rehash.foreach";
        BlockBuilder loop_body(module(), "rehash.foreach.body");

        /*----- Re-insert the current element in the new hash table. -------------------------------------------------*/
        {
            /*----- Compute grouping key. ----------------------------------------------------------------------------*/
            auto ld = HT_old.load_from_slot(b_runner);
            std::vector<BinaryenExpressionRef> key;
            for (auto grp : op.group_by()) {
                auto k = fn_rehash.add_local(get_binaryen_type(grp->type()));
                loop_body += BinaryenLocalSet(
                    /* module= */ module(),
                    /* index=  */ BinaryenLocalGetGetIndex(k),
                    /* value=  */ ld.compile(*grp)
                );
                key.push_back(k);
            }

            /*----- Compute group hash. ------------------------------------------------------------------------------*/
            auto b_hash = hasher.emit(module(), fn_rehash, loop_body, key);
            auto b_hash_i32 = BinaryenUnary(
                /* module= */ module(),
                /* op=     */ BinaryenWrapInt64(),
                /* value=  */ b_hash
            );

            /*----- Compute address of bucket. -----------------------------------------------------------------------*/
            auto b_bucket_addr = fn_rehash.add_local(BinaryenTypeInt32());
            loop_body += BinaryenLocalSet(
                /* module= */ module(),
                /* index=  */ BinaryenLocalGetGetIndex(b_bucket_addr),
                /* value=  */ HT_new.hash_to_bucket(b_hash_i32)
            );

            /*----- Locate entry with key `key` in the bucket, or end of bucket if no such key exists. ---------------*/
            auto [ b_slot_addr, b_steps ] = HT_new.find_in_bucket(loop_body, b_bucket_addr, key);

            /*----- Create new entry in new hash table. --------------------------------------------------------------*/
            HT_new.emplace(loop_body, b_bucket_addr, b_steps, b_slot_addr, key);

            /*----- Copy group payload. ------------------------------------------------------------------------------*/
            for (std::size_t i = 0; i != op.aggregates().size(); ++i) {
                auto e = op.schema()[op.group_by().size() + i];
                loop_body += HT_new.store_value_to_slot(b_slot_addr, e.id, ld.get_value(e.id));
            }
        }

        /*----- Advance to next occupied slot. -----------------------------------------------------------------------*/
        {
            BlockBuilder next(module());

            next += BinaryenLocalSet(
                /* module= */ module(),
                /* index=  */ BinaryenLocalGetGetIndex(b_runner),
                /* value=  */ HT_new.compute_next_slot(b_runner)
            );

            auto b_in_bounds = BinaryenBinary(
                /* module= */ module(),
                /* op=     */ BinaryenLtUInt32(),
                /* left=   */ b_runner,
                /* right=  */ b_table_end_old
            );
            auto b_slot_is_empty = HT_new.is_slot_empty(b_runner);

            auto b_if_slot_empty = BinaryenBreak(
                /* module=    */ module(),
                /* name=      */ "groupby.advance",
                /* condition= */ b_slot_is_empty,
                /* value=     */ nullptr
            );
            next += BinaryenIf(
                /* module=    */ module(),
                /* condition= */ b_in_bounds,
                /* ifTrue=    */ b_if_slot_empty,
                /* ifFalse=   */ nullptr
            );

            auto b_loop_advance = BinaryenLoop(
                /* module= */ module(),
                /* in=     */ "groupby.advance",
                /* body=   */ next.finalize()
            );
            loop_body += b_loop_advance;
        }

        /*----- Create loop header. ----------------------------------------------------------------------------------*/
        {
            auto b_loop_cond = BinaryenBinary(
                /* module= */ module(),
                /* op=     */ BinaryenLtUInt32(),
                /* left=   */ b_runner,
                /* right=  */ b_table_end_old
            );
            loop_body += BinaryenBreak(
                /* module=    */ module(),
                /* name=      */ loop_name,
                /* condition= */ b_loop_cond,
                /* value=     */ nullptr
            );
        }

        /*----- Create loop. -----------------------------------------------------------------------------------------*/
        auto b_loop = BinaryenLoop(
            /* module= */ module(),
            /* in=     */ loop_name,
            /* body=   */ loop_body.finalize()
        );
        fn_rehash.block() += b_loop;

        fn_rehash.finalize();
    }

    /*----- Emit code to resize the hash table. ----------------------------------------------------------------------*/
    BlockBuilder perform_rehash(module(), "groupby.rehash");

    auto b_mask_x2 = BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenShlInt32(),
        /* left=   */ HT->mask(),
        /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(1))
    );

    /*----- Compute the mask for the new size. -----------------------------------------------------------------------*/
    auto b_mask_new = CG.fn().add_local(BinaryenTypeInt32());
    perform_rehash += BinaryenLocalSet(
        /* module= */ module(),
        /* index=  */ BinaryenLocalGetGetIndex(b_mask_new),
        /* value=  */ BinaryenBinary(
                          /* module= */ module(),
                          /* op=     */ BinaryenAddInt32(),
                          /* left=   */ b_mask_x2,
                          /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(1))
                      )
    );

    /*----- Compute new hash table size. -----------------------------------------------------------------------------*/
    auto b_size_in_bytes_old = BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenSubInt32(),
        /* left=   */ data->b_end,
        /* right=  */ data->b_begin
    );
    auto b_size_in_bytes_new = BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenShlInt32(),
        /* left=   */ b_size_in_bytes_old,
        /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(1))
    );

    /*----- Allocate new hash table. ---------------------------------------------------------------------------------*/
    auto b_HT_new_begin = CG.fn().add_local(BinaryenTypeInt32());
    perform_rehash += BinaryenLocalSet(
        /* module= */ module(),
        /* index=  */ BinaryenLocalGetGetIndex(b_HT_new_begin),
        /* value=  */ CG.head_of_heap()
    );
    auto b_HT_new_end = CG.fn().add_local(BinaryenTypeInt32());
    perform_rehash += BinaryenLocalSet(
        /* module= */ module(),
        /* index=  */ BinaryenLocalGetGetIndex(b_HT_new_end),
        /* value=  */ BinaryenBinary(
                          /* module= */ module(),
                          /* op=     */ BinaryenAddInt32(),
                          /* left=   */ b_HT_new_begin,
                          /* right=  */ b_size_in_bytes_new
                      )
    );
    perform_rehash += BinaryenLocalSet(
        /* module= */ module(),
        /* index=  */ BinaryenLocalGetGetIndex(CG.head_of_heap()),
        /* value=  */ b_HT_new_end
    );
    CG.align_head_of_heap(perform_rehash);

    /*----- Rehash to double size. -----------------------------------------------------------------------------------*/
    BinaryenExpressionRef rehash_args[4] = {
        /* addr_old= */ HT->addr(),
        /* mask_old= */ HT->mask(),
        /* addr_new= */ b_HT_new_begin,
        /* mask_new= */ b_mask_new
    };
    perform_rehash += BinaryenCall(
        /* module=      */ module(),
        /* target=      */ fn_rehash.name(),
        /* operands=    */ rehash_args,
        /* numOperands= */ ARR_SIZE(rehash_args),
        /* returnType=  */ BinaryenTypeNone()
    );

    /*----- Update hash table and grouping data. ---------------------------------------------------------------------*/
    perform_rehash += BinaryenLocalSet(
        /* module= */ module(),
        /* index=  */ BinaryenLocalGetGetIndex(HT->addr()),
        /* value=  */ b_HT_new_begin
    );
    perform_rehash += BinaryenLocalSet(
        /* module= */ module(),
        /* index=  */ BinaryenLocalGetGetIndex(HT->mask()),
        /* value=  */ b_mask_new
    );

    perform_rehash += BinaryenLocalSet(
        /* module= */ module(),
        /* index=  */ BinaryenLocalGetGetIndex(data->b_begin),
        /* value=  */ b_HT_new_begin
    );
    perform_rehash += BinaryenLocalSet(
        /* module= */ module(),
        /* index=  */ BinaryenLocalGetGetIndex(data->b_end),
        /* value=  */ b_HT_new_end
    );

    /*----- Update high watermark. -----------------------------------------------------------------------------------*/
    auto b_size_new = BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ HT->mask(),
        /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(1))

    );
    auto b_watermark_high = BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenDivUInt32(),
        /* left=   */ BinaryenBinary(
                          /* module= */ module(),
                          /* op=     */ BinaryenMulInt32(),
                          /* left=   */ b_size_new,
                          /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(8))
        ),
        /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(10))
    );
    perform_rehash += BinaryenLocalSet(
        /* module= */ module(),
        /* index=  */ BinaryenLocalGetGetIndex(data->b_watermark_high),
        /* value=  */ b_watermark_high
    );

    /*----- Perform resizing to twice the capacity when the high watermark is reached. -------------------------------*/
    auto b_has_reached_watermark = BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenGeUInt32(),
        /* left=   */ b_num_groups,
        /* right=  */ data->b_watermark_high
    );
    block_ += BinaryenIf(
        /* module=    */ module(),
        /* condition= */ b_has_reached_watermark,
        /* ifTrue=    */ perform_rehash.finalize(),
        /* ifFalse=   */ nullptr
    );

    /*----- Compute grouping key. ------------------------------------------------------------------------------------*/
    std::vector<BinaryenExpressionRef> key;
    for (auto grp : op.group_by()) {
        auto k = CG.fn().add_local(get_binaryen_type(grp->type()));
        block_ += BinaryenLocalSet(
            /* module= */ module(),
            /* index=  */ BinaryenLocalGetGetIndex(k),
            /* value=  */ context().compile(*grp)
        );
        key.push_back(k);
    }

    /*----- Compute hash. --------------------------------------------------------------------------------------------*/
    auto b_hash = hasher.emit(module(), CG.fn(), block_, key);
    auto b_hash_i32 = BinaryenUnary(
        /* module= */ module(),
        /* op=     */ BinaryenWrapInt64(),
        /* value=  */ b_hash
    );

    /*----- Compute address of bucket. -------------------------------------------------------------------------------*/
    auto b_bucket_addr = CG.fn().add_local(BinaryenTypeInt32());
    block_ += BinaryenLocalSet(
        /* module= */ module(),
        /* index=  */ BinaryenLocalGetGetIndex(b_bucket_addr),
        /* value=  */ data->HT->hash_to_bucket(b_hash_i32)
    );

    /*----- Locate entry with key `key` in the bucket, or end of bucket if no such key exists. -----------------------*/
    auto [ b_slot_addr, b_steps ] = data->HT->find_in_bucket(block_, b_bucket_addr, key);

    /*----- Create or update the group. ------------------------------------------------------------------------------*/
    BlockBuilder create_group(module(), "group_by.create_group");
    BlockBuilder update_group(module(), "group_by.update");

    auto b_num_groups_inc = BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ b_num_groups,
        /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(1))
    );
    create_group += BinaryenLocalSet(
        /* module= */ module(),
        /* index=  */ BinaryenLocalGetGetIndex(b_num_groups),
        /* value=  */ b_num_groups_inc
    );

    auto ld_slot = data->HT->load_from_slot(b_slot_addr);
    data->HT->emplace(create_group, b_bucket_addr, b_steps, b_slot_addr, key);

    for (std::size_t i = 0; i != op.aggregates().size(); ++i) {
        auto e = op.schema()[op.group_by().size() + i];
        auto agg = op.aggregates()[i];

        auto fn_expr = as<const FnApplicationExpr>(agg);
        auto &fn = fn_expr->get_function();
        insist(fn.kind == Function::FN_Aggregate, "not an aggregation function");

        /*----- Emit code to evaluate arguments. ---------------------------------------------------------------------*/
        insist(fn_expr->args.size() <= 1, "unsupported aggregate with more than one argument");
        std::vector<BinaryenExpressionRef> args;
        for (auto arg : fn_expr->args) {
            auto eval_arg = context().compile(*arg);
            args.push_back(eval_arg);
        }

        switch (fn.fnid) {
            default:
                unreachable("unsupported aggregate function");

            case Function::FN_MIN: {
                insist(args.size() == 1, "aggregate function expects exactly one argument");
                create_group += data->HT->store_value_to_slot(b_slot_addr, e.id, args[0]);
                auto b_old_val = ld_slot.get_value(e.id);
                auto n = as<const Numeric>(e.type);
                switch (n->kind) {
                    case Numeric::N_Int: {
                        if (n->size() <= 32) {
                            auto b_less = BinaryenBinary(
                                /* module= */ module(),
                                /* op=     */ BinaryenLtSInt32(),
                                /* left=   */ b_old_val,
                                /* right=  */ args[0]
                            );
                            auto b_new_val = BinaryenSelect(
                                /* module=    */ module(),
                                /* condition= */ b_less,
                                /* ifTrue=    */ b_old_val,
                                /* ifFalse=   */ args[0],
                                /* type=      */ BinaryenTypeInt32()
                            );
                            update_group += data->HT->store_value_to_slot(b_slot_addr, e.id, b_new_val);
                        } else {
                            auto b_less = BinaryenBinary(
                                /* module= */ module(),
                                /* op=     */ BinaryenLtSInt64(),
                                /* left=   */ b_old_val,
                                /* right=  */ args[0]
                            );
                            auto b_new_val = BinaryenSelect(
                                /* module=    */ module(),
                                /* condition= */ b_less,
                                /* ifTrue=    */ b_old_val,
                                /* ifFalse=   */ args[0],
                                /* type=      */ BinaryenTypeInt32()
                            );
                            update_group += data->HT->store_value_to_slot(b_slot_addr, e.id, b_new_val);
                        }
                        break;
                    }

                    case Numeric::N_Decimal:
                        unreachable("not implemented");

                    case Numeric::N_Float:
                        if (n->size() == 32) {
                            auto b_new_val = BinaryenBinary(
                                /* module= */ module(),
                                /* op=     */ BinaryenMinFloat32(),
                                /* left=   */ b_old_val,
                                /* right=  */ args[0]
                            );
                            update_group += data->HT->store_value_to_slot(b_slot_addr, e.id, b_new_val);
                        } else {
                            auto b_new_val = BinaryenBinary(
                                /* module= */ module(),
                                /* op=     */ BinaryenMinFloat64(),
                                /* left=   */ b_old_val,
                                /* right=  */ args[0]
                            );
                            update_group += data->HT->store_value_to_slot(b_slot_addr, e.id, b_new_val);
                        }
                        break;
                }
                break;
            }

            case Function::FN_MAX: {
                insist(args.size() == 1, "aggregate function expects exactly one argument");
                create_group += data->HT->store_value_to_slot(b_slot_addr, e.id, args[0]);
                auto b_old_val = ld_slot.get_value(e.id);
                auto n = as<const Numeric>(e.type);
                switch (n->kind) {
                    case Numeric::N_Int: {
                        if (n->size() <= 32) {
                            auto b_greater = BinaryenBinary(
                                /* module= */ module(),
                                /* op=     */ BinaryenGtSInt32(),
                                /* left=   */ b_old_val,
                                /* right=  */ args[0]
                            );
                            auto b_new_val = BinaryenSelect(
                                /* module=    */ module(),
                                /* condition= */ b_greater,
                                /* ifTrue=    */ b_old_val,
                                /* ifFalse=   */ args[0],
                                /* type=      */ BinaryenTypeInt32()
                            );
                            update_group += data->HT->store_value_to_slot(b_slot_addr, e.id, b_new_val);
                        } else {
                            auto b_greater = BinaryenBinary(
                                /* module= */ module(),
                                /* op=     */ BinaryenGtSInt64(),
                                /* left=   */ b_old_val,
                                /* right=  */ args[0]
                            );
                            auto b_new_val = BinaryenSelect(
                                /* module=    */ module(),
                                /* condition= */ b_greater,
                                /* ifTrue=    */ b_old_val,
                                /* ifFalse=   */ args[0],
                                /* type=      */ BinaryenTypeInt32()
                            );
                            update_group += data->HT->store_value_to_slot(b_slot_addr, e.id, b_new_val);
                        }
                        break;
                    }

                    case Numeric::N_Decimal:
                        unreachable("not implemented");

                    case Numeric::N_Float:
                        if (n->size() == 32) {
                            auto b_new_val = BinaryenBinary(
                                /* module= */ module(),
                                /* op=     */ BinaryenMaxFloat32(),
                                /* left=   */ b_old_val,
                                /* right=  */ args[0]
                            );
                            update_group += data->HT->store_value_to_slot(b_slot_addr, e.id, b_new_val);
                        } else {
                            auto b_new_val = BinaryenBinary(
                                /* module= */ module(),
                                /* op=     */ BinaryenMaxFloat64(),
                                /* left=   */ b_old_val,
                                /* right=  */ args[0]
                            );
                            update_group += data->HT->store_value_to_slot(b_slot_addr, e.id, b_new_val);
                        }
                        break;
                }
                break;
            }

            case Function::FN_SUM: {
                insist(args.size() == 1, "aggregate function expects exactly one argument");
                auto b_old_val = ld_slot.get_value(e.id);
                auto n = as<const Numeric>(fn_expr->args[0]->type());
                switch (n->kind) {
                    case Numeric::N_Int: {
                        auto b_extd = convert(module(), args[0], n, Type::Get_Integer(Type::TY_Vector, 8));
                        create_group += data->HT->store_value_to_slot(b_slot_addr, e.id, b_extd);
                        auto b_new_val = BinaryenBinary(
                            /* module= */ module(),
                            /* op=     */ BinaryenAddInt64(),
                            /* left=   */ b_old_val,
                            /* right=  */ b_extd
                        );
                        update_group += data->HT->store_value_to_slot(b_slot_addr, e.id, b_new_val);
                        break;
                    }

                    case Numeric::N_Decimal:
                        unreachable("not implemented");

                    case Numeric::N_Float:
                        auto b_extd = convert(module(), args[0], n, Type::Get_Double(Type::TY_Vector));
                        create_group += data->HT->store_value_to_slot(b_slot_addr, e.id, b_extd);
                        auto b_new_val = BinaryenBinary(
                            /* module= */ module(),
                            /* op=     */ BinaryenAddFloat64(),
                            /* left=   */ b_old_val,
                            /* right=  */ b_extd
                        );
                        update_group += data->HT->store_value_to_slot(b_slot_addr, e.id, b_new_val);
                        break;
                }
                break;
            }

            case Function::FN_COUNT: {
                if (args.size() == 0) {
                    create_group += data->HT->store_value_to_slot(b_slot_addr, e.id,
                                                                  BinaryenConst(module(), BinaryenLiteralInt64(1)));
                    auto b_old_val = ld_slot.get_value(e.id);
                    auto b_new_val = BinaryenBinary(
                        /* module= */ module(),
                        /* op=     */ BinaryenAddInt64(),
                        /* left=   */ b_old_val,
                        /* right=  */ BinaryenConst(module(), BinaryenLiteralInt64(1))
                    );
                    update_group += data->HT->store_value_to_slot(b_slot_addr, e.id, b_new_val);
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
        /* condition= */ data->HT->is_slot_empty(b_slot_addr),
        /* ifTrue=    */ create_group.finalize(),
        /* ifFalse=   */ update_group.finalize()
    );
}

void WasmPipelineCG::operator()(const SortingOperator &op)
{
    auto data = as<SortingData>(op.data());

    /* Save the current head of heap as the beginning of the data to sort. */
    data->b_data_begin = CG.fn().add_local(BinaryenTypeInt32());
    CG.fn().block() += BinaryenLocalSet(
        /* module= */ module(),
        /* index=  */ BinaryenLocalGetGetIndex(data->b_data_begin),
        /* value=  */ CG.head_of_heap()
    );

    /* Append current tuple to consecutive memory of tuples to sort. */
    std::size_t offset = 0;
    std::size_t alignment = 0;
    auto &S = op.child(0)->schema();
    for (auto &attr : S) {
        const std::size_t size_in_bytes = attr.type->size() < 8 ? 1 : attr.type->size() / 8;
        alignment = std::max(alignment, size_in_bytes);
        if (offset % size_in_bytes)
            offset += size_in_bytes - (offset % size_in_bytes); // self-align

        auto b_val = context()[attr.id];
        block_ += BinaryenStore(
            /* module= */ module(),
            /* bytes=  */ size_in_bytes,
            /* offset= */ offset,
            /* align=  */ 0,
            /* ptr=    */ CG.head_of_heap(),
            /* value=  */ b_val,
            /* type=   */ BinaryenExpressionGetType(b_val)
        );
        offset += size_in_bytes;
    }
    if (offset % alignment)
        offset += alignment - (offset % alignment); // align tuple

    /* Advance head of heap. */
    auto b_inc = BinaryenBinary(
        /* module= */ module(),
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ CG.head_of_heap(),
        /* right=  */ BinaryenConst(module(), BinaryenLiteralInt32(offset))
    );
    block_ += BinaryenLocalSet(
        /* module= */ module(),
        /* index=  */ BinaryenLocalGetGetIndex(CG.head_of_heap()),
        /* value=  */ b_inc
    );
}


/*======================================================================================================================
 * WasmStoreCG
 *====================================================================================================================*/

void WasmStoreCG::operator()(const RowStore &store)
{
    std::ostringstream oss;
    auto &table = store.table();

    /*----- Import table address. ------------------------------------------------------------------------------------*/
    auto b_row_addr = pipeline.CG.add_import(table.name, BinaryenTypeInt32(), /* is_mutable= */ true);

    /*----- Generate code to access null bitmap and value of all required attributes. --------------------------------*/
    const auto null_bitmap_offset = store.offset(table.size());
    for (auto &e : op.schema()) {
        auto &attr = table[e.id.name];

        /*----- Generate code for null bit check. --------------------------------------------------------------------*/
        {
            const auto null_bit_offset = null_bitmap_offset + attr.id;
            const auto qwords = null_bit_offset / 32;
            const auto bits = null_bit_offset % 32;

            auto b_null_bitmap_addr = BinaryenBinary(
                /* module= */ pipeline.module(),
                /* op=     */ BinaryenAddInt32(),
                /* left=   */ b_row_addr,
                /* right=  */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(qwords))
            );
            auto b_qword = BinaryenLoad(
                /* module= */ pipeline.module(),
                /* bytes=  */ 4,
                /* signed= */ false,
                /* offset= */ 0,
                /* align=  */ 0,
                /* type=   */ BinaryenTypeInt32(),
                /* ptr=    */ b_null_bitmap_addr
            );
            auto b_shr = BinaryenBinary(
                /* module= */ pipeline.module(),
                /* op=     */ BinaryenShrUInt32(),
                /* left=   */ b_qword,
                /* right=  */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(bits))
            );
            auto b_isnull = BinaryenBinary(
                /* module= */ pipeline.module(),
                /* op=     */ BinaryenAndInt32(),
                /* left=   */ b_shr,
                /* right=  */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(1))
            );
            // TODO add to context nulls
        }

        /*----- Generate code for value access. ----------------------------------------------------------------------*/
        if (attr.type->size() < 8) {
            auto b_byte = BinaryenLoad(
                /* module= */ pipeline.module(),
                /* bytes=  */ 1,
                /* signed= */ false,
                /* offset= */ store.offset(attr.id) / 8,
                /* align=  */ 0,
                /* type=   */ BinaryenTypeInt32(),
                /* ptr=    */ b_row_addr
            );
            auto b_shifted = BinaryenBinary(
                /* module= */ pipeline.module(),
                /* op=     */ BinaryenShrUInt32(),
                /* lhs=    */ b_byte,
                /* rhs=    */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(store.offset(attr.id) % 8))
            );
            auto b_value = BinaryenBinary(
                /* module= */ pipeline.module(),
                /* op=     */ BinaryenAndInt32(),
                /* lhs=    */ b_shifted,
                /* rhs=    */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32((1 << attr.type->size()) - 1))
            );
            pipeline.context().add(e.id, b_value);
        } else {
            auto b_value = BinaryenLoad(
                /* module= */ pipeline.module(),
                /* bytes=  */ attr.type->size() / 8,
                /* signed= */ true,
                /* offset= */ store.offset(attr.id) / 8,
                /* align=  */ 0,
                /* type=   */ get_binaryen_type(attr.type),
                /* ptr=    */ b_row_addr
            );
            pipeline.context().add(e.id, b_value);
        }
    }

    /*----- Generate code for rest of the pipeline. ------------------------------------------------------------------*/
    pipeline(*op.parent());

    /*----- Emit code to advance to next row. ------------------------------------------------------------------------*/
    auto b_row_addr_updated = BinaryenBinary(
        /* module= */ pipeline.module(),
        /* op=     */ BinaryenAddInt32(),
        /* left=   */ b_row_addr,
        /* right=  */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(store.row_size() / 8))
    );
    pipeline.block_ += BinaryenLocalSet(
        /* module= */ pipeline.module(),
        /* index=  */ BinaryenLocalGetGetIndex(b_row_addr),
        /* value=  */ b_row_addr_updated
    );
}

void WasmStoreCG::operator()(const ColumnStore &store)
{
    std::ostringstream oss;
    auto &table = store.table();

    /* Import null bitmap column address. */
    // TODO

    /*----- Generate code to access null bitmap and value of all required attributes. --------------------------------*/
    std::vector<BinaryenExpressionRef> col_addrs;
    for (auto &e : op.schema()) {
        auto &attr = table[e.id.name];

        oss.str("");
        oss << table.name << '.' << attr.name;
        auto name = oss.str();

        /*----- Import column address. -------------------------------------------------------------------------------*/
        auto b_col_addr = pipeline.CG.add_import(name, BinaryenTypeInt32(), /* is_mutable= */ true);
        col_addrs.push_back(b_col_addr);

        if (attr.type->size() < 8) {
            unreachable("not implemented");
        } else {
            auto b_value = BinaryenLoad(
                /* module=  */ pipeline.module(),
                /* bytes=   */ attr.type->size() / 8,
                /* signed=  */ true,
                /* offset=  */ 0,
                /* align=   */ 0,
                /* type=    */ get_binaryen_type(attr.type),
                /* ptr=     */ b_col_addr
            );
            pipeline.context().add(e.id, b_value);
        }
    }

    /*----- Generate code for rest of the pipeline. ------------------------------------------------------------------*/
    pipeline(*op.parent());

    /*----- Emit code to advance each column address to next row. ----------------------------------------------------*/
    auto col_addr_it = col_addrs.cbegin();
    for (auto &e : op.schema()) {
        auto &attr = table[e.id.name];
        auto b_col_addr = *col_addr_it++;
        const auto attr_size = std::max<std::size_t>(1, attr.type->size() / 8);

        auto b_col_addr_updated = BinaryenBinary(
            /* module= */ pipeline.module(),
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ b_col_addr,
            /* right=  */ BinaryenConst(pipeline.module(), BinaryenLiteralInt32(attr_size))
        );
        pipeline.block_ += BinaryenLocalSet(
            /* module= */ pipeline.module(),
            /* index=  */ BinaryenLocalGetGetIndex(b_col_addr),
            /* value=  */ b_col_addr_updated
        );
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

std::ostream & db::operator<<(std::ostream &out, const WasmModule &module)
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

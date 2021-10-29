#include "V8Platform.hpp"

#include "backend/Interpreter.hpp"
#include "catalog/Schema.hpp"
#include "globals.hpp"
#include "storage/Store.hpp"
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <mutable/IR/Tuple.hpp>
#include <mutable/storage/Store.hpp>
#include <mutable/util/Timer.hpp>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_set>


using namespace m;
using args_t = v8::Local<v8::Value>[];


/*======================================================================================================================
 * V8Inspector and helper classes
 *====================================================================================================================*/

void v8_helper::V8InspectorClientImpl::register_context(v8::Local<v8::Context> context)
{
    std::string ctx_name("query");
    inspector_->contextCreated(
        v8_inspector::V8ContextInfo(
            context,
            1,
            v8_helper::make_string_view(ctx_name)
        )
    );
}

void v8_helper::V8InspectorClientImpl::deregister_context(v8::Local<v8::Context> context)
{
    inspector_->contextDestroyed(context);
}

void v8_helper::V8InspectorClientImpl::on_message(std::string_view sv)
{
    v8_inspector::StringView msg(reinterpret_cast<const uint8_t*>(sv.data()), sv.length());

    auto Ctx = isolate_->GetCurrentContext();
    v8::HandleScope handle_scope(isolate_);
    auto obj = v8_helper::parse_json(isolate_, sv);

    session_->dispatchProtocolMessage(msg);

    if (not obj.IsEmpty()) {
        auto method = obj->Get(Ctx, v8_helper::to_v8_string(isolate_, "method")).ToLocalChecked();
        auto method_name = v8_helper::to_std_string(isolate_, method);

        if (method_name == "Runtime.runIfWaitingForDebugger") {
            std::string reason("CDT");
            session_->schedulePauseOnNextStatement(v8_helper::make_string_view(reason),
                                                   v8_helper::make_string_view(reason));
            waitFrontendMessageOnPause();
            code_(); // execute the code to debug
        }
    }
}

void v8_helper::V8InspectorClientImpl::runMessageLoopOnPause(int)
{
    static bool is_nested = false;
    if (is_nested) return;

    is_terminated_ = false;
    is_nested = true;
    while (not is_terminated_ and conn_->wait_on_message())
        while (v8::platform::PumpMessageLoop(V8Platform::platform(), isolate_)) { }
    is_terminated_ = true;
    is_nested = false;
}


/*======================================================================================================================
 * V8 Callback Functions
 *
 * Functions to be called from the WebAssembly module to give control flow and pass data to the host.
 *====================================================================================================================*/

namespace {

void print(const v8::FunctionCallbackInfo<v8::Value> &info)
{
#ifndef NDEBUG
    std::cout << "v8 function callback: ";
#endif
    for (int i = 0; i != info.Length(); ++i) {
        v8::HandleScope handle_scope(info.GetIsolate());
        if (i != 0) std::cout << ',';
        v8::Local<v8::Value> v = info[i];
        if (v->IsInt32())
            std::cout << "0x" << std::hex << uint32_t(v.As<v8::Int32>()->Value()) << std::dec;
        else
            std::cout << *v8::String::Utf8Value(info.GetIsolate(), v);
    }
    std::cout << std::endl;
}

void set_wasm_instance_raw_memory(const v8::FunctionCallbackInfo<v8::Value> &info)
{
    v8::Local<v8::WasmModuleObject> wasm_instance = info[0].As<v8::WasmModuleObject>();
    v8::Local<v8::Int32> wasm_context_id = info[1].As<v8::Int32>();

    auto &wasm_context = WasmPlatform::Get_Wasm_Context_By_ID(wasm_context_id->Value());
#ifndef NDEBUG
    std::cerr << "Setting Wasm instance raw memory of the given instance to the VM of Wasm context "
              << wasm_context_id->Value() << " at " << wasm_context.vm.addr() << " of " << wasm_context.vm.size()
              << " bytes" << std::endl;
#endif
    v8::SetWasmInstanceRawMemory(wasm_instance, wasm_context.vm.as<uint8_t*>(), wasm_context.vm.size());
}

void throw_invalid_escape_sequence(const v8::FunctionCallbackInfo<v8::Value>&)
{
    throw m::runtime_error("invalid escape sequence");
}

}


/*======================================================================================================================
 * V8Platform helper classes
 *====================================================================================================================*/

namespace {

struct Store2Wasm : ConstStoreVisitor
{
    private:
    v8::Isolate *isolate_;
    v8::Local<v8::Object> env_;
    const WasmPlatform::WasmContext &wasm_context_;
    std::size_t next_page_ = 0;

    public:
    Store2Wasm(v8::Isolate *isolate, v8::Local<v8::Object> env, const WasmPlatform::WasmContext &wasm_context)
        : isolate_(isolate)
        , env_(env)
        , wasm_context_(wasm_context)
    { }

    std::size_t get_next_page() const { return next_page_; }

    using ConstStoreVisitor::operator();

    void operator()(const ColumnStore &s) override {
        std::ostringstream oss;
        auto Ctx = isolate_->GetCurrentContext();
        auto &table = s.table();

        /* Add "table_name: num_rows" mapping to env. */
        DISCARD env_->Set(Ctx, v8_helper::to_v8_string(isolate_, table.name), v8::Int32::New(isolate_, table.store().num_rows()));

        for (auto &attr : table) {
            oss.str("");
            oss << table.name << '.' << attr.name;
            auto env_name = oss.str();

            /* Map next column into WebAssembly linear memory. */
            const auto ptr = get_pagesize() * next_page_;
            const auto bytes = s.num_rows() * attr.type->size() / 8;
            const auto aligned_bytes = Ceil_To_Next_Page(bytes);
            const auto num_pages = aligned_bytes / get_pagesize();
            auto &mem = s.memory(attr.id);
            if (aligned_bytes) {
                mem.map(aligned_bytes, 0, wasm_context_.vm, ptr);
                next_page_ += num_pages + 1; // "install" a guard page after the mapping to fault on oob
            }

            /* Add column address to env. */
            DISCARD env_->Set(Ctx, v8_helper::to_v8_string(isolate_, env_name), v8::Int32::New(isolate_, ptr));
        }

        // TODO add null bitmap column

        /* Add table size (num_rows) to env. */
        oss.str("");
        oss << table.name << "_num_rows";
        DISCARD env_->Set(Ctx, v8_helper::to_v8_string(isolate_, oss.str()), v8::Int32::New(isolate_, table.store().num_rows()));
    }

    void operator()(const PaxStore&) override { unreachable("not supported"); }

    void operator()(const RowStore &s) override {
        auto Ctx = isolate_->GetCurrentContext();
        auto &table = s.table();

        /* Map entire row store to WebAssembly linear memory. */
        const auto ptr = get_pagesize() * next_page_;
        const auto bytes = s.num_rows() * s.row_size() / 8;
        const auto aligned_bytes = Ceil_To_Next_Page(bytes);
        const auto num_pages = aligned_bytes / get_pagesize();
        auto &mem = s.memory();
        if (aligned_bytes) {
            mem.map(aligned_bytes, 0, wasm_context_.vm, ptr);
            next_page_ += num_pages + 1; // "install" a guard page after the mapping to fault on oob
        }

        /* Add table address to env. */
        DISCARD env_->Set(Ctx, v8_helper::to_v8_string(isolate_, table.name), v8::Int32::New(isolate_, ptr));

        /* Add table size (num_rows) to env. */
        std::ostringstream oss;
        oss << table.name << "_num_rows";
        DISCARD env_->Set(Ctx, v8_helper::to_v8_string(isolate_, oss.str()), v8::Int32::New(isolate_, table.store().num_rows()));
    }
};

struct CollectStringLiterals : ConstOperatorVisitor, ConstASTExprVisitor
{
    private:
    std::unordered_set<const char*> literals_; ///< the collected literals

    public:
    static std::vector<const char*> Collect(const Operator &plan) {
        CollectStringLiterals CSL;
        CSL(plan);
        return std::vector<const char*>(CSL.literals_.begin(), CSL.literals_.end());
    }

    private:
    CollectStringLiterals() = default;

    using ConstOperatorVisitor::operator();
    using ConstASTExprVisitor::operator();
    void recurse(const Consumer &C) {
        for (auto &c : C.children())
            (*this)(*c);
    }

    /*----- Operator -------------------------------------------------------------------------------------------------*/
    void operator()(const ScanOperator&) override { /* nothing to be done */ }
    void operator()(const CallbackOperator &op) override { recurse(op); }
    void operator()(const PrintOperator &op) override { recurse(op); }
    void operator()(const NoOpOperator &op) override { recurse(op); }
    void operator()(const FilterOperator &op) override {
        for (auto &c : op.filter()) {
            for (auto &p : c)
                (*this)(*p.expr());
        }
        recurse(op);
    }
    void operator()(const JoinOperator &op) override {
        for (auto &c : op.predicate()) {
            for (auto &p : c)
                (*this)(*p.expr());
        }
        recurse(op);
    }
    void operator()(const ProjectionOperator &op) override {
        for (auto &p : op.projections())
            (*this)(*p.first);
        recurse(op);
    }
    void operator()(const LimitOperator &op) override { recurse(op); }
    void operator()(const GroupingOperator &op) override {
        for (auto &g : op.group_by())
            (*this)(*g);
        recurse(op);
    }
    void operator()(const AggregationOperator &op) override { recurse(op); }
    void operator()(const SortingOperator &op) override { recurse(op); }

    /*----- Expr -----------------------------------------------------------------------------------------------------*/
    void operator()(const ErrorExpr&) override { unreachable("no errors at this stage"); }
    void operator()(const Designator&) override { /* nothing to be done */ }
    void operator()(const Constant &e) override {
        if (e.is_string()) {
            auto s = Interpreter::eval(e);
            literals_.emplace(s.as<const char*>());
        }
    }
    void operator()(const FnApplicationExpr&) override { /* nothing to be done */ } // XXX can string literals be arguments?
    void operator()(const UnaryExpr &e) override { (*this)(*e.expr); }
    void operator()(const BinaryExpr &e) override { (*this)(*e.lhs); (*this)(*e.rhs); }
    void operator()(const QueryExpr&) override { /* nothing to be done */ }
};

}


/*======================================================================================================================
 * V8Platform
 *====================================================================================================================*/

std::unique_ptr<v8::Platform> V8Platform::PLATFORM_(nullptr);

V8Platform::V8Platform()
{
    if (PLATFORM_ == nullptr) {
        PLATFORM_ = v8::platform::NewDefaultPlatform();
        v8::V8::InitializePlatform(PLATFORM_.get());
        v8::V8::Initialize();
    }

    /*----- Set V8 flags. --------------------------------------------------------------------------------------------*/
    std::ostringstream flags;
    flags << "--stack_size 1000000 ";
    if (Options::Get().wasm_adaptive) {
        flags << "--opt "
              << "--liftoff "
              << "--wasm-tier-up "
              << "--wasm-dynamic-tiering "
              << "--wasm-lazy-compilation ";
    } else {
        flags << "--no-liftoff ";
    }
    if (Options::Get().cdt_port > 0) {
        flags << "--log "
              << "--log-all "
              << "--expose-wasm "
              << "--trace-wasm "
              << "--trace-wasm-instances "
              << "--prof ";
    } else {
        flags << "--no-wasm-bounds-checks "
              << "--no-wasm-stack-checks "
              << "--experimental-wasm-simd ";
    }
    v8::V8::SetFlagsFromString(flags.str().c_str());

    v8::Isolate::CreateParams create_params;
    create_params.array_buffer_allocator = allocator_ = v8::ArrayBuffer::Allocator::NewDefaultAllocator();
    isolate_ = v8::Isolate::New(create_params);

    /* If a debugging port is specified, set up the inspector. */
    if (Options::Get().cdt_port > 0)
        inspector_ = std::make_unique<v8_helper::V8InspectorClientImpl>(Options::Get().cdt_port, isolate_);
}

V8Platform::~V8Platform()
{
    inspector_.reset();
    isolate_->Dispose();
    delete allocator_;
}

WasmModule V8Platform::compile(const Operator &plan) const
{
    WasmModule module; // fresh module
    BinaryenModuleSetFeatures(module.ref(), BinaryenFeatureBulkMemory());

    WasmModuleCG codegen(/* module= */ module, /* main= */ "run");

    /*----- Set up head of heap. -------------------------------------------------------------------------------------*/
    codegen.import("head_of_heap", BinaryenTypeInt32());
    codegen.main().block() += codegen.head_of_heap().set(codegen.get_imported("head_of_heap", BinaryenTypeInt32()));
    codegen.main().block() += codegen.literals().set(codegen.head_of_heap());

    /*----- Collect all string literals. -----------------------------------------------------------------------------*/
    auto literals = CollectStringLiterals::Collect(plan);
    const std::size_t num_literals = literals.size();

    /*----- Add memory. ----------------------------------------------------------------------------------------------*/
    if (num_literals > 0) {
        /*----- Create data segments for literals. -------------------------------------------------------------------*/
        int8_t *segments_passive = new int8_t[num_literals]();
        std::fill_n(segments_passive, num_literals, true);

        std::vector<BinaryenExpressionRef> segment_offsets;
        segment_offsets.reserve(num_literals);
        std::vector<BinaryenIndex> segment_sizes;
        segment_sizes.reserve(num_literals);

        std::size_t current_offset = 0;
        for (auto l : literals) {
            codegen.add_literal(l, current_offset);
            const auto len = strlen(l);
            segment_offsets.emplace_back(nullptr);
            segment_sizes.emplace_back(len);
            current_offset += len;
        }

        BinaryenSetMemory(
            /* module=         */ codegen,
            /* initial=        */ 1,
            /* maximum=        */ WasmPlatform::WASM_MAX_MEMORY / WasmPlatform::WASM_PAGE_SIZE, // allowed maximum
            /* exportName=     */ "memory",
            /* segments=       */ &literals[0],
            /* segmentPassive= */ segments_passive,
            /* segmentOffsets= */ &segment_offsets[0],
            /* segmentSizes=   */ &segment_sizes[0],
            /* numSegments=    */ literals.size(),
            /* shared=         */ 0
        );

        for (std::size_t i = 0; i != literals.size(); ++i) {
            const auto literal = literals[i];
            const auto size = segment_sizes[i];
            const auto offset = codegen.get_literal_offset(literal);
            WasmTemporary dest = BinaryenBinary(
                /* module= */ codegen,
                /* op=     */ BinaryenAddInt32(),
                /* left=   */ codegen.literals(),
                /* right=  */ BinaryenConst(codegen, BinaryenLiteralInt32(offset))
            );
            codegen.main().block() += BinaryenMemoryInit(
                /* module=  */ codegen,
                /* segment= */ i,
                /* dest=    */ dest,
                /* offset=  */ BinaryenConst(codegen, BinaryenLiteralInt32(0)),
                /* size=    */ BinaryenConst(codegen, BinaryenLiteralInt32(size))
            );
        }

        delete[] segments_passive;

        codegen.main().block() += codegen.head_of_heap().set(BinaryenBinary(
            /* module= */ codegen,
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ codegen.head_of_heap(),
            /* right=  */ BinaryenConst(codegen, BinaryenLiteralInt32(current_offset))
        ));
        codegen.main().block() += codegen.align_head_of_heap();
    } else {
        BinaryenSetMemory(
            /* module=         */ codegen,
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
    }

#if 1
    /*----- Add print function. --------------------------------------------------------------------------------------*/
    std::vector<BinaryenType> print_param_types;
    print_param_types.push_back(BinaryenTypeInt32());

    BinaryenAddFunctionImport(
        /* module=             */ codegen,
        /* internalName=       */ "print",
        /* externalModuleName= */ "env",
        /* externalBaseName=   */ "print",
        /* params=             */ BinaryenTypeCreate(&print_param_types[0], print_param_types.size()),
        /* results=            */ BinaryenTypeNone()
    );
#endif

    /*----- Add throw_invalid_escape_sequence function. --------------------------------------------------------------*/
    BinaryenAddFunctionImport(
        /* module=             */ codegen,
        /* internalName=       */ "throw_invalid_escape_sequence",
        /* externalModuleName= */ "env",
        /* externalBaseName=   */ "throw_invalid_escape_sequence",
        /* params=             */ BinaryenTypeCreate(nullptr, 0),
        /* results=            */ BinaryenTypeNone()
    );

    /*----- Compile plan. --------------------------------------------------------------------------------------------*/
    codegen.compile(plan); // emit code

    /*----- Write number of results. ---------------------------------------------------------------------------------*/
    {
        /* Align head of heap to 4-byte address. */
        WasmTemporary head_inc = BinaryenBinary(
            /* module= */ codegen,
            /* op=     */ BinaryenAddInt32(),
            /* left=   */ codegen.head_of_heap(),
            /* right=  */ BinaryenConst(codegen, BinaryenLiteralInt32(3))
        );
        WasmTemporary head_aligned = BinaryenBinary(
            /* module= */ codegen,
            /* op=     */ BinaryenAndInt32(),
            /* left=   */ head_inc,
            /* right=  */ BinaryenConst(codegen, BinaryenLiteralInt32(~3U))
        );
        codegen.main().block() += codegen.head_of_heap().set(std::move(head_aligned));
        codegen.main().block() += BinaryenStore(
            /* module= */ codegen,
            /* bytes=  */ 4,
            /* offset= */ 0,
            /* align=  */ 0,
            /* ptr=    */ codegen.head_of_heap(),
            /* value=  */ codegen.num_tuples(),
            /* type=   */ BinaryenTypeInt32()
        );
    }

    /*----- Return the new head of heap . ----------------------------------------------------------------------------*/
    codegen.main().block() += codegen.head_of_heap();

    /*----- Add function. --------------------------------------------------------------------------------------------*/
    codegen.main().finalize();
    BinaryenAddFunctionExport(codegen, "run", "run");

#ifndef NDEBUG
    /*----- Validate module before optimization. ---------------------------------------------------------------------*/
    if (not BinaryenModuleValidate(module.ref())) {
        module.dump();
        throw std::logic_error("invalid module");
    }
#endif

    /*----- Optimize module. -----------------------------------------------------------------------------------------*/
#ifndef NDEBUG
    std::ostringstream dump_before_opt;
    module.dump(dump_before_opt);
#endif
    if (Options::Get().wasm_optimization_level) {
        BinaryenSetOptimizeLevel(Options::Get().wasm_optimization_level);
        BinaryenSetShrinkLevel(0); // shrinking not required
        BinaryenModuleOptimize(module.ref());
    }

#ifndef NDEBUG
    /*----- Validate module after optimization. ----------------------------------------------------------------------*/
    if (not BinaryenModuleValidate(module.ref())) {
        std::cerr << "Module invalid after optimization!" << std::endl;
        std::cerr << "WebAssembly before optimization:\n" << dump_before_opt.str() << std::endl;
        std::cerr << "WebAssembly after optimization:\n";
        module.dump(std::cerr);
        throw std::logic_error("invalid module");
    }
#endif

    return module;
}

void V8Platform::execute(const Operator &plan)
{
    Catalog &C = Catalog::Get();
    auto module = TIME_EXPR(compile(plan), "Compile to WebAssembly", C.timer());

    /* Create required V8 scopes. */
    v8::Isolate::Scope isolate_scope(isolate_);
    v8::HandleScope handle_scope(isolate_); // tracks and disposes of all object handles

    /* Create global template and context. */
    v8::Local<v8::ObjectTemplate> global = v8::ObjectTemplate::New(isolate_);
    global->Set(isolate_, "set_wasm_instance_raw_memory", v8::FunctionTemplate::New(isolate_, set_wasm_instance_raw_memory));
    v8::Local<v8::Context> context = v8::Context::New(isolate_, /* extensions= */ nullptr, global);
    v8::Context::Scope context_scope(context);

    auto &wasm_context = Create_Wasm_Context(WASM_MAX_MEMORY);

    /* Create the import object for instantiating the WebAssembly module. */
    auto imports = v8::Object::New(isolate_);
    auto env = create_env(wasm_context, plan);
    DISCARD imports->Set(context, mkstr("env"), env);

    /* Map the remaining address space to the output buffer. */
    const auto bytes_remaining = wasm_context.vm.size() - wasm_context.heap;
    memory::Memory mem = Catalog::Get().allocator().allocate(bytes_remaining);
    mem.map(bytes_remaining, 0, wasm_context.vm, wasm_context.heap);

    /* Create a WebAssembly instance object. */
    auto instance = TIME_EXPR(instantiate(module, imports), "Compile Wasm to machine code", C.timer());

    /* Set the underlying memory for the instance. */
    v8::SetWasmInstanceRawMemory(instance, wasm_context.vm.as<uint8_t*>(), wasm_context.vm.size());

    /* Get the exports of the created WebAssembly instance. */
    auto exports = instance->Get(context, mkstr("exports")).ToLocalChecked().As<v8::Object>();
    auto run = exports->Get(context, mkstr("run")).ToLocalChecked().As<v8::Function>();

    if (bool(inspector_)) {
        inspector_->register_context(context);
        inspector_->start([&]() {
            /* Create JS script file that instantiates the Wasm module and invokes `run()`. */
            auto filename = create_js_debug_script(module, env, wasm_context);
            /* Create a `v8::Script` for that JS file. */
            std::ifstream js_in(filename);
            std::string js(std::istreambuf_iterator<char>(js_in), std::istreambuf_iterator<char>{});
            v8::Local<v8::String> js_src = mkstr(js);
            std::string path = std::string("file://./") + filename;
            v8::ScriptOrigin js_origin = v8::ScriptOrigin(mkstr(path));
            auto script = v8::Script::Compile(context, js_src, &js_origin);
            if (script.IsEmpty())
                throw std::runtime_error("failed to compile script");
            /* Execute the `v8::Script`. */
            auto result = script.ToLocalChecked()->Run(context);
            if (result.IsEmpty())
                throw std::runtime_error("execution failed");
        });
        inspector_->deregister_context(context);
        return;
    }

    /* Invoke the exported function `run` of the module. */
    args_t args { v8::Int32::New(isolate_, wasm_context.id), };
    const uint32_t head_of_heap =
        run->Call(context, context->Global(), 1, args).ToLocalChecked().As<v8::Int32>()->Value();

    /* Get the number of result tuples written. */
    const uint32_t num_tuples = mem.as<uint32_t*>()[(head_of_heap - wasm_context.heap) / sizeof(uint32_t)];

    /* Create physical schema used as data layout. */
    auto schema = plan.schema();
    auto deduplicated_schema = schema.deduplicate();
    RuntimeStruct layout(deduplicated_schema); // TODO: add bitmap-type for null bitmap
    const std::size_t num_bytes_result_set = layout.size_in_bytes() * num_tuples;
    const std::size_t remainder = num_bytes_result_set % 4;
    const std::size_t gap = remainder ? sizeof(uint32_t) - remainder : 0UL;
    const uint8_t *result_set = mem.as<uint8_t*>() + head_of_heap - wasm_context.heap - gap - num_bytes_result_set;

    Table RS("$RS");
    std::size_t i = 0;
    for (auto &e : deduplicated_schema)
        RS.push_back(C.pool(std::to_string(i++).c_str()), as<const PrimitiveType>(e.type)->as_vectorial());
    auto lin = Linearization::CreateInfinite(1);
    auto child = std::make_unique<Linearization>(Linearization::CreateFinite(RS.size(), 1));
    for (auto &attr : RS)
        child->add_sequence(layout.offset(attr.id), 0, attr);
    lin.add_sequence(uintptr_t(result_set), layout.size_in_bytes(), std::move(child));

    Schema S = RS.schema();

    /* Extract results. */
    if (auto callback_op = cast<const CallbackOperator>(&plan)) {
        auto loader = Interpreter::compile_load(S, lin);
        if (S.num_entries() == schema.num_entries()) {
            /* No deduplication was performed. */
            Tuple tup(S);
            Tuple *args[] = { &tup };
            for (std::size_t i = 0; i != num_tuples; ++i) {
                loader(args);
                callback_op->callback()(S, tup);
                tup.clear();
            }
        } else {
            /* Deduplication was performed. Compute a `Tuple` with duplicates. */
            Tuple tup_dedupl(S);
            Tuple tup_dupl(schema);
            Tuple *args[] = { &tup_dedupl, &tup_dupl };
            for (std::size_t i = 0; i != S.num_entries(); ++i) {
                auto &entry = deduplicated_schema[i];
                loader.emit_Ld_Tup(0, i);
                for (std::size_t j = 0; j != schema.num_entries(); ++j) {
                    auto &e = schema[j];
                    if (e.id == entry.id) {
                        insist(e.type == entry.type);
                        loader.emit_St_Tup(1, j, e.type);
                    }
                }
                loader.emit_Pop();
            }
            for (std::size_t i = 0; i != num_tuples; ++i) {
                loader(args);
                callback_op->callback()(schema, tup_dupl);
            }
        }
    } else if (auto print_op = cast<const PrintOperator>(&plan)) {
        Tuple tup(S);
        Tuple *args[] = { &tup };
        auto printer = Interpreter::compile_load(S, lin);
        auto ostream_index = printer.add(&print_op->out);
        std::size_t old_idx = -1UL;
        for (std::size_t i = 0; i != schema.num_entries(); ++i) {
            if (i != 0)
                printer.emit_Putc(ostream_index, ',');
            auto &e = schema[i];
            auto idx = deduplicated_schema[e.id].first;
            if (idx != old_idx) {
                printer.emit_Pop();
                printer.emit_Ld_Tup(0, idx);
                old_idx = idx;
            }
            printer.emit_Print(ostream_index, e.type);
        }
        printer.emit_Pop(); // to remove last loaded value
        for (std::size_t i = 0; i != num_tuples; ++i) {
            printer(args);
            print_op->out << '\n';
        }
        if (not Options::Get().quiet)
            print_op->out << num_tuples << " rows\n";
    } else if (auto noop_op = cast<const NoOpOperator>(&plan)) {
        if (not Options::Get().quiet)
            noop_op->out << num_tuples << " rows\n";
    }

    Dispose_Wasm_Context(wasm_context);
}

v8::Local<v8::WasmModuleObject> V8Platform::instantiate(const WasmModule &module, v8::Local<v8::Object> imports)
{
    auto Ctx = isolate_->GetCurrentContext();
    auto [binary_addr, binary_size] = module.binary();
    auto bs = v8::ArrayBuffer::NewBackingStore(
        /* data =        */ binary_addr,
        /* byte_length=  */ binary_size,
        /* deleter=      */ v8::BackingStore::EmptyDeleter,
        /* deleter_data= */ nullptr
    );
    auto buffer = v8::ArrayBuffer::New(isolate_, std::move(bs));
    args_t module_args { buffer };

    auto wasm = Ctx->Global()->Get(Ctx, mkstr("WebAssembly")).ToLocalChecked().As<v8::Object>(); // WebAssembly class
    auto wasm_module = wasm->Get(Ctx, mkstr("Module")).ToLocalChecked().As<v8::Object>()
                           ->CallAsConstructor(Ctx, 1, module_args).ToLocalChecked().As<v8::Object>();
    free(binary_addr);

    args_t instance_args { wasm_module, imports };
    return wasm->Get(Ctx, mkstr("Instance")).ToLocalChecked().As<v8::Object>()
               ->CallAsConstructor(Ctx, 2, instance_args).ToLocalChecked().As<v8::WasmModuleObject>();
}

v8::Local<v8::Object> V8Platform::create_env(WasmContext &wasm_context, const Operator &plan) const
{
    (void) plan; // TODO map only tables/indexes that are being accessed
    auto Ctx = isolate_->GetCurrentContext();

    /* Map the entire database into the Wasm module. */
    auto &DB = Catalog::Get().get_database_in_use();
    auto env = v8::Object::New(isolate_);
    Store2Wasm S2W(isolate_, env, wasm_context);
    for (auto it = DB.begin_tables(); it != DB.end_tables(); ++it)
        S2W(it->second->store());

    /* Import next free page, usable for heap allcoations. */
    auto head_of_heap = S2W.get_next_page() * get_pagesize();
    DISCARD env->Set(Ctx, mkstr("head_of_heap"), v8::Int32::New(isolate_, head_of_heap));
    wasm_context.heap = head_of_heap;

    /* Add functions to environment. */
#define ADD_FUNC(FUNC) { \
    auto func = v8::Function::New(Ctx, (FUNC)).ToLocalChecked(); \
    DISCARD env->Set(Ctx, mkstr(#FUNC), func); \
}
    ADD_FUNC(print);
    ADD_FUNC(throw_invalid_escape_sequence);
#undef ADD_FUNC

    return env;
}

v8::Local<v8::String> V8Platform::mkstr(const std::string &str) const
{
    return v8_helper::to_v8_string(isolate_, str);
}

v8::Local<v8::String> V8Platform::to_json(v8::Local<v8::Value> val) const
{
    return v8_helper::to_json(isolate_, val);
}

std::string V8Platform::create_js_debug_script(const WasmModule &module, v8::Local<v8::Object> env,
                                               const WasmPlatform::WasmContext &wasm_context)
{
    std::ostringstream oss;

    auto binary = module.binary();

    std::string env_str = *v8::String::Utf8Value(isolate_, to_json(env));
    env_str.insert(env_str.length() - 1, ", \"print\": function (arg) { console.log(arg); }");

    /* Construct import object. */
    oss << "\
let importObject = { \"env\": " << env_str << " };\n\
const bytes = Uint8Array.from([";
    for (auto p = binary.first, end = p + binary.second; p != end; ++p) {
        if (p != binary.first) oss << ", ";
        oss << unsigned(*p);
    }
    /* Emit code to instantiate module and invoke exported `run()` function. */
    oss << "]);\n\
WebAssembly.compile(bytes).then(module => {\n\
    const instance = new WebAssembly.Instance(module, importObject);\n\
    set_wasm_instance_raw_memory(instance, " << wasm_context.id << ");\n\
    const head_of_heap = instance.exports.run();\n\
    console.log('The head of heap is at byte offset %i.', head_of_heap);\n\
    const heap_size = head_of_heap - " << wasm_context.heap << ";\n\
    console.log('There are %i bytes on the heap.', heap_size);\n\
    var df = new DataView(instance.exports.memory.buffer);\n\
    const num_tuples = df.getUint32(head_of_heap, true);\n\
    console.log('The result set contains %i tuples.', num_tuples);\n\
    debugger;\n\
});\n\
debugger;";

    /* Create a new temporary file. */
    char name[] = "query.js.XXXXXX";
    int fd = mkostemp(name, O_WRONLY | O_CREAT | O_TRUNC);
    if (fd == -1)
        throw std::runtime_error("failed to create a temporary file");
    auto file = fdopen(fd, "w");
    if (not file)
        throw std::runtime_error(strerror(errno));
    std::cerr << "Creating debug JS script " << name << std::endl;

    /* Write the JS code to instantiate the module and invoke `run()` to the temporary file. */
    auto src = oss.str();
    if (fwrite(src.c_str(), 1, src.length(), file) != src.length())
        throw std::runtime_error("failed to write JS script");
    if (fflush(file))
        throw std::runtime_error(strerror(errno));
    fclose(file);

    /* Return the name of the temporary file. */
    return std::string(name);
}


/*======================================================================================================================
 * Backend
 *====================================================================================================================*/

std::unique_ptr<Backend> Backend::CreateWasmV8()
{
    return std::make_unique<WasmBackend>(std::make_unique<V8Platform>());
}

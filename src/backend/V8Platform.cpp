#include "V8Platform.hpp"

#include "catalog/Schema.hpp"
#include "globals.hpp"
#include "IR/Tuple.hpp"
#include "mutable/storage/Store.hpp"
#include "mutable/util/Timer.hpp"
#include "storage/ColumnStore.hpp"
#include "storage/RowStore.hpp"
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <stdexcept>
#include <string>
#include <string_view>


using namespace m;
using args_t = v8::Local<v8::Value>[];


/*======================================================================================================================
 * v8_helper
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
    std::cerr << "Setting Wasm instance raw memory of the given instance to the VM of Wasm context "
              << wasm_context_id->Value() << " at " << wasm_context.vm.addr() << " of " << wasm_context.vm.size()
              << " bytes" << std::endl;
    v8::SetWasmInstanceRawMemory(wasm_instance, wasm_context.vm.as<uint8_t*>(), wasm_context.vm.size());
}

struct print_value : ConstTypeVisitor
{
    using base = ConstTypeVisitor;

    std::ostream &out;
    const uint64_t *ptr;

    print_value(std::ostream &out, const uint64_t *ptr) : out(out), ptr(ptr) { }

    using base::operator();

    void operator()(Const<ErrorType>&) override { unreachable("Value cannot have error type"); }

    void operator()(Const<Boolean>&) override { out << (*reinterpret_cast<const uint8_t*>(ptr) ? "TRUE" : "FALSE"); }

    void operator()(Const<CharacterSequence>&) override { unreachable("not implemented"); }

    void operator()(Const<Numeric> &n) override {
        switch (n.kind) {
            case Numeric::N_Int:
                if (n.size() <= 32) {
                    out << *reinterpret_cast<const int32_t*>(ptr);
                } else {
                    out << *reinterpret_cast<const int64_t*>(ptr);
                }
                break;

            case Numeric::N_Float:
                if (n.size() == 32) {
                    const auto old_precision = out.precision(std::numeric_limits<float>::max_digits10 - 1);
                    out << *reinterpret_cast<const float*>(ptr);
                    out.precision(old_precision);
                } else {
                    const auto old_precision = out.precision(std::numeric_limits<double>::max_digits10 - 1);
                    out << *reinterpret_cast<const double*>(ptr);
                    out.precision(old_precision);
                }
                break;

            case Numeric::N_Decimal: {
                auto i = *reinterpret_cast<const int64_t*>(ptr);
                auto factor = powi(10, n.scale);
                auto pre = i / factor;
                auto post = i % factor;
                out << pre << '.';
                auto old_fill = out.fill('0');
                out << std::setw(n.scale) << post;
                out.fill(old_fill);
                break;
            }
        }
    }

    void operator()(Const<FnType>&) override { unreachable("FnType is not a value"); }
};


/*======================================================================================================================
 * V8Platform
 *====================================================================================================================*/

struct EnvGen : ConstStoreVisitor
{
    private:
    v8::Isolate *isolate_;
    v8::Local<v8::Object> env_;
    const WasmPlatform::WasmContext &wasm_context_;
    std::size_t next_page_ = 0;

    public:
    EnvGen(v8::Isolate *isolate, v8::Local<v8::Object> env, const WasmPlatform::WasmContext &wasm_context)
        : isolate_(isolate)
        , env_(env)
        , wasm_context_(wasm_context)
    { }

    std::size_t get_next_page() const { return next_page_; }

    using ConstStoreVisitor::operator();

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
};

std::unique_ptr<v8::Platform> V8Platform::PLATFORM_(nullptr);

V8Platform::V8Platform()
    : mem_(Catalog::Get().allocator().allocate(1UL << 32)) // 2 GiB
{
    if (PLATFORM_ == nullptr) {
        PLATFORM_ = v8::platform::NewDefaultPlatform();
        v8::V8::InitializePlatform(PLATFORM_.get());
        v8::V8::Initialize();
    }

#if 1
    v8::V8::SetFlagsFromString("--stack_size 1000000 --no-liftoff --experimental-wasm-simd");
#else
    v8::V8::SetFlagsFromString("--no-liftoff --print-wasm-code");
#endif

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
            auto filename = create_js_debug_script(plan, module, env, wasm_context);
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

    /* Compute the size of the heap in bytes. */
    const uint32_t heap_size = head_of_heap - wasm_context.heap;

    /* Compute the number of result tuples written. */
    const uint32_t num_tuples = mem_.as<uint32_t*>()[heap_size / sizeof(uint32_t)];

    /* Compute the beginning of the output buffer, located on the heap. */
    const uint64_t *output_buffer = reinterpret_cast<uint64_t*>(mem_.as<uint8_t*>() + heap_size - num_tuples * plan.schema().num_entries() * 8);

    /* Extract results. */
    auto &S = plan.schema();
    if (auto callback_op = cast<const CallbackOperator>(&plan)) {
        Tuple tup(plan.schema());
        for (const uint64_t *ptr = output_buffer, *end = ptr + num_tuples * S.num_entries(); ptr < end;) {
            for (std::size_t i = 0; i != S.num_entries(); ++i, ++ptr) {
                auto ty = S[i].type;
                if (ty->is_boolean()) {
                    tup.set(i, bool(ptr[0] & 0b1), false);
                } else if (auto n = cast<const Numeric>(ty)) {
                    switch (n->kind) {
                        case Numeric::N_Int:
                        case Numeric::N_Decimal: {
                            switch (n->size()) {
                                case 8:
                                case 16:
                                case 32:
                                    tup.set(i, int64_t(*reinterpret_cast<const int32_t*>(ptr)), false);
                                    break;

                                case 64:
                                    tup.set(i, *reinterpret_cast<const int64_t*>(ptr), false);
                                    break;
                            }
                            break;
                        }

                        case Numeric::N_Float: {
                            if (n->size() == 32)
                                tup.set(i, *reinterpret_cast<const float*>(ptr), false);
                            else
                                tup.set(i, *reinterpret_cast<const double*>(ptr), false);
                            break;
                        }
                    }
                } else {
                    unreachable("unsupported type");
                }
            }
            callback_op->callback()(S, tup);
            tup.clear();
        }
    } else if (auto print_op = cast<const PrintOperator>(&plan)) {
        print_value P(print_op->out, output_buffer);
        for (std::size_t i = 0; i != num_tuples; ++i) {
            for (std::size_t j = 0; j != S.num_entries(); ++j, ++P.ptr) {
                if (j != 0) P.out << ',';
                P(*S[j].type);
            }
            P.out << '\n';
        }
        if (not Options::Get().quiet)
            print_op->out << num_tuples << " rows\n";
    } else if (auto noop_op = cast<const NoOpOperator>(&plan)) {
        if (not Options::Get().quiet)
            noop_op->out << num_tuples << " rows\n";
    }
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
    auto Ctx = isolate_->GetCurrentContext();
    auto &DB = Catalog::Get().get_database_in_use();
    auto env = v8::Object::New(isolate_);
    EnvGen G(isolate_, env, wasm_context);
    for (auto it = DB.begin_tables(); it != DB.end_tables(); ++it)
        G(it->second->store());

    /* Map the remaining address space to the output buffer. */
    auto head_of_heap = G.get_next_page() * get_pagesize();
    const auto remaining = wasm_context.vm.size() - head_of_heap;
    mem_.map(remaining, 0, wasm_context.vm, head_of_heap);

    /* Import next free page, usable for heap allcoations. */
    DISCARD env->Set(Ctx, mkstr("head_of_heap"), v8::Int32::New(isolate_, head_of_heap));
    wasm_context.heap = head_of_heap;

    /* Add printing functions to environment. */
#define ADD_FUNC(FUNC) { \
    auto func = v8::Function::New(Ctx, (FUNC)).ToLocalChecked(); \
    DISCARD env->Set(Ctx, mkstr(#FUNC), func); \
}
    ADD_FUNC(print);
#undef ADD_FUNC

#if 0
    {
        auto json = to_json(env);
        std::cerr << "env: " << *v8::String::Utf8Value(isolate_, json) << '\n';
        std::cerr.flush();
    }
#endif

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

std::string V8Platform::create_js_debug_script(const Operator &plan, const WasmModule &module,
                                               v8::Local<v8::Object> env, const WasmPlatform::WasmContext &wasm_context)
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

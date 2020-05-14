#include "V8Platform.hpp"

#include "catalog/Schema.hpp"
#include "globals.hpp"
#include "IR/Tuple.hpp"
#include "storage/ColumnStore.hpp"
#include "storage/RowStore.hpp"
#include "storage/Store.hpp"

#include <chrono>


#define V8STR(str) ( v8::String::NewFromUtf8(this->isolate_, (str)).ToLocalChecked() )


using namespace db;
using args_t = v8::Local<v8::Value>[];


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
        std::cout << *v8::String::Utf8Value(info.GetIsolate(), info[i]);
    }
    std::cout << std::endl;
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
                if (n.size() <= 32)
                    out << *reinterpret_cast<const int32_t*>(ptr);
                else
                    out << *reinterpret_cast<const int64_t*>(ptr);
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
        const auto ptr = rewire::Pagesize * next_page_;
        const auto bytes = s.num_rows() * s.row_size() / 8;
        const auto aligned_bytes = rewire::Ceil_To_Next_Page(bytes);
        const auto num_pages = aligned_bytes / rewire::Pagesize;
        auto &mem = s.memory();
        if (aligned_bytes) {
            mem.map(aligned_bytes, 0, wasm_context_.vm, ptr);
            next_page_ += num_pages + 1; // "install" a guard page after the mapping to fault on oob
        }

        /* Add table address to env. */
        DISCARD env_->Set(Ctx, V8STR(table.name), v8::Int32::New(isolate_, ptr));

        /* Add table size (num_rows) to env. */
        std::ostringstream oss;
        oss << table.name << "_num_rows";
        DISCARD env_->Set(Ctx, V8STR(oss.str().c_str()), v8::Int32::New(isolate_, table.store().num_rows()));
    }

    void operator()(const ColumnStore &s) override {
        std::ostringstream oss;
        auto Ctx = isolate_->GetCurrentContext();
        auto &table = s.table();

        /* Add "table_name: num_rows" mapping to env. */
        DISCARD env_->Set(Ctx, V8STR(table.name), v8::Int32::New(isolate_, table.store().num_rows()));

        for (auto &attr : table) {
            oss.str("");
            oss << table.name << '.' << attr.name;
            auto env_name = oss.str();

            /* Map next column into WebAssembly linear memory. */
            const auto ptr = rewire::Pagesize * next_page_;
            const auto bytes = s.num_rows() * attr.type->size() / 8;
            const auto aligned_bytes = rewire::Ceil_To_Next_Page(bytes);
            const auto num_pages = aligned_bytes / rewire::Pagesize;
            auto &mem = s.memory(attr.id);
            if (aligned_bytes) {
                mem.map(aligned_bytes, 0, wasm_context_.vm, ptr);
                next_page_ += num_pages + 1; // "install" a guard page after the mapping to fault on oob
            }

            /* Add column address to env. */
            DISCARD env_->Set(Ctx, V8STR(env_name.c_str()), v8::Int32::New(isolate_, ptr));
        }

        // TODO add null bitmap column

        /* Add table size (num_rows) to env. */
        oss.str("");
        oss << table.name << "_num_rows";
        DISCARD env_->Set(Ctx, V8STR(oss.str().c_str()), v8::Int32::New(isolate_, table.store().num_rows()));
    }
};

std::unique_ptr<v8::Platform> V8Platform::PLATFORM_(nullptr);

V8Platform::V8Platform()
    : output_buffer_(Catalog::Get().allocator().allocate(1UL << 32)) // 2 GiB
{
    if (PLATFORM_ == nullptr) {
        PLATFORM_ = v8::platform::NewDefaultPlatform();
        v8::V8::InitializePlatform(PLATFORM_.get());
        v8::V8::Initialize();
    }

#if 1
    v8::V8::SetFlagsFromString("--no-liftoff --experimental-wasm-simd");
#else
    v8::V8::SetFlagsFromString("--no-liftoff --print-wasm-code");
#endif

    v8::Isolate::CreateParams create_params;
    create_params.array_buffer_allocator = allocator_ = v8::ArrayBuffer::Allocator::NewDefaultAllocator();
    isolate_ = v8::Isolate::New(create_params);
}

V8Platform::~V8Platform()
{
    isolate_->Dispose();
    delete allocator_;
}

void V8Platform::execute(const Operator &plan)
{
    auto module = compile(plan);

    /* Create required V8 scopes. */
    v8::Isolate::Scope isolate_scope(isolate_);
    v8::HandleScope handle_scope(isolate_); // tracks and disposes of all object handles
    v8::Local<v8::Context> context = v8::Context::New(isolate_);
    v8::Context::Scope context_scope(context);

    auto &wasm_context = Create_Wasm_Context(WASM_MAX_MEMORY);

    /* Create the import object for instantiating the WebAssembly module. */
    auto imports = v8::Object::New(isolate_);
    DISCARD imports->Set(context, V8STR("env"), create_env(wasm_context, plan));

    /* Create a WebAssembly instance object. */
    auto instance = instantiate(module, imports);

    /* Set the underlying memory for the instance. */
    v8::SetWasmInstanceRawMemory(instance, wasm_context.vm.as<uint8_t*>(), wasm_context.vm.size());

    /* Get the exports of the created WebAssembly instance. */
    auto exports = instance->Get(context, V8STR("exports")).ToLocalChecked().As<v8::Object>();
    auto run = exports->Get(context, V8STR("run")).ToLocalChecked().As<v8::Function>();

    /* Invoke the exported function `run` of the module. */
    args_t args { v8::Int32::New(isolate_, wasm_context.id), };
    const uint32_t num_tuples =
        run->Call(context, context->Global(), 1, args).ToLocalChecked().As<v8::Int32>()->Value();

    /* Extract results. */
    auto &S = plan.schema();
    if (auto callback_op = cast<const CallbackOperator>(&plan)) {
        Tuple tup(plan.schema());
        if constexpr (WasmPlatform::WRITE_RESULTS_COLUMN_MAJOR) {
            unreachable("callback with results in column-major order not supported");
        } else {
            for (auto ptr = output_buffer_.as<uint64_t*>(), end = ptr + num_tuples * S.num_entries(); ptr < end;) {
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
                                        tup.set(i, int64_t(*reinterpret_cast<int32_t*>(ptr)), false);
                                        break;

                                    case 64:
                                        tup.set(i, *reinterpret_cast<int64_t*>(ptr), false);
                                        break;
                                }
                                break;
                            }

                            case Numeric::N_Float: {
                                if (n->size() == 32)
                                    tup.set(i, *reinterpret_cast<float*>(ptr), false);
                                else
                                    tup.set(i, *reinterpret_cast<double*>(ptr), false);
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
        }
    } else if (auto print_op = cast<const PrintOperator>(&plan)) {
        if constexpr (WasmPlatform::WRITE_RESULTS_COLUMN_MAJOR) {
            unreachable("printing results in column-major order not supported");
        } else {
            print_value P(print_op->out, output_buffer_.as<uint64_t*>());
            for (std::size_t i = 0; i != num_tuples; ++i) {
                for (std::size_t j = 0; j != S.num_entries(); ++j, ++P.ptr) {
                    if (j != 0) P.out << ',';
                    P(*S[j].type);
                }
                P.out << '\n';
            }
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

    auto wasm = Ctx->Global()->Get(Ctx, V8STR("WebAssembly")).ToLocalChecked().As<v8::Object>(); // WebAssembly class
    auto wasm_module = wasm->Get(Ctx, V8STR("Module")).ToLocalChecked().As<v8::Object>()
                           ->CallAsConstructor(Ctx, 1, module_args).ToLocalChecked().As<v8::Object>();
    free(binary_addr);

    args_t instance_args { wasm_module, imports };
    return wasm->Get(Ctx, V8STR("Instance")).ToLocalChecked().As<v8::Object>()
               ->CallAsConstructor(Ctx, 2, instance_args).ToLocalChecked().As<v8::WasmModuleObject>();
}

v8::Local<v8::Object> V8Platform::create_env(const WasmContext &wasm_context, const Operator &plan) const
{
    auto Ctx = isolate_->GetCurrentContext();
    auto &DB = Catalog::Get().get_database_in_use();
    auto env = v8::Object::New(isolate_);
    EnvGen G(isolate_, env, wasm_context);
    for (auto it = DB.begin_tables(); it != DB.end_tables(); ++it)
        G(it->second->store());

    /* Map the remaining address space to the output buffer. */
    auto ptr_out = G.get_next_page() * rewire::Pagesize;
    const auto remaining = wasm_context.vm.size() - ptr_out;
    output_buffer_.map(remaining, 0, wasm_context.vm, ptr_out);

    /* Output location for row-major layout. */
    DISCARD env->Set(Ctx, V8STR("out"), v8::Int32::New(isolate_, ptr_out));

    /* Output location(s) for column-major layout.  Similar to PAX-blocks. */
    std::ostringstream oss;
    for (auto attr : plan.schema()) {
        auto ty = attr.type;
        oss.str("");
        oss << "out_" << attr.id;
        DISCARD env->Set(Ctx, V8STR(oss.str().c_str()), v8::Int32::New(isolate_, ptr_out));
        if (ty->size() <= 32)
            ptr_out += 4 * WasmPlatform::NUM_TUPLES_OUTPUT_BUFFER; // 4 byte per value
        else
            ptr_out += (ty->size() * WasmPlatform::NUM_TUPLES_OUTPUT_BUFFER + 7) / 8;
    }

    /* Add printing functions to environment. */
#define ADD_FUNC(FUNC) { \
    auto func = v8::Function::New(Ctx, (FUNC)).ToLocalChecked(); \
    DISCARD env->Set(Ctx, V8STR(#FUNC), func); \
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

v8::Local<v8::String> V8Platform::to_json(v8::Local<v8::Value> val) const
{
    auto Ctx = isolate_->GetCurrentContext();
    auto json = Ctx->Global()->Get(Ctx, V8STR("JSON")).ToLocalChecked().As<v8::Object>();
    auto stringify = json->Get(Ctx, V8STR("stringify")).ToLocalChecked().As<v8::Function>();
    v8::Local<v8::Value> args[] = { val };
    return stringify->Call(Ctx, Ctx->Global(), 1, args).ToLocalChecked().As<v8::String>();
}

#undef V8STR


/*======================================================================================================================
 * Backend
 *====================================================================================================================*/

std::unique_ptr<Backend> Backend::CreateWasmV8()
{
    return std::make_unique<WasmBackend>(std::make_unique<V8Platform>());
}

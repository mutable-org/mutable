#include "V8Platform.hpp"

#include "storage/ColumnStore.hpp"
#include "storage/RowStore.hpp"
#include "storage/Store.hpp"

#include <chrono>


#define V8STR(str) ( v8::String::NewFromUtf8(this->isolate_, (str)).ToLocalChecked() )


using namespace db;
using args_t = v8::Local<v8::Value>[];


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
        std::ostringstream oss;
        auto Ctx = isolate_->GetCurrentContext();
        auto &table = s.table();

        const auto ptr = rewire::Pagesize * next_page_;
        const auto bytes = s.num_rows() * s.row_size() / 8;
        const auto aligned_bytes = rewire::Ceil_To_Next_Page(bytes);
        const auto num_pages = aligned_bytes / rewire::Pagesize;
        auto &mem = s.memory();
        mem.map(aligned_bytes, 0, wasm_context_.vm, ptr);
        next_page_ += num_pages + 1; // "install" a guard page after the mapping to fault on oob

        /* Add table address to env. */
        DISCARD env_->Set(Ctx, V8STR(table.name), v8::Int32::New(isolate_, ptr));

        /* Add table size (num_rows) to env. */
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

            const auto ptr = rewire::Pagesize * next_page_;
            const auto bytes = s.num_rows() * attr.type->size() / 8;
            const auto aligned_bytes = rewire::Ceil_To_Next_Page(bytes);
            const auto num_pages = aligned_bytes / rewire::Pagesize;
            auto &mem = s.memory(attr.id);
            mem.map(aligned_bytes, 0, wasm_context_.vm, ptr);
            DISCARD env_->Set(Ctx, V8STR(env_name.c_str()), v8::Int32::New(isolate_, ptr));
            next_page_ += num_pages + 1; // "install" a guard page after the mapping to fault on oob
        }
    }
};

void print_i32(const v8::FunctionCallbackInfo<v8::Value> &info)
{
    insist(info.Length() == 1);
    std::cerr << "print_i32(" << info[0].As<v8::Int32>()->Value() << ')' << std::endl;
}

std::unique_ptr<v8::Platform> V8Platform::PLATFORM_(nullptr);

V8Platform::V8Platform()
    : output_buffer_(Catalog::Get().allocator().allocate(1UL << 32)) // 2 GiB
{
    if (PLATFORM_ == nullptr) {
        PLATFORM_ = v8::platform::NewDefaultPlatform();
        v8::V8::InitializePlatform(PLATFORM_.get());
        v8::V8::Initialize();
    }

    v8::Isolate::CreateParams create_params;
    create_params.array_buffer_allocator = allocator_ = v8::ArrayBuffer::Allocator::NewDefaultAllocator();
    isolate_ = v8::Isolate::New(create_params);
}

V8Platform::~V8Platform()
{
    isolate_->Dispose();
    delete allocator_;
}

void V8Platform::execute(const WasmModule &module)
{
    /* Create required V8 scopes. */
    v8::Isolate::Scope isolate_scope(isolate_);
    v8::HandleScope handle_scope(isolate_); // tracks and disposes of all object handles
    v8::Local<v8::Context> context = v8::Context::New(isolate_);
    v8::Context::Scope context_scope(context);

    auto &wasm_context = Create_Wasm_Context(WASM_MAX_MEMORY);
    auto v8_wasm_module = compile_wasm_module(module);

    /* Create the import object for instantiating the WebAssembly module. */
    auto imports = v8::Object::New(isolate_);
    DISCARD imports->Set(context, V8STR("env"), create_env(wasm_context));

    /* Create a WebAssembly instance object. */
    auto instance = create_wasm_instance(v8_wasm_module, imports);

    /* Set the underlying memory for the instance. */
    v8::SetWasmInstanceRawMemory(instance, wasm_context.vm.as<uint8_t*>(), wasm_context.vm.size());

    /* Get the exports of the created WebAssembly instance. */
    auto exports = instance->Get(context, V8STR("exports")).ToLocalChecked().As<v8::Object>();
    auto run = exports->Get(context, V8STR("run")).ToLocalChecked().As<v8::Function>();

    /* Invoke the exported function `run` of the module. */
    args_t args { v8::Int32::New(isolate_, wasm_context.id), };
    auto result = run->Call(context, context->Global(), 1, args).ToLocalChecked().As<v8::Int32>()->Value();

    /* Print results. */
    std::cerr << result << " results:\n"
              << std::hex << std::setfill('0');
    for (uint8_t *ptr = output_buffer_.as<uint8_t*>(); result != 0; --result, ptr += 16) {
        std::cerr << "0x"
                  << std::setw(2) << (unsigned)(ptr[3]) << ' '
                  << std::setw(2) << (unsigned)(ptr[2]) << ' '
                  << std::setw(2) << (unsigned)(ptr[1]) << ' '
                  << std::setw(2) << (unsigned)(ptr[0]) << ' '
                  << std::setw(2) << (unsigned)(ptr[7]) << ' '
                  << std::setw(2) << (unsigned)(ptr[6]) << ' '
                  << std::setw(2) << (unsigned)(ptr[5]) << ' '
                  << std::setw(2) << (unsigned)(ptr[4]) << '\n'
                  << "0x"
                  << std::setw(2) << (unsigned)(ptr[11]) << ' '
                  << std::setw(2) << (unsigned)(ptr[10]) << ' '
                  << std::setw(2) << (unsigned)(ptr[9]) << ' '
                  << std::setw(2) << (unsigned)(ptr[8]) << ' '
                  << std::setw(2) << (unsigned)(ptr[15]) << ' '
                  << std::setw(2) << (unsigned)(ptr[14]) << ' '
                  << std::setw(2) << (unsigned)(ptr[13]) << ' '
                  << std::setw(2) << (unsigned)(ptr[12]) << '\n';
    }
}

v8::Local<v8::WasmModuleObject> V8Platform::compile_wasm_module(const WasmModule &module)
{
    auto [binary_addr, binary_size] = module.binary();
    auto v8_wasm_module = v8::WasmModuleObject::DeserializeOrCompile(
        /* isolate=           */ isolate_,
        /* serialized_module= */ { nullptr, 0 },
        /* wire_bytes=        */ v8::MemorySpan<const uint8_t>(binary_addr, binary_size)
    ).ToLocalChecked();
    free(binary_addr);
    return v8_wasm_module;
}

v8::Local<v8::Object> V8Platform::create_wasm_instance(v8::Local<v8::WasmModuleObject> module,
                                                       v8::Local<v8::Object> imports)
{
    auto Ctx = isolate_->GetCurrentContext();
    args_t instance_args { module, imports };
    return
        Ctx->Global()->                                                             // get the `global` object
        Get(Ctx, V8STR("WebAssembly")).ToLocalChecked().As<v8::Object>()->          // get WebAssembly class
        Get(Ctx, V8STR("Instance")).ToLocalChecked().As<v8::Object>()->             // get WebAssembly.Instance class
        CallAsConstructor(Ctx, 2, instance_args).ToLocalChecked().As<v8::Object>(); // instantiate WebAssembly.Instance
}

v8::Local<v8::Object> V8Platform::create_env(const WasmContext &wasm_context) const
{
    auto Ctx = isolate_->GetCurrentContext();
    auto &DB = Catalog::Get().get_database_in_use();
    auto env = v8::Object::New(isolate_);
    EnvGen G(isolate_, env, wasm_context);
    for (auto it = DB.begin_tables(); it != DB.end_tables(); ++it)
        G(it->second->store());

    /* Map the remaining address space to the output buffer. */
    const auto ptr_out = G.get_next_page() * rewire::Pagesize;
    const auto remaining = wasm_context.vm.size() - ptr_out;
    DISCARD env->Set(Ctx, V8STR("out"), v8::Int32::New(isolate_, ptr_out));
    output_buffer_.map(remaining, 0, wasm_context.vm, ptr_out);

    auto v8_print_i32 = v8::Function::New(Ctx, print_i32).ToLocalChecked();
    DISCARD env->Set(Ctx, V8STR("print_i32"), v8_print_i32);

#ifndef NDEBUG
    {
        auto json = to_json(env);
        std::cerr << "env: " << *v8::String::Utf8Value(isolate_, json) << '\n';
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

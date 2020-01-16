#include "V8Platform.hpp"

#include <chrono>


#define V8STR(str) ( v8::String::NewFromUtf8(this->isolate_, (str)).ToLocalChecked() )


using namespace db;
using args_t = v8::Local<v8::Value>[];


constexpr std::size_t KiB = 1024 * 1024;
constexpr std::size_t CHUNK_SIZE = 64 * KiB;
constexpr std::size_t INTS_PER_CHUNK = CHUNK_SIZE / sizeof(int);


std::unique_ptr<v8::Platform> V8Platform::PLATFORM_(nullptr);

V8Platform::V8Platform()
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

void V8Platform::execute(const WASMModule &module)
{
    /* Create required V8 scopes. */
    v8::Isolate::Scope isolate_scope(isolate_);
    v8::HandleScope handle_scope(isolate_); // tracks and disposes of all object handles
    v8::Local<v8::Context> context = v8::Context::New(isolate_);
    v8::Context::Scope context_scope(context);

    auto &wasm_context = Create_Wasm_Context(WASM_MAX_MEMORY);
    auto v8_wasm_module = compile_wasm_module(module);

    /* Allocate and initialize host memory. */
    auto memory = Catalog::Get().allocator().allocate(WASM_MAX_MEMORY);
    for (auto p = memory.as<int*>(), end = p + INTS_PER_CHUNK; p != end; ++p)
        *p = 0;
    for (auto p = memory.as<int*>() + INTS_PER_CHUNK, end = p + INTS_PER_CHUNK; p != end; ++p)
        *p = 1;

    /* Create the import object for instantiating the WebAssembly module. */
    auto imports = v8::Object::New(isolate_);
    DISCARD imports->Set(context, V8STR("env"), create_env());

    /* Create a WebAssembly instance object. */
    auto instance = create_wasm_instance(v8_wasm_module, imports);

    /* Set the underlying memory for the instance. */
    memory.map(2 * CHUNK_SIZE, 0, wasm_context.vm, 0);
    v8::SetWasmInstanceRawMemory(instance, wasm_context.vm.as<uint8_t*>(), wasm_context.vm.size());

    /* Get the exports of the created WebAssembly instance. */
    auto exports = instance->Get(context, V8STR("exports")).ToLocalChecked().As<v8::Object>();
    auto run = exports->Get(context, V8STR("run")).ToLocalChecked().As<v8::Function>();

    /* Invoke the exported function `run` of the module. */
    args_t args {
        v8::Int32::New(isolate_, wasm_context.id),
        v8::Int32::New(isolate_, INTS_PER_CHUNK),
    };
    auto result = run->Call(context, context->Global(), 2, args).ToLocalChecked().As<v8::Int32>();
    std::cerr << "Result is " << result->Value() << std::endl;
}

v8::Local<v8::WasmModuleObject> V8Platform::compile_wasm_module(const WASMModule &module)
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

v8::Local<v8::Object> V8Platform::create_env() const
{
    auto env = v8::Object::New(isolate_);

    auto &C = Catalog::Get();
    auto &DB = C.get_database_in_use();
    auto Ctx = isolate_->GetCurrentContext();

    std::ostringstream oss;
    for (auto it = DB.begin_tables(); it != DB.end_tables(); ++it) {
        auto &table = *it->second;
        for (auto &attr : table) {
            oss.str("");
            oss << table.name << '.' << attr.name;
            auto import_name = oss.str();
            DISCARD env->Set(Ctx, V8STR(import_name.c_str()), v8::Int32::New(isolate_, 0));
        }
    }

#ifndef NDEBUG
    {
        auto json = to_json(env);
        std::cerr << "env: " << *v8::String::Utf8Value(isolate_, json) << "}\n";
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


/*======================================================================================================================
 * Backend
 *====================================================================================================================*/

std::unique_ptr<Backend> Backend::CreateWasmV8()
{
    return std::make_unique<WasmBackend>(std::make_unique<V8Platform>());
}

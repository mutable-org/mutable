#include "V8Platform.hpp"

#include "v8-internal.h"

using namespace db;


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
    using args_t = v8::Local<v8::Value>[];

    /* Create required V8 scopes. */
    v8::Isolate::Scope isolate_scope(isolate_);
    v8::HandleScope handle_scope(isolate_); // tracks and disposes of all object handles
    v8::Local<v8::Context> context = v8::Context::New(isolate_);
    v8::Context::Scope context_scope(context);

    /* Create WASM module. */
    std::cerr << "Compiling and loading WASM module...";
    auto [binary_addr, binary_size] = module.binary();
    std::cerr << " Finished.  WASM module is " << binary_size << " bytes.\n";
    auto v8_wasm_module = v8::WasmModuleObject::DeserializeOrCompile(
            /* isolate=           */ isolate_,
            /* serialized_module= */ { nullptr, 0 },
            /* wire_bytes=        */ v8::MemorySpan<const uint8_t>(binary_addr, binary_size)
            ).ToLocalChecked();
    free(binary_addr);

#define V8STR(str) ( v8::String::NewFromUtf8(isolate_, (str)).ToLocalChecked() )

    /* Get the WebAssembly instance class prototype. */
    auto web_assembly_class = context->Global()->Get(context, V8STR("WebAssembly")).ToLocalChecked().As<v8::Object>();

    /* Create a WebAssembly memory object. */
    auto wasm_memory_class = web_assembly_class->Get(context, V8STR("Memory")).ToLocalChecked().As<v8::Object>();
    auto memory_params_object = v8::Object::New(isolate_);
    memory_params_object->Set(context, V8STR("initial"), v8::Int32::New(isolate_, 1));
    memory_params_object->Set(context, V8STR("maximum"), v8::Int32::New(isolate_, 1));
    args_t memory_args { memory_params_object };
    auto wasm_memory = wasm_memory_class->CallAsConstructor(context, 1, memory_args).ToLocalChecked().As<v8::Object>();
    insist(not wasm_memory.IsEmpty());

    /* Create host memory. */
    auto host_memory = new int32_t[1024];
    for (int32_t i = 0; i != 1024; ++i)
        host_memory[i] = i;
    auto store = v8::ArrayBuffer::NewBackingStore(host_memory, 1024 * sizeof(int32_t), [](void* data, size_t, void*) { delete[] reinterpret_cast<int32_t*>(data); }, nullptr);
    auto buffer = v8::ArrayBuffer::New(isolate_, std::move(store));
    wasm_memory->Set(context, V8STR("buffer"), buffer); /// XXX: the underlying buffer is not replaced

    /* Create the import object for instantiating the WebAssembly module. */
    auto host_object = v8::Object::New(isolate_);
    host_object->Set(context, V8STR("mem"), wasm_memory);
    auto import_object = v8::Object::New(isolate_);
    import_object->Set(context, V8STR("env"), host_object);

    /* Create a WebAssembly instance object. */
    auto instance_class = web_assembly_class->Get(context, V8STR("Instance")).ToLocalChecked().As<v8::Object>();
    insist(not instance_class.IsEmpty());
    args_t instance_args { v8_wasm_module, import_object };
    auto instance = instance_class->CallAsConstructor(context, 2, instance_args).ToLocalChecked().As<v8::Object>();
    insist(not instance.IsEmpty());

    /* Get the exports of the created WebAssembly instance. */
    auto exports = instance->Get(context, V8STR("exports")).ToLocalChecked().As<v8::Object>();
    insist(not exports.IsEmpty());

    /* Get exported function `run` from the exports. */
    auto run = exports->Get(context, V8STR("run")).ToLocalChecked().As<v8::Function>();
    insist(not run.IsEmpty());

    /* Invoke the exported function `run` of the module. */
    std::cerr << "Executing the module...";
    args_t args{ };
    auto result = run->Call(context, context->Global(), 0, args).ToLocalChecked().As<v8::Object>();
    insist(not result.IsEmpty());
    std::cerr << " Finished.\n";

    std::cout << "Result is " << *v8::String::Utf8Value(isolate_, v8::Local<v8::String>::Cast(result)) << std::endl;
}

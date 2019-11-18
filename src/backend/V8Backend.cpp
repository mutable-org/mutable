#include "V8Backend.hpp"


using namespace db;


std::unique_ptr<v8::Platform> V8Backend::PLATFORM_(nullptr);

V8Backend::V8Backend()
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

V8Backend::~V8Backend()
{
    isolate_->Dispose();
    delete allocator_;
}

void V8Backend::execute(const WASMModule &module)
{
    using args_t = v8::Local<v8::Value>[];

    /* Create requires V8 scopes. */
    v8::Isolate::Scope isolate_scope(isolate_);
    v8::HandleScope handle_scope(isolate_); // tracks and disposes of all object handles
    v8::Local<v8::Context> context = v8::Context::New(isolate_);
    v8::Context::Scope context_scope(context);

    /* Create WASM module. */
    std::cerr << "Compiling and loading WASM module...";
    auto [binary, binary_size] = module.binary();
    std::cerr << " Finished.  WASM module is " << binary_size << " bytes.\n";
    auto v8_wasm_module = v8::WasmModuleObject::DeserializeOrCompile(
            /* isolate=           */ isolate_,
            /* serialized_module= */ { nullptr, 0 },
            /* wire_bytes=        */ v8::MemorySpan<const uint8_t>(binary, binary_size)
            ).ToLocalChecked();
    free(binary);

    /* Create V8 strings. */
    auto str_WebAssembly = v8::String::NewFromUtf8(isolate_, "WebAssembly").ToLocalChecked();
    auto str_Instance    = v8::String::NewFromUtf8(isolate_, "Instance").ToLocalChecked();
    auto str_export      = v8::String::NewFromUtf8(isolate_, "exports").ToLocalChecked();
    auto str_main        = v8::String::NewFromUtf8(isolate_, "main").ToLocalChecked();

    /* Get the WebAssembly instance class prototype. */
    auto instance_class = context->Global()
        ->Get(context, str_WebAssembly).ToLocalChecked().As<v8::Object>()
        ->Get(context, str_Instance).ToLocalChecked().As<v8::Object>();
    insist(not instance_class.IsEmpty());

    /* Create a WebAssembly instance. */
    args_t instance_args { v8_wasm_module };
    auto instance = instance_class->CallAsConstructor(context, 1, instance_args).ToLocalChecked().As<v8::Object>();
    insist(not instance.IsEmpty());

    /* Get the exports of the created WebAssembly instance. */
    auto exports = instance->Get(context, str_export).ToLocalChecked().As<v8::Object>();
    insist(not exports.IsEmpty());

    /* Get exported function `main` from the exports. */
    auto main = exports->Get(context, str_main).ToLocalChecked().As<v8::Function>();
    insist(not main.IsEmpty());

    /* Invoke the exported function `main` of the module. */
    std::cerr << "Executing the module...";
    args_t args{};
    main->Call(context, context->Global(), 0, args);
    std::cerr << " Finished.\n";
}

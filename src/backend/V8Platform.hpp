#pragma once

#include "mutable/util/memory.hpp"
#include "util/WebSocketServer.hpp"
#include "WebAssembly.hpp"
#include <libplatform/libplatform.h>
#include <v8-inspector.h>
#include <v8.h>


namespace m {

namespace v8_helper {

    inline v8::Local<v8::String> to_v8_string(v8::Isolate *isolate, std::string_view sv) {
        insist(isolate);
        return v8::String::NewFromUtf8(isolate, sv.data(), v8::NewStringType::kNormal, sv.length()).ToLocalChecked();
    }

    inline std::string to_std_string(v8::Isolate *isolate, v8::Local<v8::Value> val) {
        v8::String::Utf8Value utf8(isolate, val);
        return *utf8;
    }

    inline v8::Local<v8::String> to_json(v8::Isolate *isolate, v8::Local<v8::Value> val) {
        insist(isolate);
        auto Ctx = isolate->GetCurrentContext();
        return v8::JSON::Stringify(Ctx, val).ToLocalChecked();
    }

    inline v8::Local<v8::Object> parse_json(v8::Isolate *isolate, std::string_view json) {
        insist(isolate);
        auto Ctx = isolate->GetCurrentContext();
        auto value = v8::JSON::Parse(Ctx, to_v8_string(isolate, json)).ToLocalChecked();
        if (value.IsEmpty())
            return v8::Local<v8::Object>();
        return value->ToObject(Ctx).ToLocalChecked();
    }

    inline v8_inspector::StringView make_string_view(const std::string &str) {
        return v8_inspector::StringView(reinterpret_cast<const uint8_t*>(str.data()), str.length());
    }

    inline std::string to_std_string(v8::Isolate *isolate, const v8_inspector::StringView sv) {
        int length = static_cast<int>(sv.length());
        v8::Local<v8::String> message = (
            sv.is8Bit()
            ? v8::String::NewFromOneByte(isolate, reinterpret_cast<const uint8_t*>(sv.characters8()), v8::NewStringType::kNormal, length)
            : v8::String::NewFromTwoByte(isolate, reinterpret_cast<const uint16_t*>(sv.characters16()), v8::NewStringType::kNormal, length)
        ).ToLocalChecked();
        v8::String::Utf8Value result(isolate, message);
        return std::string(*result, result.length());
    }

    struct V8InspectorClientImpl;

    struct WebSocketChannel : v8_inspector::V8Inspector::Channel
    {
        private:
        v8::Isolate *isolate_;
        WebSocketServer::Connection &conn_;

        public:
        WebSocketChannel(v8::Isolate *isolate, WebSocketServer::Connection &conn)
            : isolate_(isolate)
            , conn_(conn)
        { }

        WebSocketServer::Connection & connection() const { return conn_; }

        private:
        void sendResponse(int, std::unique_ptr<v8_inspector::StringBuffer> message) override {
            v8::HandleScope handle_scope(isolate_);
            auto str = to_std_string(isolate_, message->string());
            conn_.send(str);
        }

        void sendNotification(std::unique_ptr<v8_inspector::StringBuffer> message) override {
            v8::HandleScope handle_scope(isolate_);
            auto str = to_std_string(isolate_, message->string());
            conn_.send(str);
        }

        void flushProtocolNotifications() override { }
    };

    struct V8InspectorClientImpl : v8_inspector::V8InspectorClient
    {
        private:
        v8::Isolate *isolate_ = nullptr;
        WebSocketServer server_;
        std::unique_ptr<WebSocketServer::Connection> conn_;
        std::unique_ptr<WebSocketChannel> channel_;
        std::unique_ptr<v8_inspector::V8Inspector> inspector_;
        std::unique_ptr<v8_inspector::V8InspectorSession> session_;
        std::function<void(void)> code_; ///< the code to execute in the debugger
        bool is_terminated_ = true;

        public:
        V8InspectorClientImpl(int16_t port, v8::Isolate *isolate)
            : isolate_(notnull(isolate))
            , server_(port, std::bind(&V8InspectorClientImpl::on_message, this, std::placeholders::_1))
        {
            std::cout << "Initiating the V8 inspector server.  To attach to the inspector, open Chrome/Chromium and "
                         "visit\n\n\t"
                         "devtools://devtools/bundled/inspector.html?experiments=true&v8only=true&ws=127.0.0.1:"
                      << port << '\n' << std::endl;

            inspector_ = v8_inspector::V8Inspector::create(isolate, this);
            conn_ = std::make_unique<WebSocketServer::Connection>(server_.await());
            channel_ = std::make_unique<WebSocketChannel>(isolate_, *conn_);

            /* Create a debugging session by connecting the V8Inspector instance to the channel. */
            std::string state("mutable");
            session_ = inspector_->connect(
                /* contextGroupId= */ 1,
                /* channel=        */ channel_.get(),
                /* state=          */ make_string_view(state)
            );
        }

        V8InspectorClientImpl(const V8InspectorClientImpl&) = delete;

        ~V8InspectorClientImpl() {
            session_.reset();
            channel_.reset();
            conn_.reset();
            inspector_.reset();
        }

        void start(std::function<void(void)> code) {
            code_ = std::move(code);
            conn_->listen();
        }

        /* Register the context object in the V8Inspector instance. */
        void register_context(v8::Local<v8::Context> context);

        /* Deregister the context object in the V8Inspector instance. */
        void deregister_context(v8::Local<v8::Context> context);

        void on_message(std::string_view sv);

        /** Synchronously consume all front end (CDT) debugging messages. */
        void runMessageLoopOnPause(int) override;

        /** Called by V8 when no more inspector messages are pending. */
        void quitMessageLoopOnPause() override { is_terminated_ = true; }

        void waitFrontendMessageOnPause() { is_terminated_ = false; }
    };

}

/** The `V8Platform` is a `WasmPlatform` using [V8, Google's open source high-performance JavaScript and WebAssembly
 * engine] (https://v8.dev/). */
struct V8Platform : WasmPlatform
{
    private:
    static std::unique_ptr<v8::Platform> PLATFORM_;
    v8::ArrayBuffer::Allocator *allocator_ = nullptr;
    v8::Isolate *isolate_ = nullptr;
    rewire::Memory mem_; ///< heap memory of the Wasm module

    /*----- Objects for remote debugging via CDT. --------------------------------------------------------------------*/
    std::unique_ptr<v8_helper::V8InspectorClientImpl> inspector_;

    public:
    V8Platform();
    V8Platform(const V8Platform&) = delete;
    V8Platform(V8Platform&&) = default;
    ~V8Platform();

    static v8::Platform * platform() { insist(bool(PLATFORM_)); return PLATFORM_.get(); }

    void execute(const Operator &plan) override;

    private:
    /** Compile the `WasmModule` `module` and instantiate a `v8::WasmModuleObject` instance. */
    v8::Local<v8::WasmModuleObject> instantiate(const WasmModule &module, v8::Local<v8::Object> imports);
    /** Creates a V8 object that captures the entire environment.  TODO Only capture things relevant to the module. */
    v8::Local<v8::Object> create_env(WasmContext &wasm_context, const Operator &plan) const;
    /** Convert a `std::string` to a `v8::String`. */
    v8::Local<v8::String> mkstr(const std::string &str) const;
    /** Converts any V8 value to JSON. */
    v8::Local<v8::String> to_json(v8::Local<v8::Value> val) const;
    /** Create a JavaScript document for debugging via CDT. */
    std::string create_js_debug_script(const Operator &plan, const WasmModule &module, v8::Local<v8::Object> env,
                                       const WasmPlatform::WasmContext &wasm_context);
};

}

#pragma once

#include "backend/WebAssembly.hpp"
#include "util/WebSocketServer.hpp"
#include <v8-inspector.h>
#include <v8.h>


namespace m {

namespace wasm {

namespace detail {

struct WebSocketChannel : v8_inspector::V8Inspector::Channel
{
    private:
    v8::Isolate *isolate_;
    WebSocketServer::Connection &conn_;

    public:
    WebSocketChannel(v8::Isolate *isolate, WebSocketServer::Connection &conn)
        : isolate_(isolate)
        , conn_(conn) {}

    WebSocketServer::Connection &connection() const { return conn_; }

    private:
    void sendResponse(int, std::unique_ptr<v8_inspector::StringBuffer> message) override;
    void sendNotification(std::unique_ptr<v8_inspector::StringBuffer> message) override;

    void flushProtocolNotifications() override {}
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
    V8InspectorClientImpl(int16_t port, v8::Isolate *isolate);
    V8InspectorClientImpl(const V8InspectorClientImpl &) = delete;

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

    /** Register the context object in the V8Inspector instance. */
    void register_context(v8::Local<v8::Context> context);

    /** Deregister the context object in the V8Inspector instance. */
    void deregister_context(v8::Local<v8::Context> context);

    void on_message(std::string_view sv);

    /** Synchronously consume all front end (CDT) debugging messages. */
    void runMessageLoopOnPause(int) override;

    /** Called by V8 when no more inspector messages are pending. */
    void quitMessageLoopOnPause() override { is_terminated_ = true; }

    void waitFrontendMessageOnPause() { is_terminated_ = false; }
};

void insist(const v8::FunctionCallbackInfo<v8::Value> &info);
void _throw(const v8::FunctionCallbackInfo<v8::Value> &info);
void print(const v8::FunctionCallbackInfo<v8::Value> &info);
void set_wasm_instance_raw_memory(const v8::FunctionCallbackInfo<v8::Value> &info);
void read_result_set(const v8::FunctionCallbackInfo<v8::Value> &info);

v8::Local<v8::String> mkstr(v8::Isolate &isolate, const std::string &str);
v8::Local<v8::WasmModuleObject> instantiate(v8::Isolate &isolate, v8::Local<v8::Object> imports);
v8::Local<v8::Object> create_env(v8::Isolate &isolate, const Operator &plan);
v8::Local<v8::String> to_json(v8::Isolate &isolate, v8::Local<v8::Value> val);
std::string create_js_debug_script(v8::Isolate &isolate, v8::Local<v8::Object> env,
                                   const WasmEngine::WasmContext &wasm_context);
void run_inspector(V8InspectorClientImpl &inspector, v8::Isolate &isolate, v8::Local<v8::Object> env);

}

}

}

#include "util/WebSocketServer.hpp"

#include <iostream>


namespace ip = boost::asio::ip;
namespace ws = boost::beast::websocket;

using namespace m;
using namespace std::chrono_literals;


/*======================================================================================================================
 * WebSocketServer
 *====================================================================================================================*/

WebSocketServer::WebSocketServer(uint16_t port, on_message_t on_message)
    : io_ctx_(/* concurrency_level= */ 1)
    , acceptor_(io_ctx_, { ip::make_address("127.0.0.1"), port })
    , on_message_(on_message)
    , port_(port)
{ }

WebSocketServer::Connection WebSocketServer::await()
{
    ip::tcp::socket socket(io_ctx_);
    acceptor_.accept(socket);
    return Connection(*this, std::make_unique<web_socket_t>(std::move(socket)));
}


/*======================================================================================================================
 * WebSocketServer::Connection
 *====================================================================================================================*/

WebSocketServer::Connection::Connection(WebSocketServer &server, std::unique_ptr<web_socket_t> ws)
    : server_(server)
    , ws_(std::move(ws))
{ }

void WebSocketServer::Connection::send(std::string_view msg)
{
    ws_->text(ws_->got_text());
    ws_->write(boost::asio::buffer(msg.data(), msg.length()));
}

void WebSocketServer::Connection::listen()
{
    boost::beast::multi_buffer buffer;
    ws_->accept();
    for (;;) {
        try {
            buffer.clear();
            if (ws_->read(buffer) == 0)
                return;
        } catch (const boost::system::system_error &e) {
            const auto ec = e.code();
            if (ec == ws::error::closed)
                return;
            else
                throw;
        }
        std::string msg = boost::beast::buffers_to_string(buffer.data());
        server().on_message(msg);
    }
}

bool WebSocketServer::Connection::wait_on_message()
{
    boost::beast::multi_buffer buffer;
    try {
        buffer.clear();
        if (ws_->read(buffer) == 0)
            return false;
    } catch (const boost::system::system_error &e) {
        const auto ec = e.code();
        if (ec == ws::error::closed)
            return false;
        else
            throw;
    }
    std::string msg = boost::beast::buffers_to_string(buffer.data());
    server().on_message(msg);
    return true;
}

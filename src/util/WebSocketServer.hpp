#pragma once

#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <functional>
#include <future>
#include <string>
#include <thread>


namespace m {


struct WebSocketServer
{
    using on_message_t = std::function<void(std::string)>;
    using web_socket_t = boost::beast::websocket::stream<boost::asio::ip::tcp::socket>;

    struct Connection
    {
        private:
        WebSocketServer &server_;
        std::unique_ptr<web_socket_t> ws_;

        public:
        Connection(WebSocketServer &server, std::unique_ptr<web_socket_t> ws);
        Connection(const Connection&) = delete;
        Connection(Connection&&) = default;

        WebSocketServer & server() const { return server_; }
        web_socket_t & ws() const { return *ws_; }

        void send(std::string_view msg);
        void listen();
        bool wait_on_message();
    };

    private:
    boost::asio::io_context io_ctx_;
    boost::asio::ip::tcp::acceptor acceptor_;
    on_message_t on_message_;
    uint16_t port_;

    public:
    WebSocketServer(uint16_t port, on_message_t onMessage);
    WebSocketServer(const WebSocketServer&) = delete;

    uint16_t port() const { return port_; }
    void on_message(std::string str) { on_message_(str); }

    Connection await();
};

}

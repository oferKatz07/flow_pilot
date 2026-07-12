
// http_server.cpp - Implementation of HTTP server for FlowPilot

#include "http_server.h"
#include "logger.h"
#include "config.h"
#include "request_handlers.h"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <memory>

namespace flow_pilot {

namespace beast = boost::beast;
namespace http = beast::http;
namespace asio = boost::asio;
using tcp = asio::ip::tcp;
using json = nlohmann::json;
using asio::awaitable;
using asio::co_spawn;
using asio::detached;
using asio::use_awaitable;

static void fail(beast::error_code ec, char const* what)
{
    auto logger = get_logger();
    logger->error("HTTP server error: {}: {}", what, ec.message());
}


class session : public std::enable_shared_from_this<session> {
    tcp::socket socket_;
    beast::flat_buffer buffer_;
    http::request<http::string_body> req_;

public:
    explicit session(tcp::socket socket)
        : socket_(std::move(socket))
    {
    }

    awaitable<void> run()
    {
        try {
            for (;;) {
                req_ = {};
                co_await http::async_read(socket_, buffer_, req_, use_awaitable);

                Logger::get_instance()->debug("{} {}", req_.method_string(), req_.target());

                
                HandlerCtxData ctx;
                IHandler& handler = HandlerFactory::get_instance().get_handler(std::move(req_), ctx);
                auto res = co_await handler.handle(ctx);
                co_await http::async_write(socket_, res, use_awaitable);

                if (!res.keep_alive()) {
                    break;
                }
            }
        }
        catch (const boost::system::system_error& ex) {
            if (ex.code() != asio::error::eof && ex.code() != http::error::end_of_stream) {
                fail(ex.code(), "session");
            }
        }
        catch (const std::exception& ex) {
            fail(beast::error_code{}, ex.what());
        }

        beast::error_code ec;
        socket_.shutdown(tcp::socket::shutdown_send, ec);
    }
};

class listener : public std::enable_shared_from_this<listener> {
    asio::io_context& ioc_;
    tcp::acceptor acceptor_;

public:
    listener(asio::io_context& ioc, tcp::endpoint endpoint)
        : ioc_(ioc)
        , acceptor_(asio::make_strand(ioc))
    {
        beast::error_code ec;

        acceptor_.open(endpoint.protocol(), ec);
        if (ec) {
            fail(ec, "open");
            return;
        }

        acceptor_.set_option(asio::socket_base::reuse_address(true), ec);
        if (ec) {
            fail(ec, "set_option");
            return;
        }

        acceptor_.bind(endpoint, ec);
        if (ec) {
            fail(ec, "bind");
            return;
        }

        acceptor_.listen(asio::socket_base::max_listen_connections, ec);
        if (ec) {
            fail(ec, "listen");
            return;
        }
    }

    awaitable<void> run()
    {
        for (;;) {
            try {
                tcp::socket socket = co_await acceptor_.async_accept(use_awaitable);
                Logger::get_instance()->debug("New client connected from {}", socket.remote_endpoint().address().to_string());
                co_spawn(acceptor_.get_executor(), 
                         session(std::move(socket)).run(),
                         detached);
            }
            catch (const boost::system::system_error& ex) {
                fail(ex.code(), "accept");
                co_return;
            }
        }
    }
};

void run_http_server(asio::io_context& ioc)
{
    Logger::get_instance()->info("Initializing FlowPilot HTTP server");
    const ServerConfig& config = Config::get().server();
    auto const address = asio::ip::make_address(config.address);
    tcp::endpoint endpoint{address, config.port};
    auto listener_ptr = std::make_shared<listener>(ioc, endpoint);
    co_spawn(ioc, 
             listener_ptr->run(),
             detached);
    Logger::get_instance()->info("HTTP server listening on {}:{}", config.address, config.port);
}

} // namespace flow_pilot
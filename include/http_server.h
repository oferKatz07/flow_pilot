
// http_server.h - HTTP server for FlowPilot

#pragma once

#include <boost/asio/io_context.hpp>

namespace flow_pilot {

void run_http_server(boost::asio::io_context& ioc);

} // namespace flow_pilot

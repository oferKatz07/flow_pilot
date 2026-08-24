#pragma once

#include <boost/asio/io_context.hpp>

namespace flow_pilot::test {

inline boost::asio::io_context& redis_ioc()
{
    static boost::asio::io_context ioc;
    return ioc;
}

} // namespace flow_pilot::test

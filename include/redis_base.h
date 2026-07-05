// redis_db.h - Redis persistence interface for FlowPilot

#pragma once

#include <string>
#include <vector>
#include <memory>
#include <boost/asio/io_context.hpp>

namespace flow_pilot {

enum class RequestStatus {
    VALIDATING,
    ACCEPTED,
    COMPLETED
};

struct RedisReply {
    enum class Type {
        SimpleString,
        Error,
        Integer,
        BulkString,
        Array,
        Nil
    } type;
    std::string string_value;
    long long integer_value = 0;
    std::vector<std::string> array_value;
};

class RedisBase {
public:
    explicit RedisBase() = default;
    virtual ~RedisBase() = default;


protected:
    std::string connection_string_;
    std::string host_;
    std::string port_;
    std::string password_;
    bool connected_ = false;
};

} // namespace flow_pilot

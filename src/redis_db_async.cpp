
// redis_db_async.cpp - Redis communication implementation for FlowPilot

#include <boost/asio.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/read_until.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/write.hpp>
#include <sstream>
#include <chrono>
#include <ctime>
#include <stdexcept>

#include "flow_pilot_error_msgs.h"
#include "logger.h"
#include "config.h"
#include "http_server.h"
#include "redis_db_async.h"

namespace flow_pilot {

using boost::asio::use_awaitable;

namespace {

std::string trim_crlf(std::string line)
{
    if (!line.empty() && line.back() == '\r') {
        line.pop_back();
    }
    return line;
}

bool parse_connection_string(const std::string& connection_string,
                             std::string& host,
                             std::string& port,
                             std::string& password)
{
    if (connection_string.rfind("redis://", 0) == 0) {
        auto endpoint = connection_string.substr(8);
        auto auth_pos = endpoint.find('@');
        if (auth_pos != std::string::npos) {
            auto auth = endpoint.substr(0, auth_pos);
            endpoint = endpoint.substr(auth_pos + 1);
            if (!auth.empty() && auth[0] == ':') {
                password = auth.substr(1);
            }
        }

        auto pos = endpoint.find(':');
        if (pos == std::string::npos) {
            return false;
        }
        host = endpoint.substr(0, pos);
        port = endpoint.substr(pos + 1);
        return true;
    }

    auto pos = connection_string.find(':');
    if (pos == std::string::npos) {
        host = connection_string;
        return true;
    }

    host = connection_string.substr(0, pos);
    port = connection_string.substr(pos + 1);
    return true;
}

class RedisParseException : public std::runtime_error {
public:
    explicit RedisParseException(const std::string& message)
        : std::runtime_error(message) {}
};

} // namespace

struct RedisDatabaseAsync::ImplAsync {
    explicit ImplAsync(boost::asio::io_context& ioc)
        : socket_(ioc), resolver_(ioc), read_buffer_() {}

    bool connect(const std::string& host, const std::string& port, std::string& error_message)
    {
        boost::system::error_code ec;
        if (socket_.is_open()) {
            socket_.close(ec);
        }

        auto endpoints = resolver_.resolve(host, port, ec);
        if (ec) {
            error_message = "Redis resolver error: " + ec.message();
            return false;
        }

        boost::asio::connect(socket_, endpoints, ec);
        if (ec) {
            error_message = "Redis connect error: " + ec.message();
            return false;
        }

        socket_.non_blocking(true, ec);
        if (ec) {
            error_message = "Failed to set Redis socket non-blocking: " + ec.message();
            socket_.close(ec);
            return false;
        }

        return true;
    }

    boost::asio::awaitable<RedisReply> execute_async(const std::vector<std::string>& args)
    {
        co_await write_command_async(args);
        co_return co_await parse_reply_async();
    }

private:
    boost::asio::awaitable<void> write_command_async(const std::vector<std::string>& args)
    {
        std::ostringstream request;
        request << '*' << args.size() << "\r\n";
        for (const auto& arg : args) {
            request << '$' << arg.size() << "\r\n" << arg << "\r\n";
        }

        auto request_string = request.str();
        co_await boost::asio::async_write(socket_, boost::asio::buffer(request_string), use_awaitable);
    }

    boost::asio::awaitable<RedisReply> parse_reply_async()
    {
        std::string line = co_await read_line_async();
        if (line.empty()) {
            throw RedisParseException("Empty reply from Redis");
        }

        char prefix = line[0];
        std::string payload = line.substr(1);

        switch (prefix) {
            case '+':
                co_return RedisReply{RedisReply::Type::SimpleString, payload, 0, {}};
            case '-':
                throw RedisParseException("Redis error: " + payload);
            case ':':
                co_return RedisReply{RedisReply::Type::Integer, std::string(), std::stoll(payload), {}};
            case '$': {
                int length = std::stoi(payload);
                if (length == -1) {
                    co_return RedisReply{RedisReply::Type::Nil, std::string(), 0, {}};
                }

                std::size_t required_bytes = static_cast<std::size_t>(length + 2);
                if (read_buffer_.size() < required_bytes) {
                    co_await boost::asio::async_read(socket_, read_buffer_, boost::asio::transfer_exactly(required_bytes - read_buffer_.size()), use_awaitable);
                }

                std::istream response_stream(&read_buffer_);
                std::string blob(length, '\0');
                response_stream.read(&blob[0], length);
                char crlf[2];
                response_stream.read(crlf, 2);
                co_return RedisReply{RedisReply::Type::BulkString, std::move(blob), 0, {}};
            }
            case '*': {
                int count = std::stoi(payload);
                if (count == -1) {
                    co_return RedisReply{RedisReply::Type::Nil, std::string(), 0, {}};
                }

                RedisReply reply;
                reply.type = RedisReply::Type::Array;
                reply.array_value.reserve(count);
                for (int i = 0; i < count; ++i) {
                    auto element = co_await parse_reply_async();
                    if (element.type == RedisReply::Type::BulkString || element.type == RedisReply::Type::SimpleString) {
                        reply.array_value.emplace_back(element.string_value);
                    } else if (element.type == RedisReply::Type::Integer) {
                        reply.array_value.emplace_back(std::to_string(element.integer_value));
                    } else {
                        reply.array_value.emplace_back(std::string());
                    }
                }
                co_return reply;
            }
            default:
                throw RedisParseException("Unsupported Redis response type");
        }
    }

    boost::asio::awaitable<std::string> read_line_async()
    {
        co_await boost::asio::async_read_until(socket_, read_buffer_, "\r\n", use_awaitable);
        std::istream response_stream(&read_buffer_);
        std::string line;
        std::getline(response_stream, line);
        co_return trim_crlf(std::move(line));
    }

    boost::asio::ip::tcp::socket socket_;
    boost::asio::ip::tcp::resolver resolver_;
    mutable boost::asio::streambuf read_buffer_;
};

std::shared_ptr<RedisDatabaseAsync> RedisDatabaseAsync::instance_ = nullptr;
std::once_flag RedisDatabaseAsync::init_flag_;

RedisDatabaseAsync::RedisDatabaseAsync(boost::asio::io_context& ioc)
    : impl_async_(std::make_unique<ImplAsync>(ioc))
{
    const InMemoryDBConfig& config = Config::get().redis();
        std::string connection_string = config.host + ":" + std::to_string(config.port);
        if (!connect(connection_string, config.password)) {
            throw std::runtime_error("Unable to connect to Redis at " + connection_string);
        }
}

RedisDatabaseAsync::~RedisDatabaseAsync() = default;

std::shared_ptr<RedisDatabaseAsync> RedisDatabaseAsync::init(boost::asio::io_context& ioc, const InMemoryDBConfig& config)
{
    std::call_once(init_flag_, [&]() {
        auto instance = std::shared_ptr<RedisDatabaseAsync>(new RedisDatabaseAsync(ioc));
        std::string connection_string = config.host + ":" + std::to_string(config.port);
        if (!instance->connect(connection_string, config.password)) {
            throw std::runtime_error("Unable to connect to Redis at " + connection_string);
        }
        instance_ = std::move(instance);
    });
    if (!instance_) {
        throw std::runtime_error("RedisDatabaseAsync initialization failed");
    }
    return instance_;
}

std::shared_ptr<RedisDatabaseAsync> RedisDatabaseAsync::init(boost::asio::io_context& ioc)
{
    return init(ioc, Config::get().redis());
}

std::shared_ptr<RedisDatabaseAsync> RedisDatabaseAsync::get_instance()
{
    if (!instance_) {
        throw std::runtime_error("RedisDatabaseAsync has not been initialized");
    }
    return instance_;
}

bool RedisDatabaseAsync::connect(const std::string& connection_string, const std::string& password)
{
    std::string host = "127.0.0.1";
    std::string port = "6379";
    std::string auth_password = password;
    if (!connection_string.empty()) {
        if (!parse_connection_string(connection_string, host, port, auth_password)) {
            return false;
        }

        if (host.empty()) {
            host = "127.0.0.1";
        }

        if (port.empty()) {
            port = "6379";
        }
    }

    std::string error_message;
    bool connected = impl_async_->connect(host, port, error_message);
    if (!connected) {
        auto logger = get_logger();
        logger->error("Redis connection failed: {}", error_message);
        return false;
    }
    // TBD Authentication is deffered to a later phase.
    // The password is currently empty and we want to allow connecting without authentication first.
    // if (!impl_async_->authenticate(auth_password, error_message)) {
    //     auto logger = get_logger();
    //     logger->error("Redis authentication failed: {}", error_message);
    //     return false;
    // }

    connection_string_ = connection_string;
    host_ = host;
    port_ = port;
    password_ = auth_password;
    connected_ = true;
    return true;
}

boost::asio::awaitable<bool> RedisDatabaseAsync::execute_integer_command_async(const std::vector<std::string>& args, 
                                                                               long long& value) const {
    try {
        auto reply = co_await impl_async_->execute_async(args);
        if (reply.type == RedisReply::Type::Integer) {
            value = reply.integer_value;
            co_return true;
        }
        if (reply.type == RedisReply::Type::SimpleString) {
            try {
                value = std::stoll(reply.string_value);
                co_return true;
            } catch (...) {
                co_return false;
            }
        }
    } catch (const RedisParseException& ex) {
        auto logger = get_logger();
        logger->error("Redis command parse error: {}", ex.what());
    } catch (const std::exception& ex) {
        auto logger = get_logger();
        logger->error("Redis integer command failed: {}", ex.what());
    }
    co_return false;
}

boost::asio::awaitable<bool> RedisDatabaseAsync::request_exists_async(const std::string& client_id, const std::string& request_id) const {
    std::string key = "fp:req:" + client_id + ":" + request_id;
    std::vector<std::string> args{"EXISTS", key};
    long long value = 0;
    auto ok = co_await execute_integer_command_async(args, value);
    co_return ok && value > 0;
}

boost::asio::awaitable<bool> RedisDatabaseAsync::reserve_request_id_async(const std::string& client_id, const std::string& request_id)
{
    std::string key = "fp:req:" + client_id + ":" + request_id;
    co_return co_await execute_set_command_async(key, "VALIDATING", 900, true);
}

boost::asio::awaitable<bool> RedisDatabaseAsync::release_request_id_async(const std::string& client_id, const std::string& request_id)
{
    std::string key = "fp:req:" + client_id + ":" + request_id;
    std::vector<std::string> args{"DEL", key};
    long long value = 0;
    auto ok = co_await execute_integer_command_async(args, value);
    co_return ok;
}

boost::asio::awaitable<bool> RedisDatabaseAsync::can_accept_request_async(const std::string& client_id,
                                                                         int max_active_workflows,
                                                                         int max_requests,
                                                                         int window_seconds,
                                                                         std::string& rejection_reason) const
{
    auto active_workflows_key = std::string("fp:active:") + client_id + ":workflows";
    std::vector<std::string> active_args{"SCARD", active_workflows_key};
    long long active_count_value = 0;
    auto active_count_ok = co_await execute_integer_command_async(active_args, active_count_value);
    if (!active_count_ok) {
        active_count_value = 0;
    }

    if (active_count_value >= max_active_workflows) {
        rejection_reason = "Client exceeded the maximum allowed concurrent workflows.";
        co_return false;
    }

    long long epoch = static_cast<long long>(std::time(nullptr));
    std::vector<std::string> keys;
    keys.reserve(window_seconds);
    for (int i = 0; i < window_seconds; ++i) {
        keys.push_back("fp:rate:" + client_id + ":" + std::to_string(epoch - i));
    }

    std::vector<std::string> counts;
    auto mget_ok = co_await execute_mget_command_async(keys, counts);
    long long total_requests = 0;
    if (mget_ok) {
        for (const auto& value : counts) {
            if (!value.empty()) {
                try {
                    total_requests += std::stoll(value);
                } catch (...) {
                    // ignore parse errors for rate buckets
                }
            }
        }
    }

    if (total_requests >= max_requests) {
        rejection_reason = "Rate limit exceeded: too many workflow requests in the recent time window.";
        co_return false;
    }

    co_return true;
}

boost::asio::awaitable<bool> RedisDatabaseAsync::admit_request_async(const std::string& client_id,
                                                                     const std::string& request_id,
                                                                     const std::string& workflow_id,
                                                                     int max_active_workflows,
                                                                     int max_requests,
                                                                     int window_seconds,
                                                                     std::string& rejection_reason)
{
    std::string request_key = "fp:req:" + client_id + ":" + request_id;
    std::string active_workflows_key = "fp:active:" + client_id + ":workflows";

    std::vector<std::string> keys;
    keys.push_back(request_key);
    keys.push_back(active_workflows_key);

    std::string lua_script = R"lua(
        -- This Lua script performs atomic admission checks for incoming workflow requests.
        --
        -- Redis is the authoritative enforcement mechanism for client request rate limiting.
        --
        -- All other Redis checks are admission optimizations intended to reject
        -- requests early and reduce unnecessary validation and database activity.
        -- SQLite remains the authoritative source for workflow state, workflow
        -- identity and request history.

        -- Set request key to track it and prevent duplicates
        local request_set = redis.call('SET', KEYS[1], 'RECEIVED', 'NX', 'EX', 900)
        if not request_set then
            return {0, 'duplicate'}
        end

        -- Use Redis server time for rate buckets
        local redis_time = redis.call('TIME')
        local server_epoch = tonumber(redis_time[1])

        -- Update rate limit bucket using Redis server time
        local rate_key_server_time = 'fp:rate:' .. ARGV[5] .. ':' .. server_epoch
        local current_rate = redis.call('INCR', rate_key_server_time)
        if current_rate == 1 then
            -- expire slightly after the window to ensure correct accounting in case of redis clock skew
            redis.call('EXPIRE', rate_key_server_time, ARGV[3] + 1)
        end

        -- Compute total across the window
        local total = current_rate
        for i = 1, tonumber(ARGV[3]) - 1 do
            local historical_key = 'fp:rate:' .. ARGV[5] .. ':' .. (server_epoch - i)
            local value = tonumber(redis.call('GET', historical_key) or '0')
            total = total + value
        end

        if total > tonumber(ARGV[2]) then
            -- rollback rate increment
            redis.call('DECR', rate_key_server_time)
            return {0, 'rate_limit'}
        end

        -- Add workflow_id to active workflows set only after checks
        local added = redis.call('SADD', KEYS[2], ARGV[4])
        if added == 1 then
            -- Active count check after successfully adding a new workflow ID
            local final_active_count = redis.call('SCARD', KEYS[2])
            if final_active_count > tonumber(ARGV[1]) then
                -- Max concurrent workflows exceeded after adding the new workflow id
                -- Remove the newly added workflow and rollback rate
                redis.call('SREM', KEYS[2], ARGV[4])
                redis.call('DECR', rate_key_server_time)
                return {0, 'active_limit'}
            end
        else
            redis.call('DECR', rate_key_server_time)
            return {0, 'duplicate_workflow'}
        end

        return {1, 'ok'}
    )lua";

    std::vector<std::string> script_args;
    script_args.push_back(std::to_string(max_active_workflows));
    script_args.push_back(std::to_string(max_requests));
    script_args.push_back(std::to_string(window_seconds));
    script_args.push_back(workflow_id);
    script_args.push_back(client_id);

    std::vector<std::string> lua_values;
    auto lua_ok = co_await execute_lua_script_async(lua_script, keys, script_args, lua_values);
    if (!lua_ok || lua_values.size() < 2 || lua_values[0].empty()) {
        rejection_reason = error_msgs::INTERNAL_DB_FAILURE;
        co_return false;
    }

    std::string status = lua_values[0];
    std::string detail = lua_values[1];
    if (status == "1") {
        co_return true;
    }

    if (detail == "duplicate") {
        rejection_reason = error_msgs::DUPLICATE_REQUEST;
    } else if (detail == "rate_limit") {
        rejection_reason = error_msgs::RATE_LIMIT_EXCEEDED;
    } else if (detail == "active_limit") {
        rejection_reason = error_msgs::CONCURRENT_WORKFLOW_LIMIT_EXCEEDED;
    } else if (detail == "duplicate_workflow") {
        rejection_reason = error_msgs::WORKFLOW_ID_EXISTS;
    } else {
        Logger::get_instance()->error("Unexpected Lua script failure: {}", detail);
        rejection_reason = error_msgs::INTERNAL_DB_FAILURE;
    }
    co_return false;
}

boost::asio::awaitable<bool> RedisDatabaseAsync::update_request_status_async(const std::string& client_id,
                                                                             const std::string& request_id,
                                                                             const std::string& status)
{
    std::string key = "fp:req:" + client_id + ":" + request_id;
    co_return co_await execute_set_command_async(key, status, 900, false);
}

boost::asio::awaitable<bool> RedisDatabaseAsync::fetch_request_status_async(const std::string& client_id,
                                                                           const std::string& request_id,
                                                                           std::string& value) const
{
    std::string key = "fp:req:" + client_id + ":" + request_id;
    std::vector<std::string> args{"GET", key};
    co_return co_await execute_bulk_string_command_async(args, value);
}

boost::asio::awaitable<bool> RedisDatabaseAsync::remove_active_workflow_async(const std::string& client_id,
                                                                             const std::string& workflow_id)
{
    std::string active_workflows_key = "fp:active:" + client_id + ":workflows";
    std::vector<std::string> args{"SREM", active_workflows_key, workflow_id};
    long long value = 0;
    auto ok = co_await execute_integer_command_async(args, value);
    co_return ok && value > 0;
}

std::shared_ptr<IRedisDatabaseAsync> get_redis_database_async()
{
    return RedisDatabaseAsync::get_instance();
}

boost::asio::awaitable<bool> RedisDatabaseAsync::execute_bulk_string_command_async(const std::vector<std::string>& args, std::string& value) const {
    try {
        auto reply = co_await impl_async_->execute_async(args);
        if (reply.type == RedisReply::Type::BulkString || reply.type == RedisReply::Type::SimpleString) {
            value = reply.string_value;
            co_return true;
        }
        co_return false;
    } catch (const RedisParseException& ex) {
        auto logger = get_logger();
        logger->error("Redis command parse error: {}", ex.what());
    } catch (const std::exception& ex) {
        auto logger = get_logger();
        logger->error("Redis bulk string command failed: {}", ex.what());
    }
    co_return false;
}

boost::asio::awaitable<bool> RedisDatabaseAsync::execute_lua_script_async(const std::string& script,
                                                                 const std::vector<std::string>& keys,
                                                                 const std::vector<std::string>& args,
                                                                 std::vector<std::string>& values) const {
    try {
        std::vector<std::string> redis_args;
        redis_args.reserve(2 + keys.size() + args.size());
        redis_args.push_back("EVAL");
        redis_args.push_back(script);
        redis_args.push_back(std::to_string(keys.size()));
        redis_args.insert(redis_args.end(), keys.begin(), keys.end());
        redis_args.insert(redis_args.end(), args.begin(), args.end());

        auto reply = co_await impl_async_->execute_async(redis_args);
        if (reply.type == RedisReply::Type::Array) {
            values = reply.array_value;
            co_return true;
        }
    } catch (const RedisParseException& ex) {
        auto logger = get_logger();
        logger->error("Redis Lua script parse error: {}", ex.what());
    } catch (const std::exception& ex) {
        auto logger = get_logger();
        logger->error("Redis Lua script failed: {}", ex.what());
    }
    values.clear();
    co_return false;
}

boost::asio::awaitable<bool> RedisDatabaseAsync::execute_set_command_async(const std::string& key,
                                                                           const std::string& value,
                                                                           int expire_seconds,
                                                                           bool only_if_not_exists) const {
    try {
        std::vector<std::string> args = {"SET", key, value};
        if (only_if_not_exists) {
            args.push_back("NX");
        }
        if (expire_seconds > 0) {
            args.push_back("EX");
            args.push_back(std::to_string(expire_seconds));
        }

        auto reply = co_await impl_async_->execute_async(args);
        if (reply.type == RedisReply::Type::SimpleString && reply.string_value == "OK") {
            co_return true;
        }
        co_return false;
    } catch (const RedisParseException& ex) {
        auto logger = get_logger();
        logger->error("Redis SET command parse error: {}", ex.what());
    } catch (const std::exception& ex) {
        auto logger = get_logger();
        logger->error("Redis SET command failed: {}", ex.what());
    }
    co_return false;
}

boost::asio::awaitable<bool> RedisDatabaseAsync::execute_mget_command_async(const std::vector<std::string>& keys, 
                                                                            std::vector<std::string>& values) const {
    try {
        auto args = std::vector<std::string>{"MGET"};
        args.insert(args.end(), keys.begin(), keys.end());
        auto reply = co_await impl_async_->execute_async(args);
        if (reply.type == RedisReply::Type::Array) {
            values = reply.array_value;
            co_return true;
        }
    } catch (const RedisParseException& ex) {
        auto logger = get_logger();
        logger->error("Redis command parse error: {}", ex.what());
    } catch (const std::exception& ex) {
        auto logger = get_logger();
        logger->error("Redis MGET command failed: {}", ex.what());
    }
    values.clear();
    co_return false;
}

} // namespace flow_pilot

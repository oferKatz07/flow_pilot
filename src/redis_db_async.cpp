
// redis_db_async.cpp - Redis communication implementation for FlowPilot

#include <boost/asio.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/read_until.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/write.hpp>
#include <nlohmann/json.hpp>
#include <sstream>
#include <chrono>
#include <ctime>

#include "flow_pilot_error_msgs.h"
#include "logger.h"
#include "config.h"
#include "db_interface.h"
#include "redis_db_async.h"

namespace flow_pilot {

using boost::asio::use_awaitable;
using json = nlohmann::json;

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

std::shared_ptr<IRedisDatabaseAsync> get_redis_database_async()
{
    return RedisDatabaseAsync::get_instance();
}

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
        Logger::get_logger()->error("Redis connection failed: {}", error_message);
        return false;
    }
    // TBD Authentication is deffered to a later phase.
    // The password is currently empty and we want to allow connecting without authentication first.
    // if (!impl_async_->authenticate(auth_password, error_message)) {
    //     Logger::get_logger()->error("Redis authentication failed: {}", error_message);
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
        Logger::get_logger()->error("Redis command parse error: {}", ex.what());
    } catch (const std::exception& ex) {
        Logger::get_logger()->error("Redis integer command failed: {}", ex.what());
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
                                                                     StatusCodes& rejection_reason)
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

        local request_key = KEYS[1]
        local active_workflows_key = KEYS[2]

        -- Configuration parameters
        local max_active_workflows = tonumber(ARGV[1])
        local max_requests = tonumber(ARGV[2])
        local window = tonumber(ARGV[3])
        local key_ttl = tonumber(ARGV[8])
        
        -- Request identity parameters
        local workflow_id = ARGV[4]
        local client_id = ARGV[5]

        -- Request status parameters
        local received_status = ARGV[6]
        local rejected_status = ARGV[7]

        local function reject(reason) 
            -- set request status to rejected
            redis.call('SET', request_key, rejected_status, 'XX', 'KEEPTTL') 
            return {0, reason} 
        end

        -- Set request key to track it and prevent duplicates
        local request_set = redis.call('SET', request_key, received_status, 'NX', 'EX', key_ttl)
        if not request_set then
            return {0, 'duplicate'}
        end

        -- Use Redis server time for rate buckets
        local redis_time = redis.call('TIME')
        local server_epoch = tonumber(redis_time[1])

        -- Update rate limit bucket using Redis server time
        local rate_key_server_time = 'fp:rate:' .. client_id .. ':' .. server_epoch
        local current_rate = redis.call('INCR', rate_key_server_time)
        if current_rate == 1 then
            -- expire slightly after the window to ensure correct accounting in case of redis clock skew
            redis.call('EXPIRE', rate_key_server_time, window + 1)
        end

        -- Compute total across the window
        local total = current_rate
        for i = 1, window - 1 do
            local historical_key = 'fp:rate:' .. client_id .. ':' .. (server_epoch - i)
            local value = tonumber(redis.call('GET', historical_key) or '0')
            total = total + value
        end

        if total > max_requests then
            -- rollback rate increment
            redis.call('DECR', rate_key_server_time)
            return reject('rate_limit')
        end

        -- Add workflow_id to active workflows set only after checks
        local added = redis.call('SADD', active_workflows_key, workflow_id)
        if added == 1 then
            -- Active count check after successfully adding a new workflow ID
            local final_active_count = redis.call('SCARD', active_workflows_key)
            if final_active_count > max_active_workflows then
                -- Max concurrent workflows exceeded after adding the new workflow id
                -- Remove the newly added workflow and rollback rate
                redis.call('SREM', active_workflows_key, workflow_id)
                redis.call('DECR', rate_key_server_time)
                return reject('active_limit')
            end
        else
            redis.call('DECR', rate_key_server_time)
            return reject('duplicate_workflow')
        end

        return {1, 'ok'}
    )lua";

    std::vector<std::string> script_args;
    // Add the configured client policy rate limit parameters
    script_args.push_back(std::to_string(max_active_workflows));
    script_args.push_back(std::to_string(max_requests));
    script_args.push_back(std::to_string(window_seconds));
    // Add request identity parameters
    script_args.push_back(workflow_id);
    script_args.push_back(client_id);
    // Add request status parametrs
    script_args.push_back(std::string(to_string(RequestStatus::RECEIVED)));
    script_args.push_back(std::string(to_string(RequestStatus::REJECTED)));
    // Add configured Redis key TTL 
    script_args.push_back(std::to_string(Config::get().redis().key_retention_ttl));

    std::vector<std::string> lua_values;
    auto lua_ok = co_await execute_lua_script_async(lua_script, keys, script_args, lua_values);
    if (!lua_ok || lua_values.size() < 2 || lua_values[0].empty()) {
        rejection_reason = StatusCodes::INTERNAL_DB_FAILURE;
        co_return false;
    }

    std::string status = lua_values[0];
    std::string detail = lua_values[1];
    if (status == "1") {
        rejection_reason = StatusCodes::REQUEST_ADMITTED;
        co_return true;
    }

    if (detail == "duplicate") {
        rejection_reason = StatusCodes::DUPLICATE_REQUEST;
    } else if (detail == "rate_limit") {
        rejection_reason = StatusCodes::RATE_LIMIT_EXCEEDED;
    } else if (detail == "active_limit") {
        rejection_reason = StatusCodes::CONCURRENT_WORKFLOW_LIMIT_EXCEEDED;
    } else if (detail == "duplicate_workflow") {
        rejection_reason = StatusCodes::WORKFLOW_ID_EXISTS;
    } else {
        Logger::get_logger()->error("Unexpected Lua script failure: {}", detail);
        rejection_reason = StatusCodes::INTERNAL_DB_FAILURE;
    }
    
    Logger::get_logger()->error("Request was not admitted due to the following reason: {}", status_code_to_string(rejection_reason));
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

boost::asio::awaitable<bool> RedisDatabaseAsync::create_workflow_runtime_data_async(const workflow_runtime_info& workflow_info) {
    if (!co_await set_workflow_runtime_async(workflow_info.identity, workflow_info.workflow)) {
        co_return false;
    }

    for (const auto& job_data : workflow_info.jobs) {
        if (!co_await set_job_runtime_async(workflow_info.identity, job_data)) {
            co_await delete_all_workflow_jobs_async(workflow_info.identity, workflow_info.jobs);
            co_await delete_workflow_runtime_async(workflow_info.identity);

            co_return false;
        }
    }

    co_return true;
}

boost::asio::awaitable<bool> RedisDatabaseAsync::delete_workflow_runtime_data_async(const workflow_runtime_info& workflow_info) {
    bool ret_val = true;
    if (!co_await delete_all_workflow_jobs_async(workflow_info.identity, workflow_info.jobs)) {
        ret_val = false;
    }

    if (!co_await delete_workflow_runtime_async(workflow_info.identity)) {
        ret_val = false;
    }

    co_return ret_val;
}


boost::asio::awaitable<bool> RedisDatabaseAsync::set_workflow_runtime_async(const workflow_identity& workflow_id, 
                                                                            const workflow_runtime_data& workflow_data) {
    const std::string workflow_key = make_workflow_key(workflow_id);
    std::string creation_time = std::to_string(std::time(nullptr));
    std::unordered_map<std::string, std::string> fields{
        {"pending_jobs", std::to_string(workflow_data.pending_jobs)},
        {"completed_jobs",std::to_string(0)},
        {"failed_jobs", std::to_string(0)},
        {"status", workflow_data.status},
        {"creation_time", creation_time},
        {"last_update_time", creation_time}
    };

    co_return co_await execute_hset_command_async(workflow_key, fields);
}

boost::asio::awaitable<bool> RedisDatabaseAsync::update_workflow_runtime_async(
     const workflow_identity& workflow_id,
    const std::unordered_map<std::string, std::string>& fields) {
    const auto workflow_key = make_workflow_key(workflow_id);
    std::vector<std::string> keys{workflow_key};
    std::vector<std::string> args;
    args.push_back(std::to_string(fields.size()));
    for (const auto& [field, value] : fields) {
        args.push_back(field);
        args.push_back(value);
    }

    std::string lua_script = R"lua(
        local workflow_key = KEYS[1]
        local field_count = tonumber(ARGV[1])
        local idx = 2
        for i = 1, field_count do
            local field_name = ARGV[idx]
            local field_value = ARGV[idx + 1]
            redis.call('HSET', workflow_key, field_name, field_value)
            idx = idx + 2
        end
        return {1}
    )lua";

    std::vector<std::string> lua_values;
    auto ok = co_await execute_lua_script_async(lua_script, keys, args, lua_values);
    co_return ok && !lua_values.empty() && lua_values[0] == "1";
}

boost::asio::awaitable<bool> RedisDatabaseAsync::fetch_workflow_runtime_async(
    const workflow_identity& workflow_id,
    std::unordered_map<std::string, std::string>& workflow_data) const {
    const auto workflow_key = make_workflow_key(workflow_id);
    co_return co_await execute_hgetall_command_async(workflow_key, workflow_data);
}

boost::asio::awaitable<bool> RedisDatabaseAsync::publish_workflow_ready_jobs_async(const workflow_identity& workflow_id,
                                                                                   const std::vector<std::string>& ready_jobs) {
    std::vector<std::string> ready_jobs_keys;

    for (const auto& job_id : ready_jobs) {
        const auto job_key = make_job_key(workflow_id, job_id);
        ready_jobs_keys.push_back(job_key);
    }
    co_return co_await execute_list_add_command_async("fp:ready_jobs", ready_jobs_keys);
}

boost::asio::awaitable<bool> RedisDatabaseAsync::enqueue_ready_job_async(const workflow_identity& workflow_id, std::string& ready_job) {
    const auto job_key = make_job_key(workflow_id, ready_job);
    co_return co_await execute_rpush_command_async("fp:ready_jobs", ready_job);
}

boost::asio::awaitable<bool> RedisDatabaseAsync::dequeue_ready_job_async(std::string& ready_job) {
    if (!co_await execute_lpop_command_async("fp:ready_jobs", ready_job)) {
        co_return false;
    }

    size_t pos = ready_job.find_last_of(":");
    if (pos == std::string::npos) {
        Logger::get_logger()->error("dequeue_ready_job_async - Invalid ready_job key {} was dequeud", ready_job);
        // Don't expose erroneous values
        ready_job.clear();

        co_return false;
    }

    ready_job = ready_job.substr(pos+1);

    co_return true;
}

boost::asio::awaitable<void> RedisDatabaseAsync::clear_ready_job_async() {
    std::vector<std::string> args{"DEL", "fp:ready_jobs"};
    long long value = 0;
    co_await execute_integer_command_async(args, value);
}

boost::asio::awaitable<bool> RedisDatabaseAsync::set_job_runtime_async(const workflow_identity& workflow_id, const job_runtime_data& job_data) {
    const auto job_key = make_job_key(workflow_id, job_data.job_id);
    std::unordered_map<std::string, std::string> fields{
        {"status", job_data.status},
        {"remaining_dependencies", std::to_string(job_data.remaining_dependencies)},
        {"priority", std::to_string(job_data.priority)},
        {"timeout_sec", std::to_string(job_data.timeout_sec)},
        {"max_retries", std::to_string(job_data.max_retries)},
        {"current_retry_count", std::to_string(job_data.current_retry_count)},
        {"retry_delay_sec", std::to_string(job_data.retry_delay_sec)},
        {"retry_backoff_policy", job_data.retry_backoff_policy}
    };

    if (!co_await execute_hset_command_async(job_key, fields)) {
        co_return false;
    }

    bool retval = true;
    if (job_data.successors.size() > 0) {
        const auto successors = make_successors_key(workflow_id, job_data.job_id);
        retval = co_await execute_list_add_command_async(successors, job_data.successors);
    }
    co_return retval;
}

boost::asio::awaitable<bool> RedisDatabaseAsync::fetch_job_runtime_async(
    const workflow_identity& workflow_id,
    const std::string& job_id,
    std::unordered_map<std::string, std::string>& job_data) const {
    const auto job_key = make_job_key(workflow_id, job_id);
    co_return co_await execute_hgetall_command_async(job_key, job_data);
}

boost::asio::awaitable<bool> RedisDatabaseAsync::set_job_payload_async(
    const workflow_identity& workflow_id,
    const std::string& job_id,
    const std::vector<uint8_t>& payload) {
    const auto payload_key = make_payload_key(workflow_id, job_id);
    const std::string payload_value(payload.begin(), payload.end());
    co_return co_await execute_set_command_async(payload_key, payload_value, 0, false);
}

boost::asio::awaitable<bool> RedisDatabaseAsync::fetch_job_payload_async(
    const workflow_identity& workflow_id,
    const std::string& job_id,
    std::vector<uint8_t>& payload) const {
    const auto payload_key = make_payload_key(workflow_id, job_id);
    std::vector<std::string> args{"GET", payload_key};
    std::string bulk_string_payload;
    auto ok = co_await execute_bulk_string_command_async(args, bulk_string_payload);
    if (!ok) {
        payload.clear();
        co_return false;
    }

    payload.assign(bulk_string_payload.begin(), bulk_string_payload.end());
    co_return true;
}

boost::asio::awaitable<bool> RedisDatabaseAsync::delete_workflow_runtime_async(const workflow_identity& workflow_id) {
    const auto workflow_key = make_workflow_key(workflow_id);
    std::vector<std::string> args{"DEL", workflow_key};
    long long value = 0;
    auto ok = co_await execute_integer_command_async(args, value);
    co_return ok && value > 0;
}

boost::asio::awaitable<bool> RedisDatabaseAsync::delete_all_workflow_jobs_async(const workflow_identity& workflow_id, const workflow_jobs_list& jobs) {
    for (const auto& job_data : jobs) {
        const auto job_key = make_job_key(workflow_id, job_data.job_id);
        std::vector<std::string> args{"DEL", job_key};
        long long value = 0;
        auto ok = co_await execute_integer_command_async(args, value);
        if (!ok || value == 0) {
            Logger::get_logger()->info("Failed to delete job runtime data for job_id: {} in workflow_id: {}", job_data.job_id, workflow_id.workflow_id);
        }
        const auto successors_key = make_successors_key(workflow_id, job_data.job_id);
        args = {"DEL", successors_key};
        value = 0;
        ok = co_await execute_integer_command_async(args, value);
        if (!ok || value == 0) {
            Logger::get_logger()->info("Failed to delete job successors data for job_id: {} in workflow_id: {}", job_data.job_id, workflow_id.workflow_id);
        }
    }
    co_return true;
}

boost::asio::awaitable<bool> RedisDatabaseAsync::delete_job_payload_async(const workflow_identity& workflow_id, const std::string& job_id) {
    const auto payload_key = make_payload_key(workflow_id, job_id);
    std::vector<std::string> args{"DEL", payload_key};
    long long value = 0;
    auto ok = co_await execute_integer_command_async(args, value);
    if (!ok || value == 0) {
        Logger::get_logger()->info("Failed to delete job payload data for job_id: {} in workflow_id: {}", job_id, workflow_id.workflow_id);
    }

    co_return true;
}

boost::asio::awaitable<bool> RedisDatabaseAsync::delete_all_jobs_payload_async(const workflow_identity& workflow_id, const std::vector<std::string>& job_ids) {
    for (const std::string& job_id : job_ids) {
        const auto payload_key = make_payload_key(workflow_id, job_id);
        std::vector<std::string> args{"DEL", payload_key};
        long long value = 0;
        auto ok = co_await execute_integer_command_async(args, value);
        if (!ok || value == 0) {
            Logger::get_logger()->info("Failed to delete job payload data for job_id: {} in workflow_id: {}", job_id, workflow_id.workflow_id);
        }
    }

    co_return true;
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
        Logger::get_logger()->error("Redis command parse error: {}", ex.what());
    } catch (const std::exception& ex) {
        Logger::get_logger()->error("Redis bulk string command failed: {}", ex.what());
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
        Logger::get_logger()->error("Redis Lua script parse error: {}", ex.what());
    } catch (const std::exception& ex) {
        Logger::get_logger()->error("Redis Lua script failed: {}", ex.what());
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
        Logger::get_logger()->error("Redis SET command parse error: {}", ex.what());
    } catch (const std::exception& ex) {
        Logger::get_logger()->error("Redis SET command failed: {}", ex.what());
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
        Logger::get_logger()->error("Redis command parse error: {}", ex.what());
    } catch (const std::exception& ex) {
        Logger::get_logger()->error("Redis MGET command failed: {}", ex.what());
    }
    values.clear();
    co_return false;
}

boost::asio::awaitable<bool> RedisDatabaseAsync::execute_hset_command_async(
    const std::string& key,
    const std::unordered_map<std::string, std::string>& fields) const {
    if (fields.empty()) {
        co_return false;
    }

    try {
        std::vector<std::string> args;
        args.reserve(2 + fields.size() * 2);
        args.push_back("HSET");
        args.push_back(key);
        for (const auto& [field, value] : fields) {
            args.push_back(field);
            args.push_back(value);
        }

        auto reply = co_await impl_async_->execute_async(args);
        if (reply.type == RedisReply::Type::Integer || reply.type == RedisReply::Type::SimpleString) {
            co_return true;
        }
    } catch (const RedisParseException& ex) {
        Logger::get_logger()->error("Redis HSET command parse error: {}", ex.what());
    } catch (const std::exception& ex) {
        Logger::get_logger()->error("Redis HSET command failed: {}", ex.what());
    }
    co_return false;
}

boost::asio::awaitable<bool> RedisDatabaseAsync::execute_hgetall_command_async(
    const std::string& key,
    std::unordered_map<std::string, std::string>& fields) const {
    try {
        std::vector<std::string> args{"HGETALL", key};
        auto reply = co_await impl_async_->execute_async(args);
        if (reply.type == RedisReply::Type::Array) {
            fields.clear();
            for (size_t i = 0; i + 1 < reply.array_value.size(); i += 2) {
                fields[reply.array_value[i]] = reply.array_value[i + 1];
            }
            co_return true;
        }
    } catch (const RedisParseException& ex) {
        Logger::get_logger()->error("Redis HGETALL command parse error: {}", ex.what());
    } catch (const std::exception& ex) {
        Logger::get_logger()->error("Redis HGETALL command failed: {}", ex.what());
    }
    fields.clear();
    co_return false;
}

boost::asio::awaitable<bool> RedisDatabaseAsync::execute_smembers_command_async(
    const std::string& key,
    std::vector<std::string>& values) const {
    try {
        std::vector<std::string> args{"SMEMBERS", key};
        auto reply = co_await impl_async_->execute_async(args);
        if (reply.type == RedisReply::Type::Array) {
            values = reply.array_value;
            co_return true;
        }
    } catch (const RedisParseException& ex) {
        Logger::get_logger()->error("Redis SMEMBERS command parse error: {}", ex.what());
    } catch (const std::exception& ex) {
        Logger::get_logger()->error("Redis SMEMBERS command failed: {}", ex.what());
    }
    values.clear();
    co_return false;
}

boost::asio::awaitable<bool> RedisDatabaseAsync::execute_rpush_command_async(
    const std::string& key,
    const std::string& value) const {
    try {
        std::vector<std::string> args{"RPUSH", key, value};
        auto reply = co_await impl_async_->execute_async(args);
        if (reply.type == RedisReply::Type::Integer) {
            co_return true;
        }
    } catch (const RedisParseException& ex) {
        Logger::get_logger()->error("Redis RPUSH command parse error: {}", ex.what());
    } catch (const std::exception& ex) {
        Logger::get_logger()->error("Redis RPUSH command failed: {}", ex.what());
    }
    co_return false;
}

boost::asio::awaitable<bool> RedisDatabaseAsync::execute_lpop_command_async(
    const std::string& key,
    std::string& value) const {
    try {
        std::vector<std::string> args{"LPOP", key};
        auto reply = co_await impl_async_->execute_async(args);
        if (reply.type == RedisReply::Type::BulkString || reply.type == RedisReply::Type::SimpleString) {
            value = reply.string_value;
            co_return true;
        }
        if (reply.type == RedisReply::Type::Nil) {
            value.clear();
            co_return true;
        }
    } catch (const RedisParseException& ex) {
        Logger::get_logger()->error("Redis LPOP command parse error: {}", ex.what());
    } catch (const std::exception& ex) {
        Logger::get_logger()->error("Redis LPOP command failed: {}", ex.what());
    }
    value.clear();
    co_return false;
}

boost::asio::awaitable<bool> RedisDatabaseAsync::execute_list_add_command_async(
    const std::string& key,
    const std::vector<std::string>& values) const {
    try {
        if (values.empty()) {
            co_return true; // nothing to push
        }

        // Build RPUSH command with all values
        std::vector<std::string> args;
        args.reserve(2 + values.size());
        args.push_back("RPUSH");
        args.push_back(key);
        for (const auto& v : values) {
            args.push_back(v);
        }

        auto reply = co_await impl_async_->execute_async(args);
        if (reply.type == RedisReply::Type::Integer) {
            co_return true;
        }
    } catch (const RedisParseException& ex) {
        Logger::get_logger()->error("Redis RPUSH (list set) command parse error: {}", ex.what());
    } catch (const std::exception& ex) {
        Logger::get_logger()->error("Redis RPUSH (list set) command failed: {}", ex.what());
    }
    co_return false;
}

boost::asio::awaitable<bool> RedisDatabaseAsync::execute_list_get_command_async(
    const std::string& key,
    std::vector<std::string>& values) const {
    try {
        // Use LRANGE 0 -1 to get all list elements
        std::vector<std::string> args{"LRANGE", key, "0", "-1"};
        auto reply = co_await impl_async_->execute_async(args);
        if (reply.type == RedisReply::Type::Array) {
            values = reply.array_value;
            co_return true;
        }
        if (reply.type == RedisReply::Type::Nil) {
            values.clear();
            co_return true;
        }
    } catch (const RedisParseException& ex) {
        Logger::get_logger()->error("Redis LRANGE (list get) command parse error: {}", ex.what());
    } catch (const std::exception& ex) {
        Logger::get_logger()->error("Redis LRANGE (list get) command failed: {}", ex.what());
    }
    values.clear();
    co_return false;
}

std::string RedisDatabaseAsync::make_workflow_key(const workflow_identity& workflow_id) const {
    return "fp:workflow:" + workflow_id.client_id + ":" + workflow_id.workflow_id;
}

std::string RedisDatabaseAsync::make_job_key(const workflow_identity& workflow_id, const std::string& job_id) const {
    return "fp:job:" + workflow_id.client_id + ":" + workflow_id.workflow_id + ":" + job_id;
}

std::string RedisDatabaseAsync::make_successors_key(const workflow_identity& workflow_id, const std::string& job_id) const {
    return "fp:successors:" + workflow_id.client_id + ":" + workflow_id.workflow_id + ":" + job_id;
}

std::string RedisDatabaseAsync::make_payload_key(const workflow_identity& workflow_id, const std::string& job_id) const {
    return "fp:payload:" + workflow_id.client_id + ":" + workflow_id.workflow_id + ":" + job_id;
}

} // namespace flow_pilot


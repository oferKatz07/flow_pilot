// redis_db_async.cpp - Redis connection implementation for FlowPilot

#include <boost/asio.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/read_until.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/write.hpp>
#include <atomic>
#include <chrono>
#include <sstream>
#include <string_view>
#include <utility>
#include <vector>

#include "config.h"
#include "logger.h"
#include "redis_db_async.h"

namespace flow_pilot {

using boost::asio::use_awaitable;

namespace {

constexpr const char* EXECUTION_QUEUE_KEY = "fp:execution_queue";
constexpr const char* EXECUTION_QUEUE_READY_CHANNEL = "fp:execution_queue:ready";
constexpr const char* JOB_COMPLETION_STREAM_KEY = "fp:job_completion_stream";
constexpr const char* JOB_COMPLETION_READY_CHANNEL = "fp:job_completion_stream:ready";

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

    void close()
    {
        boost::system::error_code ec;
        socket_.close(ec);
    }

    boost::asio::awaitable<bool> subscribe_async(const std::string& channel)
    {
        std::vector<std::string> subscribe_args{"SUBSCRIBE", channel};
        co_await write_command_async(subscribe_args);

        auto subscribe_reply = co_await parse_reply_async();
        if (subscribe_reply.type != RedisReply::Type::Array ||
            subscribe_reply.array_value.size() < 3 ||
            subscribe_reply.array_value[0] != "subscribe" ||
            subscribe_reply.array_value[1] != channel) {
            co_return false;
        }

        co_return true;
    }

    boost::asio::awaitable<void> wait_for_subscribed_pubsub_message_async(const std::string& channel)
    {
        while (true) {
            auto message_reply = co_await parse_reply_async();
            if (message_reply.type == RedisReply::Type::Array &&
                message_reply.array_value.size() >= 3 &&
                message_reply.array_value[0] == "message" &&
                message_reply.array_value[1] == channel) {
                co_return;
            }
        }
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
                    co_await boost::asio::async_read(
                        socket_,
                        read_buffer_,
                        boost::asio::transfer_exactly(required_bytes - read_buffer_.size()),
                        use_awaitable);
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
    : ioc_(ioc),
      impl_async_(std::make_unique<ImplAsync>(ioc)),
      command_executor_(std::make_unique<RedisCommandExecutor>(
          [this](const std::vector<std::string>& args) -> boost::asio::awaitable<RedisReply> {
              co_return co_await impl_async_->execute_async(args);
          }))
{
    const InMemoryDBConfig& config = Config::get().redis();
    std::string connection_string = config.host + ":" + std::to_string(config.port);
    if (!connect(connection_string, config.password)) {
        throw std::runtime_error("Unable to connect to Redis at " + connection_string);
    }
}

RedisDatabaseAsync::~RedisDatabaseAsync() = default;

const RedisCommandExecutor& RedisDatabaseAsync::command_executor() const
{
    return *command_executor_;
}

boost::asio::io_context& RedisDatabaseAsync::io_context() noexcept
{
    return ioc_;
}

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

    connection_string_ = connection_string;
    host_ = host;
    port_ = port;
    password_ = auth_password;
    connected_ = true;
    return true;
}

bool RedisDatabaseAsync::register_scheduler(const std::string& scheduler_id)
{
    if (scheduler_id.empty()) {
        Logger::get_logger()->error("register_scheduler - Scheduler id cannot be empty");
        return false;
    }

    std::lock_guard<std::mutex> lock(scheduler_clients_mutex_);
    if (scheduler_blocking_clients_.find(scheduler_id) != scheduler_blocking_clients_.end()) {
        return true;
    }

    auto client = std::make_shared<ImplAsync>(ioc_);
    std::string error_message;
    if (!client->connect(host_, port_, error_message)) {
        Logger::get_logger()->error("register_scheduler - Scheduler {} failed to connect blocking client: {}",
                                    scheduler_id,
                                    error_message);
        return false;
    }

    scheduler_blocking_clients_[scheduler_id] = std::move(client);
    return true;
}

void RedisDatabaseAsync::deregister_scheduler(const std::string& scheduler_id)
{
    std::lock_guard<std::mutex> lock(scheduler_clients_mutex_);
    auto client_it = scheduler_blocking_clients_.find(scheduler_id);
    if (client_it != scheduler_blocking_clients_.end()) {
        client_it->second->close();
        scheduler_blocking_clients_.erase(client_it);
    }
}

boost::asio::awaitable<bool> RedisDatabaseAsync::blocking_dequeue_job_for_execution_async(
    WorkflowIdentity& workflow_id,
    std::string& ready_job,
    std::string scheduler_id) {
    // Each scheduler owns a dedicated Redis connection for blocking queue reads, so
    // the normal command executor is never tied up waiting for work to arrive.
    std::shared_ptr<ImplAsync> blocking_client;
    {
        std::lock_guard<std::mutex> lock(scheduler_clients_mutex_);
        auto client_it = scheduler_blocking_clients_.find(scheduler_id);
        if (client_it != scheduler_blocking_clients_.end()) {
            blocking_client = client_it->second;
        }
    }

    if (!blocking_client) {
        Logger::get_logger()->error("blocking_dequeue_job_for_execution_async - Scheduler {} is not registered",
                                    scheduler_id);
        co_return false;
    }

    RedisCommandExecutor blocking_executor(
        [blocking_client](const std::vector<std::string>& args) -> boost::asio::awaitable<RedisReply> {
            co_return co_await blocking_client->execute_async(args);
        });

    std::string ready_job_key;
    while (true) {
        // Block for up to one second waiting for the next prioritized job key.
        // An empty key is a timeout/no-work result, not a failure, so keep polling.
        if (!co_await blocking_executor.execute_zset_blocking_dequeue_command_async(EXECUTION_QUEUE_KEY, ready_job_key, 1)) {
            co_return false;
        }

        if (ready_job_key.empty()) {
            continue;
        }

        // The queue stores Redis job hash keys. If a dequeued key was deleted by
        // cancellation/cleanup before we inspect it, ignore it and wait again.
        std::string job_type;
        std::vector<std::string> type_args{"TYPE", ready_job_key};
        if (!co_await blocking_executor.execute_bulk_string_command_async(type_args, job_type)) {
            co_return false;
        }

        if (job_type == "none") {
            continue;
        }

        if (job_type != "hash") {
            Logger::get_logger()->error("blocking_dequeue_job_for_execution_async - Invalid ready_job key {} type {} was dequeued",
                                        ready_job_key,
                                        job_type);
            continue;
        }

        constexpr std::string_view job_key_prefix = "fp:job:";
        if (ready_job_key.rfind(job_key_prefix, 0) != 0) {
            Logger::get_logger()->error("blocking_dequeue_job_for_execution_async - Invalid ready_job key {} was dequeud",
                                        ready_job_key);
            continue;
        }

        size_t substr_start_pos = job_key_prefix.size();
        size_t str_size;
        size_t substr_end_pos = ready_job_key.find(":", substr_start_pos);
        if (substr_end_pos == std::string::npos) {
            Logger::get_logger()->error("blocking_dequeue_job_for_execution_async - Invalid ready_job key {} was dequeud",
                                        ready_job_key);
            continue;
        }

        str_size = substr_end_pos - substr_start_pos;
        std::string client_id = ready_job_key.substr(substr_start_pos, str_size);
        substr_start_pos = substr_end_pos + 1;

        substr_end_pos = ready_job_key.find(":", substr_start_pos);
        if (substr_end_pos == std::string::npos) {
            Logger::get_logger()->error("blocking_dequeue_job_for_execution_async - Invalid ready_job key {} was dequeud",
                                        ready_job_key);
            continue;
        }

        str_size = substr_end_pos - substr_start_pos;
        std::string workflow_id_value = ready_job_key.substr(substr_start_pos, str_size);
        substr_start_pos = substr_end_pos + 1;

        std::string job_id = ready_job_key.substr(substr_start_pos);
        if (client_id.empty() || workflow_id_value.empty() || job_id.empty()) {
            Logger::get_logger()->error("blocking_dequeue_job_for_execution_async - Invalid ready_job key {} was dequeud",
                                        ready_job_key);
            continue;
        }

        std::unordered_map<std::string, std::string> fields{{"owned_by", scheduler_id}};
        if (!co_await blocking_executor.execute_hset_command_async(ready_job_key, fields)) {
            co_return false;
        }

        workflow_id.client_id = std::move(client_id);
        workflow_id.workflow_id = std::move(workflow_id_value);
        ready_job = std::move(job_id);

        co_return true;
    }
}

boost::asio::awaitable<void> RedisDatabaseAsync::wait_for_ready_job_event_async(std::string scheduler_id) {
    ImplAsync subscriber(ioc_);
    std::string error_message;
    if (!subscriber.connect(host_, port_, error_message)) {
        Logger::get_logger()->error("wait_for_ready_job_event_async - Scheduler {} failed to connect subscriber: {}",
                                    scheduler_id,
                                    error_message);
        co_return;
    }

    try {
        if (!co_await subscriber.subscribe_async(EXECUTION_QUEUE_READY_CHANNEL)) {
            Logger::get_logger()->error("wait_for_ready_job_event_async - Scheduler {} received invalid subscribe reply",
                                        scheduler_id);
            co_return;
        }

        ImplAsync queue_checker(ioc_);
        if (!queue_checker.connect(host_, port_, error_message)) {
            Logger::get_logger()->error("wait_for_ready_job_event_async - Scheduler {} failed to connect queue checker: {}",
                                        scheduler_id,
                                        error_message);
            co_return;
        }
        RedisCommandExecutor queue_check_executor(
            [&queue_checker](const std::vector<std::string>& args) -> boost::asio::awaitable<RedisReply> {
                co_return co_await queue_checker.execute_async(args);
            });

        long long queue_size = 0;
        std::string queue_type;
        std::vector<std::string> type_args{"TYPE", EXECUTION_QUEUE_KEY};
        if (co_await queue_check_executor.execute_bulk_string_command_async(type_args, queue_type) &&
            queue_type == "zset") {
            std::vector<std::string> card_args{"ZCARD", EXECUTION_QUEUE_KEY};
            co_await queue_check_executor.execute_integer_command_async(card_args, queue_size);
        }

        if (queue_size > 0) {
            co_return;
        }

        co_await subscriber.wait_for_subscribed_pubsub_message_async(EXECUTION_QUEUE_READY_CHANNEL);
    } catch (const std::exception& ex) {
        Logger::get_logger()->error("wait_for_ready_job_event_async - Scheduler {} failed while waiting: {}",
                                    scheduler_id,
                                    ex.what());
    }
}

boost::asio::awaitable<JobCompletionWaitResult> RedisDatabaseAsync::wait_for_job_completion_event_async(
    std::chrono::milliseconds timeout)
{
    ImplAsync subscriber(ioc_);
    std::string error_message;
    if (!subscriber.connect(host_, port_, error_message)) {
        Logger::get_logger()->error("wait_for_job_completion_event_async - failed to connect subscriber: {}",
                                    error_message);
        co_return JobCompletionWaitResult::ERROR;
    }

    auto timed_out = std::make_shared<std::atomic<bool>>(false);
    std::shared_ptr<boost::asio::steady_timer> timer;

    try {
        if (!co_await subscriber.subscribe_async(JOB_COMPLETION_READY_CHANNEL)) {
            Logger::get_logger()->error("wait_for_job_completion_event_async - received invalid subscribe reply");
            co_return JobCompletionWaitResult::ERROR;
        }

        ImplAsync stream_checker(ioc_);
        if (!stream_checker.connect(host_, port_, error_message)) {
            Logger::get_logger()->error("wait_for_job_completion_event_async - failed to connect stream checker: {}",
                                        error_message);
            co_return JobCompletionWaitResult::ERROR;
        }
        RedisCommandExecutor stream_check_executor(
            [&stream_checker](const std::vector<std::string>& args) -> boost::asio::awaitable<RedisReply> {
                co_return co_await stream_checker.execute_async(args);
            });

        long long stream_size = 0;
        std::vector<std::string> len_args{"XLEN", JOB_COMPLETION_STREAM_KEY};
        if (!co_await stream_check_executor.execute_integer_command_async(len_args, stream_size)) {
            Logger::get_logger()->error("wait_for_job_completion_event_async - failed to check completion stream length");
            co_return JobCompletionWaitResult::ERROR;
        }

        if (stream_size > 0) {
            co_return JobCompletionWaitResult::EVENT;
        }

        timer = std::make_shared<boost::asio::steady_timer>(ioc_, timeout);
        timer->async_wait([timed_out, &subscriber](const boost::system::error_code& ec) {
            if (!ec) {
                timed_out->store(true, std::memory_order_release);
                subscriber.close();
            }
        });

        co_await subscriber.wait_for_subscribed_pubsub_message_async(JOB_COMPLETION_READY_CHANNEL);
        timer->cancel();
        co_return JobCompletionWaitResult::EVENT;
    } catch (const std::exception& ex) {
        if (timer) {
            timer->cancel();
        }

        if (timed_out->load(std::memory_order_acquire)) {
            co_return JobCompletionWaitResult::TIMEOUT;
        }

        Logger::get_logger()->error("wait_for_job_completion_event_async - failed while waiting: {}",
                                    ex.what());
        co_return JobCompletionWaitResult::ERROR;
    }
}

} // namespace flow_pilot

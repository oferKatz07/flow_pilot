// redis_db.h - Redis persistence interface for FlowPilot

#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>

namespace flow_pilot {

struct ServerConfig;
struct InMemoryDBConfig;

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
    std::vector<std::optional<std::string>> array_value;
};

class IRedisDatabase {
public:
    virtual ~IRedisDatabase() = default;

    /// Connect to a Redis instance using a connection string or URI.
    /// If a password is provided, it is used to authenticate after connection.
    virtual bool connect(const std::string& connection_string, const std::string& password = {}) = 0;

    /// Return true if the pair {client_id, request_id} already exists.
    virtual bool request_exists(const std::string& client_id, const std::string& request_id) const = 0;

    /// Reserve the request id in Redis for validation. Returns false when the request is a duplicate.
    virtual bool reserve_request_id(const std::string& client_id, const std::string& request_id) = 0;

    /// Release a reserved request id when validation fails and the request should not be admitted.
    virtual bool release_request_id(const std::string& client_id, const std::string& request_id) = 0;

    /// Validate active workflow and rate limits without mutating counters.
    virtual bool can_accept_request(const std::string& client_id,
                                    int max_active_workflows,
                                    int max_requests,
                                    int window_seconds,
                                    std::string& rejection_reason) const = 0;

    /// Admit the request by updating the active workflow counter and rate counter.
    virtual bool admit_request(const std::string& client_id,
                               const std::string& request_id,
                               const std::string& workflow_id,
                               int max_active_workflows,
                               int max_requests,
                               int window_seconds,
                               std::string& rejection_reason) = 0;

    /// Update the status for a workflow request in Redis.
    virtual bool update_request_status(const std::string& client_id, const std::string& request_id, const std::string& status) = 0;

    /// Fetch the status for a workflow request from Redis.
    virtual std::optional<std::string> fetch_request_status(const std::string& client_id, const std::string& request_id) const = 0;

    /// Remove a workflow from the active set when it completes.
    virtual bool remove_active_workflow(const std::string& client_id, const std::string& workflow_id) = 0;

    /// Async APIs using Boost.Asio coroutines.
    virtual boost::asio::awaitable<bool> connect_async(const std::string& connection_string, const std::string& password = {}) = 0;
    virtual boost::asio::awaitable<bool> request_exists_async(const std::string& client_id, const std::string& request_id) const = 0;
    virtual boost::asio::awaitable<bool> reserve_request_id_async(const std::string& client_id, const std::string& request_id) = 0;
    virtual boost::asio::awaitable<bool> release_request_id_async(const std::string& client_id, const std::string& request_id) = 0;
    virtual boost::asio::awaitable<bool> can_accept_request_async(const std::string& client_id,
                                                                  int max_active_workflows,
                                                                  int max_requests,
                                                                  int window_seconds,
                                                                  std::string& rejection_reason) const = 0;
    virtual boost::asio::awaitable<bool> admit_request_async(const std::string& client_id,
                                                             const std::string& request_id,
                                                             const std::string& workflow_id,
                                                             int max_active_workflows,
                                                             int max_requests,
                                                             int window_seconds,
                                                             std::string& rejection_reason) = 0;
    virtual boost::asio::awaitable<bool> update_request_status_async(const std::string& client_id, const std::string& request_id, const std::string& status) = 0;
    virtual boost::asio::awaitable<std::optional<std::string>> fetch_request_status_async(const std::string& client_id, const std::string& request_id) const = 0;
    virtual boost::asio::awaitable<bool> remove_active_workflow_async(const std::string& client_id, const std::string& workflow_id) = 0;
};

class RedisDatabase : public IRedisDatabase {
public:
    ~RedisDatabase() override;
    static std::shared_ptr<RedisDatabase> init(boost::asio::io_context& ioc, const InMemoryDBConfig& config);
    static std::shared_ptr<RedisDatabase> init(boost::asio::io_context& ioc);
    static std::shared_ptr<RedisDatabase> get_instance();

    // Delete copy/move
    RedisDatabase(const RedisDatabase&) = delete;
    RedisDatabase& operator=(const RedisDatabase&) = delete;
    RedisDatabase(RedisDatabase&&) = delete;
    RedisDatabase& operator=(RedisDatabase&&) = delete;

    bool connect(const std::string& connection_string, const std::string& password = {}) override;
    bool reconnect();
    boost::asio::awaitable<bool> connect_async(const std::string& connection_string, const std::string& password = {}) override;
    boost::asio::awaitable<bool> authenticate_async(const std::string& password, std::string& error_message);
    boost::asio::awaitable<bool> async_run(const std::vector<std::string>& args, RedisReply& reply);
    std::optional<RedisReply> execute_command(const std::vector<std::string>& args) const;
    boost::asio::awaitable<std::optional<RedisReply>> execute_command_async(const std::vector<std::string>& args) const;

    std::optional<std::string> get(const std::string& key) const;
    bool set(const std::string& key, const std::string& value, int expire_seconds = 0, bool only_if_not_exists = false) const;
    std::optional<long long> incr(const std::string& key);

    bool request_exists(const std::string& client_id, const std::string& request_id) const override;
    bool reserve_request_id(const std::string& client_id, const std::string& request_id) override;
    bool release_request_id(const std::string& client_id, const std::string& request_id) override;
    bool can_accept_request(const std::string& client_id,
                            int max_active_workflows,
                            int max_requests,
                            int window_seconds,
                            std::string& rejection_reason) const override;
    bool admit_request(const std::string& client_id,
                       const std::string& request_id,
                       const std::string& workflow_id,
                       int max_active_workflows,
                       int max_requests,
                       int window_seconds,
                       std::string& rejection_reason) override;
    bool update_request_status(const std::string& client_id, const std::string& request_id, const std::string& status) override;
    std::optional<std::string> fetch_request_status(const std::string& client_id, const std::string& request_id) const override;
    bool remove_active_workflow(const std::string& client_id, const std::string& workflow_id) override;

    boost::asio::awaitable<bool> request_exists_async(const std::string& client_id, const std::string& request_id) const override;
    boost::asio::awaitable<bool> reserve_request_id_async(const std::string& client_id, const std::string& request_id) override;
    boost::asio::awaitable<bool> release_request_id_async(const std::string& client_id, const std::string& request_id) override;
    boost::asio::awaitable<bool> can_accept_request_async(const std::string& client_id,
                                                          int max_active_workflows,
                                                          int max_requests,
                                                          int window_seconds,
                                                          std::string& rejection_reason) const override;
    boost::asio::awaitable<bool> admit_request_async(const std::string& client_id,
                                                     const std::string& request_id,
                                                     const std::string& workflow_id,
                                                     int max_active_workflows,
                                                     int max_requests,
                                                     int window_seconds,
                                                     std::string& rejection_reason) override;
    boost::asio::awaitable<bool> update_request_status_async(const std::string& client_id, const std::string& request_id, const std::string& status) override;
    boost::asio::awaitable<std::optional<std::string>> fetch_request_status_async(const std::string& client_id, const std::string& request_id) const override;
    boost::asio::awaitable<bool> remove_active_workflow_async(const std::string& client_id, const std::string& workflow_id) override;

private:
    explicit RedisDatabase(boost::asio::io_context& ioc);

    std::optional<long long> execute_integer_command(const std::vector<std::string>& args) const;
    std::optional<std::string> execute_bulk_string_command(const std::vector<std::string>& args) const;
    std::vector<std::optional<std::string>> execute_mget_command(const std::vector<std::string>& keys) const;
    std::optional<std::vector<std::optional<std::string>>> execute_lua_script(
        const std::string& script,
        const std::vector<std::string>& keys,
        const std::vector<std::string>& args) const;
    bool execute_set_command(const std::string& key,
                             const std::string& value,
                             int expire_seconds,
                             bool only_if_not_exists) const;

    boost::asio::awaitable<std::optional<long long>> execute_integer_command_async(const std::vector<std::string>& args) const;
    boost::asio::awaitable<std::optional<std::string>> execute_bulk_string_command_async(const std::vector<std::string>& args) const;
    boost::asio::awaitable<std::vector<std::optional<std::string>>> execute_mget_command_async(const std::vector<std::string>& keys) const;
    boost::asio::awaitable<std::optional<std::vector<std::optional<std::string>>>> execute_lua_script_async(
        const std::string& script,
        const std::vector<std::string>& keys,
        const std::vector<std::string>& args) const;
    boost::asio::awaitable<bool> execute_set_command_async(const std::string& key,
                                                           const std::string& value,
                                                           int expire_seconds,
                                                           bool only_if_not_exists) const;

    struct Impl;
    std::unique_ptr<Impl> impl_;
    std::string connection_string_;
    std::string host_;
    std::string port_;
    std::string password_;
    bool connected_ = false;

    static std::shared_ptr<RedisDatabase> instance_;
    static std::once_flag init_flag_;
};

void init_redis_database(boost::asio::io_context& ioc, const InMemoryDBConfig& config);
std::shared_ptr<IRedisDatabase> get_redis_database();

} // namespace flow_pilot

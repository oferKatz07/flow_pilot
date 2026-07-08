// redis_db.h - Redis persistence interface for FlowPilot

#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <vector>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>

namespace flow_pilot {

struct ServerConfig;
struct InMemoryDBConfig;

// enum class RequestStatus {
//     VALIDATING,
//     ACCEPTED,
//     COMPLETED
// };

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

class IRedisDatabase {
public:
    virtual ~IRedisDatabase() = default;

    virtual bool connect(const std::string& connection_string, const std::string& password = {}) = 0;
    virtual bool request_exists(const std::string& client_id, const std::string& request_id) const = 0;
    virtual bool reserve_request_id(const std::string& client_id, const std::string& request_id) = 0;
    virtual bool release_request_id(const std::string& client_id, const std::string& request_id) = 0;
    virtual bool can_accept_request(const std::string& client_id,
                                    int max_active_workflows,
                                    int max_requests,
                                    int window_seconds,
                                    std::string& rejection_reason) const = 0;
    virtual bool admit_request(const std::string& client_id,
                               const std::string& request_id,
                               const std::string& workflow_id,
                               int max_active_workflows,
                               int max_requests,
                               int window_seconds,
                               std::string& rejection_reason) = 0;
    virtual bool update_request_status(const std::string& client_id, const std::string& request_id, const std::string& status) = 0;
    virtual bool fetch_request_status(const std::string& client_id, const std::string& request_id, std::string& value) const = 0;
    virtual bool remove_active_workflow(const std::string& client_id, const std::string& workflow_id) = 0;
};

class IRedisDatabaseAsync {
public:
    virtual ~IRedisDatabaseAsync() = default;

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
    virtual boost::asio::awaitable<bool> fetch_request_status_async(const std::string& client_id, const std::string& request_id, std::string& value) const = 0;
    virtual boost::asio::awaitable<bool> remove_active_workflow_async(const std::string& client_id, const std::string& workflow_id) = 0;
};

class RedisBase {
public:
    virtual ~RedisBase();

    bool execute_command(const std::vector<std::string>& args, RedisReply& reply) const;
    boost::asio::awaitable<bool> execute_command_async(const std::vector<std::string>& args, RedisReply& reply) const;

protected:
    explicit RedisBase(boost::asio::io_context& ioc);

    bool connect(const std::string& connection_string, const std::string& password = {});
    bool reconnect();
    boost::asio::awaitable<bool> connect_async(const std::string& connection_string, const std::string& password = {});
    boost::asio::awaitable<bool> authenticate_async(const std::string& password, std::string& error_message);
    boost::asio::awaitable<bool> async_run(const std::vector<std::string>& args, RedisReply& reply);

    std::string get(const std::string& key) const;
    bool set(const std::string& key, const std::string& value, int expire_seconds = 0, bool only_if_not_exists = false) const;
    long long incr(const std::string& key);

    bool request_exists(const std::string& client_id, const std::string& request_id) const;
    bool reserve_request_id(const std::string& client_id, const std::string& request_id);
    bool release_request_id(const std::string& client_id, const std::string& request_id);
    bool can_accept_request(const std::string& client_id,
                            int max_active_workflows,
                            int max_requests,
                            int window_seconds,
                            std::string& rejection_reason) const;
    bool admit_request(const std::string& client_id,
                       const std::string& request_id,
                       const std::string& workflow_id,
                       int max_active_workflows,
                       int max_requests,
                       int window_seconds,
                       std::string& rejection_reason);
    bool update_request_status(const std::string& client_id, const std::string& request_id, const std::string& status);
    bool fetch_request_status(const std::string& client_id, const std::string& request_id, std::string& value) const;
    bool remove_active_workflow(const std::string& client_id, const std::string& workflow_id);

    boost::asio::awaitable<bool> request_exists_async(const std::string& client_id, const std::string& request_id) const;
    boost::asio::awaitable<bool> reserve_request_id_async(const std::string& client_id, const std::string& request_id);
    boost::asio::awaitable<bool> release_request_id_async(const std::string& client_id, const std::string& request_id);
    boost::asio::awaitable<bool> can_accept_request_async(const std::string& client_id,
                                                          int max_active_workflows,
                                                          int max_requests,
                                                          int window_seconds,
                                                          std::string& rejection_reason) const;
    boost::asio::awaitable<bool> admit_request_async(const std::string& client_id,
                                                     const std::string& request_id,
                                                     const std::string& workflow_id,
                                                     int max_active_workflows,
                                                     int max_requests,
                                                     int window_seconds,
                                                     std::string& rejection_reason);
    boost::asio::awaitable<bool> update_request_status_async(const std::string& client_id, const std::string& request_id, const std::string& status);
    boost::asio::awaitable<bool> fetch_request_status_async(const std::string& client_id, const std::string& request_id, std::string& value) const;
    boost::asio::awaitable<bool> remove_active_workflow_async(const std::string& client_id, const std::string& workflow_id);

    bool authenticate(const std::string& password, std::string& error_message);
    bool execute_integer_command(const std::vector<std::string>& args, long long& value) const;
    bool execute_bulk_string_command(const std::vector<std::string>& args, std::string& value) const;
    bool execute_mget_command(const std::vector<std::string>& keys, std::vector<std::string>& values) const;
    bool execute_lua_script(const std::string& script,
                            const std::vector<std::string>& keys,
                            const std::vector<std::string>& args,
                            std::vector<std::string>& values) const;
    bool execute_set_command(const std::string& key,
                             const std::string& value,
                             int expire_seconds,
                             bool only_if_not_exists) const;

    boost::asio::awaitable<bool> execute_integer_command_async(const std::vector<std::string>& args, long long& value) const;
    boost::asio::awaitable<bool> execute_bulk_string_command_async(const std::vector<std::string>& args, std::string& value) const;
    boost::asio::awaitable<bool> execute_mget_command_async(const std::vector<std::string>& keys, std::vector<std::string>& values) const;
    boost::asio::awaitable<bool> execute_lua_script_async(const std::string& script,
                                                         const std::vector<std::string>& keys,
                                                         const std::vector<std::string>& args,
                                                         std::vector<std::string>& values) const;
    boost::asio::awaitable<bool> execute_set_command_async(const std::string& key,
                                                           const std::string& value,
                                                           int expire_seconds,
                                                           bool only_if_not_exists) const;

protected:
    struct Impl;
    std::unique_ptr<Impl> impl_;
    std::string connection_string_;
    std::string host_;
    std::string port_;
    std::string password_;
    bool connected_ = false;
};

class RedisDatabase : public RedisBase, public IRedisDatabase {
public:
    ~RedisDatabase() override;
    static std::shared_ptr<RedisDatabase> init(boost::asio::io_context& ioc, const InMemoryDBConfig& config);
    static std::shared_ptr<RedisDatabase> init(boost::asio::io_context& ioc);
    static std::shared_ptr<RedisDatabase> get_instance();

    RedisDatabase(const RedisDatabase&) = delete;
    RedisDatabase& operator=(const RedisDatabase&) = delete;
    RedisDatabase(RedisDatabase&&) = delete;
    RedisDatabase& operator=(RedisDatabase&&) = delete;

    bool connect(const std::string& connection_string, const std::string& password = {}) override;
    bool reconnect();
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
    bool fetch_request_status(const std::string& client_id, const std::string& request_id, std::string& value) const override;
    bool remove_active_workflow(const std::string& client_id, const std::string& workflow_id) override;

    boost::asio::awaitable<bool> connect_async(const std::string& connection_string, const std::string& password = {});
    boost::asio::awaitable<bool> request_exists_async(const std::string& client_id, const std::string& request_id) const;
    boost::asio::awaitable<bool> reserve_request_id_async(const std::string& client_id, const std::string& request_id);
    boost::asio::awaitable<bool> release_request_id_async(const std::string& client_id, const std::string& request_id);
    boost::asio::awaitable<bool> can_accept_request_async(const std::string& client_id,
                                                          int max_active_workflows,
                                                          int max_requests,
                                                          int window_seconds,
                                                          std::string& rejection_reason) const;
    boost::asio::awaitable<bool> admit_request_async(const std::string& client_id,
                                                     const std::string& request_id,
                                                     const std::string& workflow_id,
                                                     int max_active_workflows,
                                                     int max_requests,
                                                     int window_seconds,
                                                     std::string& rejection_reason);
    boost::asio::awaitable<bool> update_request_status_async(const std::string& client_id, const std::string& request_id, const std::string& status);
    boost::asio::awaitable<bool> fetch_request_status_async(const std::string& client_id, const std::string& request_id, std::string& value) const;
    boost::asio::awaitable<bool> remove_active_workflow_async(const std::string& client_id, const std::string& workflow_id);

private:
    explicit RedisDatabase(boost::asio::io_context& ioc);

    static std::shared_ptr<RedisDatabase> instance_;
    static std::once_flag init_flag_;
};

class RedisDatabaseAsync : public RedisBase, public IRedisDatabaseAsync {
public:
    ~RedisDatabaseAsync() override;
    static std::shared_ptr<RedisDatabaseAsync> init(boost::asio::io_context& ioc, const InMemoryDBConfig& config);
    static std::shared_ptr<RedisDatabaseAsync> init(boost::asio::io_context& ioc);
    static std::shared_ptr<RedisDatabaseAsync> get_instance();

    RedisDatabaseAsync(const RedisDatabaseAsync&) = delete;
    RedisDatabaseAsync& operator=(const RedisDatabaseAsync&) = delete;
    RedisDatabaseAsync(RedisDatabaseAsync&&) = delete;
    RedisDatabaseAsync& operator=(RedisDatabaseAsync&&) = delete;

    boost::asio::awaitable<bool> connect_async(const std::string& connection_string, const std::string& password = {}) override;
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
    boost::asio::awaitable<bool> fetch_request_status_async(const std::string& client_id, const std::string& request_id, std::string& value) const override;
    boost::asio::awaitable<bool> remove_active_workflow_async(const std::string& client_id, const std::string& workflow_id) override;

private:
    explicit RedisDatabaseAsync(boost::asio::io_context& ioc);

    static std::shared_ptr<RedisDatabaseAsync> instance_;
    static std::once_flag init_flag_;
};

void init_redis_database(boost::asio::io_context& ioc, const InMemoryDBConfig& config);
std::shared_ptr<IRedisDatabase> get_redis_database();
std::shared_ptr<IRedisDatabaseAsync> get_redis_database_async();

} // namespace flow_pilot

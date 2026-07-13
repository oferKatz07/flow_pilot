
// redis_db.h - Redis persistence interface for FlowPilot

#pragma once

#include <boost/asio/awaitable.hpp>
#include <mutex>

#include "redis_base.h"

namespace flow_pilot {

// struct ServerConfig;
struct InMemoryDBConfig;

class IRedisDatabaseAsync {
public:
    virtual ~IRedisDatabaseAsync() = default;

    virtual bool connect(const std::string& connection_string, const std::string& password = {}) = 0;
    
    virtual boost::asio::awaitable<bool> request_exists_async(const std::string& client_id, 
                                                              const std::string& request_id) const = 0;
    virtual boost::asio::awaitable<bool> reserve_request_id_async(const std::string& client_id, 
                                                                  const std::string& request_id) = 0;
    virtual boost::asio::awaitable<bool> release_request_id_async(const std::string& client_id, 
                                                                  const std::string& request_id) = 0;
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
                                                             StatusCodes& rejection_reason) = 0;
    virtual boost::asio::awaitable<bool> update_request_status_async(const std::string& client_id, 
                                                                     const std::string& request_id, 
                                                                     const std::string& status) = 0;
    virtual boost::asio::awaitable<bool> fetch_request_status_async(const std::string& client_id, 
                                                                    const std::string& request_id, 
                                                                    std::string& value) const = 0;
    virtual boost::asio::awaitable<bool> remove_active_workflow_async(const std::string& client_id, 
                                                                      const std::string& workflow_id) = 0;
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

    bool connect(const std::string& connection_string, const std::string& password = {}) override;

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
                                                     StatusCodes& rejection_reason) override;
    boost::asio::awaitable<bool> update_request_status_async(const std::string& client_id, const std::string& request_id, const std::string& status) override;
    boost::asio::awaitable<bool> fetch_request_status_async(const std::string& client_id, const std::string& request_id, std::string& value) const override;
    boost::asio::awaitable<bool> remove_active_workflow_async(const std::string& client_id, const std::string& workflow_id) override;

private:
    explicit RedisDatabaseAsync(boost::asio::io_context& ioc);

    boost::asio::awaitable<bool> execute_lua_script_async(const std::string& script,
                                                          const std::vector<std::string>& keys,
                                                          const std::vector<std::string>& args,
                                                          std::vector<std::string>& values) const;
    boost::asio::awaitable<bool> execute_integer_command_async(const std::vector<std::string>& args,
                                                               long long& value) const;
    boost::asio::awaitable<bool> execute_mget_command_async(const std::vector<std::string>& keys, 
                                                            std::vector<std::string>& values) const;
    boost::asio::awaitable<bool> execute_bulk_string_command_async(const std::vector<std::string>& args, 
                                                                   std::string& value) const;
    boost::asio::awaitable<bool> execute_set_command_async(const std::string& key,
                                                           const std::string& value,
                                                           int expire_seconds,
                                                           bool only_if_not_exists) const;
    struct ImplAsync;
    std::unique_ptr<ImplAsync> impl_async_;

    static std::shared_ptr<RedisDatabaseAsync> instance_;
    static std::once_flag init_flag_;
};

std::shared_ptr<IRedisDatabaseAsync> get_redis_database_async();

} // namespace flow_pilot

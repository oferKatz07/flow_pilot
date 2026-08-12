
// redis_db.h - Redis persistence interface for FlowPilot

#pragma once

#include <boost/asio/awaitable.hpp>
#include <mutex>
#include <unordered_map>

#include "redis_base.h"

namespace flow_pilot {

struct workflow_identity {
    std::string client_id;
    std::string workflow_id;
};

struct workflow_runtime_data {
    std::string workflow_id;
    std::string status;
    int max_concurrent_jobs;
    int max_runtime_sec;
    int total_jobs;
    int pending_jobs;
    int completed_jobs;
    int failed_jobs;
};

struct job_runtime_data {
    std::string job_id;
    std::string status;
    int remaining_dependencies;
    int priority; // Currently not used
    int timeout_sec;
    int max_retries;
    int current_retry_count;
    int retry_delay_sec;
    std::string retry_backoff_policy; // Currently not used
    std::vector<std::string> successors;
};

using workflow_jobs_list = std::vector<job_runtime_data>;

struct workflow_runtime_info
{
    workflow_identity identity;
    workflow_runtime_data workflow;
    workflow_jobs_list jobs;
};

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
    virtual boost::asio::awaitable<bool> set_workflow_runtime_async(const workflow_identity& workflow_id, const workflow_runtime_data& workflow_data) = 0;
    virtual boost::asio::awaitable<bool> create_workflow_runtime_data_async(const workflow_runtime_info& workflow_info) = 0;
    virtual boost::asio::awaitable<bool> delete_workflow_runtime_data_async(const workflow_runtime_info& workflow_info) = 0;
    virtual boost::asio::awaitable<bool> update_workflow_runtime_async(
        const workflow_identity& workflow_id,
        const std::unordered_map<std::string, std::string>& fields) = 0;
    virtual boost::asio::awaitable<bool> fetch_workflow_runtime_async(
        const workflow_identity& workflow_id,
        std::unordered_map<std::string, std::string>& workflow_data) const = 0;
    virtual boost::asio::awaitable<bool> publish_workflow_ready_jobs_async(const workflow_identity& workflow_id,
                                                                           const std::vector<std::string>& ready_jobs) = 0;
    virtual boost::asio::awaitable<bool> dequeue_ready_job_async(std::string& ready_job) = 0;
    virtual boost::asio::awaitable<bool> set_job_runtime_async(const workflow_identity& workflow_id, const job_runtime_data& job_data) = 0;
    virtual boost::asio::awaitable<bool> fetch_job_runtime_async(
        const workflow_identity& workflow_id,
        const std::string& job_id,
        std::unordered_map<std::string, std::string>& job_data) const = 0;
    virtual boost::asio::awaitable<bool> set_job_payload_async(
        const workflow_identity& workflow_id,
        const std::string& job_id,
        const std::vector<uint8_t>& payload) = 0;
    virtual boost::asio::awaitable<bool> fetch_job_payload_async(
        const workflow_identity& workflow_id,
        const std::string& job_id,
        std::vector<uint8_t>& payload) const = 0;
    virtual boost::asio::awaitable<bool> delete_workflow_runtime_async(const workflow_identity& workflow_id) = 0;
    virtual boost::asio::awaitable<bool> delete_all_workflow_jobs_async(const workflow_identity& workflow_id, const workflow_jobs_list& jobs) = 0;
    virtual boost::asio::awaitable<bool> delete_job_payload_async(const workflow_identity& workflow_id, const std::string& job_id) = 0;
    virtual boost::asio::awaitable<bool> delete_all_jobs_payload_async(const workflow_identity& workflow_id, 
                                                                       const std::vector<std::string>& job_ids) = 0;
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
    // Workflow runtime state operations
    boost::asio::awaitable<bool> set_workflow_runtime_async(const workflow_identity& workflow_id, const workflow_runtime_data& workflow_data) override;
    boost::asio::awaitable<bool> create_workflow_runtime_data_async(const workflow_runtime_info& workflow_info) override;
    boost::asio::awaitable<bool> delete_workflow_runtime_data_async(const workflow_runtime_info& workflow_info) override;
    boost::asio::awaitable<bool> update_workflow_runtime_async(
        const workflow_identity& workflow_id,
        const std::unordered_map<std::string, std::string>& fields) override;
    boost::asio::awaitable<bool> fetch_workflow_runtime_async(
        const workflow_identity& workflow_id,
        std::unordered_map<std::string, std::string>& workflow_data) const override;
    boost::asio::awaitable<bool> publish_workflow_ready_jobs_async(const workflow_identity& workflow_id, 
                                                                   const std::vector<std::string>& ready_jobs) override;
    boost::asio::awaitable<bool> dequeue_ready_job_async(std::string& ready_job) override;

    boost::asio::awaitable<bool> set_job_runtime_async(const workflow_identity& workflow_id, const job_runtime_data& job_data) override;
    boost::asio::awaitable<bool> fetch_job_runtime_async(
        const workflow_identity& workflow_id,
        const std::string& job_id,
        std::unordered_map<std::string, std::string>& job_data) const override;

    boost::asio::awaitable<bool> set_job_payload_async(
        const workflow_identity& workflow_id,
        const std::string& job_id,
        const std::vector<uint8_t>& payload) override;
    boost::asio::awaitable<bool> fetch_job_payload_async(
        const workflow_identity& workflow_id,
        const std::string& job_id,
        std::vector<uint8_t>& payload) const override;
    boost::asio::awaitable<bool> delete_workflow_runtime_async(const workflow_identity& workflow_id) override;
    boost::asio::awaitable<bool> delete_all_workflow_jobs_async(const workflow_identity& workflow_id, const workflow_jobs_list& jobs) override;
    boost::asio::awaitable<bool> delete_job_payload_async(const workflow_identity& workflow_id, const std::string& job_id) override;
    boost::asio::awaitable<bool> delete_all_jobs_payload_async(const workflow_identity& workflow_id, const std::vector<std::string>& job_ids) override;
    
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
    boost::asio::awaitable<bool> execute_hset_command_async(const std::string& key,
                                                            const std::unordered_map<std::string, std::string>& fields) const;
    boost::asio::awaitable<bool> execute_hgetall_command_async(const std::string& key,
                                                               std::unordered_map<std::string, std::string>& fields) const;
    boost::asio::awaitable<bool> execute_smembers_command_async(const std::string& key,
                                                                std::vector<std::string>& values) const;
    boost::asio::awaitable<bool> execute_rpush_command_async(const std::string& key,
                                                              const std::string& value) const;
    boost::asio::awaitable<bool> execute_lpop_command_async(const std::string& key,
                                                             std::string& value) const;
    boost::asio::awaitable<bool> execute_list_add_command_async(const std::string& key,
                                                               const std::vector<std::string>& values) const;
    boost::asio::awaitable<bool> execute_list_get_command_async(const std::string& key,
                                                               std::vector<std::string>& values) const;
    std::string make_workflow_key(const workflow_identity& workflow_id) const;
    std::string make_job_key(const workflow_identity& workflow_id, const std::string& job_id) const;
    std::string make_successors_key(const workflow_identity& workflow_id, const std::string& job_id) const;
    std::string make_payload_key(const workflow_identity& workflow_id, const std::string& job_id) const;
    
    struct ImplAsync;
    std::unique_ptr<ImplAsync> impl_async_;

    static std::shared_ptr<RedisDatabaseAsync> instance_;
    static std::once_flag init_flag_;
};

std::shared_ptr<IRedisDatabaseAsync> get_redis_database_async();

} // namespace flow_pilot

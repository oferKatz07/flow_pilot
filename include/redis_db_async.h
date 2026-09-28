
// redis_db_async.h - Redis persistence interface for FlowPilot

#pragma once

#include <memory>
#include <mutex>
#include <set>
#include <unordered_map>
#include <chrono>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/uuid/uuid.hpp>

#include "redis_base.h"
#include "redis_command_executor.h"
#include "redis_keys.h"
#include "database_models.h"
#include "flow_pilot_error_msgs.h"

namespace flow_pilot {

enum class StartJobResult {
    FIRST_TO_START,
    STARTED,
    CANCELED_BY_WORKFLOW_STATE,
    ALREADY_CANCELED,
    INVARIANT_VIOLATION_JOB_STATUS,
    INVARIANT_VIOLATION_WORKFLOW_STATUS,
    INVARIANT_VIOLATION_WORKFLOW_TIME,
    INTERNAL_ERROR
};

struct PrioritizedJob {
    std::string job_id;
    boost::uuids::uuid job_uuid;
    int priority;
};

struct JobPriorityComparator {
    bool operator()(const PrioritizedJob lhs,
                    const PrioritizedJob rhs) const {
        if (lhs.priority != rhs.priority) {
            return lhs.priority > rhs.priority;
        }

        return lhs.job_uuid < rhs.job_uuid;
    }
};

using JobPriorityQueue = std::set<PrioritizedJob, JobPriorityComparator>;

struct WorkflowIdentity {
    std::string client_id;
    std::string workflow_id;
};

struct WorkflowRuntimeData {
    std::string workflow_id;
    std::string status;
    int max_concurrent_jobs;
    int reserved_execution_slots;
    int max_runtime_sec;
    int total_jobs;
    int pending_jobs;
    int completed_jobs;
    int failed_jobs;
};

struct JobRuntimeData {
    boost::uuids::uuid job_uuid;
    std::string job_id;
    std::string job_name;
    std::string status;
    int remaining_dependencies;
    int priority; // Currently not used
    int timeout_sec;
    int max_retries;
    int current_retry_count;
    int retry_delay_sec;
    int payload_size_bytes;
    std::string retry_backoff_policy; // Currently not used
    std::vector<std::string> successors;
};

using JobRuntimeMap = std::unordered_map<std::string, JobRuntimeData>;
using WorkflowJobsList = std::vector<JobRuntimeData>;
using PrioritizedJobsList = std::vector<PrioritizedJob>;

struct WorkflowRuntimeInfo
{
    WorkflowIdentity identity;
    WorkflowRuntimeData workflow;
    WorkflowJobsList jobs;
    PrioritizedJobsList ready_job_list;
    PrioritizedJobsList jobs_queued_for_execution;
};

struct JobCompletionData {
    std::string stream_id;
    WorkflowIdentity identity;
    std::string job_id;
    JobStatus status = JobStatus::UNKNOWN;
    StatusCodes error_code = StatusCodes::OK;
};

enum class JobCompletionWaitResult {
    EVENT,
    TIMEOUT,
    ERROR
};

struct InMemoryDBConfig;
struct ImplAsync;

class RedisConnectionContext {
public:
    explicit RedisConnectionContext(boost::asio::io_context& ioc);
    ~RedisConnectionContext();

    RedisConnectionContext(const RedisConnectionContext&) = delete;
    RedisConnectionContext& operator=(const RedisConnectionContext&) = delete;
    RedisConnectionContext(RedisConnectionContext&&) = delete;
    RedisConnectionContext& operator=(RedisConnectionContext&&) = delete;

    RedisCommandExecutor& command_executor();
    const RedisCommandExecutor& command_executor() const;

    void close();

private:
    friend class RedisDatabaseAsync;

    std::unique_ptr<ImplAsync> connection_;
    std::unique_ptr<RedisCommandExecutor> command_executor_;
};

class IRedisDatabaseAsync {
public:
    virtual ~IRedisDatabaseAsync() = default;

    virtual bool connect(const std::string& connection_string, const std::string& password = {}) = 0;
    virtual bool register_scheduler(const std::string& scheduler_id) = 0;
    virtual void deregister_scheduler(const std::string& scheduler_id) = 0;
    
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
    virtual boost::asio::awaitable<bool> try_acquire_job_slot_async(const WorkflowIdentity& workflow_id, std::string& workflow_status) = 0;
    virtual boost::asio::awaitable<bool> set_workflow_runtime_async(const WorkflowIdentity& workflow_id, const WorkflowRuntimeData& workflow_data) = 0;
    virtual boost::asio::awaitable<bool> create_workflow_runtime_data_async(const WorkflowRuntimeInfo& workflow_info) = 0;
    virtual boost::asio::awaitable<bool> delete_workflow_runtime_data_async(const WorkflowRuntimeInfo& workflow_info) = 0;
    virtual boost::asio::awaitable<bool> update_workflow_runtime_async(
        const WorkflowIdentity& workflow_id,
        const std::unordered_map<std::string, std::string>& fields) = 0;
    virtual boost::asio::awaitable<bool> fetch_workflow_runtime_async(const WorkflowIdentity& workflow_id,
                                                                      std::unordered_map<std::string, std::string>& workflow_data) const = 0;
    virtual boost::asio::awaitable<bool> create_workflow_waiting_ready_jobs(const WorkflowIdentity& workflow_id, 
                                                                            const PrioritizedJobsList& ready_jobs) = 0;
    virtual boost::asio::awaitable<bool> delete_workflow_waiting_ready_jobs(const WorkflowIdentity& workflow_id) = 0;
    virtual boost::asio::awaitable<bool> release_execution_slot_and_promote_ready_job_async(
        const WorkflowIdentity& workflow_id,
        std::string& promoted_job_id) = 0;
    virtual boost::asio::awaitable<bool> queue_workflow_jobs_for_execution_async(const WorkflowIdentity& workflow_id,
                                                                                 const PrioritizedJobsList& queued_jobs) = 0;
    virtual boost::asio::awaitable<bool> remove_jobs_from_execution_queue_async(const WorkflowIdentity& workflow_id,
                                                                                const PrioritizedJobsList& queued_jobs) = 0;
    virtual boost::asio::awaitable<bool> enqueue_job_for_execution_async(const WorkflowIdentity& workflow_id, 
                                                                         const PrioritizedJob& ready_job) = 0;
    virtual boost::asio::awaitable<bool> blocking_dequeue_job_for_execution_async(WorkflowIdentity& workflow_id,
                                                                                  std::string& ready_job,
                                                                                  std::string scheduler_id) = 0;
    virtual boost::asio::awaitable<void> wait_for_ready_job_event_async(std::string scheduler_id) = 0;
    virtual boost::asio::awaitable<bool> get_execution_queue_size_async(long long& size) const = 0;
    virtual boost::asio::awaitable<void> clear_execution_queue_async() = 0;
    virtual boost::asio::awaitable<bool> set_job_runtime_async(const WorkflowIdentity& workflow_id, const JobRuntimeData& job_data) = 0;
    virtual boost::asio::awaitable<bool> update_job_runtime_async(
        const WorkflowIdentity& workflow_id,
        const std::string& job_id,
        const std::unordered_map<std::string, std::string>& fields) = 0;
    virtual boost::asio::awaitable<bool> fetch_job_runtime_async(const WorkflowIdentity& workflow_id,
                                                                 const std::string& job_id,
                                                                 JobRuntimeData& job_data) const = 0;
    virtual boost::asio::awaitable<bool> try_set_job_to_running_async(const WorkflowIdentity& workflow_id, 
                                                                      const std::string& job_id, 
                                                                      StartJobResult& result) = 0;
    virtual boost::asio::awaitable<bool> enqueue_job_completion_async(const JobCompletionData& message) = 0;
    virtual boost::asio::awaitable<bool> dequeue_job_completion_async(JobCompletionData& message) = 0;
    virtual boost::asio::awaitable<JobCompletionWaitResult> wait_for_job_completion_event_async(
        std::chrono::milliseconds timeout) = 0;
    virtual boost::asio::awaitable<bool> set_job_payload_async(const WorkflowIdentity& workflow_id,
                                                               const std::string& job_id,
                                                               const std::vector<uint8_t>& payload) = 0;
    virtual boost::asio::awaitable<bool> fetch_job_payload_async(const WorkflowIdentity& workflow_id,
                                                                 const std::string& job_id,
                                                                 std::vector<uint8_t>& payload) const = 0;
    virtual boost::asio::awaitable<bool> delete_workflow_runtime_async(const WorkflowIdentity& workflow_id) = 0;
    virtual boost::asio::awaitable<bool> delete_all_workflow_jobs_async(const WorkflowIdentity& workflow_id, 
                                                                        const WorkflowJobsList& jobs) = 0;
    virtual boost::asio::awaitable<bool> delete_job_payload_async(const WorkflowIdentity& workflow_id, const std::string& job_id) = 0;
    virtual boost::asio::awaitable<bool> delete_all_jobs_payload_async(const WorkflowIdentity& workflow_id, 
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
    bool register_scheduler(const std::string& scheduler_id) override;
    void deregister_scheduler(const std::string& scheduler_id) override;
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
    boost::asio::awaitable<bool> try_acquire_job_slot_async(const WorkflowIdentity& workflow_id, std::string& workflow_status) override;
    boost::asio::awaitable<bool> set_workflow_runtime_async(const WorkflowIdentity& workflow_id, const WorkflowRuntimeData& workflow_data) override;
    boost::asio::awaitable<bool> create_workflow_runtime_data_async(const WorkflowRuntimeInfo& workflow_info) override;
    boost::asio::awaitable<bool> delete_workflow_runtime_data_async(const WorkflowRuntimeInfo& workflow_info) override;
    boost::asio::awaitable<bool> update_workflow_runtime_async(const WorkflowIdentity& workflow_id,
                                                               const std::unordered_map<std::string, std::string>& fields) override;
    boost::asio::awaitable<bool> update_workflow_runtime_async(RedisConnectionContext& context,
                                                               const WorkflowIdentity& workflow_id,
                                                               const std::unordered_map<std::string, std::string>& fields);
    boost::asio::awaitable<bool> fetch_workflow_runtime_async(const WorkflowIdentity& workflow_id,
                                                              std::unordered_map<std::string, std::string>& workflow_data) const override;
    boost::asio::awaitable<bool> fetch_workflow_runtime_async(RedisConnectionContext& context,
                                                              const WorkflowIdentity& workflow_id,
                                                              std::unordered_map<std::string, std::string>& workflow_data) const;
    boost::asio::awaitable<bool> create_workflow_waiting_ready_jobs(const WorkflowIdentity& workflow_id, 
                                                                    const PrioritizedJobsList& ready_jobs) override;
    boost::asio::awaitable<bool> delete_workflow_waiting_ready_jobs(const WorkflowIdentity& workflow_id) override;
    boost::asio::awaitable<bool> delete_workflow_waiting_ready_jobs(RedisConnectionContext& context,
                                                                    const WorkflowIdentity& workflow_id);
    boost::asio::awaitable<bool> release_execution_slot_and_promote_ready_job_async(
        const WorkflowIdentity& workflow_id,
        std::string& promoted_job_id) override;
    boost::asio::awaitable<bool> release_execution_slot_and_promote_ready_job_async(
        RedisConnectionContext& context,
        const WorkflowIdentity& workflow_id,
        std::string& promoted_job_id);
    boost::asio::awaitable<bool> queue_workflow_jobs_for_execution_async(const WorkflowIdentity& workflow_id, 
                                                                         const PrioritizedJobsList& queued_jobs) override;
    boost::asio::awaitable<bool> remove_jobs_from_execution_queue_async(const WorkflowIdentity& workflow_id,
                                                                        const PrioritizedJobsList& queued_jobs) override;
    boost::asio::awaitable<bool> remove_jobs_from_execution_queue_async(RedisConnectionContext& context,
                                                                        const WorkflowIdentity& workflow_id,
                                                                        const PrioritizedJobsList& queued_jobs);
    boost::asio::awaitable<bool> enqueue_job_for_execution_async(const WorkflowIdentity& workflow_id, 
                                                                 const PrioritizedJob& ready_job) override;
    boost::asio::awaitable<bool> blocking_dequeue_job_for_execution_async(WorkflowIdentity& workflow_id,
                                                                          std::string& ready_job,
                                                                          std::string scheduler_id) override;
    boost::asio::awaitable<bool> blocking_dequeue_job_for_execution_async(RedisConnectionContext& context,
                                                                          WorkflowIdentity& workflow_id,
                                                                          std::string& ready_job,
                                                                          const std::string& scheduler_id);
    boost::asio::awaitable<void> wait_for_ready_job_event_async(std::string scheduler_id) override;
    boost::asio::awaitable<bool> get_execution_queue_size_async(long long& size) const override;
    boost::asio::awaitable<void> clear_execution_queue_async() override;
    boost::asio::awaitable<bool> set_job_runtime_async(const WorkflowIdentity& workflow_id, const JobRuntimeData& job_data) override;
    boost::asio::awaitable<bool> update_job_runtime_async(const WorkflowIdentity& workflow_id, 
                                                          const std::string& job_id,
                                                          const std::unordered_map<std::string, std::string>& fields) override;
    boost::asio::awaitable<bool> update_job_runtime_async(RedisConnectionContext& context,
                                                          const WorkflowIdentity& workflow_id,
                                                          const std::string& job_id,
                                                          const std::unordered_map<std::string, std::string>& fields);
    boost::asio::awaitable<bool> fetch_job_runtime_async(const WorkflowIdentity& workflow_id,
                                                         const std::string& job_id,
                                                         JobRuntimeData& job_data) const override;
    boost::asio::awaitable<bool> fetch_job_runtime_async(RedisConnectionContext& context,
                                                         const WorkflowIdentity& workflow_id,
                                                         const std::string& job_id,
                                                         JobRuntimeData& job_data) const;
    boost::asio::awaitable<bool> try_set_job_to_running_async(const WorkflowIdentity& workflow_id, 
                                                              const std::string& job_id, 
                                                              StartJobResult& result) override;
    boost::asio::awaitable<bool> try_set_job_to_running_async(RedisConnectionContext& context,
                                                              const WorkflowIdentity& workflow_id,
                                                              const std::string& job_id,
                                                              StartJobResult& result);
    boost::asio::awaitable<bool> enqueue_job_completion_async(const JobCompletionData& message) override;
    boost::asio::awaitable<bool> enqueue_job_completion_async(RedisConnectionContext& context,
                                                              const JobCompletionData& message);
    boost::asio::awaitable<bool> dequeue_job_completion_async(JobCompletionData& message) override;
    boost::asio::awaitable<bool> dequeue_job_completion_async(RedisConnectionContext& context,
                                                              JobCompletionData& message);
    boost::asio::awaitable<JobCompletionWaitResult> wait_for_job_completion_event_async(
        std::chrono::milliseconds timeout) override;
    boost::asio::awaitable<JobCompletionWaitResult> wait_for_job_completion_event_async(
        RedisConnectionContext& context,
        std::chrono::milliseconds timeout);
    boost::asio::awaitable<bool> set_job_payload_async(const WorkflowIdentity& workflow_id,
                                                       const std::string& job_id, 
                                                       const std::vector<uint8_t>& payload) override;
    boost::asio::awaitable<bool> fetch_job_payload_async(const WorkflowIdentity& workflow_id,
                                                         const std::string& job_id,
                                                         std::vector<uint8_t>& payload) const override;
    boost::asio::awaitable<bool> fetch_job_payload_async(RedisConnectionContext& context,
                                                         const WorkflowIdentity& workflow_id,
                                                         const std::string& job_id,
                                                         std::vector<uint8_t>& payload) const;
    boost::asio::awaitable<bool> delete_workflow_runtime_async(const WorkflowIdentity& workflow_id) override;
    boost::asio::awaitable<bool> delete_all_workflow_jobs_async(const WorkflowIdentity& workflow_id, 
                                                                const WorkflowJobsList& jobs) override;
    boost::asio::awaitable<bool> delete_job_payload_async(const WorkflowIdentity& workflow_id, const std::string& job_id) override;
    boost::asio::awaitable<bool> delete_all_jobs_payload_async(const WorkflowIdentity& workflow_id, 
                                                               const std::vector<std::string>& job_ids) override;
    const RedisCommandExecutor& command_executor() const;
    boost::asio::io_context& io_context() noexcept;
    
private:
    explicit RedisDatabaseAsync(boost::asio::io_context& ioc);
    
    boost::asio::io_context& ioc_;
    std::unique_ptr<RedisConnectionContext> default_connection_context_;
    RedisCommandExecutor* command_executor_;
    std::chrono::milliseconds queue_read_timeout_;
    std::mutex scheduler_clients_mutex_;
    std::unordered_map<std::string, std::shared_ptr<ImplAsync>> scheduler_blocking_clients_;

    static std::shared_ptr<RedisDatabaseAsync> instance_;
    static std::once_flag init_flag_;
};

std::shared_ptr<IRedisDatabaseAsync> get_redis_database_async();

} // namespace flow_pilot

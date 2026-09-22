// redis_db_async_execution_queue.cpp - Redis execution queue operations for FlowPilot

#include <string>
#include <unordered_map>
#include <vector>

#include "redis_db_async.h"

namespace flow_pilot {
namespace {

constexpr const char* EXECUTION_QUEUE_KEY = "fp:execution_queue";

} // namespace

boost::asio::awaitable<bool> RedisDatabaseAsync::queue_workflow_jobs_for_execution_async(const WorkflowIdentity& workflow_id,
                                                                                         const PrioritizedJobsList& queued_jobs) {
    std::unordered_map<std::string, unsigned int> members;

    for (const auto& job_info : queued_jobs) {
        const auto job_key = RedisKeys::job_key(workflow_id, job_info.job_id);
        members[job_key] = job_info.priority;
    }

    if (members.empty()) {
        co_return true;
    }

    if (!co_await command_executor_->execute_zset_enqueue_command_async(EXECUTION_QUEUE_KEY, members)) {
        co_return false;
    }

    co_return true;
}

boost::asio::awaitable<bool> RedisDatabaseAsync::remove_jobs_from_execution_queue_async(const WorkflowIdentity& workflow_id,
                                                                                        const PrioritizedJobsList& queued_jobs) {
    std::vector<std::string> members;
    members.reserve(queued_jobs.size());

    for (const auto& job_info : queued_jobs) {
        members.push_back(RedisKeys::job_key(workflow_id, job_info.job_id));
    }
    co_return co_await command_executor_->execute_zset_remove_command_async(EXECUTION_QUEUE_KEY, std::move(members));
}

boost::asio::awaitable<bool> RedisDatabaseAsync::release_execution_slot_and_promote_ready_job_async(
    const WorkflowIdentity& workflow_id,
    std::string& promoted_job_id)
{
    promoted_job_id.clear();

    const auto workflow_key = RedisKeys::workflow_key(workflow_id);
    const auto waiting_ready_key = RedisKeys::workflow_waiting_jobs_key(workflow_id);
    const std::vector<std::string> keys{workflow_key, waiting_ready_key, EXECUTION_QUEUE_KEY};
    const std::vector<std::string> args{
        std::string(to_string(WorkflowStatus::RUNNING)),
        std::string(to_string(JobStatus::QUEUED))
    };

    const std::string lua_script = R"lua(
        local workflow_key = KEYS[1]
        local waiting_ready_key = KEYS[2]
        local execution_queue_key = KEYS[3]
        local workflow_running = ARGV[1]
        local job_queued = ARGV[2]

        local now = redis.call('TIME')[1]
        local reserved_slots = tonumber(redis.call('HGET', workflow_key, 'reserved_execution_slots') or '0')
        if reserved_slots > 0 then
            reserved_slots = reserved_slots - 1
        end

        redis.call('HSET',
                   workflow_key,
                   'reserved_execution_slots', reserved_slots,
                   'last_update_time', now)

        local workflow_status = redis.call('HGET', workflow_key, 'status')
        if workflow_status ~= workflow_running then
            return {1, ''}
        end

        local max_concurrent_jobs = tonumber(redis.call('HGET', workflow_key, 'max_concurrent_jobs') or '0')
        if reserved_slots >= max_concurrent_jobs then
            return {1, ''}
        end

        local popped = redis.call('ZPOPMAX', waiting_ready_key, 1)
        if #popped == 0 then
            return {1, ''}
        end

        local job_key = popped[1]
        local priority = popped[2]
        redis.call('HSET',
                   job_key,
                   'status', job_queued,
                   'last_update_time', now)
        redis.call('ZADD', execution_queue_key, priority, job_key)
        redis.call('HINCRBY', workflow_key, 'reserved_execution_slots', 1)

        return {1, job_key}
    )lua";

    std::vector<std::string> values;
    const auto ok = co_await command_executor_->execute_lua_script_async(lua_script, keys, args, values);
    if (!ok || values.empty() || values[0] != "1") {
        co_return false;
    }

    if (values.size() > 1 && !values[1].empty()) {
        const auto job_key_prefix = RedisKeys::job_key(workflow_id, "");
        if (values[1].rfind(job_key_prefix, 0) != 0) {
            co_return false;
        }

        promoted_job_id = values[1].substr(job_key_prefix.size());
    }

    co_return true;
}

boost::asio::awaitable<bool> RedisDatabaseAsync::enqueue_job_for_execution_async(const WorkflowIdentity& workflow_id, 
                                                                                 const PrioritizedJob& ready_job) {
    std::unordered_map<std::string, unsigned int> members;
    const auto job_key = RedisKeys::job_key(workflow_id, ready_job.job_id);

    members[job_key] = ready_job.priority;

    if (!co_await command_executor_->execute_zset_enqueue_command_async(EXECUTION_QUEUE_KEY, members)) {
        co_return false;
    }

    co_return true;
}

boost::asio::awaitable<void> RedisDatabaseAsync::clear_execution_queue_async() {
    std::vector<std::string> args{"DEL", EXECUTION_QUEUE_KEY};
    long long value = 0;
    co_await command_executor_->execute_integer_command_async(args, value);
}

boost::asio::awaitable<bool> RedisDatabaseAsync::get_execution_queue_size_async(long long& size) const {
    std::string queue_type;
    std::vector<std::string> type_args{"TYPE", EXECUTION_QUEUE_KEY};
    if (!co_await command_executor_->execute_bulk_string_command_async(type_args, queue_type)) {
        size = 0;
        co_return false;
    }

    if (queue_type == "none") {
        size = 0;
        co_return true;
    }

    if (queue_type != "zset") {
        size = 0;
        co_return false;
    }

    std::vector<std::string> args{"ZCARD", EXECUTION_QUEUE_KEY};
    if (!co_await command_executor_->execute_integer_command_async(args, size)) {
        size = 0;
        co_return false;
    }

    co_return true;
}

} // namespace flow_pilot

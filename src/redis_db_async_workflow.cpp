// redis_db_async_workflow.cpp - Redis workflow runtime operations for FlowPilot

#include <algorithm>
#include <ctime>
#include <string>
#include <unordered_map>
#include <vector>

#include "redis_db_async.h"

namespace flow_pilot {

boost::asio::awaitable<bool> RedisDatabaseAsync::try_acquire_job_slot_async(const WorkflowIdentity& workflow_id, std::string& workflow_status)
{
    const auto workflow_key = RedisKeys::workflow_key(workflow_id);
    const std::vector<std::string> keys{workflow_key};
    const std::string lua_script = R"lua(
        local workflow_key = KEYS[1]
        local status = redis.call('HGET', workflow_key, 'status') or 'UNKNOWN'
        if status == 'READY' or status == 'RUNNING' then
            local running_jobs = tonumber(redis.call('HGET', workflow_key, 'reserved_execution_slots') or '0')
            local max_concurrent_jobs = tonumber(redis.call('HGET', workflow_key, 'max_concurrent_jobs'))

            if running_jobs < max_concurrent_jobs then
                redis.call('HINCRBY', workflow_key, 'reserved_execution_slots', 1)

                return {1, status}
            end

            return {0, status}
        end
        -- If the workflow status is not READY or RUNNING return false
        return {0, status}
    )lua";

    std::vector<std::string> values;
    const auto ok = co_await command_executor_->execute_lua_script_async(lua_script, keys, {}, values);
    bool ret_val = ok && values.size() == 2 && values[0] == "1";
    if (values.size() == 2) {
        workflow_status = values[1];
    } else {
        workflow_status = to_string(WorkflowStatus::UNKNOWN);
    }

    co_return ret_val;
}

boost::asio::awaitable<bool> RedisDatabaseAsync::create_workflow_runtime_data_async(const WorkflowRuntimeInfo& workflow_info) {
    if (!co_await set_workflow_runtime_async(workflow_info.identity, workflow_info.workflow)) {
        co_await delete_workflow_runtime_async(workflow_info.identity);
        co_return false;
    }

    for (const auto& job_data : workflow_info.jobs) {
        if (!co_await set_job_runtime_async(workflow_info.identity, job_data)) {
            co_await delete_all_workflow_jobs_async(workflow_info.identity, workflow_info.jobs);
            co_await delete_workflow_runtime_async(workflow_info.identity);

            co_return false;
        }
    }

    if (workflow_info.ready_job_list.size() > 0) {
        if (!co_await create_workflow_waiting_ready_jobs(workflow_info.identity, workflow_info.ready_job_list)) {
            co_await delete_all_workflow_jobs_async(workflow_info.identity, workflow_info.jobs);
            co_await delete_workflow_runtime_async(workflow_info.identity);
            co_return false;
        }
    }

    co_return true;
}

boost::asio::awaitable<bool> RedisDatabaseAsync::delete_workflow_runtime_data_async(const WorkflowRuntimeInfo& workflow_info) {
    bool ret_val = true;
    if (!co_await delete_all_workflow_jobs_async(workflow_info.identity, workflow_info.jobs)) {
        ret_val = false;
    }

    if (!co_await delete_workflow_runtime_async(workflow_info.identity)) {
        ret_val = false;
    }

    if (! co_await delete_workflow_waiting_ready_jobs(workflow_info.identity)) {
        ret_val = false;
    }

    co_return ret_val;
}

boost::asio::awaitable<bool> RedisDatabaseAsync::set_workflow_runtime_async(const WorkflowIdentity& workflow_id, 
                                                                            const WorkflowRuntimeData& workflow_data) {
    const std::string workflow_key = RedisKeys::workflow_key(workflow_id);
    std::string creation_time = std::to_string(std::time(nullptr));
    std::unordered_map<std::string, std::string> fields{
        {"status", workflow_data.status},
        {"total_jobs", std::to_string(workflow_data.total_jobs)},
        {"max_concurrent_jobs", std::to_string(workflow_data.max_concurrent_jobs)},
        {"reserved_execution_slots", std::to_string(workflow_data.reserved_execution_slots)},
        {"pending_jobs", std::to_string(workflow_data.pending_jobs)},
        {"completed_jobs",std::to_string(0)},
        {"failed_jobs", std::to_string(0)},
        {"max_run_time", std::to_string(workflow_data.max_runtime_sec)},
        {"creation_time", creation_time},
        {"start_run_time", ""},
        {"last_update_time", creation_time}
    };

    co_return co_await command_executor_->execute_hset_command_async(workflow_key, fields);
}

boost::asio::awaitable<bool> RedisDatabaseAsync::update_workflow_runtime_async(
     const WorkflowIdentity& workflow_id,
    const std::unordered_map<std::string, std::string>& fields) {
    co_return co_await update_workflow_runtime_async(*default_connection_context_, workflow_id, fields);
}

boost::asio::awaitable<bool> RedisDatabaseAsync::update_workflow_runtime_async(
    RedisConnectionContext& context,
    const WorkflowIdentity& workflow_id,
    const std::unordered_map<std::string, std::string>& fields) {
    const auto workflow_key = RedisKeys::workflow_key(workflow_id);
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
    auto ok = co_await context.command_executor().execute_lua_script_async(lua_script, keys, args, lua_values);
    co_return ok && !lua_values.empty() && lua_values[0] == "1";
}

boost::asio::awaitable<bool> RedisDatabaseAsync::increment_completed_jobs_and_complete_workflow_if_ready_async(
    const WorkflowIdentity& workflow_id,
    const std::string& last_update_time,
    bool& workflow_completed)
{
    co_return co_await increment_completed_jobs_and_complete_workflow_if_ready_async(
        *default_connection_context_,
        workflow_id,
        last_update_time,
        workflow_completed);
}

boost::asio::awaitable<bool> RedisDatabaseAsync::increment_completed_jobs_and_complete_workflow_if_ready_async(
    RedisConnectionContext& context,
    const WorkflowIdentity& workflow_id,
    const std::string& last_update_time,
    bool& workflow_completed)
{
    workflow_completed = false;

    const auto workflow_key = RedisKeys::workflow_key(workflow_id);
    const std::vector<std::string> keys{workflow_key};
    const std::vector<std::string> args{
        last_update_time,
        std::string(to_string(WorkflowStatus::RUNNING)),
        std::string(to_string(WorkflowStatus::COMPLETED)),
        std::string(to_string(WorkflowStatus::FAILED))
    };

    const std::string lua_script = R"lua(
        local workflow_key = KEYS[1]
        local last_update_time = ARGV[1]
        local running_status = ARGV[2]
        local completed_status = ARGV[3]
        local failed_status = ARGV[4]

        local completed_jobs = redis.call('HINCRBY', workflow_key, 'completed_jobs', 1)
        redis.call('HSET', workflow_key, 'last_update_time', last_update_time)

        local status = redis.call('HGET', workflow_key, 'status') or ''
        if status ~= running_status then
            return {0}
        end

        local now = tonumber(redis.call('TIME')[1])
        local start_run_time = tonumber(redis.call('HGET', workflow_key, 'start_run_time') or '') or 0
        local max_run_time = tonumber(redis.call('HGET', workflow_key, 'max_run_time') or '') or 0
        if start_run_time > 0 and max_run_time > 0 and now - start_run_time > max_run_time then
            redis.call('HSET', workflow_key,
                       'status', failed_status,
                       'last_update_time', tostring(now))
            return {-1}
        end

        local total_jobs = tonumber(redis.call('HGET', workflow_key, 'total_jobs') or '') or 0
        if total_jobs > 0 and completed_jobs == total_jobs then
            redis.call('HSET', workflow_key, 'status', completed_status)
            return {1}
        end

        return {0}
    )lua";

    std::vector<std::string> lua_values;
    const auto ok = co_await context.command_executor().execute_lua_script_async(lua_script, keys, args, lua_values);
    if (!ok || lua_values.empty() || lua_values[0] == "-1") {
        co_return false;
    }

    workflow_completed = lua_values[0] == "1";
    co_return true;
}

boost::asio::awaitable<bool> RedisDatabaseAsync::complete_successful_job_runtime_async(
    RedisConnectionContext& context,
    const WorkflowIdentity& workflow_id,
    const std::string& completed_job_id,
    const std::string& last_update_time,
    std::vector<std::string>& ready_job_ids,
    std::string& promoted_job_id,
    bool& workflow_completed)
{
    ready_job_ids.clear();
    promoted_job_id.clear();
    workflow_completed = false;

    const auto workflow_key = RedisKeys::workflow_key(workflow_id);
    const auto waiting_ready_key = RedisKeys::workflow_waiting_jobs_key(workflow_id);
    const std::string execution_queue_key = "fp:execution_queue";
    const auto completed_job_key = RedisKeys::job_key(workflow_id, completed_job_id);
    const auto successors_key = RedisKeys::successors_key(workflow_id, completed_job_id);
    const auto job_key_prefix = RedisKeys::job_key(workflow_id, "");
    const std::vector<std::string> keys{
        workflow_key,
        waiting_ready_key,
        execution_queue_key,
        completed_job_key,
        successors_key
    };
    const std::vector<std::string> args{
        last_update_time,
        std::string(to_string(WorkflowStatus::RUNNING)),
        std::string(to_string(WorkflowStatus::COMPLETED)),
        std::string(to_string(WorkflowStatus::FAILED)),
        std::string(to_string(JobStatus::PENDING)),
        std::string(to_string(JobStatus::READY)),
        std::string(to_string(JobStatus::QUEUED)),
        std::string(to_string(JobStatus::COMPLETED)),
        job_key_prefix
    };

    const std::string lua_script = R"lua(
        local workflow_key = KEYS[1]
        local waiting_ready_key = KEYS[2]
        local execution_queue_key = KEYS[3]
        local completed_job_key = KEYS[4]
        local successors_key = KEYS[5]

        local last_update_time = ARGV[1]
        local workflow_running = ARGV[2]
        local workflow_completed = ARGV[3]
        local workflow_failed = ARGV[4]
        local job_pending = ARGV[5]
        local job_ready = ARGV[6]
        local job_queued = ARGV[7]
        local job_completed = ARGV[8]
        local job_key_prefix = ARGV[9]

        local workflow_status = redis.call('HGET', workflow_key, 'status')
        local completed_job_status = redis.call('HGET', completed_job_key, 'status')
        if workflow_status == false or completed_job_status == false then
            return {0}
        end
        if completed_job_status == job_completed then
            return {1, '0', '', '0'}
        end
        if workflow_status ~= workflow_running then
            return {0}
        end

        local now = tonumber(redis.call('TIME')[1])
        local start_run_time = tonumber(redis.call('HGET', workflow_key, 'start_run_time') or '') or 0
        local max_run_time = tonumber(redis.call('HGET', workflow_key, 'max_run_time') or '') or 0
        if start_run_time > 0 and max_run_time > 0 and now - start_run_time > max_run_time then
            redis.call('HSET',
                       workflow_key,
                       'status', workflow_failed,
                       'last_update_time', tostring(now))
            return {-1}
        end

        redis.call('HSET',
                   completed_job_key,
                   'status', job_completed,
                   'last_update_time', last_update_time)

        local newly_ready = {}
        local successors = redis.call('LRANGE', successors_key, 0, -1)
        redis.call('DEL', successors_key)
        for _, successor_id in ipairs(successors) do
            local successor_key = job_key_prefix .. successor_id
            local successor_status = redis.call('HGET', successor_key, 'status') or ''
            if successor_status == job_pending then
                local remaining = redis.call('HINCRBY', successor_key, 'remaining_dependencies', -1)
                redis.call('HSET', successor_key, 'last_update_time', last_update_time)

                if remaining <= 0 then
                    redis.call('HSET',
                               successor_key,
                               'remaining_dependencies', '0',
                               'status', job_ready,
                               'last_update_time', last_update_time)
                    local pending_jobs = tonumber(redis.call('HGET', workflow_key, 'pending_jobs') or '0') or 0
                    if pending_jobs > 0 then
                        redis.call('HINCRBY', workflow_key, 'pending_jobs', -1)
                    end

                    local priority = tonumber(redis.call('HGET', successor_key, 'priority') or '0') or 0
                    redis.call('ZADD', waiting_ready_key, priority, successor_key)
                    table.insert(newly_ready, successor_id)
                end
            end
        end

        local completed_jobs = redis.call('HINCRBY', workflow_key, 'completed_jobs', 1)
        local reserved_slots = tonumber(redis.call('HGET', workflow_key, 'reserved_execution_slots') or '0') or 0
        if reserved_slots > 0 then
            reserved_slots = reserved_slots - 1
        end
        redis.call('HSET',
                   workflow_key,
                   'reserved_execution_slots', reserved_slots,
                   'last_update_time', last_update_time)

        local total_jobs = tonumber(redis.call('HGET', workflow_key, 'total_jobs') or '0') or 0
        local completed_workflow = 0
        if total_jobs > 0 and completed_jobs == total_jobs then
            completed_workflow = 1
            redis.call('HSET',
                       workflow_key,
                       'status', workflow_completed,
                       'last_update_time', last_update_time)
        end

        local promoted_job_id = ''
        if completed_workflow == 0 then
            local max_concurrent_jobs = tonumber(redis.call('HGET', workflow_key, 'max_concurrent_jobs') or '0') or 0
            if reserved_slots < max_concurrent_jobs then
                local popped = redis.call('ZPOPMAX', waiting_ready_key, 1)
                if #popped > 0 then
                    local promoted_job_key = popped[1]
                    local priority = popped[2]
                    promoted_job_id = string.sub(promoted_job_key, string.len(job_key_prefix) + 1)
                    redis.call('HSET',
                               promoted_job_key,
                               'status', job_queued,
                               'last_update_time', last_update_time)
                    redis.call('ZADD', execution_queue_key, priority, promoted_job_key)
                    redis.call('HINCRBY', workflow_key, 'reserved_execution_slots', 1)
                end
            end
        end

        local result = {1, tostring(completed_workflow), promoted_job_id, tostring(#newly_ready)}
        for _, job_id in ipairs(newly_ready) do
            table.insert(result, job_id)
        end
        return result
    )lua";

    std::vector<std::string> values;
    const auto ok = co_await context.command_executor().execute_lua_script_async(lua_script, keys, args, values);
    if (!ok || values.size() < 4 || values[0] == "-1" || values[0] != "1") {
        co_return false;
    }

    workflow_completed = values[1] == "1";
    promoted_job_id = values[2];

    std::size_t ready_count = 0;
    try {
        ready_count = static_cast<std::size_t>(std::stoul(values[3]));
    } catch (...) {
        co_return false;
    }

    if (values.size() != 4 + ready_count) {
        co_return false;
    }

    ready_job_ids.assign(values.begin() + 4, values.end());
    if (!promoted_job_id.empty()) {
        const auto promoted_it = std::remove(ready_job_ids.begin(), ready_job_ids.end(), promoted_job_id);
        ready_job_ids.erase(promoted_it, ready_job_ids.end());
    }

    co_return true;
}

boost::asio::awaitable<bool> RedisDatabaseAsync::complete_failed_job_runtime_async(
    const WorkflowIdentity& workflow_id,
    const std::string& failed_job_id,
    int updated_retry_count,
    const std::vector<std::string>& workflow_job_ids,
    std::vector<std::string>& canceled_job_ids,
    bool& workflow_failed,
    bool& workflow_completed)
{
    co_return co_await complete_failed_job_runtime_async(
        *default_connection_context_,
        workflow_id,
        failed_job_id,
        updated_retry_count,
        workflow_job_ids,
        canceled_job_ids,
        workflow_failed,
        workflow_completed);
}

boost::asio::awaitable<bool> RedisDatabaseAsync::complete_failed_job_runtime_async(
    RedisConnectionContext& context,
    const WorkflowIdentity& workflow_id,
    const std::string& failed_job_id,
    int updated_retry_count,
    const std::vector<std::string>& workflow_job_ids,
    std::vector<std::string>& canceled_job_ids,
    bool& workflow_failed,
    bool& workflow_completed)
{
    // This is the Redis-side terminal failure path. The SQL database is updated
    // by the caller after this returns, so keep the Redis mutation atomic and
    // return enough detail for the caller to mirror the affected job statuses.
    canceled_job_ids.clear();
    workflow_failed = false;
    workflow_completed = false;

    const auto workflow_key = RedisKeys::workflow_key(workflow_id);
    const auto waiting_ready_key = RedisKeys::workflow_waiting_jobs_key(workflow_id);
    const std::string execution_queue_key = "fp:execution_queue";
    const std::string retry_stream_key = "fp:job_retry_stream";
    const auto failed_job_key = RedisKeys::job_key(workflow_id, failed_job_id);
    const std::vector<std::string> keys{workflow_key, waiting_ready_key, execution_queue_key, retry_stream_key, failed_job_key};

    std::vector<std::string> args{
        std::to_string(updated_retry_count),
        std::string(to_string(WorkflowStatus::RUNNING)),
        std::string(to_string(WorkflowStatus::FAILED)),
        std::string(to_string(JobStatus::PENDING)),
        std::string(to_string(JobStatus::READY)),
        std::string(to_string(JobStatus::QUEUED)),
        std::string(to_string(JobStatus::RETRY_DELAY)),
        std::string(to_string(JobStatus::FAILED)),
        std::string(to_string(JobStatus::ABORTED)),
        std::to_string(workflow_job_ids.size())
    };

    for (const auto& job_id : workflow_job_ids) {
        // The Lua script needs both the external job id to report back and the
        // Redis hash key to inspect/update the runtime state atomically.
        args.push_back(job_id);
        args.push_back(RedisKeys::job_key(workflow_id, job_id));
    }

    const std::string lua_script = R"lua(
        local workflow_key = KEYS[1]
        local waiting_ready_key = KEYS[2]
        local execution_queue_key = KEYS[3]
        local retry_stream_key = KEYS[4]
        local failed_job_key = KEYS[5]

        local updated_retry_count = ARGV[1]
        local workflow_running = ARGV[2]
        local workflow_failed = ARGV[3]
        local job_pending = ARGV[4]
        local job_ready = ARGV[5]
        local job_queued = ARGV[6]
        local job_retry_delay = ARGV[7]
        local job_failed = ARGV[8]
        local job_canceled = ARGV[9]
        local job_count = tonumber(ARGV[10]) or 0
        local now = redis.call('TIME')[1]

        redis.call('HSET', failed_job_key,
                   'status', job_failed,
                   'current_retry_count', updated_retry_count,
                   'last_update_time', now)

        local workflow_status = redis.call('HGET', workflow_key, 'status') or ''
        local completed_increment = 1
        local failed_workflow = 0
        local canceled = {}

        -- Only the first terminal job failure should fail the workflow and
        -- cancel sibling jobs. Later completions for the same workflow still
        -- record their own failed job, but do not re-cancel the workflow.
        if workflow_status == workflow_running then
            failed_workflow = 1
            local reserved_slots = tonumber(redis.call('HGET', workflow_key, 'reserved_execution_slots') or '0') or 0
            if reserved_slots > 0 then
                reserved_slots = reserved_slots - 1
            end
            redis.call('HSET', workflow_key,
                       'status', workflow_failed,
                       'reserved_execution_slots', reserved_slots,
                       'last_update_time', now)

            local idx = 11
            for _ = 1, job_count do
                local job_id = ARGV[idx]
                local job_key = ARGV[idx + 1]
                idx = idx + 2

                if job_key ~= failed_job_key then
                    local job_status = redis.call('HGET', job_key, 'status') or ''

                    if job_status == job_pending or job_status == job_ready or job_status == job_queued or job_status == job_retry_delay then
                        -- Pending/ready/queued/retry-delay jobs have not completed yet, so
                        -- cancel them and remove them from the appropriate redis queue
                        -- where they may still be pending.
                        -- Scheduler-owned jobs are PENDING_EXECUTION, so they are
                        -- deliberately left for their worker/completion path.
                        redis.call('HSET', job_key,
                                   'status', job_canceled,
                                   'last_update_time', now)
                        redis.call('ZREM', waiting_ready_key, job_key)
                        if job_status == job_queued then
                            redis.call('ZREM', execution_queue_key, job_key)
                        end
                        if job_status == job_retry_delay then
                            redis.call('ZREM', retry_stream_key, job_key)
                        end
                        table.insert(canceled, job_id)
                        completed_increment = completed_increment + 1
                    end
                end
            end
        end

        -- Canceled siblings count as completed runtime work because they will
        -- never produce their own completion event after the workflow fails.
        local completed_jobs = redis.call('HINCRBY', workflow_key, 'completed_jobs', completed_increment)
        redis.call('HINCRBY', workflow_key, 'failed_jobs', 1)
        redis.call('HSET', workflow_key, 'last_update_time', now)

        local total_jobs = tonumber(redis.call('HGET', workflow_key, 'total_jobs') or '0') or 0
        local completed_workflow = 0
        if total_jobs > 0 and completed_jobs == total_jobs then
            completed_workflow = 1
        end

        -- Result contract:
        -- {ok, workflow_failed_now, workflow_completed, canceled_count, canceled_job_id...}
        local result = {1, tostring(failed_workflow), tostring(completed_workflow), tostring(#canceled)}
        for _, job_id in ipairs(canceled) do
            table.insert(result, job_id)
        end
        return result
    )lua";

    std::vector<std::string> values;
    const auto ok = co_await context.command_executor().execute_lua_script_async(lua_script, keys, args, values);
    if (!ok || values.size() < 4 || values[0] != "1") {
        co_return false;
    }

    workflow_failed = values[1] == "1";
    workflow_completed = values[2] == "1";

    // Validate the variable-length result before exposing canceled_job_ids to
    // the caller. A malformed script response means the SQL mirror should not
    // be updated from partial data.
    std::size_t canceled_count = 0;
    try {
        canceled_count = static_cast<std::size_t>(std::stoul(values[3]));
    } catch (...) {
        co_return false;
    }

    if (values.size() != 4 + canceled_count) {
        co_return false;
    }

    canceled_job_ids.assign(values.begin() + 4, values.end());
    co_return true;
}

boost::asio::awaitable<bool> RedisDatabaseAsync::fetch_workflow_runtime_async(
    const WorkflowIdentity& workflow_id,
    std::unordered_map<std::string, std::string>& workflow_data) const {
    co_return co_await fetch_workflow_runtime_async(*default_connection_context_, workflow_id, workflow_data);
}

boost::asio::awaitable<bool> RedisDatabaseAsync::fetch_workflow_runtime_async(
    RedisConnectionContext& context,
    const WorkflowIdentity& workflow_id,
    std::unordered_map<std::string, std::string>& workflow_data) const {
    const auto workflow_key = RedisKeys::workflow_key(workflow_id);
    co_return co_await context.command_executor().execute_hgetall_command_async(workflow_key, workflow_data);
}

boost::asio::awaitable<bool> RedisDatabaseAsync::create_workflow_waiting_ready_jobs(const WorkflowIdentity& workflow_id, 
                                                                                    const PrioritizedJobsList& ready_jobs) {
    std::string waiting_ready_job_key = RedisKeys::workflow_waiting_jobs_key(workflow_id);
    std::unordered_map<std::string, unsigned int> members;

    for (const auto& job_info : ready_jobs) {
         const auto job_key = RedisKeys::job_key(workflow_id, job_info.job_id);
        members[job_key] = job_info.priority;
    }
    co_return co_await command_executor_->execute_zset_enqueue_command_async(waiting_ready_job_key, members);
}

boost::asio::awaitable<bool> RedisDatabaseAsync::delete_workflow_waiting_ready_jobs(const WorkflowIdentity& workflow_id) {
    co_return co_await delete_workflow_waiting_ready_jobs(*default_connection_context_, workflow_id);
}

boost::asio::awaitable<bool> RedisDatabaseAsync::delete_workflow_waiting_ready_jobs(
    RedisConnectionContext& context,
    const WorkflowIdentity& workflow_id) {
    std::string waiting_ready_job_key = RedisKeys::workflow_waiting_jobs_key(workflow_id);
    std::vector<std::string> args{"DEL", waiting_ready_job_key};
    long long value = 0;
    co_return co_await context.command_executor().execute_integer_command_async(args, value);
}

boost::asio::awaitable<bool> RedisDatabaseAsync::delete_workflow_runtime_async(const WorkflowIdentity& workflow_id) {
    const auto workflow_key = RedisKeys::workflow_key(workflow_id);
    std::vector<std::string> args{"DEL", workflow_key};
    long long value = 0;
    auto ok = co_await command_executor_->execute_integer_command_async(args, value);
    co_return ok && value > 0;
}

} // namespace flow_pilot

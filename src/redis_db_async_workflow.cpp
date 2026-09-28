// redis_db_async_workflow.cpp - Redis workflow runtime operations for FlowPilot

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

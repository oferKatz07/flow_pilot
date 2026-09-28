// redis_db_async_jobs.cpp - Redis job runtime operations for FlowPilot

#include <string>
#include <unordered_map>
#include <vector>

#include "logger.h"
#include "redis_db_async.h"

namespace flow_pilot {

boost::asio::awaitable<bool> RedisDatabaseAsync::set_job_runtime_async(const WorkflowIdentity& workflow_id, const JobRuntimeData& job_data) {
    const auto job_key = RedisKeys::job_key(workflow_id, job_data.job_id);
    std::unordered_map<std::string, std::string> fields{
        {"status", job_data.status},
        {"remaining_dependencies", std::to_string(job_data.remaining_dependencies)},
        {"priority", std::to_string(job_data.priority)},
        {"timeout_sec", std::to_string(job_data.timeout_sec)},
        {"max_retries", std::to_string(job_data.max_retries)},
        {"current_retry_count", std::to_string(job_data.current_retry_count)},
        {"retry_delay_sec", std::to_string(job_data.retry_delay_sec)},
        {"retry_backoff_policy", job_data.retry_backoff_policy},
        {"owned_by", ""},
        {"start_run_time", ""}
    };

    if (!co_await command_executor_->execute_hset_command_async(job_key, fields)) {
        co_return false;
    }

    bool retval = true;
    if (job_data.successors.size() > 0) {
        const auto successors = RedisKeys::successors_key(workflow_id, job_data.job_id);
        retval = co_await command_executor_->execute_list_add_command_async(successors, job_data.successors);
    }
    co_return retval;
}

boost::asio::awaitable<bool> RedisDatabaseAsync::update_job_runtime_async(
    const WorkflowIdentity& workflow_id,
    const std::string& job_id,
    const std::unordered_map<std::string, std::string>& fields) {
    co_return co_await update_job_runtime_async(*default_connection_context_, workflow_id, job_id, fields);
}

boost::asio::awaitable<bool> RedisDatabaseAsync::update_job_runtime_async(
    RedisConnectionContext& context,
    const WorkflowIdentity& workflow_id,
    const std::string& job_id,
    const std::unordered_map<std::string, std::string>& fields) {
    if (fields.empty()) {
        co_return true;
    }

    const auto job_key = RedisKeys::job_key(workflow_id, job_id);
    co_return co_await context.command_executor().execute_hset_command_async(job_key, fields);
}

boost::asio::awaitable<bool> RedisDatabaseAsync::fetch_job_runtime_async(const WorkflowIdentity& workflow_id,
                                                                         const std::string& job_id,
                                                                         JobRuntimeData& job_data) const {
    co_return co_await fetch_job_runtime_async(*default_connection_context_, workflow_id, job_id, job_data);
}

boost::asio::awaitable<bool> RedisDatabaseAsync::fetch_job_runtime_async(RedisConnectionContext& context,
                                                                         const WorkflowIdentity& workflow_id,
                                                                         const std::string& job_id,
                                                                         JobRuntimeData& job_data) const {
    std::unordered_map<std::string, std::string> job_fields;
    const auto job_key = RedisKeys::job_key(workflow_id, job_id);
    if (!co_await context.command_executor().execute_hgetall_command_async(job_key, job_fields)) {
        Logger::get_logger()->error("fetch_job_runtime_async- Failed to fetched job {} fileds!!!", job_key);
        co_return false;
    }

    job_data.job_id = job_id;
    job_data.status = job_fields["status"];
    job_data.remaining_dependencies = std::stoi(job_fields["remaining_dependencies"]);
    job_data.priority = std::stoi(job_fields["priority"]);
    job_data.timeout_sec = std::stoi(job_fields["timeout_sec"]);
    job_data.max_retries = std::stoi(job_fields["max_retries"]);
    job_data.current_retry_count = std::stoi(job_fields["current_retry_count"]);
    job_data.retry_delay_sec = std::stoi(job_fields["retry_delay_sec"]);
    job_data.retry_backoff_policy = job_fields["retry_backoff_policy"];

    co_return true;
}

boost::asio::awaitable<bool> RedisDatabaseAsync::try_set_job_to_running_async(const WorkflowIdentity& workflow_id, 
                                                                              const std::string& job_id, 
                                                                              StartJobResult& result) {
    co_return co_await try_set_job_to_running_async(*default_connection_context_, workflow_id, job_id, result);
}

boost::asio::awaitable<bool> RedisDatabaseAsync::try_set_job_to_running_async(RedisConnectionContext& context,
                                                                              const WorkflowIdentity& workflow_id, 
                                                                              const std::string& job_id, 
                                                                              StartJobResult& result) {
    const auto workflow_key = RedisKeys::workflow_key(workflow_id);
    const auto job_key = RedisKeys::job_key(workflow_id, job_id);
    const std::vector<std::string> keys{workflow_key, job_key};
    const std::vector<std::string> args{
        std::string(to_string(WorkflowStatus::READY)),
        std::string(to_string(WorkflowStatus::RUNNING)),
        std::string(to_string(WorkflowStatus::FAILED)),
        std::string(to_string(WorkflowStatus::CANCELED)),
        std::string(to_string(JobStatus::QUEUED)),
        std::string(to_string(JobStatus::RUNNING)),
        std::string(to_string(JobStatus::CANCELED))
    };

    const std::string lua_script = R"lua(
        local workflow_key = KEYS[1]
        local job_key = KEYS[2]

        local workflow_ready = ARGV[1]
        local workflow_running = ARGV[2]
        local workflow_failed = ARGV[3]
        local workflow_canceled = ARGV[4]
        local job_queued = ARGV[5]
        local job_running = ARGV[6]
        local job_canceled = ARGV[7]

        local job_status = redis.call('HGET', job_key, 'status')
        if job_status == job_canceled then
            return {'ALREADY_CANCELED'}
        end

        local now = redis.call('TIME')[1]
        if job_status ~= job_queued then
            -- Set workflow status to failed.
            redis.call('HSET', workflow_key,
                       'status', workflow_failed,
                       'last_update_time', now)
            return {'INVARIANT_VIOLATION_JOB_STATUS'}
        end

        local workflow_status = redis.call('HGET', workflow_key, 'status')
        if workflow_status == workflow_ready then
            local start_run_time = redis.call('HGET', workflow_key, 'start_run_time')
            if start_run_time ~= false and start_run_time ~= '' then
                redis.call('HSET', job_key, 'status', job_canceled)
                redis.call('HSET', workflow_key,
                           'status', workflow_failed,
                           'last_update_time', now)
                return {'INVARIANT_VIOLATION_WORKFLOW_TIME'}
            end

            redis.call('HSET', workflow_key,
                       'status', workflow_running,
                       'start_run_time', now,
                       'last_update_time', now)
            redis.call('HSET', job_key, 
                       'status', job_running,
                       'start_run_time', now)
            return {'FIRST_TO_START'}
        end

        if workflow_status == workflow_running then
            redis.call('HSET', job_key, 
                       'status', job_running,
                       'start_run_time', now)
            return {'STARTED'}
        end

        if workflow_status == workflow_failed or
           workflow_status == workflow_canceled then
            redis.call('HSET', job_key, 'status', job_canceled)
            return {'CANCELED_BY_WORKFLOW_STATE'}
        end
        -- In case workflow status is neither READY, RUNNING, FAILED or CANCELED,
        -- it is an invariant violation and job status is set to canceled.
        redis.call('HSET', job_key, 'status', job_canceled)
        redis.call('HSET', workflow_key,
                   'status', workflow_failed,
                   'last_update_time', now)
        return {'INVARIANT_VIOLATION_WORKFLOW_STATUS'}
    )lua";

    std::vector<std::string> values;
    const auto ok = co_await context.command_executor().execute_lua_script_async(lua_script, keys, args, values);
    if (!ok || values.size() != 1) {
        result = StartJobResult::INTERNAL_ERROR;
        co_return false;
    }

    if (values[0] == "FIRST_TO_START") {
        result = StartJobResult::FIRST_TO_START;
        co_return true;
    }

    if (values[0] == "STARTED") {
        result = StartJobResult::STARTED;
        co_return true;
    }
    
    if (values[0] == "CANCELED_BY_WORKFLOW_STATE") {
        result = StartJobResult::CANCELED_BY_WORKFLOW_STATE;
    } else if (values[0] == "ALREADY_CANCELED") {
        result = StartJobResult::ALREADY_CANCELED;
    } else if (values[0] == "INVARIANT_VIOLATION_JOB_STATUS") {
        result = StartJobResult::INVARIANT_VIOLATION_JOB_STATUS;
    } else if (values[0] == "INVARIANT_VIOLATION_WORKFLOW_STATUS") {
        result = StartJobResult::INVARIANT_VIOLATION_WORKFLOW_STATUS;
    } else if (values[0] == "INVARIANT_VIOLATION_WORKFLOW_TIME") {
        result = StartJobResult::INVARIANT_VIOLATION_WORKFLOW_TIME;
    } else {
        Logger::get_logger()->error("try_set_job_to_running_async - unexpected Lua result {}", values[0]);
        result = StartJobResult::INTERNAL_ERROR;
    }

    co_return false;
}

boost::asio::awaitable<bool> RedisDatabaseAsync::set_job_payload_async(
    const WorkflowIdentity& workflow_id,
    const std::string& job_id,
    const std::vector<uint8_t>& payload) {
    const auto payload_key = RedisKeys::payload_key(workflow_id, job_id);
    const std::string payload_value(payload.begin(), payload.end());
    co_return co_await command_executor_->execute_set_command_async(payload_key, payload_value, 0, false);
}

boost::asio::awaitable<bool> RedisDatabaseAsync::fetch_job_payload_async(
    const WorkflowIdentity& workflow_id,
    const std::string& job_id,
    std::vector<uint8_t>& payload) const {
    co_return co_await fetch_job_payload_async(*default_connection_context_, workflow_id, job_id, payload);
}

boost::asio::awaitable<bool> RedisDatabaseAsync::fetch_job_payload_async(
    RedisConnectionContext& context,
    const WorkflowIdentity& workflow_id,
    const std::string& job_id,
    std::vector<uint8_t>& payload) const {
    const auto payload_key = RedisKeys::payload_key(workflow_id, job_id);
    std::vector<std::string> args{"GET", payload_key};
    std::string bulk_string_payload;
    auto ok = co_await context.command_executor().execute_bulk_string_command_async(args, bulk_string_payload);
    if (!ok) {
        payload.clear();
        co_return false;
    }

    payload.assign(bulk_string_payload.begin(), bulk_string_payload.end());
    co_return true;
}

boost::asio::awaitable<bool> RedisDatabaseAsync::delete_all_workflow_jobs_async(const WorkflowIdentity& workflow_id, const WorkflowJobsList& jobs) {
    for (const auto& job_data : jobs) {
        const auto job_key = RedisKeys::job_key(workflow_id, job_data.job_id);
        std::vector<std::string> args{"DEL", job_key};
        long long value = 0;
        auto ok = co_await command_executor_->execute_integer_command_async(args, value);
        if (!ok || value == 0) {
            Logger::get_logger()->info("Failed to delete job runtime data for job_id: {} in workflow_id: {}", job_data.job_id, workflow_id.workflow_id);
        }
        const auto successors_key = RedisKeys::successors_key(workflow_id, job_data.job_id);
        args = {"DEL", successors_key};
        value = 0;
        ok = co_await command_executor_->execute_integer_command_async(args, value);
        if (!ok || value == 0) {
            Logger::get_logger()->info("Failed to delete job successors data for job_id: {} in workflow_id: {}", job_data.job_id, workflow_id.workflow_id);
        }
    }
    co_return true;
}

boost::asio::awaitable<bool> RedisDatabaseAsync::delete_job_payload_async(const WorkflowIdentity& workflow_id, const std::string& job_id) {
    const auto payload_key = RedisKeys::payload_key(workflow_id, job_id);
    std::vector<std::string> args{"DEL", payload_key};
    long long value = 0;
    auto ok = co_await command_executor_->execute_integer_command_async(args, value);
    if (!ok || value == 0) {
        Logger::get_logger()->info("Failed to delete job payload data for job_id: {} in workflow_id: {}", job_id, workflow_id.workflow_id);
    }

    co_return true;
}

boost::asio::awaitable<bool> RedisDatabaseAsync::delete_all_jobs_payload_async(const WorkflowIdentity& workflow_id, const std::vector<std::string>& job_ids) {
    for (const std::string& job_id : job_ids) {
        const auto payload_key = RedisKeys::payload_key(workflow_id, job_id);
        std::vector<std::string> args{"DEL", payload_key};
        long long value = 0;
        auto ok = co_await command_executor_->execute_integer_command_async(args, value);
        if (!ok || value == 0) {
            Logger::get_logger()->info("Failed to delete job payload data for job_id: {} in workflow_id: {}", job_id, workflow_id.workflow_id);
        }
    }

    co_return true;
}

} // namespace flow_pilot

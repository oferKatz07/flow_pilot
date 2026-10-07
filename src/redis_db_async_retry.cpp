// redis_db_async_retry.cpp - Redis delayed retry queue operations for FlowPilot

#include <chrono>
#include <string>
#include <string_view>
#include <vector>

#include "logger.h"
#include "redis_db_async.h"

namespace flow_pilot {
namespace {

constexpr const char* EXECUTION_QUEUE_KEY = "fp:execution_queue";
constexpr const char* JOB_RETRY_STREAM_KEY = "fp:job_retry_stream";
constexpr const char* JOB_RETRY_READY_CHANNEL = "fp:job_retry_stream:ready";

bool parse_job_key(const std::string& job_key, JobRetryData& retry_data)
{
    constexpr std::string_view job_key_prefix = "fp:job:";
    if (job_key.rfind(job_key_prefix, 0) != 0) {
        return false;
    }

    std::size_t start = job_key_prefix.size();
    std::size_t end = job_key.find(':', start);
    if (end == std::string::npos) {
        return false;
    }
    retry_data.identity.client_id = job_key.substr(start, end - start);

    start = end + 1;
    end = job_key.find(':', start);
    if (end == std::string::npos) {
        return false;
    }
    retry_data.identity.workflow_id = job_key.substr(start, end - start);

    retry_data.job_id = job_key.substr(end + 1);
    return !retry_data.identity.client_id.empty() &&
           !retry_data.identity.workflow_id.empty() &&
           !retry_data.job_id.empty();
}

} // namespace

boost::asio::awaitable<bool> RedisDatabaseAsync::schedule_job_retry_async(
    const WorkflowIdentity& workflow_id,
    const std::string& job_id,
    int updated_retry_count,
    long long retry_at_ms,
    std::string& promoted_job_id)
{
    co_return co_await schedule_job_retry_async(
        *default_connection_context_,
        workflow_id,
        job_id,
        updated_retry_count,
        retry_at_ms,
        promoted_job_id);
}

boost::asio::awaitable<bool> RedisDatabaseAsync::schedule_job_retry_async(
    RedisConnectionContext& context,
    const WorkflowIdentity& workflow_id,
    const std::string& job_id,
    int updated_retry_count,
    long long retry_at_ms,
    std::string& promoted_job_id)
{
    promoted_job_id.clear();

    const auto workflow_key = RedisKeys::workflow_key(workflow_id);
    const auto waiting_ready_key = RedisKeys::workflow_waiting_jobs_key(workflow_id);
    const auto job_key = RedisKeys::job_key(workflow_id, job_id);
    const std::vector<std::string> keys{
        workflow_key,
        waiting_ready_key,
        EXECUTION_QUEUE_KEY,
        job_key,
        JOB_RETRY_STREAM_KEY
    };
    const std::vector<std::string> args{
        std::to_string(updated_retry_count),
        std::to_string(retry_at_ms),
        std::string(to_string(WorkflowStatus::RUNNING)),
        std::string(to_string(JobStatus::PENDING)),
        std::string(to_string(JobStatus::QUEUED)),
        JOB_RETRY_READY_CHANNEL
    };

    const std::string lua_script = R"lua(
        local workflow_key = KEYS[1]
        local waiting_ready_key = KEYS[2]
        local execution_queue_key = KEYS[3]
        local retry_job_key = KEYS[4]
        local retry_stream_key = KEYS[5]

        local updated_retry_count = ARGV[1]
        local retry_at_ms = ARGV[2]
        local workflow_running = ARGV[3]
        local job_pending = ARGV[4]
        local job_queued = ARGV[5]
        local ready_channel = ARGV[6]

        local now = redis.call('TIME')[1]
        redis.call('HSET',
                   retry_job_key,
                   'status', job_pending,
                   'current_retry_count', updated_retry_count,
                   'start_run_time', '',
                   'owned_by', '',
                   'last_update_time', now)
        redis.call('ZADD', retry_stream_key, retry_at_ms, retry_job_key)
        redis.call('PUBLISH', ready_channel, retry_job_key)

        local reserved_slots = tonumber(redis.call('HGET', workflow_key, 'reserved_execution_slots') or '0') or 0
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

        local max_concurrent_jobs = tonumber(redis.call('HGET', workflow_key, 'max_concurrent_jobs') or '0') or 0
        if reserved_slots >= max_concurrent_jobs then
            return {1, ''}
        end

        local popped = redis.call('ZPOPMAX', waiting_ready_key, 1)
        if #popped == 0 then
            return {1, ''}
        end

        local promoted_job_key = popped[1]
        local priority = popped[2]
        redis.call('HSET',
                   promoted_job_key,
                   'status', job_queued,
                   'last_update_time', now)
        redis.call('ZADD', execution_queue_key, priority, promoted_job_key)
        redis.call('HINCRBY', workflow_key, 'reserved_execution_slots', 1)

        return {1, promoted_job_key}
    )lua";

    std::vector<std::string> values;
    const auto ok = co_await context.command_executor().execute_lua_script_async(lua_script, keys, args, values);
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

boost::asio::awaitable<bool> RedisDatabaseAsync::dequeue_due_job_retry_async(
    long long now_ms,
    JobRetryData& retry_data)
{
    co_return co_await dequeue_due_job_retry_async(*default_connection_context_, now_ms, retry_data);
}

boost::asio::awaitable<bool> RedisDatabaseAsync::dequeue_due_job_retry_async(
    RedisConnectionContext& context,
    long long now_ms,
    JobRetryData& retry_data)
{
    retry_data = JobRetryData{};

    const std::vector<std::string> keys{JOB_RETRY_STREAM_KEY};
    const std::vector<std::string> args{std::to_string(now_ms)};
    const std::string lua_script = R"lua(
        local retry_stream_key = KEYS[1]
        local now_ms = ARGV[1]

        local entries = redis.call('ZRANGEBYSCORE', retry_stream_key, '-inf', now_ms, 'LIMIT', 0, 1)
        if #entries == 0 then
            return {}
        end

        local job_key = entries[1]
        local removed = redis.call('ZREM', retry_stream_key, job_key)
        if removed == 0 then
            return {}
        end

        return {job_key}
    )lua";

    std::vector<std::string> values;
    const auto ok = co_await context.command_executor().execute_lua_script_async(lua_script, keys, args, values);
    if (!ok) {
        co_return false;
    }

    if (values.empty()) {
        co_return true;
    }

    if (values.size() != 1 || !parse_job_key(values[0], retry_data)) {
        Logger::get_logger()->error("dequeue_due_job_retry_async - malformed retry job key");
        retry_data = JobRetryData{};
        co_return false;
    }

    co_return true;
}

boost::asio::awaitable<bool> RedisDatabaseAsync::promote_retry_job_async(
    const WorkflowIdentity& workflow_id,
    const std::string& job_id,
    std::string& promoted_job_id)
{
    co_return co_await promote_retry_job_async(*default_connection_context_, workflow_id, job_id, promoted_job_id);
}

boost::asio::awaitable<bool> RedisDatabaseAsync::promote_retry_job_async(
    RedisConnectionContext& context,
    const WorkflowIdentity& workflow_id,
    const std::string& job_id,
    std::string& promoted_job_id)
{
    promoted_job_id.clear();

    const auto workflow_key = RedisKeys::workflow_key(workflow_id);
    const auto waiting_ready_key = RedisKeys::workflow_waiting_jobs_key(workflow_id);
    const auto retry_job_key = RedisKeys::job_key(workflow_id, job_id);
    const std::vector<std::string> keys{workflow_key, waiting_ready_key, EXECUTION_QUEUE_KEY, retry_job_key};
    const std::vector<std::string> args{
        std::string(to_string(WorkflowStatus::RUNNING)),
        std::string(to_string(JobStatus::PENDING)),
        std::string(to_string(JobStatus::READY)),
        std::string(to_string(JobStatus::QUEUED)),
        std::string(to_string(JobStatus::CANCELED))
    };

    const std::string lua_script = R"lua(
        local workflow_key = KEYS[1]
        local waiting_ready_key = KEYS[2]
        local execution_queue_key = KEYS[3]
        local retry_job_key = KEYS[4]

        local workflow_running = ARGV[1]
        local job_pending = ARGV[2]
        local job_ready = ARGV[3]
        local job_queued = ARGV[4]
        local job_canceled = ARGV[5]

        local now = redis.call('TIME')[1]
        local workflow_status = redis.call('HGET', workflow_key, 'status')
        local job_status = redis.call('HGET', retry_job_key, 'status')
        if workflow_status == false or job_status == false then
            return {1, ''}
        end
        if workflow_status ~= workflow_running then
            redis.call('HSET', retry_job_key, 'status', job_canceled, 'last_update_time', now)
            return {1, ''}
        end
        if job_status ~= job_pending and job_status ~= job_ready then
            return {1, ''}
        end

        local reserved_slots = tonumber(redis.call('HGET', workflow_key, 'reserved_execution_slots') or '0') or 0
        local max_concurrent_jobs = tonumber(redis.call('HGET', workflow_key, 'max_concurrent_jobs') or '0') or 0
        local priority = tonumber(redis.call('HGET', retry_job_key, 'priority') or '0') or 0
        if reserved_slots < max_concurrent_jobs then
            local waiting_count = redis.call('ZCARD', waiting_ready_key)
            if waiting_count == 0 then
                redis.call('HSET', retry_job_key, 'status', job_queued, 'last_update_time', now)
                redis.call('ZADD', execution_queue_key, priority, retry_job_key)
                redis.call('HINCRBY', workflow_key, 'reserved_execution_slots', 1)
                return {1, retry_job_key}
            end
        end

        redis.call('HSET', retry_job_key, 'status', job_ready, 'last_update_time', now)
        redis.call('ZADD', waiting_ready_key, priority, retry_job_key)

        if reserved_slots >= max_concurrent_jobs then
            return {1, ''}
        end

        local popped = redis.call('ZPOPMAX', waiting_ready_key, 1)
        if #popped == 0 then
            return {1, ''}
        end

        local promoted_job_key = popped[1]
        local promoted_priority = popped[2]
        redis.call('HSET', promoted_job_key, 'status', job_queued, 'last_update_time', now)
        redis.call('ZADD', execution_queue_key, promoted_priority, promoted_job_key)
        redis.call('HINCRBY', workflow_key, 'reserved_execution_slots', 1)

        return {1, promoted_job_key}
    )lua";

    std::vector<std::string> values;
    const auto ok = co_await context.command_executor().execute_lua_script_async(lua_script, keys, args, values);
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

} // namespace flow_pilot

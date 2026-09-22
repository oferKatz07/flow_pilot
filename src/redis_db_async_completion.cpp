// redis_db_async_completion.cpp - Redis job completion stream operations for FlowPilot

#include <string>
#include <unordered_map>
#include <vector>

#include "logger.h"
#include "redis_db_async.h"

namespace flow_pilot {
namespace {

constexpr const char* JOB_COMPLETION_STREAM_KEY = "fp:job_completion_stream";
constexpr const char* JOB_COMPLETION_READY_CHANNEL = "fp:job_completion_stream:ready";

JobStatus job_status_from_string(const std::string& value)
{
    if (value == to_string(JobStatus::PENDING)) {
        return JobStatus::PENDING;
    }
    if (value == to_string(JobStatus::READY)) {
        return JobStatus::READY;
    }
    if (value == to_string(JobStatus::QUEUED)) {
        return JobStatus::QUEUED;
    }
    if (value == to_string(JobStatus::RUNNING)) {
        return JobStatus::RUNNING;
    }
    if (value == to_string(JobStatus::COMPLETED)) {
        return JobStatus::COMPLETED;
    }
    if (value == to_string(JobStatus::FAILED)) {
        return JobStatus::FAILED;
    }
    if (value == to_string(JobStatus::CANCELED)) {
        return JobStatus::CANCELED;
    }
    return JobStatus::UNKNOWN;
}

StatusCodes status_code_from_string(const std::string& value)
{
    try {
        const auto parsed = std::stoi(value);
        if (parsed >= static_cast<int>(StatusCodes::INVALID_JSON_FORMAT) &&
            parsed <= static_cast<int>(StatusCodes::OK)) {
            return static_cast<StatusCodes>(parsed);
        }
    } catch (...) {
    }

    return StatusCodes::INTERNAL_DB_FAILURE;
}

} // namespace

boost::asio::awaitable<bool> RedisDatabaseAsync::enqueue_job_completion_async(const JobCompletionData& message)
{
    const std::vector<std::string> keys{JOB_COMPLETION_STREAM_KEY};
    const std::vector<std::string> args{
        message.identity.client_id,
        message.identity.workflow_id,
        message.job_id,
        std::string(to_string(message.status)),
        std::to_string(static_cast<int>(message.error_code)),
        JOB_COMPLETION_READY_CHANNEL
    };

    const std::string lua_script = R"lua(
        local stream_key = KEYS[1]
        local client_id = ARGV[1]
        local workflow_id = ARGV[2]
        local job_id = ARGV[3]
        local status = ARGV[4]
        local error_code = ARGV[5]
        local ready_channel = ARGV[6]

        local id = redis.call('XADD',
                              stream_key,
                              '*',
                              'client_id', client_id,
                              'workflow_id', workflow_id,
                              'job_id', job_id,
                              'status', status,
                              'error_code', error_code)
        redis.call('PUBLISH', ready_channel, id)
        return {id}
    )lua";

    std::vector<std::string> values;
    const auto ok = co_await command_executor_->execute_lua_script_async(lua_script, keys, args, values);
    co_return ok && values.size() == 1 && !values[0].empty();
}

boost::asio::awaitable<bool> RedisDatabaseAsync::dequeue_job_completion_async(JobCompletionData& message)
{
    const std::vector<std::string> keys{JOB_COMPLETION_STREAM_KEY};
    const std::string lua_script = R"lua(
        local stream_key = KEYS[1]
        local entries = redis.call('XRANGE', stream_key, '-', '+', 'COUNT', 1)
        if #entries == 0 then
            return {}
        end

        local id = entries[1][1]
        local fields = entries[1][2]
        redis.call('XDEL', stream_key, id)

        local result = {id}
        for i = 1, #fields do
            table.insert(result, fields[i])
        end
        return result
    )lua";

    std::vector<std::string> values;
    const auto ok = co_await command_executor_->execute_lua_script_async(lua_script, keys, {}, values);
    if (!ok) {
        co_return false;
    }

    if (values.empty()) {
        message = JobCompletionData{};
        co_return true;
    }

    if (values.size() < 11) {
        Logger::get_logger()->error("dequeue_job_completion_async - malformed completion stream entry");
        co_return false;
    }

    std::unordered_map<std::string, std::string> fields;
    for (std::size_t i = 1; i + 1 < values.size(); i += 2) {
        fields[values[i]] = values[i + 1];
    }

    JobCompletionData parsed;
    parsed.stream_id = values[0];
    parsed.identity.client_id = fields["client_id"];
    parsed.identity.workflow_id = fields["workflow_id"];
    parsed.job_id = fields["job_id"];
    parsed.status = job_status_from_string(fields["status"]);
    parsed.error_code = status_code_from_string(fields["error_code"]);

    if (parsed.identity.client_id.empty() ||
        parsed.identity.workflow_id.empty() ||
        parsed.job_id.empty() ||
        parsed.status == JobStatus::UNKNOWN) {
        Logger::get_logger()->error("dequeue_job_completion_async - invalid completion stream entry {}", parsed.stream_id);
        co_return false;
    }

    message = std::move(parsed);
    co_return true;
}

} // namespace flow_pilot

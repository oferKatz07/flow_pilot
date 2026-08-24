// redis_keys.h - Redis key naming helpers for FlowPilot

#pragma once

#include <string>

namespace flow_pilot {

struct WorkflowIdentity;

class RedisKeys {
public:
    static std::string workflow_key(const WorkflowIdentity& workflow_id);
    static std::string workflow_waiting_jobs_key(const WorkflowIdentity& workflow_id);
    static std::string job_key(const WorkflowIdentity& workflow_id, const std::string& job_id);
    static std::string successors_key(const WorkflowIdentity& workflow_id, const std::string& job_id);
    static std::string payload_key(const WorkflowIdentity& workflow_id, const std::string& job_id);
};

} // namespace flow_pilot

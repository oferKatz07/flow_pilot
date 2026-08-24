// redis_keys.cpp - Redis key naming helpers for FlowPilot

#include "redis_db_async.h"
#include "redis_keys.h"

namespace flow_pilot {

std::string RedisKeys::workflow_key(const WorkflowIdentity& workflow_id)
{
    return "fp:workflow:" + workflow_id.client_id + ":" + workflow_id.workflow_id;
}

std::string RedisKeys::workflow_waiting_jobs_key(const WorkflowIdentity& workflow_id)
{
    return "fp:workflow:" + workflow_id.client_id + ":" + workflow_id.workflow_id + ":waiting_ready";
}

std::string RedisKeys::job_key(const WorkflowIdentity& workflow_id, const std::string& job_id)
{
    return "fp:job:" + workflow_id.client_id + ":" + workflow_id.workflow_id + ":" + job_id;
}

std::string RedisKeys::successors_key(const WorkflowIdentity& workflow_id, const std::string& job_id)
{
    return "fp:successors:" + workflow_id.client_id + ":" + workflow_id.workflow_id + ":" + job_id;
}

std::string RedisKeys::payload_key(const WorkflowIdentity& workflow_id, const std::string& job_id)
{
    return "fp:payload:" + workflow_id.client_id + ":" + workflow_id.workflow_id + ":" + job_id;
}

} // namespace flow_pilot

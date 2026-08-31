// workflow_runtime_generator.h - Build and persist workflow runtime data for FlowPilot

#pragma once

#include <boost/asio.hpp>
#include <nlohmann/json.hpp>
#include <string>
#include <unordered_map>

#include "client_config.h"
#include "database_models.h"
#include "redis_db_async.h"

namespace flow_pilot {

class WorkflowRuntimeGenerator {
public:
    bool build_jobs_runtime_info(const nlohmann::json& workflow_data,
                                 const PolicyPlan& policy_config,
                                 JobRuntimeMap& jobs_runtime_info,
                                 JobPriorityQueue& ready_jobs) const;

    boost::asio::awaitable<bool> initialize_runtime_data(const nlohmann::json& workflow_data,
                                                         const PolicyPlan& policy_config,
                                                         const WorkflowData& workflow_info,
                                                         const JobPriorityQueue& ready_jobs,
                                                         JobRuntimeMap& jobs_runtime_info) const;
};

} // namespace flow_pilot

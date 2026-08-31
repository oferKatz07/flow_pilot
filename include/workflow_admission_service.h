
// workflow_addmision_service.h - Workflow submission and validation for FlowPilot

#pragma once

#include <string_view>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <nlohmann/json.hpp>
#include <nlohmann/json-schema.hpp>
#include <boost/asio.hpp>

#include "admission_types.h"
#include "db_interface.h"
#include "redis_db_async.h"

namespace flow_pilot {

using json = nlohmann::json;

class PolicyPlan;

struct DagData {
    std::string job_id;
    std::unordered_set<std::string> dependencies;
    std::unordered_set<std::string> incoming_edges;
    std::unordered_set<std::string> outgoing_edges;
};

class WorkflowAdmissionService {
public:
    explicit WorkflowAdmissionService(const std::string& schema_path);                             

     boost::asio::awaitable<ValidationResult> submit_workflow(const std::string& body);

private:
    bool parse_request(const std::string& body, json& workflow_data, 
                       RequestData& request_info, ValidationResult& result);
    bool get_client_config_data(const std::string& client_id, ClientConfig& client_config, ValidationResult& result);
    boost::asio::awaitable<bool> admit_request(const RequestData& request_info, 
                                               const RateLimitConfig& rate_limit_config, 
                                               StatusCodes& rejection_reason);
    boost::asio::awaitable<bool> persist_request(const RequestData& request_info, const std::string& body, 
                                                 const ClientConfig& client_config, StatusCodes& rejection_reason);
    bool validate_workflow(const json& workflow_data, const PolicyPlan& policy_config, 
                           JobRuntimeMap& jobs_runtime_info, 
                           WorkflowData& workflow_info, 
                           StatusCodes& rejection_reason);
     boost::asio::awaitable<bool>  persist_workflow(const WorkflowData& workflow_info, 
                                                    const JobRuntimeMap& jobs_runtime_info,
                                                    const int policy_jobs_retry_num, 
                                                    StatusCodes& rejection_reason);
    bool validate_admission_client_workflow_policy(const json& workflow_data, 
                                                   const PolicyPlan& policy_config, 
                                                   StatusCodes& rejection_reason);
    bool validate_semantic(const json& data, 
                           std::unordered_map<std::string, DagData>& jobs_map, 
                           StatusCodes& rejection_reason);
    bool validate_dependencies(const json& data, 
                               JobRuntimeMap& jobs_runtime_info, 
                               std::unordered_map<std::string, DagData>& jobs_map, 
                               StatusCodes& rejection_reason);
    boost::asio::awaitable<ValidationResult> handle_request_accepted(ValidationResult& result, 
                                                                     RequestData& request_info);

    static constexpr int DEFAULT_MAX_ACTIVE_WORKFLOWS = 10;
    static constexpr int DEFAULT_RATE_REQUESTS = 3;
    static constexpr int DEFAULT_RATE_WINDOW_SECONDS = 10;

    nlohmann::json_schema::json_validator validator_;
};

} // namespace flow_pilot

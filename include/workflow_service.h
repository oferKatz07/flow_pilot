
// workflow_service.h - Workflow submission and validation for FlowPilot

#pragma once

#include <string_view>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <nlohmann/json.hpp>
#include <nlohmann/json-schema.hpp>
#include <boost/asio.hpp>

#include "db_interface.h"

namespace flow_pilot {

using json = nlohmann::json;

class PolicyPlan;

struct ValidationResult {
    bool valid;
    std::string_view status_str;
    std::string errors_msg;
};

struct DagData {
    std::string job_id;
    std::unordered_set<std::string> dependencies;
    std::unordered_set<std::string> incoming_edges;
    std::unordered_set<std::string> outgoing_edges;
};

class WorkflowService {
public:
    explicit WorkflowService(const std::string& schema_path);                             

     boost::asio::awaitable<ValidationResult> submit_workflow(const std::string& body);

private:
    bool validate_admission_client_workflow_policy(const json& workflow_data, 
                                                   const PolicyPlan& policy_config, 
                                                   std::string& rejection_reason);
    bool validate_semantic(const json& data, 
                           std::unordered_map<std::string, DagData>& jobs_map, 
                           std::string& rejection_reason);
    bool validate_dependencies(const json& data, 
                               std::unordered_map<std::string, DagData>& jobs_map, 
                               std::string& rejection_reason);
    boost::asio::awaitable<void> handle_request_rejection(ValidationResult& result, 
                                                          RequestData& request_info, 
                                                          const std::string& rejection_reason,
                                                          bool update_redis = true);
    boost::asio::awaitable<void> handle_request_accepted(ValidationResult& result, 
                                                         RequestData& request_info);
    boost::asio::awaitable<void> update_redis_request_status(const RequestData& request_info);                               

    static constexpr int DEFAULT_MAX_ACTIVE_WORKFLOWS = 10;
    static constexpr int DEFAULT_RATE_REQUESTS = 3;
    static constexpr int DEFAULT_RATE_WINDOW_SECONDS = 10;

    nlohmann::json_schema::json_validator validator_;
};

} // namespace flow_pilot

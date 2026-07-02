
// workflow_service.h - Workflow submission and validation for FlowPilot

#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <nlohmann/json.hpp>
#include <nlohmann/json-schema.hpp>
#include <boost/asio.hpp>

namespace flow_pilot {

using json = nlohmann::json;

class PolicyPlan;

struct ValidationResult {
    bool valid;
    std::string message;
    std::string errors;
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
    void validate_admission_client_workflow_policy(const json& workflow_data, const size_t workflow_size_bytes,
                                                   const PolicyPlan& policy_config, ValidationResult& result);
    void validate_semantic(const json& data, std::unordered_map<std::string, DagData>& jobs_map, 
                           ValidationResult& result);
    void validate_dependencies(const json& data, std::unordered_map<std::string, DagData>& jobs_map, 
                               ValidationResult& result);
    boost::asio::awaitable<void> update_redis_request_status(const std::string& client_id, const std::string& request_id, const std::string& status);                               

    static constexpr int DEFAULT_MAX_ACTIVE_WORKFLOWS = 10;
    static constexpr int DEFAULT_RATE_REQUESTS = 3;
    static constexpr int DEFAULT_RATE_WINDOW_SECONDS = 10;

    nlohmann::json_schema::json_validator validator_;
};

} // namespace flow_pilot

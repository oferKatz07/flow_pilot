// client_data_manager.h - Client data manager for FlowPilot

#pragma once

#include <unordered_map>
#include <string>
#include "client_data.h"

namespace flow_pilot {

class ClientDataManager {
public:
    ClientDataManager();

    const std::unordered_map<std::string, RateLimitConfig>& rate_limit_plans() const { return rate_limit_plans_; }
    const std::unordered_map<std::string, PolicyPlan>& policy_plans() const { return policy_plans_; }
    const std::unordered_map<std::string, ClientData>& clients() const { return clients_; }

private:
    std::unordered_map<std::string, RateLimitConfig> rate_limit_plans_;
    std::unordered_map<std::string, PolicyPlan> policy_plans_;
    std::unordered_map<std::string, ClientData> clients_;

    void load_from_db();
    void ensure_defaults();
};

// Helper to fetch the resolved client config (rate + policy) by client id
bool get_client_config(const std::string& client_id, ClientConfig& out);

} // namespace flow_pilot

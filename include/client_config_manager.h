// client_data_manager.h - Client data manager for FlowPilot

#pragma once

#include <unordered_map>
#include <string>

#include "client_config.h"
#include "client_config_manager_interface.h"

namespace flow_pilot {

class ClientConfigManager : public IClientConfigManager {
public:
    virtual ~ClientConfigManager() = default;
    ClientConfigManager() = default;

    const std::unordered_map<std::string, RateLimitConfig>& rate_limit_plans() const { return rate_limit_plans_; }
    const std::unordered_map<std::string, PolicyPlan>& policy_plans() const { return policy_plans_; }
    const std::unordered_map<std::string, ClientData>& clients() const { return clients_; }

protected:
    std::unordered_map<std::string, RateLimitConfig> rate_limit_plans_;
    std::unordered_map<std::string, PolicyPlan> policy_plans_;
    std::unordered_map<std::string, ClientData> clients_;

    virtual void load_from_db() = 0;
    virtual void ensure_defaults() = 0;
};

} // namespace flow_pilot

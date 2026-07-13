// client_test_config_manager.cpp - Implementation of ClientTestConfigManager

#include <memory>
#include <vector>

#include "logger.h"
#include "client_test_config_manager.h"

namespace flow_pilot {

ClientTestConfigManager& ClientTestConfigManager::get_instance() {
    static ClientTestConfigManager instance;

    return instance;
}

bool ClientTestConfigManager::get_client_config(const std::string& client_id, ClientConfig& out) const {
    out.client_id = client_id;
    out.rate_limit_config = default_plans::rplans[0]; // Default to "sandbox" plan
    out.policy_config = default_plans::pplans[0]; // Default to "sandbox" plan

    return true;
}

void ClientTestConfigManager::load_from_db() {
}

ClientTestConfigManager::ClientTestConfigManager() {
}

void ClientTestConfigManager::ensure_defaults() {
}

} // namespace flow_pilot

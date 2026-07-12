// client_config_manager_interface.h - Client configuration manager interface for FlowPilot

#pragma once

#include <string>

#include "client_config.h"

namespace flow_pilot {

class IClientConfigManager {
public:
    virtual ~IClientConfigManager() = default;
    virtual bool get_client_config(const std::string& client_id, ClientConfig& out) const = 0;
};

} // namespace flow_pilot

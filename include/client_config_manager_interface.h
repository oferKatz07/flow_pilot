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

namespace default_plans {
    // Default rate limit plans
    RateLimitConfig const rplans[] = {
        {"sandbox", 10, 60, 1},
        {"basic", 100, 60, 5},
        {"professional", 1000, 60, 10},
        {"enterprise", 10000, 60, 50}
    };
    
    // Default policy plans
    PolicyPlan const pplans[] = {
        // name,         wfz, jwf, js,   wfrt, wfr,jrt, jr, cj, pj, wfr, rr,  pr
        {"sandbox",      10,  5,   300,  300,   3,  60,  1, 2,  18,  1,   1,  1},
        {"basic",        20,  10,  500,  600,   10, 300, 2, 5,  55,  10,  10, 10},
        {"professional", 50,  50,  800,  6000,  20, 600, 3, 10, 90,  30,  30, 30},
        {"enterprise",   100, 100, 1024, 12000, 40, 900, 5, 20, 180, 60,  60, 60}
    };

} // namespace default_plans

} // namespace flow_pilot

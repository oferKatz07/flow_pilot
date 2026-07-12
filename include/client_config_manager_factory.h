// client_config_manager_factory.h - Factory for creating instances of ClientConfigManager for FlowPilot

#pragma once

#include <memory>

#include "client_config_manager_interface.h"

namespace flow_pilot {

class ClientConfigManagerFactory {
public:
    ~ClientConfigManagerFactory() = default;

    ClientConfigManagerFactory(const ClientConfigManagerFactory&) = delete;
    ClientConfigManagerFactory& operator=(const ClientConfigManagerFactory&) = delete;
    ClientConfigManagerFactory(ClientConfigManagerFactory&&) = delete;
    ClientConfigManagerFactory& operator=(ClientConfigManagerFactory&&) = delete;

    static IClientConfigManager& get();
private:
    ClientConfigManagerFactory() = default;

    static IClientConfigManager& create_client_config_manager();
};

} // namespace flow_pilot
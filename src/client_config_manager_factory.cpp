
// sqlite_db.cpp - SQLite persistence implementation for FlowPilot

#include "config.h"
#include "client_sqlite_config_manager.h"
#include "client_config_manager_factory.h"

namespace flow_pilot {

IClientConfigManager& ClientConfigManagerFactory::get() {
    static IClientConfigManager& config_manager = create_client_config_manager();

    return config_manager;
}

IClientConfigManager& ClientConfigManagerFactory::create_client_config_manager() {
    switch (Config::get().client_config().config_type) {
        case ClientDataConfig::ConfigManagerTypes::SQLITE_MANAGER:
            return ClientSqliteConfigManager::get_instance();
        case ClientDataConfig::ConfigManagerTypes::TEST_MANAGER:
            throw std::runtime_error("Test manager not implemented");
        default:
            throw std::runtime_error("Unsupported client config manager type");
    }
    
    // The code should not reach this point, but return a default instance to satisfy the compiler
    return ClientSqliteConfigManager::get_instance();
}

}  // namespace flow_pilot
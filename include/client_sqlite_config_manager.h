// client_sqlite_config_manager.h - Client config manager for FlowPilot

#pragma once

#include <unordered_map>
#include <string>

#include "client_config_manager.h"

namespace flow_pilot {

class ClientSqliteConfigManager : public ClientConfigManager {
public:
    virtual ~ClientSqliteConfigManager() = default;

    static ClientSqliteConfigManager& get_instance();

    bool get_client_config(const std::string& client_id, ClientConfig& out) const;

protected:
    void load_from_db();
    void ensure_defaults();
    
private:
    ClientSqliteConfigManager();
};

} // namespace flow_pilot

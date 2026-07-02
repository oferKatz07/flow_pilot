
// sqlite_db.cpp - SQLite persistence implementation for FlowPilot

#include "config.h"
#include "async_db.h"
#include "db_factory.h"



namespace flow_pilot {

DBFactory::DBFactory() {
    if (Config::get_config().db_config().db_type == DBConfig::DBType::ASYNC_SQLITE) {
        get_db_instance_ = []() -> IAsyncDatabase& {
            return AsyncDatabase::get_instance();
        };
    } else {
        throw std::runtime_error("Unsupported database type! not implemented yet");
    }
}

IAsyncDatabase& DBFactory::get_database() {
    static DBFactory instance;
    return instance.get_db_instance_();
}

}  // namespace flow_pilot
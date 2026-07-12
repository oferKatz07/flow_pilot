
// sqlite_db.cpp - SQLite persistence implementation for FlowPilot

#include "config.h"
#include "async_db.h"
#include "db_factory.h"

namespace flow_pilot {

IAsyncDatabase& DBFactory::get() {
    static IAsyncDatabase& instance = create_db();

    return instance;
}

IAsyncDatabase& DBFactory::create_db() {
    switch (Config::get().db_config().db_type) {
        case DBConfig::DBTypes::ASYNC_SQLITE:
            return AsyncDatabase::get_instance();
        default:
            throw std::runtime_error("Unsupported database type");
    }
    
    // The code should not reach this point, but return a default instance to satisfy the compiler
    return AsyncDatabase::get_instance();
}

}  // namespace flow_pilot
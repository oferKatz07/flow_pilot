// sqlite_db.h - SQLite persistence interface for FlowPilot

#pragma once

#include <memory>
#include <functional>

#include "async_db_interface.h"

namespace flow_pilot {

class DBFactory {
public:
    DBFactory(const DBFactory&) = delete;
    DBFactory& operator=(const DBFactory&) = delete;
    DBFactory(DBFactory&&) = delete;
    DBFactory& operator=(DBFactory&&) = delete;
    static IAsyncDatabase& get_database();
private:
    DBFactory();
    std::function<IAsyncDatabase&()> get_db_instance_;
};
} // namespace flow_pilot
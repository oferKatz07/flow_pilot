// sqlite_db.h - SQLite persistence interface for FlowPilot

#pragma once

#include "async_db_interface.h"

namespace flow_pilot {

class DBFactory {
public:
    DBFactory(const DBFactory&) = delete;
    DBFactory& operator=(const DBFactory&) = delete;
    DBFactory(DBFactory&&) = delete;
    DBFactory& operator=(DBFactory&&) = delete;
    static IAsyncDatabase& get();
private:
    DBFactory();
    static IAsyncDatabase& create_db();
};
} // namespace flow_pilot
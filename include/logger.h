
// logger.h - Logger initialization and access for FlowPilot

#pragma once

#include <memory>
#include <mutex>
#include <spdlog/spdlog.h>

namespace flow_pilot {

class Logger {
public:
    static void init();

    static inline std::shared_ptr<spdlog::logger> get_logger()
    {
        init();
        return logger_;
    }

private:
    static void initialize();

    static std::shared_ptr<spdlog::logger> logger_;
    static std::once_flag init_flag_;
};

inline void init_logger()
{
    Logger::init();
}

} // namespace flow_pilot

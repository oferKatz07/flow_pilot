
// logger.cpp - Logger implementation for FlowPilot

#include "logger.h"
#include "config.h"
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <filesystem>
#include <iostream>
#include <mutex>

namespace flow_pilot {

std::shared_ptr<spdlog::logger> Logger::logger_ = nullptr;
std::once_flag Logger::init_flag_;

void Logger::initialize()
{
    auto& config = Config::get().logger();

    // Convert our enum to spdlog level
    spdlog::level::level_enum spd_level;
    switch (config.level) {
        case LogLevel::DEBUG: spd_level = spdlog::level::debug; break;
        case LogLevel::INFO:  spd_level = spdlog::level::info; break;
        case LogLevel::WARN:  spd_level = spdlog::level::warn; break;
        case LogLevel::ERROR: spd_level = spdlog::level::err; break;
        default: spd_level = spdlog::level::info; break;
    }

    std::vector<spdlog::sink_ptr> sinks;

    // Add console sink if needed
    if (config.output == LogOutput::CONSOLE_ONLY || config.output == LogOutput::DUAL) {
        auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
        sinks.push_back(console_sink);
    }

    // Add file sink if needed
    if (config.output == LogOutput::FILE_ONLY || config.output == LogOutput::DUAL) {
        // Create logs directory if it doesn't exist
        std::filesystem::path log_path(config.filename);
        std::filesystem::path log_dir = log_path.parent_path();
        if (!log_dir.empty()) {
            std::error_code ec;
            std::filesystem::create_directories(log_dir, ec);
            if (ec) {
                std::cerr << "Warning: Failed to create log directory: " << ec.message() << std::endl;
            }
        }

        auto file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
            config.filename, config.max_file_size, config.max_files);
        sinks.push_back(file_sink);
    }

    if (sinks.empty()) {
        throw std::runtime_error("Logger initialization failed: no sinks configured");
    }

    logger_ = std::make_shared<spdlog::logger>("flow_pilot", sinks.begin(), sinks.end());
    if (!logger_) {
        throw std::runtime_error("Logger initialization failed: logger instance could not be created");
    }
    
    spdlog::register_logger(logger_);

    logger_->set_level(spd_level);
    logger_->set_pattern("[%Y-%m-%d %H:%M:%S] [%l] %v");

    // Flush on every message for immediate visibility
    logger_->flush_on(spdlog::level::info);
}

void Logger::init()
{
    std::call_once(init_flag_, initialize);
}

} // namespace flow_pilot

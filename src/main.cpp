
// main.cpp - Entry point for FlowPilot application

#include "http_server.h"
#include "workflow_admission_service.h"
#include "config.h"
#include "logger.h"
#include "redis_db_async.h"
#include "db_factory.h"
#include "flow_pilot_runtime.h"
#include <boost/asio/io_context.hpp>
#include <algorithm>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <string>
#include <pthread.h>
#include <stdexcept>
#include <thread>

int main(int argc, char* argv[])
{
    try {
        auto& server_config = flow_pilot::Config::get().server();
        auto& redis_config = flow_pilot::Config::get().redis();
        auto& sqlite_config = flow_pilot::Config::get().db_config();
        auto& thread_config = flow_pilot::Config::get().threads();

        for (int i = 1; i < argc; ++i) {
            std::string arg = argv[i];
            if (arg == "--port" && i + 1 < argc) {
                server_config.port = static_cast<unsigned short>(std::stoi(argv[++i]));
            } else if (arg == "--redis-host" && i + 1 < argc) {
                redis_config.host = argv[++i];
            } else if (arg == "--redis-port" && i + 1 < argc) {
                redis_config.port = static_cast<unsigned short>(std::stoi(argv[++i]));
            } else if (arg == "--redis-password" && i + 1 < argc) {
                redis_config.password = argv[++i];
            } else if (arg == "--redis-io-threads" && i + 1 < argc) {
                thread_config.redis_io_threads = std::max<std::size_t>(1, static_cast<std::size_t>(std::stoul(argv[++i])));
            } else if (arg == "--schedulers" && i + 1 < argc) {
                thread_config.scheduler_count = std::max<std::size_t>(1, static_cast<std::size_t>(std::stoul(argv[++i])));
            } else if (arg == "--completion-handlers" && i + 1 < argc) {
                thread_config.completion_handler_count = std::max<std::size_t>(1, static_cast<std::size_t>(std::stoul(argv[++i])));
            } else if (i == 1 && arg.rfind("--", 0) != 0) {
                server_config.port = static_cast<unsigned short>(std::stoi(arg));
            } else if (i == 2 && arg.rfind("--", 0) != 0) {
                auto pos = arg.find(':');
                if (pos != std::string::npos) {
                    redis_config.host = arg.substr(0, pos);
                    redis_config.port = static_cast<unsigned short>(std::stoi(arg.substr(pos + 1)));
                } else {
                    redis_config.host = arg;
                }
            } else if (i == 3 && arg.rfind("--", 0) != 0) {
                redis_config.port = static_cast<unsigned short>(std::stoi(arg));
            }
        }

        flow_pilot::Logger::get_logger()->info("Starting FlowPilot on port {}", server_config.port);
        flow_pilot::Logger::get_logger()->info("Redis endpoint {}:{}", redis_config.host, redis_config.port);
        flow_pilot::Logger::get_logger()->info("Thread configuration: redis_io_threads={}, schedulers={}, completion_handlers={}",
                                               thread_config.redis_io_threads,
                                               thread_config.scheduler_count,
                                               thread_config.completion_handler_count);

        sigset_t shutdown_signals;
        sigemptyset(&shutdown_signals);
        sigaddset(&shutdown_signals, SIGINT);
        sigaddset(&shutdown_signals, SIGTERM);
        if (pthread_sigmask(SIG_BLOCK, &shutdown_signals, nullptr) != 0) {
            throw std::runtime_error("Failed to block shutdown signals");
        }

        boost::asio::io_context ioc;
        flow_pilot::RedisDatabaseAsync::init(ioc, redis_config);
        flow_pilot::Logger::get_logger()->info("Redis initialized successfully");

        std::string redis_connection = redis_config.host + ":" + std::to_string(redis_config.port);
        flow_pilot::Logger::get_logger()->info("Workflow service initialized with Redis at {}", redis_connection);

        // Initialize the persistent database (currently SQLite) based on configuration
        auto& db = flow_pilot::DBFactory::get();
        flow_pilot::Logger::get_logger()->info("Database initialized successfully at {}", sqlite_config.db_path);

        flow_pilot::FlowPilotRuntime runtime(
            ioc,
            thread_config.redis_io_threads,
            thread_config.scheduler_count,
            thread_config.completion_handler_count);

        flow_pilot::run_http_server(ioc);

        runtime.start();

        std::thread signal_thread([&]() {
            int received_signal = 0;
            sigwait(&shutdown_signals, &received_signal);
            runtime.shutdown();
        });

        if (signal_thread.joinable()) {
            signal_thread.join();
        }
    }
    catch (const std::exception& ex) {
        std::cerr << "Application error: " << ex.what() << "\n";
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

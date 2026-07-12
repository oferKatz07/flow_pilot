
// main.cpp - Entry point for FlowPilot application

#include "http_server.h"
#include "workflow_service.h"
#include "config.h"
#include "logger.h"
#include "redis_db_async.h"
#include "db_factory.h"
#include <boost/asio/signal_set.hpp>
#include <boost/asio/io_context.hpp>
#include <cstdlib>
#include <iostream>

int main(int argc, char* argv[])
{
    try {
        auto& server_config = flow_pilot::Config::get().server();
        auto& redis_config = flow_pilot::Config::get().redis();
        auto& sqlite_config = flow_pilot::Config::get().db_config();

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

        flow_pilot::get_logger()->info("Starting FlowPilot on port {}", server_config.port);
        flow_pilot::get_logger()->info("Redis endpoint {}:{}", redis_config.host, redis_config.port);

        boost::asio::io_context ioc;
        flow_pilot::RedisDatabaseAsync::init(ioc, redis_config);
        flow_pilot::get_logger()->info("Redis initialized successfully");

        std::string redis_connection = redis_config.host + ":" + std::to_string(redis_config.port);
        flow_pilot::get_logger()->info("Workflow service initialized with Redis at {}", redis_connection);

        // Initialize the persistent database (currently SQLite) based on configuration
        auto& db = flow_pilot::DBFactory::get();
        flow_pilot::get_logger()->info("Database initialized successfully at {}", sqlite_config.db_path);

        boost::asio::signal_set signals(ioc, SIGINT, SIGTERM);
        signals.async_wait([&](boost::system::error_code const&, int) {
            ioc.stop();
        });

        flow_pilot::run_http_server(ioc);

        ioc.run();
    }
    catch (const std::exception& ex) {
        std::cerr << "Application error: " << ex.what() << "\n";
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}


// config.h - Global configuration for FlowPilot

#pragma once

#include <string>

namespace flow_pilot {

// Logger Configuration
enum class LogLevel {
    DEBUG,
    INFO,
    WARN,
    ERROR
};

enum class LogOutput {
    CONSOLE_ONLY,
    FILE_ONLY,
    DUAL
};

// Set default values for HTTP server configuration
struct ServerConfig {
    std::string address = "0.0.0.0";
    unsigned short port = 8080;
    std::size_t max_request_body_size = 100 * 1024; // 100 KB
};

// Set default values for logger configuration
struct LoggerConfig {
    LogOutput output = LogOutput::DUAL;
    LogLevel level = LogLevel::INFO;
    std::string filename = "../logs/flow_pilot.log";
    size_t max_file_size = 1024 * 1024; // 1 MB
    size_t max_files = 3; // Keep 3 rotated files
};

// Set default values for in-memory database (Redis) configuration
struct InMemoryDBConfig {
    std::string host = "127.0.0.1";
    unsigned short port = 6379;
    std::string password;
    unsigned short key_retention_ttl = 900;
};

// set default values for the database configuration
struct DBConfig {
    enum class DBTypes {
        ASYNC_SQLITE,
        POSTGRESQL
    };

    DBTypes db_type = DBTypes::ASYNC_SQLITE;
    std::string db_path = "db/flow_pilot.db";
};

struct ClientDataConfig {
    enum class ConfigManagerTypes{
        SQLITE_MANAGER,
        TEST_MANAGER
    };

    ConfigManagerTypes config_type = ConfigManagerTypes::SQLITE_MANAGER;
};

// Workflow default configuration
struct WorkflowConfig {
    std::string version = "v1.0";
    std::string workflow_schema_path = "schema/workflow_request.schema.json";
};

// Global configuration instance
class Config {
public:
    static Config& get() {
        static Config instance;
        return instance;
    }

    // HTTP Server config (returns reference to ServerConfig)
    ServerConfig& server( ) {return server_config_; }
    const ServerConfig& server() const { return server_config_; }

    InMemoryDBConfig& redis() { return redis_config_; }
    const InMemoryDBConfig& redis() const { return redis_config_; }

    DBConfig& db_config() { return db_config_; }
    const DBConfig& db_config() const { return db_config_; }

    ClientDataConfig& client_config() { return client_config_; }
    const ClientDataConfig& client_config() const { return client_config_; }

    WorkflowConfig& workflow() { return workflow_config_; }
    const WorkflowConfig& workflow() const { return workflow_config_; }

    // Logger config
    LoggerConfig& logger() { return logger_config_; }
    const LoggerConfig& logger() const { return logger_config_; }

private:
    Config() = default;
    ~Config() = default;
    Config(const Config&) = delete;
    Config& operator=(const Config&) = delete;

    ServerConfig server_config_;
    LoggerConfig logger_config_;
    InMemoryDBConfig redis_config_;
    DBConfig db_config_;
    ClientDataConfig client_config_;
    WorkflowConfig workflow_config_;
};

} // namespace flow_pilot

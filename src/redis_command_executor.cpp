// redis_command_executor.cpp - Generic Redis command helpers for FlowPilot

#include <utility>

#include "logger.h"
#include "redis_command_executor.h"

namespace flow_pilot {

RedisCommandExecutor::RedisCommandExecutor(ExecuteFunction execute)
    : execute_(std::move(execute))
{
}

boost::asio::awaitable<bool> RedisCommandExecutor::execute_integer_command_async(const std::vector<std::string>& args,
                                                                                 long long& value) const
{
    try {
        auto reply = co_await execute_(args);
        if (reply.type == RedisReply::Type::Integer) {
            value = reply.integer_value;
            co_return true;
        }
        if (reply.type == RedisReply::Type::SimpleString) {
            try {
                value = std::stoll(reply.string_value);
                co_return true;
            } catch (...) {
                co_return false;
            }
        }
    } catch (const std::exception& ex) {
        Logger::get_logger()->error("Redis integer command failed: {}", ex.what());
    }
    co_return false;
}

boost::asio::awaitable<bool> RedisCommandExecutor::execute_bulk_string_command_async(const std::vector<std::string>& args,
                                                                                     std::string& value) const
{
    try {
        auto reply = co_await execute_(args);
        if (reply.type == RedisReply::Type::BulkString || reply.type == RedisReply::Type::SimpleString) {
            value = reply.string_value;
            co_return true;
        }
        co_return false;
    } catch (const std::exception& ex) {
        Logger::get_logger()->error("Redis bulk string command failed: {}", ex.what());
    }
    co_return false;
}

boost::asio::awaitable<bool> RedisCommandExecutor::execute_lua_script_async(const std::string& script,
                                                                            const std::vector<std::string>& keys,
                                                                            const std::vector<std::string>& args,
                                                                            std::vector<std::string>& values) const
{
    try {
        std::vector<std::string> redis_args;
        redis_args.reserve(3 + keys.size() + args.size());
        redis_args.push_back("EVAL");
        redis_args.push_back(script);
        redis_args.push_back(std::to_string(keys.size()));
        redis_args.insert(redis_args.end(), keys.begin(), keys.end());
        redis_args.insert(redis_args.end(), args.begin(), args.end());

        auto reply = co_await execute_(redis_args);
        if (reply.type == RedisReply::Type::Array) {
            values = reply.array_value;
            co_return true;
        }
    } catch (const std::exception& ex) {
        Logger::get_logger()->error("Redis Lua script failed: {}", ex.what());
    }
    values.clear();
    co_return false;
}

boost::asio::awaitable<bool> RedisCommandExecutor::execute_set_command_async(const std::string& key,
                                                                             const std::string& value,
                                                                             int expire_seconds,
                                                                             bool only_if_not_exists) const
{
    try {
        std::vector<std::string> args = {"SET", key, value};
        if (only_if_not_exists) {
            args.push_back("NX");
        }
        if (expire_seconds > 0) {
            args.push_back("EX");
            args.push_back(std::to_string(expire_seconds));
        }

        auto reply = co_await execute_(args);
        if (reply.type == RedisReply::Type::SimpleString && reply.string_value == "OK") {
            co_return true;
        }
        co_return false;
    } catch (const std::exception& ex) {
        Logger::get_logger()->error("Redis SET command failed: {}", ex.what());
    }
    co_return false;
}

boost::asio::awaitable<bool> RedisCommandExecutor::execute_mget_command_async(const std::vector<std::string>& keys,
                                                                              std::vector<std::string>& values) const
{
    try {
        auto args = std::vector<std::string>{"MGET"};
        args.insert(args.end(), keys.begin(), keys.end());
        auto reply = co_await execute_(args);
        if (reply.type == RedisReply::Type::Array) {
            values = reply.array_value;
            co_return true;
        }
    } catch (const std::exception& ex) {
        Logger::get_logger()->error("Redis MGET command failed: {}", ex.what());
    }
    values.clear();
    co_return false;
}

boost::asio::awaitable<bool> RedisCommandExecutor::execute_hset_command_async(
    const std::string& key,
    const std::unordered_map<std::string, std::string>& fields) const
{
    if (fields.empty()) {
        co_return false;
    }

    try {
        std::vector<std::string> args;
        args.reserve(2 + fields.size() * 2);
        args.push_back("HSET");
        args.push_back(key);
        for (const auto& [field, value] : fields) {
            args.push_back(field);
            args.push_back(value);
        }

        auto reply = co_await execute_(args);
        if (reply.type == RedisReply::Type::Integer || reply.type == RedisReply::Type::SimpleString) {
            co_return true;
        }
    } catch (const std::exception& ex) {
        Logger::get_logger()->error("Redis HSET command failed: {}", ex.what());
    }
    co_return false;
}

boost::asio::awaitable<bool> RedisCommandExecutor::execute_hgetall_command_async(
    const std::string& key,
    std::unordered_map<std::string, std::string>& fields) const
{
    try {
        std::vector<std::string> args{"HGETALL", key};
        auto reply = co_await execute_(args);
        if (reply.type == RedisReply::Type::Array) {
            fields.clear();
            for (size_t i = 0; i + 1 < reply.array_value.size(); i += 2) {
                fields[reply.array_value[i]] = reply.array_value[i + 1];
            }
            co_return true;
        }
    } catch (const std::exception& ex) {
        Logger::get_logger()->error("Redis HGETALL command failed: {}", ex.what());
    }
    fields.clear();
    co_return false;
}

boost::asio::awaitable<bool> RedisCommandExecutor::execute_smembers_command_async(
    const std::string& key,
    std::vector<std::string>& values) const
{
    try {
        std::vector<std::string> args{"SMEMBERS", key};
        auto reply = co_await execute_(args);
        if (reply.type == RedisReply::Type::Array) {
            values = reply.array_value;
            co_return true;
        }
    } catch (const std::exception& ex) {
        Logger::get_logger()->error("Redis SMEMBERS command failed: {}", ex.what());
    }
    values.clear();
    co_return false;
}

boost::asio::awaitable<bool> RedisCommandExecutor::execute_rpush_command_async(
    const std::string& key,
    const std::string& value) const
{
    try {
        std::vector<std::string> args{"RPUSH", key, value};
        auto reply = co_await execute_(args);
        if (reply.type == RedisReply::Type::Integer) {
            co_return true;
        }
    } catch (const std::exception& ex) {
        Logger::get_logger()->error("Redis RPUSH command failed: {}", ex.what());
    }
    co_return false;
}

boost::asio::awaitable<bool> RedisCommandExecutor::execute_lpop_command_async(
    const std::string& key,
    std::string& value) const
{
    try {
        std::vector<std::string> args{"LPOP", key};
        auto reply = co_await execute_(args);
        if (reply.type == RedisReply::Type::BulkString || reply.type == RedisReply::Type::SimpleString) {
            value = reply.string_value;
            co_return true;
        }
        if (reply.type == RedisReply::Type::Nil) {
            value.clear();
            co_return true;
        }
    } catch (const std::exception& ex) {
        Logger::get_logger()->error("Redis LPOP command failed: {}", ex.what());
    }
    value.clear();
    co_return false;
}

boost::asio::awaitable<bool> RedisCommandExecutor::execute_list_add_command_async(
    const std::string& key,
    const std::vector<std::string>& values) const
{
    try {
        if (values.empty()) {
            co_return true;
        }

        std::vector<std::string> args;
        args.reserve(2 + values.size());
        args.push_back("RPUSH");
        args.push_back(key);
        for (const auto& v : values) {
            args.push_back(v);
        }

        auto reply = co_await execute_(args);
        if (reply.type == RedisReply::Type::Integer) {
            co_return true;
        }
    } catch (const std::exception& ex) {
        Logger::get_logger()->error("Redis RPUSH (list set) command failed: {}", ex.what());
    }
    co_return false;
}

boost::asio::awaitable<bool> RedisCommandExecutor::execute_list_get_command_async(
    const std::string& key,
    std::vector<std::string>& values) const
{
    try {
        std::vector<std::string> args{"LRANGE", key, "0", "-1"};
        auto reply = co_await execute_(args);
        if (reply.type == RedisReply::Type::Array) {
            values = reply.array_value;
            co_return true;
        }
        if (reply.type == RedisReply::Type::Nil) {
            values.clear();
            co_return true;
        }
    } catch (const std::exception& ex) {
        Logger::get_logger()->error("Redis LRANGE (list get) command failed: {}", ex.what());
    }
    values.clear();
    co_return false;
}

boost::asio::awaitable<bool> RedisCommandExecutor::execute_list_len_command_async(const std::string& key,
                                                                                  long long& length) const
{
    std::vector<std::string> args{"LLEN", key};
    auto ok = co_await execute_integer_command_async(args, length);
    if (!ok) {
        length = 0;
    }
    co_return ok;
}

boost::asio::awaitable<bool> RedisCommandExecutor::execute_zset_enqueue_command_async(
    const std::string& key,
    const std::unordered_map<std::string, unsigned int>& members) const
{
    try {
        if (members.empty()) {
            co_return true;
        }

        std::vector<std::string> args;
        args.reserve(3 + members.size() * 2);
        args.push_back("ZADD");
        args.push_back(key);
        args.push_back("NX");
        for (const auto& [member, score] : members) {
            args.push_back(std::to_string(score));
            args.push_back(member);
        }

        auto reply = co_await execute_(args);
        if (reply.type == RedisReply::Type::Integer) {
            co_return true;
        }
    } catch (const std::exception& ex) {
        Logger::get_logger()->error("execute_zset_enqueue_command_async - Redis ZADD command failed: {}", ex.what());
    }
    co_return false;
}

boost::asio::awaitable<bool> RedisCommandExecutor::execute_zset_remove_command_async(
    std::string key,
    std::vector<std::string> members) const
{
    try {
        if (members.empty()) {
            co_return true;
        }

        std::vector<std::string> args;
        args.reserve(2 + members.size());
        args.push_back("ZREM");
        args.push_back(key);
        args.insert(args.end(), members.begin(), members.end());

        auto reply = co_await execute_(args);
        if (reply.type == RedisReply::Type::Integer) {
            co_return true;
        }
    } catch (const std::exception& ex) {
        Logger::get_logger()->error("execute_zset_remove_command_async - Redis ZREM command failed: {}", ex.what());
    }
    co_return false;
}

boost::asio::awaitable<bool> RedisCommandExecutor::execute_zset_dequeue_command_async(std::string key,
                                                                                      std::string& member) const
{
    try {
        std::vector<std::string> args{"ZPOPMAX", key};
        auto reply = co_await execute_(args);
        if (reply.type == RedisReply::Type::Array) {
            if (reply.array_value.empty()) {
                // Set is empty
                member.clear();
                co_return true;
            }

            member = reply.array_value.front();
            co_return true;
        }
    } catch (const std::exception& ex) {
        Logger::get_logger()->error("execute_zset_dequeue_command_async - Redis ZPOPMAX command failed: {}", ex.what());
    }
    member.clear();
    co_return false;
}

} // namespace flow_pilot

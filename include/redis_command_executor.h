// redis_command_executor.h - Generic Redis command helpers for FlowPilot

#pragma once

#include <boost/asio/awaitable.hpp>
#include <functional>
#include <string>
#include <unordered_map>
#include <vector>

#include "redis_base.h"

namespace flow_pilot {

class RedisCommandExecutor {
public:
    using ExecuteFunction = std::function<boost::asio::awaitable<RedisReply>(const std::vector<std::string>& args)>;

    explicit RedisCommandExecutor(ExecuteFunction execute);

    boost::asio::awaitable<bool> execute_lua_script_async(const std::string& script,
                                                          const std::vector<std::string>& keys,
                                                          const std::vector<std::string>& args,
                                                          std::vector<std::string>& values) const;
    boost::asio::awaitable<bool> execute_integer_command_async(const std::vector<std::string>& args,
                                                               long long& value) const;
    boost::asio::awaitable<bool> execute_mget_command_async(const std::vector<std::string>& keys,
                                                            std::vector<std::string>& values) const;
    boost::asio::awaitable<bool> execute_bulk_string_command_async(const std::vector<std::string>& args,
                                                                   std::string& value) const;
    boost::asio::awaitable<bool> execute_set_command_async(const std::string& key,
                                                           const std::string& value,
                                                           int expire_seconds,
                                                           bool only_if_not_exists) const;
    boost::asio::awaitable<bool> execute_hset_command_async(const std::string& key,
                                                            const std::unordered_map<std::string, std::string>& fields) const;
    boost::asio::awaitable<bool> execute_hgetall_command_async(const std::string& key,
                                                               std::unordered_map<std::string, std::string>& fields) const;
    boost::asio::awaitable<bool> execute_smembers_command_async(const std::string& key,
                                                                std::vector<std::string>& values) const;
    boost::asio::awaitable<bool> execute_rpush_command_async(const std::string& key,
                                                             const std::string& value) const;
    boost::asio::awaitable<bool> execute_lpop_command_async(const std::string& key,
                                                            std::string& value) const;
    boost::asio::awaitable<bool> execute_list_add_command_async(const std::string& key,
                                                                const std::vector<std::string>& values) const;
    boost::asio::awaitable<bool> execute_list_get_command_async(const std::string& key,
                                                                std::vector<std::string>& values) const;
    boost::asio::awaitable<bool> execute_list_len_command_async(const std::string& key,
                                                                long long& length) const;
    boost::asio::awaitable<bool> execute_zset_enqueue_command_async(const std::string& key,
                                                                    const std::unordered_map<std::string, unsigned int>& members) const;
    boost::asio::awaitable<bool> execute_zset_remove_command_async(std::string key,
                                                                   std::vector<std::string> members) const;
    boost::asio::awaitable<bool> execute_zset_dequeue_command_async(std::string key,
                                                                    std::string& member) const;
    boost::asio::awaitable<bool> execute_zset_blocking_dequeue_command_async(std::string key,
                                                                             std::string& member,
                                                                             int timeout_seconds = 0) const;

private:
    ExecuteFunction execute_;
};

} // namespace flow_pilot

#ifndef VIEW_SEARCH_COMMAND_H
#define VIEW_SEARCH_COMMAND_H

#include "redismodule.h"
#include <unordered_map>
#include <json.hpp>

using json = nlohmann::json;

int View_Search_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
void View_Search_Handler(RedisModuleCtx *ctx, std::unordered_map<long long int, std::unordered_map<std::string, json>> &query_2_value ,
                             std::unordered_map<std::string, std::unordered_map<long long int, std::vector<std::string>>> &client_2_query);
void Start_View_Search_Handler(RedisModuleCtx *ctx);

#endif /* VIEW_SEARCH_COMMAND_H */
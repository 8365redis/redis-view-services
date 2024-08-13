#ifndef VIEW_SEARCH_2_COMMAND_H
#define VIEW_SEARCH_2_COMMAND_H

#include <unordered_map>
#include "redis/redismodule.h"
#include "json/json.hpp"
#include "module_data_handler.h"
#include <hiredis/hiredis.h>

using json = nlohmann::json;

int View_Search_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
void View_Search_Handler(RedisModuleCtx *ctx);
void Start_View_Search_Handler(RedisModuleCtx *ctx);
void Forward_Request_To_Replication_Node(RedisModuleCtx *ctx, std::unordered_map<std::string, IP_Address_Port> nodes, 
                                            std::string client, std::string arguments, bool inner_key_given, long long int query_id, std::string inner_key_arg_prefix);
void Handler_Forward_Request(RedisModuleCtx *ctx, IP_Address_Port node, std::string client, std::string arguments, bool inner_key_given, long long int query_id, std::string inner_key_arg_prefix);
void Diff_View(RedisModuleCtx *ctx, std::vector<std::string> &view_new_keys, std::unordered_map<std::string, nlohmann::json> &new_values,
                    long long int current_query_id, json &diff_values_json_array, int &diff_size);
void Print_Response(redisReply *reply);
#endif /* VIEW_SEARCH_2_COMMAND_H */
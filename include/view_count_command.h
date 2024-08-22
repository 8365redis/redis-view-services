#ifndef MAIN_SEARCH_COMMAND_H
#define MAIN_SEARCH_COMMAND_H


#include "redis/redismodule.h"
#include "module_data_handler.h"

int Search_Count_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
void Start_Search_Count_Handler(RedisModuleCtx *ctx);
void Search_Count_Handler(RedisModuleCtx *ctx);
void Forward_Request_To_Replication_Node(RedisModuleCtx *ctx, std::unordered_map<std::string, IP_Address_Port> nodes, Client_Size_Info &info);
void Handler_Forward_Request(RedisModuleCtx *ctx, IP_Address_Port node, Client_Size_Info &info);                                    

#endif /* MAIN_SEARCH_COMMAND_H */
#ifndef MAIN_SEARCH_COMMAND_H
#define MAIN_SEARCH_COMMAND_H

#include <vector>
#include "redismodule.h"
#include "module_constants.h"

int Main_Search_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
void Start_Main_Search_Handler(RedisModuleCtx *ctx);
void Main_Search_Handler(RedisModuleCtx *ctx, std::vector<Client_Size_Info> &client_size_info_arg);

#endif /* MAIN_SEARCH_COMMAND_H */
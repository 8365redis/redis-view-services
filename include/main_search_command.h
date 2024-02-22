#ifndef MAIN_SEARCH_COMMAND_H
#define MAIN_SEARCH_COMMAND_H

#include <vector>
#include "redismodule.h"
#include "module_data_handler.h"

int Main_Search_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
void Start_Main_Search_Handler(RedisModuleCtx *ctx);
void Main_Search_Handler(RedisModuleCtx *ctx);

#endif /* MAIN_SEARCH_COMMAND_H */
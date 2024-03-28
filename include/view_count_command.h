#ifndef MAIN_SEARCH_COMMAND_H
#define MAIN_SEARCH_COMMAND_H

#include <vector>
#include "redismodule.h"
#include "module_data_handler.h"

int Search_Count_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
void Start_Search_Count_Handler(RedisModuleCtx *ctx);
void Search_Count_Handler(RedisModuleCtx *ctx);

#endif /* MAIN_SEARCH_COMMAND_H */
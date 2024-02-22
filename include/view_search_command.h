#ifndef VIEW_SEARCH_COMMAND_H
#define VIEW_SEARCH_COMMAND_H

#include "redismodule.h"
#include <unordered_map>
#include <json.hpp>

using json = nlohmann::json;

int View_Search_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
void View_Search_Handler(RedisModuleCtx *ctx);
void Start_View_Search_Handler(RedisModuleCtx *ctx);

#endif /* VIEW_SEARCH_COMMAND_H */
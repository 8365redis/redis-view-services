#ifndef MODULE_UTILS_H
#define MODULE_UTILS_H

#include <string>
#include "redismodule.h"


std::string Get_Client_Name(RedisModuleCtx *ctx);
std::string Get_Client_Name_From_ID(RedisModuleCtx *ctx, unsigned long long client_id);
void Print_Status(RedisModuleCtx *ctx, std::string optional = "");

#endif /* MODULE_UTILS_H */
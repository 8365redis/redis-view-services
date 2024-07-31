#ifndef MODULE_UTILS_H
#define MODULE_UTILS_H

#include <string>
#include <vector>
#include "redismodule.h"


std::string Get_Client_Name(RedisModuleCtx *ctx);
std::string Get_Client_Name_From_ID(RedisModuleCtx *ctx, unsigned long long client_id);
void Print_Status(RedisModuleCtx *ctx, std::string optional = "");
std::string Vector_2_String(std::vector<std::string> vec);

#endif /* MODULE_UTILS_H */
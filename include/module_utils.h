#ifndef MODULE_UTILS_H
#define MODULE_UTILS_H

#include <string>
#include <vector>
#include "redis/redismodule.h"
#include <hiredis/hiredis.h>

std::string Get_Client_Name(RedisModuleCtx *ctx);
std::string Get_Client_Name_From_ID(RedisModuleCtx *ctx, unsigned long long client_id);
void Print_Status(RedisModuleCtx *ctx, std::string optional = "");
std::string Vector_2_String(std::vector<std::string> vec);
void Print_Response(redisReply *reply);
void Get_Keys_From_Array(RedisModuleCtx *ctx, RedisModuleCallReply *array , std::vector<std::string> &keys, std::string kn , std::string inner_key_arg_prefix);
void Remove_Outer_Paranthesis(std::string &input);

#endif /* MODULE_UTILS_H */
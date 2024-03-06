#ifndef UNREGISTER_COMMAND_H
#define UNREGISTER_COMMAND_H

#include "redismodule.h"

int UnRegister_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int Unregister_Client(RedisModuleCtx *ctx, std::string client_to_unregister);

#endif /* UNREGISTER_COMMAND_H */
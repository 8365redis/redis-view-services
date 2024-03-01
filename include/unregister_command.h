#ifndef UNREGISTER_COMMAND_H
#define UNREGISTER_COMMAND_H

#include "redismodule.h"

int UnRegister_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

#endif /* UNREGISTER_COMMAND_H */
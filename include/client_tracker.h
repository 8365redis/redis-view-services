#ifndef CLIENT_TRACKER_H
#define CLIENT_TRACKER_H

#include "redismodule.h"
#include <string>
#include <unordered_map>


void Handle_Client_Event(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data);
void Start_Client_Handler(RedisModuleCtx *ctx);
void Client_TTL_Handler(RedisModuleCtx *ctx);


#endif /* CLIENT_TRACKER_H */
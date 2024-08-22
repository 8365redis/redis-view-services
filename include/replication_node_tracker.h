#ifndef REPLICATION_NODE_TRACKER_H
#define REPLICATION_NODE_TRACKER_H

#include <unordered_map>
#include "redis/redismodule.h"
#include "module_data_handler.h"

void Get_Replication_Nodes(RedisModuleCtx *ctx, std::unordered_map<std::string, IP_Address_Port> &replication_nodes_search);
int Get_Master_Redis_Port(RedisModuleCtx *ctx);

#endif /* REPLICATION_NODE_TRACKER_H */
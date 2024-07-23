#ifndef MODULE_MONITORING_H
#define MODULE_MONITORING_H

#include <string>
#include <vector>

#include "redismodule.h"

const unsigned long DEFAULT_RETENTION = 1000 * 60 * 60 * 24 * 7; // Default retenetion 7 day

bool Create_Metric_Stream(RedisModuleCtx *ctx, std::string stream_name, std::vector<std::pair<std::string,std::string>> tags , unsigned long retention = DEFAULT_RETENTION);

#endif /* MODULE_MONITORING_H */
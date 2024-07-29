#ifndef MODULE_MONITORING_H
#define MODULE_MONITORING_H

#include <string>
#include <vector>

#include "redismodule.h"

const unsigned long DEFAULT_RETENTION = 1000 * 60 * 60 * 24 * 7; // Default retention 7 days
bool Create_Client_Monitoring(RedisModuleCtx *ctx, std::string client_name);
bool Create_Metric_Stream(RedisModuleCtx *ctx, std::string stream_name, std::vector<std::pair<std::string,std::string>> tags , unsigned long retention = DEFAULT_RETENTION);
bool Add_Metric(RedisModuleCtx *ctx, std::string metric_name, std::string metric_value);
bool Add_Count_Metric(RedisModuleCtx *ctx, std::string metric_name, bool inc = true);
bool Reset_Count(RedisModuleCtx *ctx, std::string metric_name);
std::chrono::steady_clock::time_point Get_Start_Time();
double Get_Delta_Time(std::chrono::steady_clock::time_point start);

const std::string LATENCY_TYPE_LABEL_KEY = "LATENCY_TYPE";
const std::string COUNT_TYPE_LABEL_KEY = "COUNT_TYPE";

const std::string VIEW_COUNT_LATENCY_METRIC_STREAM_PREFIX = "VIEW_COUNT_LATENCY:";
const std::string VIEW_COUNT_LATENCY_CLIENT_LABEL_KEY = "VIEW_COUNT_LATENCY_CLIENT";
const std::string LATENCY_TYPE_VIEW_COUNT = "VIEW_COUNT";

const std::string VIEW_COUNT_COUNT_METRIC_STREAM_PREFIX = "VIEW_COUNT_COUNT:";
const std::string VIEW_COUNT_COUNT_CLIENT_LABEL_KEY = "VIEW_COUNT_COUNT_CLIENT";
const std::string COUNT_TYPE_VIEW_COUNT = "VIEW_COUNT";

const std::string VIEW_SEARCH_LATENCY_METRIC_STREAM_PREFIX = "VIEW_SEARCH_LATENCY:";
const std::string VIEW_SEARCH_LATENCY_CLIENT_LABEL_KEY = "VIEW_SEARCH_LATENCY_CLIENT";
const std::string LATENCY_TYPE_VIEW_SEARCH = "VIEW_SEARCH";

const std::string VIEW_SEARCH_COUNT_METRIC_STREAM_PREFIX = "VIEW_SEARCH_COUNT:";
const std::string VIEW_SEARCH_COUNT_CLIENT_LABEL_KEY = "VIEW_SEARCH_COUNT_CLIENT";
const std::string COUNT_TYPE_VIEW_SEARCH = "VIEW_SEARCH";

#endif /* MODULE_MONITORING_H */
#ifndef MODULE_MONITORING_H
#define MODULE_MONITORING_H

#include <string>
#include <vector>
#include "redis/redismodule.h"

#include "module_data_handler.h"

bool Add_Metric(RedisModuleCtx *ctx, unsigned long long metric_ts, std::string metric_name, std::string metric_value, std::vector<std::pair<std::string,std::string>> label_pairs);
bool Add_Replication_Metric(RedisModuleCtx *ctx, unsigned long long metric_ts, std::string metric_name, std::string metric_value, std::vector<std::pair<std::string,std::string>> label_pairs);

bool Add_Count_Metric(RedisModuleCtx *ctx, std::string metric_name, std::vector<std::pair<std::string,std::string>> label_pairs, bool inc = true);
bool Reset_Count(RedisModuleCtx *ctx, std::string metric_name);

std::chrono::steady_clock::time_point Get_Start_Time();
double Get_Delta_Time(std::chrono::steady_clock::time_point start);
unsigned long long Get_Epoch_Time();

std::vector<std::pair<std::string,std::string>> Create_Latency_Labels(std::string client, std::string search_type);
std::vector<std::pair<std::string,std::string>> Create_Replica_Latency_Labels(std::string client, std::string search_type, std::string replica);
std::vector<std::pair<std::string,std::string>> Create_Handler_Latency_Label();
std::vector<std::pair<std::string,std::string>> Create_Count_Labels(std::string client, std::string search_type);

const std::string LATENCY_TYPE_LABEL_KEY = "LATENCY_TYPE";
const std::string LATENCY_TYPE_VIEW_SEARCH = "VIEW_SEARCH";
const std::string LATENCY_TYPE_VIEW_SEARCH_HANDLER = "VIEW_SEARCH_HANDLER";
const std::string LATENCY_TYPE_VIEW_COUNT = "VIEW_COUNT";
const std::string LATENCY_TYPE_VIEW_COUNT_HANDLER = "VIEW_COUNT_HANDLER";


const std::string COUNT_TYPE_LABEL_KEY = "COUNT_TYPE";
const std::string COUNT_TYPE_VIEW_COUNT = "VIEW_COUNT";
const std::string COUNT_TYPE_VIEW_SEARCH = "VIEW_SEARCH";

const std::string CLIENT_LABEL_KEY = "CLIENT";
const std::string REPLICA_LABEL_KEY = "REPLICA";

const std::string VIEW_COUNT_LATENCY_METRIC_STREAM_PREFIX = "VIEW_COUNT_LATENCY:";
const std::string VIEW_COUNT_HANDLER_LATENCY_METRIC_STREAM_PREFIX = "VIEW_COUNT_HANDLER_LATENCY:";

const std::string VIEW_COUNT_COUNT_METRIC_STREAM_PREFIX = "VIEW_COUNT_COUNT:";

const std::string VIEW_SEARCH_LATENCY_METRIC_STREAM_PREFIX = "VIEW_SEARCH_LATENCY:";
const std::string VIEW_SEARCH_HANDLER_LATENCY_METRIC_STREAM_PREFIX = "VIEW_SEARCH_HANDLER_LATENCY:";

const std::string VIEW_SEARCH_HANDLER_REPLICA_LATENCY_METRIC_STREAM_PREFIX = "VIEW_SEARCH_HANDLER_REPLICA_LATENCY:";
const std::string VIEW_COUNT_HANDLER_REPLICA_LATENCY_METRIC_STREAM_PREFIX = "VIEW_COUNT_HANDLER_REPLICA_LATENCY:";

const std::string VIEW_SEARCH_COUNT_METRIC_STREAM_PREFIX = "VIEW_SEARCH_COUNT:";

const std::string VIEW_SEARCH_HANDLER_TOTAL_LATENCY_METRIC_STREAM_PREFIX = "VIEW_SEARCH_HANDLER_TOTAL_LATENCY";
const std::string VIEW_COUNT_HANDLER_TOTAL_LATENCY_METRIC_STREAM_PREFIX = "VIEW_COUNT_HANDLER_TOTAL_LATENCY";

#endif /* MODULE_MONITORING_H */
#ifndef MODULE_MONITORING_H
#define MODULE_MONITORING_H

#include <string>
#include <vector>
#include "redis/redismodule.h"

bool Add_Metric(RedisModuleCtx *ctx, unsigned long long metric_ts, std::string name, double value, std::string value_type, std::string module, std::string version, std::string command, const std::map<std::string, std::string>& tags);
bool Add_Replication_Metric(RedisModuleCtx *ctx, unsigned long long metric_ts, std::string name, double value, std::string value_type, std::string module, std::string version, std::string command, const std::map<std::string, std::string>& tags);

std::chrono::steady_clock::time_point Get_Start_Time();
double Get_Delta_Time(std::chrono::steady_clock::time_point start);
unsigned long long Get_Epoch_Time();

const std::string METRIC_VALUE_TYPE_NS = "ns";

const std::string OPENTRACING_STREAM_NAME = "OPENTRACING";

const std::string OPENTRACING_PREFIX = "OPENTRACING";
const std::string OPENTRACING_DELIMETER = ":";

const std::string OPENTRACING_METRIC_KEY_NAME = "TRACING";

const std::string METRIC_TAGS_KEY = "tags";
const std::string METRIC_METRIC_KEY = "metric";

const std::string METRIC_NAME_LATENCY = "latency";

const std::string COMMAND_TYPE_KEY = "command_type";
const std::string LATENCY_TYPE_VIEW_SEARCH = "VIEW.SEARCH";
const std::string LATENCY_TYPE_VIEW_SEARCH_HANDLER = "VIEW.SEARCH.HANDLER";
const std::string LATENCY_TYPE_VIEW_COUNT = "VIEW.COUNT";
const std::string LATENCY_TYPE_VIEW_COUNT_HANDLER = "VIEW.COUNT.HANDLER";

const std::string INDEX_NAME_KEY = "index_name";
const std::string COMMAND_KEY = "command";
const std::string METRIC_NAME_KEY = "name";
const std::string METRIC_VALUE_KEY = "value";
const std::string METRIC_VALUE_TYPE_KEY = "value_type";
const std::string METRIC_TS_KEY = "timestamp";
const std::string METRIC_VERSION_KEY = "version";
const std::string METRIC_MODULE_KEY = "module";

const std::string CLIENT_KEY = "client_id";
const std::string REPLICA_KEY = "replica";

const std::string VIEW_SEARCH_COUNT_METRIC_STREAM_PREFIX = "VIEW_SEARCH_COUNT:";

const std::string VIEW_SEARCH_HANDLER_TOTAL_LATENCY_METRIC_STREAM_PREFIX = "VIEW_SEARCH_HANDLER_TOTAL_LATENCY";
const std::string VIEW_COUNT_HANDLER_TOTAL_LATENCY_METRIC_STREAM_PREFIX = "VIEW_COUNT_HANDLER_TOTAL_LATENCY";

#endif /* MODULE_MONITORING_H */
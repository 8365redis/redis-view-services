#include <chrono>
#include <iostream>

#include "json/json.hpp"
#include "hiredis/hiredis.h"
#include "module_constants.h"
#include "module_monitoring.h"
#include "module_config.h"
#include "version.h"
#include "logger.h"

using json = nlohmann::json;

bool Add_Metric(RedisModuleCtx *ctx, unsigned long long metric_ts, std::string name, double value, std::string value_type, std::string module, std::string version, std::string command, const std::map<std::string, std::string>& tags) {
    Module_Config &module_config = Module_Config::getInstance();
    if (module_config.Get_Monitoring() == false){
        return true;
    } 
    
    if(ctx == nullptr || ctx == NULL) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Metric failed , context is invalid.");
        return false;
    }

    if(name.empty() || value <= 0) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Metric failed , metric name or value can not be empty.");
        return false;
    }

    // Create the JSON metric object
    nlohmann::json metric;
    metric[METRIC_VERSION_KEY] = version;
    metric[METRIC_TS_KEY] = metric_ts;
    metric[METRIC_MODULE_KEY] = module;
    metric[COMMAND_KEY] = command;
    // Insert the provided tags into the metric JSON under METRIC_TAGS_KEY
    for (const auto& tag : tags) {
        metric[METRIC_TAGS_KEY][tag.first] = tag.second;
    }
    // Add metric data
    metric[METRIC_METRIC_KEY][METRIC_NAME_KEY] = name;
    metric[METRIC_METRIC_KEY][METRIC_VALUE_KEY] = value;
    metric[METRIC_METRIC_KEY][METRIC_VALUE_TYPE_KEY] = value_type;
    std::string metric_str = metric.dump();


    const int stream_write_size_total = 2; // metric name + metric data
    RedisModuleString* stream_name = RedisModule_CreateString(ctx, OPENTRACING_STREAM_NAME.c_str(), OPENTRACING_STREAM_NAME.length());
    RedisModuleKey *stream_key = RedisModule_OpenKey(ctx, stream_name, REDISMODULE_WRITE);
    RedisModuleString **xadd_params = (RedisModuleString **) RedisModule_Alloc(sizeof(RedisModuleString *) * stream_write_size_total);

    xadd_params[0] = RedisModule_CreateString(ctx, OPENTRACING_METRIC_KEY_NAME.c_str(), OPENTRACING_METRIC_KEY_NAME.length());
    xadd_params[1] = RedisModule_CreateString(ctx, metric_str.c_str(), metric_str.length());

    int stream_add_resp = RedisModule_StreamAdd( stream_key, REDISMODULE_STREAM_ADD_AUTOID, NULL, xadd_params, stream_write_size_total/2);
    if (stream_add_resp != REDISMODULE_OK) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Metric failed to add to the stream.");
        return false;
    }
    return true;

}

std::chrono::steady_clock::time_point Get_Start_Time() {
    return std::chrono::steady_clock::now();
}
double Get_Delta_Time(std::chrono::steady_clock::time_point start) {
    auto end = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
}

unsigned long long Get_Epoch_Time(){
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

bool Add_Replication_Metric(RedisModuleCtx *ctx, unsigned long long metric_ts, std::string name, double value, std::string value_type, std::string module, std::string version, std::string command, const std::map<std::string, std::string>& tags) {
    Module_Config &module_config = Module_Config::getInstance();
    if (module_config.Get_Monitoring() == false){
        return true;
    } 
    
    if(ctx == nullptr || ctx == NULL) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Replication_Metric failed , context is invalid.");
        return false;
    }

    if(name.empty() || value <= 0) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Replication_Metric failed , metric name or value can not be empty.");
        return false;
    }

    Data_Handler &d_h = Data_Handler::getInstance();
    redisContext *hiredis_master_context;
    hiredis_master_context = redisConnect(d_h.master_node.get_IP().c_str(), d_h.master_node.get_port());
    if (hiredis_master_context == NULL || hiredis_master_context->err) {
        std::string error(hiredis_master_context->errstr);
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Replication_Metric connection error:" + error );
        redisFree(hiredis_master_context);
        return false;
    }

    // Create the JSON metric object
    nlohmann::json metric;
    metric[METRIC_VERSION_KEY] = version;
    metric[METRIC_TS_KEY] = metric_ts;
    metric[METRIC_MODULE_KEY] = module;
    metric[COMMAND_KEY] = command;
    // Insert the provided tags into the metric JSON under METRIC_TAGS_KEY
    for (const auto& tag : tags) {
        metric[METRIC_TAGS_KEY][tag.first] = tag.second;
    }
    // Add metric data
    metric[METRIC_METRIC_KEY][METRIC_NAME_KEY] = name;
    metric[METRIC_METRIC_KEY][METRIC_VALUE_KEY] = value;
    metric[METRIC_METRIC_KEY][METRIC_VALUE_TYPE_KEY] = value_type;
    std::string metric_str = metric.dump();

    std::vector<const char*> arguments_string_vector;
    arguments_string_vector.push_back("XADD");
    arguments_string_vector.push_back(OPENTRACING_STREAM_NAME.c_str());  // The stream name
    arguments_string_vector.push_back("*");  // Use * to let Redis assign the ID
    arguments_string_vector.push_back(OPENTRACING_METRIC_KEY_NAME.c_str());
    arguments_string_vector.push_back(metric_str.c_str());

    redisReply *sadd_reply;
    sadd_reply = (redisReply*) redisCommandArgv(hiredis_master_context, arguments_string_vector.size(), arguments_string_vector.data(), NULL);
    if (sadd_reply == NULL || hiredis_master_context->err || sadd_reply->type == REDIS_REPLY_ERROR) {
        std::string error(hiredis_master_context->errstr);
        if(sadd_reply->type == REDIS_REPLY_ERROR){
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Replication_Metric request error at TS.ADD : " + std::string(sadd_reply->str) );
        } else {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Replication_Metric request error at TS.ADD : " + error );
        }
        freeReplyObject(sadd_reply);
        redisFree(hiredis_master_context);
        return false;
    }

    freeReplyObject(sadd_reply);
    redisFree(hiredis_master_context);
    return true;

}
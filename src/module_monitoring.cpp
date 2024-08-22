#include <chrono>

#include "hiredis/hiredis.h"
#include "module_constants.h"
#include "module_monitoring.h"
#include "module_config.h"
#include "logger.h"

#include <iostream>


bool Add_Metric(RedisModuleCtx *ctx, unsigned long long metric_ts, std::string metric_name, std::string metric_value, std::vector<std::pair<std::string,std::string>> label_pairs) {
    Module_Config &module_config = Module_Config::getInstance();
    if (module_config.Get_Monitoring() == false){
        return true;
    } 
    
    if(ctx == nullptr || ctx == NULL) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Metric failed , context is invalid.");
        return false;
    }

    if(metric_name == "" || metric_value == "") {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Metric failed , metric name or value can not be empty.");
        return false;
    }

    std::vector<std::string> arguments_string_vector;
    arguments_string_vector.push_back(metric_name);
    arguments_string_vector.push_back(std::to_string(metric_ts));
    arguments_string_vector.push_back(metric_value);

    if(!label_pairs.empty()) {
        arguments_string_vector.push_back("LABELS");
    } 
    bool added_at_least_one_label = false;
    for (auto pair : label_pairs) {
        if (pair.first != "" && pair.second != "") {
            arguments_string_vector.push_back(pair.first);
            arguments_string_vector.push_back(pair.second);
            added_at_least_one_label = true;
        }
    }
    if(!added_at_least_one_label && !label_pairs.empty()) { // Remove added "LABELS" if no label is added
        arguments_string_vector.pop_back();
    }

    arguments_string_vector.push_back("RETENTION");
    arguments_string_vector.push_back(std::to_string(module_config.Get_Monitoring_Retention()));

    std::string arguments_string_vector_str;
    for (const auto& str : arguments_string_vector) {
        arguments_string_vector_str += str + " ";
    }
    //LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Add_Metric , parameters : "  + arguments_string_vector_str);

    int new_argc = arguments_string_vector.size();
    RedisModuleString **new_argv = (RedisModuleString **)RedisModule_Alloc(sizeof(RedisModuleString *) * new_argc);
    for (int i = 0; i < new_argc; ++i) {
        new_argv[i] = RedisModule_CreateString(ctx, arguments_string_vector[i].c_str(), arguments_string_vector[i].size());
    }

    RedisModuleCallReply *ts_add_reply = RedisModule_Call(ctx, "TS.ADD", "v", new_argv, new_argc);
    if (RedisModule_CallReplyType(ts_add_reply) == REDISMODULE_REPLY_ERROR) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Metric failed");
        return false;  
    }

    return true;
}

bool Add_Count_Metric(RedisModuleCtx *ctx, std::string metric_name, std::vector<std::pair<std::string,std::string>> label_pairs, bool inc){
    Module_Config &module_config = Module_Config::getInstance();
    if (module_config.Get_Monitoring() == false){
        return true;
    } 

    if(ctx == nullptr || ctx == NULL) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Count_Metric failed , context is invalid.");
        return false;
    }

    if(metric_name == "") {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Count_Metric failed , metric name can not be empty.");
        return false;
    }

    std::vector<std::string> arguments_string_vector;
    arguments_string_vector.push_back(metric_name);
    const char* one = "1.0";
    arguments_string_vector.push_back(one);

    if(!label_pairs.empty()) {
        arguments_string_vector.push_back("LABELS");
    } 
    bool added_at_least_one_label = false;
    for (auto pair : label_pairs) {
        if (pair.first != "" && pair.second != "") {
            arguments_string_vector.push_back(pair.first);
            arguments_string_vector.push_back(pair.second);
            added_at_least_one_label = true;
        }
    }
    if(!added_at_least_one_label && !label_pairs.empty()) { // Remove added "LABELS" if no label is added
        arguments_string_vector.pop_back();
    }

    arguments_string_vector.push_back("RETENTION");
    arguments_string_vector.push_back(std::to_string(module_config.Get_Monitoring_Retention()));

    std::string arguments_string_vector_str;
    for (const auto& str : arguments_string_vector) {
        arguments_string_vector_str += str + " ";
    }
    //LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Add_Count_Metric , parameters : "  + arguments_string_vector_str);

    int new_argc = arguments_string_vector.size();
    RedisModuleString **new_argv = (RedisModuleString **)RedisModule_Alloc(sizeof(RedisModuleString *) * new_argc);
    for (int i = 0; i < new_argc; ++i) {
        new_argv[i] = RedisModule_CreateString(ctx, arguments_string_vector[i].c_str(), arguments_string_vector[i].size());
    }


    RedisModuleCallReply *ts_add_count_reply;
    if(inc){
        ts_add_count_reply = RedisModule_Call(ctx, "TS.INCRBY", "v", new_argv, new_argc);
    } else {
        ts_add_count_reply = RedisModule_Call(ctx, "TS.DECRBY", "v", new_argv, new_argc);
    }
    if (RedisModule_CallReplyType(ts_add_count_reply) == REDISMODULE_REPLY_ERROR) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Count_Metric failed");
        return false;  
    }

    return true;

}

bool Reset_Count(RedisModuleCtx *ctx, std::string metric_name) {
    Module_Config &module_config = Module_Config::getInstance();
    if (module_config.Get_Monitoring() == false){
        return true;
    } 

    if(ctx == nullptr || ctx == NULL) {
        return false;
    }

    if(metric_name == "") {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Reset_Count failed , metric name can not be empty.");
        return false;
    }

    std::vector<std::string> arguments_string_vector;
    arguments_string_vector.push_back(metric_name);
    arguments_string_vector.push_back("*");
    const char* zero = "0";
    arguments_string_vector.push_back(zero);

    arguments_string_vector.push_back("RETENTION");
    arguments_string_vector.push_back(std::to_string(module_config.Get_Monitoring_Retention()));

    int new_argc = arguments_string_vector.size();
    RedisModuleString **new_argv = (RedisModuleString **)RedisModule_Alloc(sizeof(RedisModuleString *) * new_argc);
    for (int i = 0; i < new_argc; ++i) {
        new_argv[i] = RedisModule_CreateString(ctx, arguments_string_vector[i].c_str(), arguments_string_vector[i].size());
    }

    RedisModuleCallReply *ts_add_reply = RedisModule_Call(ctx, "TS.ADD", "v", new_argv, new_argc);
    if (RedisModule_CallReplyType(ts_add_reply) == REDISMODULE_REPLY_ERROR) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Reset_Count failed to reset to 0 for metric : " + metric_name);
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


std::vector<std::pair<std::string,std::string>> Create_Latency_Labels(std::string client, std::string search_type) {
    std::vector<std::pair<std::string,std::string>> latency_tags;
    latency_tags.emplace_back(CLIENT_LABEL_KEY, client);
    latency_tags.emplace_back(LATENCY_TYPE_LABEL_KEY, search_type);
    return latency_tags;
}

std::vector<std::pair<std::string,std::string>> Create_Replica_Latency_Labels(std::string client, std::string search_type, std::string replica) {
    std::vector<std::pair<std::string,std::string>> latency_tags;
    latency_tags.emplace_back(CLIENT_LABEL_KEY, client);
    latency_tags.emplace_back(LATENCY_TYPE_LABEL_KEY, search_type);
    latency_tags.emplace_back(REPLICA_LABEL_KEY, replica);
    return latency_tags;
}

std::vector<std::pair<std::string,std::string>> Create_Count_Labels(std::string client, std::string search_type) {
    std::vector<std::pair<std::string,std::string>> count_tags;
    count_tags.emplace_back(CLIENT_LABEL_KEY, client);
    count_tags.emplace_back(COUNT_TYPE_LABEL_KEY, search_type);
    return count_tags;
}

std::vector<std::pair<std::string,std::string>> Create_Handler_Latency_Label(){
    std::vector<std::pair<std::string,std::string>> latency_tags;
    return latency_tags;    
}

bool Add_Replication_Metric(RedisModuleCtx *ctx, unsigned long long metric_ts, std::string metric_name, std::string metric_value, std::vector<std::pair<std::string,std::string>> label_pairs) {
    Module_Config &module_config = Module_Config::getInstance();
    if (module_config.Get_Monitoring() == false){
        return true;
    } 
    
    if(ctx == nullptr || ctx == NULL) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Replication_Metric failed , context is invalid.");
        return false;
    }

    if(metric_name == "" || metric_value == "") {
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

    std::vector<const char*> arguments_string_vector;
    arguments_string_vector.push_back("TS.ADD");
    arguments_string_vector.push_back(metric_name.c_str());
    std::string metric_ts_str = std::to_string(metric_ts);
    arguments_string_vector.push_back(metric_ts_str.c_str());
    arguments_string_vector.push_back(metric_value.c_str());
    
    if(!label_pairs.empty()) {
        arguments_string_vector.push_back("LABELS");
    } 
    bool added_at_least_one_label = false;
    for (auto &pair : label_pairs) {
        if (pair.first != "" && pair.second != "") {
            arguments_string_vector.push_back(pair.first.c_str());
            arguments_string_vector.push_back(pair.second.c_str());
            added_at_least_one_label = true;
        }
    }
    if(!added_at_least_one_label && !label_pairs.empty()) { // Remove added "LABELS" if no label is added
        arguments_string_vector.pop_back();
    }
    
    arguments_string_vector.push_back("RETENTION");
    std::string retention_str = std::to_string(module_config.Get_Monitoring_Retention());
    arguments_string_vector.push_back(retention_str.c_str());
    
    std::string arguments_string_vector_str;
    for (auto str : arguments_string_vector) {
        arguments_string_vector_str += (std::string(str) + " ,, ");
    }
    //LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Add_Replication_Metric , parameters : "  + arguments_string_vector_str);

    redisReply *tsadd_reply;
    tsadd_reply = (redisReply*) redisCommandArgv(hiredis_master_context, arguments_string_vector.size(), arguments_string_vector.data(), NULL);

    if (tsadd_reply == NULL || hiredis_master_context->err || tsadd_reply->type == REDIS_REPLY_ERROR) {
        std::string error(hiredis_master_context->errstr);
        if(tsadd_reply->type == REDIS_REPLY_ERROR){
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Replication_Metric request error at TS.ADD : " + std::string(tsadd_reply->str) );
        } else {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Replication_Metric request error at TS.ADD : " + error );
        }
        redisFree(hiredis_master_context);
        freeReplyObject(tsadd_reply);
        return false;
    }

    redisFree(hiredis_master_context);
    freeReplyObject(tsadd_reply);
    return true;

}
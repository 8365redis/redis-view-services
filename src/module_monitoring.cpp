#include <chrono>

#include "module_constants.h"
#include "module_monitoring.h"
#include "module_config.h"
#include "logger.h"



bool Add_Metric(RedisModuleCtx *ctx, unsigned long long metric_ts, std::string metric_name, std::string metric_value, std::vector<std::pair<std::string,std::string>> label_pairs) {
    Module_Config &module_config = Module_Config::getInstance();
    if (module_config.Get_Monitoring() == false){
        return true;
    } 
    
    if(ctx == nullptr || ctx == NULL) {
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


std::vector<std::pair<std::string,std::string>> Create_Latency_Labels(const std::string& client, const std::string& search_type) {
    std::vector<std::pair<std::string,std::string>> latency_tags;
    latency_tags.emplace_back(CLIENT_LABEL_KEY, client);
    latency_tags.emplace_back(LATENCY_TYPE_LABEL_KEY, search_type);
    return latency_tags;
}

std::vector<std::pair<std::string,std::string>> Create_Count_Labels(const std::string& client, const std::string& search_type) {
    std::vector<std::pair<std::string,std::string>> count_tags;
    count_tags.emplace_back(CLIENT_LABEL_KEY, client);
    count_tags.emplace_back(COUNT_TYPE_LABEL_KEY, search_type);
    return count_tags;
}

std::vector<std::pair<std::string,std::string>> Create_Handler_Latency_Label(){
    std::vector<std::pair<std::string,std::string>> latency_tags;
    return latency_tags;    
}
#include <chrono>

#include "module_monitoring.h"
#include "logger.h"

bool Create_Client_Monitoring(RedisModuleCtx *ctx, std::string client_name) {
    if(ctx == nullptr || ctx == NULL) {
        return false;
    }   

    if(client_name == "") {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Create_Client_Monitoring failed , client name can not be empty." );
        return false;
    }

    // COUNT LATENCY
    std::string count_latency_stream_name = VIEW_COUNT_LATENCY_METRIC_STREAM_PREFIX + client_name;
    std::vector<std::pair<std::string,std::string>> count_latency_tags;
    std::pair<std::string, std::string> count_latency_tag_client = std::make_pair(VIEW_COUNT_LATENCY_CLIENT_LABEL_KEY, client_name);
    count_latency_tags.push_back(count_latency_tag_client);
    std::pair<std::string, std::string> count_latency_tag_type = std::make_pair(LATENCY_TYPE_LABEL_KEY, LATENCY_TYPE_VIEW_COUNT);
    count_latency_tags.push_back(count_latency_tag_type);
    Create_Metric_Stream(ctx, count_latency_stream_name, count_latency_tags);

    // COUNT COUNT
    std::string count_count_stream_name = VIEW_COUNT_COUNT_METRIC_STREAM_PREFIX + client_name;
    std::vector<std::pair<std::string,std::string>> count_count_tags;
    std::pair<std::string, std::string> count_count_tag_client = std::make_pair(VIEW_COUNT_COUNT_CLIENT_LABEL_KEY, client_name);
    count_count_tags.push_back(count_count_tag_client);
    std::pair<std::string, std::string> count_count_tag_type = std::make_pair(COUNT_TYPE_LABEL_KEY, COUNT_TYPE_VIEW_COUNT);
    count_count_tags.push_back(count_count_tag_type);
    Create_Metric_Stream(ctx, count_count_stream_name, count_count_tags); 

    // SEARCH LATENCY
    std::string search_latency_stream_name = VIEW_SEARCH_LATENCY_METRIC_STREAM_PREFIX + client_name;
    std::vector<std::pair<std::string,std::string>> search_latency_tags;
    std::pair<std::string, std::string> search_latency_tag_client = std::make_pair(VIEW_SEARCH_LATENCY_CLIENT_LABEL_KEY, client_name);
    search_latency_tags.push_back(search_latency_tag_client);
    std::pair<std::string, std::string> search_latency_tag_type = std::make_pair(LATENCY_TYPE_LABEL_KEY, LATENCY_TYPE_VIEW_SEARCH);
    search_latency_tags.push_back(search_latency_tag_type);
    Create_Metric_Stream(ctx, search_latency_stream_name, search_latency_tags);

    // SEARCH COUNT
    std::string search_count_stream_name = VIEW_SEARCH_COUNT_METRIC_STREAM_PREFIX + client_name;
    std::vector<std::pair<std::string,std::string>> search_count_tags;
    std::pair<std::string, std::string> search_count_tag_client = std::make_pair(VIEW_SEARCH_COUNT_CLIENT_LABEL_KEY, client_name);
    search_count_tags.push_back(search_count_tag_client);
    std::pair<std::string, std::string> search_count_tag_type = std::make_pair(COUNT_TYPE_LABEL_KEY, COUNT_TYPE_VIEW_SEARCH);
    search_count_tags.push_back(search_count_tag_type);
    Create_Metric_Stream(ctx, search_count_stream_name, search_count_tags);

    return true;
}

bool Create_Metric_Stream(RedisModuleCtx *ctx, std::string stream_name, std::vector<std::pair<std::string,std::string>> tags , unsigned long retention) {
    
    if(ctx == nullptr || ctx == NULL) {
        return false;
    }   

    if(stream_name == "") {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Create_Metric_Stream failed , stream name can not be empty." );
        return false;
    }

    std::vector<std::string> arguments_string_vector;

    arguments_string_vector.push_back(stream_name);

    arguments_string_vector.push_back("RETENTION");
    arguments_string_vector.push_back(std::to_string(retention));

    if(!tags.empty()) {
        arguments_string_vector.push_back("LABELS");
    } 
    bool added_at_least_one_label = false;
    for (auto pair : tags) {
        if (pair.first != "" && pair.second != "") {
            arguments_string_vector.push_back(pair.first);
            arguments_string_vector.push_back(pair.second);
            added_at_least_one_label = true;
        }
    }
    if(!added_at_least_one_label && !tags.empty()) { // Remove added "LABELS"
        arguments_string_vector.pop_back();
    }

    std::string arguments_string_vector_str;
    for (const auto& str : arguments_string_vector) {
        arguments_string_vector_str += str + " ";
    }

    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Create_Metric_Stream , parameters : "  + arguments_string_vector_str);

    int new_argc = arguments_string_vector.size();
    RedisModuleString **new_argv = (RedisModuleString **)RedisModule_Alloc(sizeof(RedisModuleString *) * new_argc);
    for (int i = 0; i < new_argc; ++i) {
        new_argv[i] = RedisModule_CreateString(ctx, arguments_string_vector[i].c_str(), arguments_string_vector[i].size());
    }

    RedisModuleCallReply *ts_create_reply = RedisModule_Call(ctx, "TS.CREATE", "v", new_argv, new_argc); 

    const std::string already_exist_error = "ERR TSDB: key already exists";
    if (RedisModule_CallReplyType(ts_create_reply) == REDISMODULE_REPLY_ERROR) {
        RedisModuleString *response = RedisModule_CreateStringFromCallReply(ts_create_reply);
        const char *key_str = RedisModule_StringPtrLen(response, NULL);
        if(already_exist_error == std::string(key_str)){
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Create_Metric_Stream didn't create new ts, it already exists." );
        } else {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Create_Metric_Stream failed , TS.CREATE returned error : " + std::string(key_str));
            return false;
        }
    }

    return true;

}

bool Add_Metric(RedisModuleCtx *ctx, std::string metric_name, std::string metric_value) {
    if(ctx == nullptr || ctx == NULL) {
        return false;
    }

    if(metric_name == "" || metric_value == "") {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Metric failed , metric name or value can not be empty.");
        return false;
    }

    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Add_Metric , parameters : "  + metric_name + " " + metric_value);

    RedisModuleCallReply *ts_add_reply = RedisModule_Call(ctx, "TS.ADD", "ccc", metric_name.c_str(), "*",  metric_value.c_str());
    if (RedisModule_CallReplyType(ts_add_reply) == REDISMODULE_REPLY_ERROR) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Metric failed");
        return false;  
    }

    return true;
}

bool Add_Count_Metric(RedisModuleCtx *ctx, std::string metric_name, bool inc){
    if(ctx == nullptr || ctx == NULL) {
        return false;
    }

    if(metric_name == "") {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Count_Metric failed , metric name can not be empty.");
        return false;
    }
    RedisModuleCallReply *ts_add_count_reply;
    const char* one = "1.0";
    if(inc){
        ts_add_count_reply = RedisModule_Call(ctx, "TS.INCRBY", "cc", metric_name.c_str(), one);
    } else {
        ts_add_count_reply = RedisModule_Call(ctx, "TS.DECRBY", "cc", metric_name.c_str(), one);
    }
    if (RedisModule_CallReplyType(ts_add_count_reply) == REDISMODULE_REPLY_ERROR) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Add_Count_Metric failed");
        return false;  
    }

    return true;

}

bool Reset_Count(RedisModuleCtx *ctx, std::string metric_name){
    if(ctx == nullptr || ctx == NULL) {
        return false;
    }

    if(metric_name == "") {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Reset_Count failed , metric name can not be empty.");
        return false;
    }

    if( Add_Metric(ctx, metric_name, "0") == false) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Reset_Count failed , resetting metric to zero failed.");
        return false;        
    }

    return true;

}

std::chrono::steady_clock::time_point Get_Start_Time() {
    return std::chrono::steady_clock::now();
}
double Get_Delta_Time(std::chrono::steady_clock::time_point start) {
    auto end = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
}
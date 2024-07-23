#include "module_monitoring.h"
#include "logger.h"

bool Create_Metric_Stream(RedisModuleCtx *ctx, std::string stream_name, std::vector<std::pair<std::string,std::string>> tags , unsigned long retention) {
    
    if(ctx == nullptr || ctx == NULL) {
        return false;
    }   

    if(stream_name == "") {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Create_Metric_Stream failed , stream name can not be empty." );
        return false;
    }

    if(retention < 1000) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Create_Metric_Stream failed , retention must be bigger than 1 second." );
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
        if (pair.first != "" || pair.second != "") {
            arguments_string_vector.push_back(pair.first);
            arguments_string_vector.push_back(pair.second);
            added_at_least_one_label = true;
        }
    }
    if(!added_at_least_one_label && !tags.empty()) {
        arguments_string_vector.pop_back();
    }

    std::string arguments_string_vector_str;
    for (const auto& str : arguments_string_vector) {
        arguments_string_vector_str += str;
    }

    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Create_Metric_Stream , parameters : "  + arguments_string_vector_str);

    int new_argc = arguments_string_vector.size();
    RedisModuleString **new_argv = (RedisModuleString **)RedisModule_Alloc(sizeof(RedisModuleString *) * new_argc);
    for (int i = 0; i < new_argc; ++i) {
        new_argv[i] = RedisModule_CreateString(ctx, arguments_string_vector[i].c_str(), arguments_string_vector[i].size());
    }

    RedisModuleCallReply *ts_create_reply = RedisModule_Call(ctx, "TS.CREATE", "v", new_argc, new_argv);

    if (RedisModule_CallReplyType(ts_create_reply) == REDISMODULE_REPLY_ERROR) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Create_Metric_Stream failed , TS.CREATE returned error.");
        return false;
    }

    return true;

}
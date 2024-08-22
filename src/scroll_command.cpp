#include <tuple>

#include "scroll_command.h"
#include "module_constants.h"
#include "logger.h"
#include "module_utils.h"
#include "module_data_handler.h"


int Scroll_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Scroll_RedisCommand called." );
    if (argc != 4) {
        return RedisModule_WrongArity(ctx);
    }

    Data_Handler &d_h = Data_Handler::getInstance();

    RedisModuleString *query_id_str = argv[1];
    long long query_id = -1;

    RedisModuleString *offset_str = argv[2];
    long long offset = -1;

    RedisModuleString *limit_str = argv[3];
    long long limit = -1;

    if ( RedisModule_StringToLongLong( query_id_str, &query_id) == REDISMODULE_OK && \
        RedisModule_StringToLongLong( offset_str, &offset) == REDISMODULE_OK &&  \
        RedisModule_StringToLongLong( limit_str, &limit) == REDISMODULE_OK) {
        if (query_id < 0 || offset < 0 || limit < 0) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Scroll_RedisCommand given params is invalid." );
            return RedisModule_ReplyWithError(ctx, "Given params must be bigger than zero");            
        }

    } else {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Scroll_RedisCommand failed parse params." );
        return RedisModule_ReplyWithError(ctx, strerror(errno)); 
    }

    std::string client_name_str = Get_Client_Name(ctx);

    LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Scroll_RedisCommand called with given params: " + client_name_str + " query id: " + std::to_string(query_id) + \
                                                " offset: " +  std::to_string(offset) + " limit: " +  std::to_string(limit));
    Print_Status(ctx , "Scroll_RedisCommand start");

    std::vector<std::string> query_vec;
    if(d_h.client_2_query.count(client_name_str) > 0 && d_h.client_2_query[client_name_str].count(query_id) > 0){
        query_vec = d_h.client_2_query[client_name_str][query_id]; // This is copy
    }else {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Scroll_RedisCommand failed: Given query is not found client: " + client_name_str + " query id: " + std::to_string(query_id) );
        return RedisModule_ReplyWithError(ctx, strerror(errno));        
    }

    bool limit_arg_found = false;
    for(long unsigned int arg_index = 0 ; arg_index < query_vec.size() ; arg_index++) {
        if (query_vec[arg_index] == "LIMIT") {
            limit_arg_found = true;
            if(query_vec.size() > arg_index + 2) {
                query_vec[arg_index+1] = std::to_string(offset);
                query_vec[arg_index+2] = std::to_string(limit);
            } else {
                LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Scroll_RedisCommand failed: LIMIT argument doesn't have params." );
                return RedisModule_ReplyWithError(ctx, strerror(errno));
            }
            break;
        }
    }

    if(limit_arg_found == false) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Scroll_RedisCommand query doesn't have LIMIT argument." );
        return RedisModule_ReplyWithError(ctx, strerror(errno));
    }

    std::vector<RedisModuleString*> arguments;
    std::string all_arg_str;
    bool current_query_inner_key_given = true;
    for(std::string arg : query_vec){
        if(arg == "@__key"){
            current_query_inner_key_given = false;
        }
        all_arg_str = all_arg_str +  arg + " ";
        arguments.push_back(RedisModule_CreateString(ctx, arg.c_str(), arg.length()));
    }

    LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Scroll_RedisCommand will call FT.AGGREGATE with given params : " +  all_arg_str);

    // Forward Search
    RedisModuleCallReply *reply = RedisModule_Call(ctx, "FT.AGGREGATE", "v", arguments.begin(), arguments.size());
    if (RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_ARRAY) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Scroll_RedisCommand FT.AGGREGATE failed." );
        return RedisModule_ReplyWithError(ctx, strerror(errno));
    }

    // Parse Search Result
    const size_t aggreate_reply_length = RedisModule_CallReplyLength(reply);
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "View_Search_RedisCommand aggregate_reply size : " + aggreate_reply_length );
    
    long long total_size_from_reply;
    RedisModuleCallReply *key_int_reply = RedisModule_CallReplyArrayElement(reply, 0);
    if (RedisModule_CallReplyType(key_int_reply) == REDISMODULE_REPLY_INTEGER){
        total_size_from_reply = RedisModule_CallReplyInteger(key_int_reply);
    }else {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "View_Search_RedisCommand failed to get aggregate_reply size." );
        return RedisModule_ReplyWithError(ctx, strerror(errno));   
    }
    
    std::string inner_key_arg_prefix = d_h.id_2_innerkey_prefix[query_id];
    const int default_key_index = 1;
    // First get keys
    std::vector<std::string> keys;
    for (size_t i = 1; i < aggreate_reply_length; i++) {   // Starting from 1 as first one count
        RedisModuleCallReply *key_reply = RedisModule_CallReplyArrayElement(reply, i);
        if ( RedisModule_CallReplyType(key_reply) == REDISMODULE_REPLY_ARRAY) {
            size_t inner_reply_length = RedisModule_CallReplyLength(key_reply);
            RedisModuleCallReply *inner_key_reply;
            if(current_query_inner_key_given){
                inner_key_reply = RedisModule_CallReplyArrayElement(key_reply, inner_reply_length-1); // if innerkey is given it is always last
            } else {
                inner_key_reply = RedisModule_CallReplyArrayElement(key_reply, default_key_index);
            }
            RedisModuleString *arr_elem_key_name = RedisModule_CreateStringFromCallReply(inner_key_reply);
            const char *arr_elem_key_name_str = RedisModule_StringPtrLen(arr_elem_key_name, NULL);
            LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "View_Search_RedisCommand pushing key: " + std::string(arr_elem_key_name_str) );
            keys.push_back(inner_key_arg_prefix + arr_elem_key_name_str);
        }
    }

    // Reply array sizing
    RedisModule_ReplyWithArray(ctx , 6); 

    // Reply Add the query id
    RedisModule_ReplyWithCString(ctx, REPLY_QUERY_ID_STR.c_str());
    RedisModule_ReplyWithLongLong(ctx, query_id);

    // Reply Add the response size
    RedisModule_ReplyWithCString(ctx, REPLY_COUNT_STR.c_str());
    RedisModule_ReplyWithLongLong(ctx, total_size_from_reply);

    // Reply key and values
    RedisModule_ReplyWithCString(ctx, REPLY_RESULT_STR.c_str());

    std::unordered_map<std::string, nlohmann::json> key_value_map;
    if(keys.size() > 0) {
        RedisModule_ReplyWithArray(ctx , keys.size() * 4); // key name , value 
        //Now get values
        int get_argc = keys.size();
        RedisModuleString **get_argv = (RedisModuleString **)RedisModule_Alloc(sizeof(RedisModuleString *) * get_argc);
        for (int i = 0; i < get_argc; ++i) {
            get_argv[i] = RedisModule_CreateString(ctx, keys[i].c_str(), keys[i].size());
        }

        RedisModuleCallReply *get_reply = RedisModule_Call(ctx, "JSON.MGET", "vc", get_argv , get_argc, "$");
        if (RedisModule_CallReplyType(get_reply) != REDISMODULE_REPLY_ARRAY) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "View_Search_RedisCommand JSON.MGET returned not an array." );
            return RedisModule_ReplyWithError(ctx, strerror(errno));
        }
        RedisModule_Free(get_argv);

        // Parse get values
        const size_t get_reply_length = RedisModule_CallReplyLength(get_reply);
        if(get_reply_length != keys.size()) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "View_Search_RedisCommand JSON.MGET returned array size is not right. Reply : " +  std::to_string(get_reply_length)  );
            return RedisModule_ReplyWithError(ctx, strerror(errno));        
        }
        for (size_t i = 0; i < get_reply_length; i++) {  
            RedisModuleCallReply *get_inner_reply = RedisModule_CallReplyArrayElement(get_reply, i);
            RedisModuleString *get_reply_elem = RedisModule_CreateStringFromCallReply(get_inner_reply);
            const char *get_reply_elem_str = RedisModule_StringPtrLen(get_reply_elem, NULL);
            if (!json::accept(get_reply_elem_str)){
                LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "View_Search_RedisCommand JSON.MGET returned an element which is not valid json" );
                continue;
            }
            std::string value = get_reply_elem_str;
            if (value.size() > 0 && value[0] == '[' && value[value.size() - 1] == ']') {
                value = value.substr(1, value.size() - 2);
            }
            key_value_map[keys[i]] = json::parse(value);
            LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "View_Search_RedisCommand key: " + std::string(keys[i]) + " value: " + value );
        }

        // Return Values
        for (auto &k : keys){
            RedisModule_ReplyWithCString(ctx, REPLY_KEY_STR.c_str());
            RedisModule_ReplyWithStringBuffer(ctx, k.c_str() ,k.length() );
            RedisModule_ReplyWithCString(ctx, REPLY_VALUE_STR.c_str());
            RedisModule_ReplyWithArray(ctx , 4);
            RedisModule_ReplyWithCString(ctx, REPLY_PATH_STR.c_str());
            RedisModule_ReplyWithCString(ctx, REPLY_DATA_ROOT_STR.c_str());
            if (key_value_map.count(k) == 0) {
                LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "View_Search_RedisCommand JSON.MGET returned an element not matching key" );
                RedisModule_ReplyWithCString(ctx, REPLY_DATA_STR.c_str());
                RedisModule_ReplyWithEmptyString(ctx);
                continue;            
            }
            std::string val = key_value_map[k].dump();
            RedisModule_ReplyWithCString(ctx, REPLY_DATA_STR.c_str());
            RedisModule_ReplyWithStringBuffer(ctx, val.c_str() , val.length());
        }
    } else {
        RedisModule_ReplyWithEmptyArray(ctx);
    }

    // Update Globals for keys and value
    d_h.query_2_value[query_id] = key_value_map;
    d_h.query_2_index[query_id] = keys;

    // Push the scroll (we do it in main loop thread to not cause sync problem)
    d_h.scroll_wait_list.push_back(std::make_tuple(query_id, offset, limit));
    

    return REDISMODULE_OK;

}
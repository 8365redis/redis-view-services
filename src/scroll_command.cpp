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
    for(std::string arg : query_vec){
        all_arg_str = all_arg_str +  arg + " ";
        arguments.push_back(RedisModule_CreateString(ctx, arg.c_str(), arg.length()));
    }

    LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Scroll_RedisCommand will call FT.SEARCH with given params : " +  all_arg_str);

    // Forward Search
    RedisModuleCallReply *reply = RedisModule_Call(ctx, "FT.SEARCH", "v", arguments.begin(), arguments.size());
    if (RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_ARRAY) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Scroll_RedisCommand FT.SEARCH failed." );
        return RedisModule_ReplyWithError(ctx, strerror(errno));
    }

    // Parse Search Result
    const size_t reply_length = RedisModule_CallReplyLength(reply);
    RedisModule_ReplyWithArray(ctx , reply_length + 1); // +1 because we are adding the query id 

    // Add the new query ID 
    RedisModule_ReplyWithLongLong(ctx, query_id);

    RedisModuleCallReply *key_int_reply = RedisModule_CallReplyArrayElement(reply, 0);
    if (RedisModule_CallReplyType(key_int_reply) == REDISMODULE_REPLY_INTEGER){
        long long size = RedisModule_CallReplyInteger(key_int_reply);
        RedisModule_ReplyWithLongLong(ctx, size);
    }else {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Scroll_RedisCommand failed to get reply size." );
        return RedisModule_ReplyWithError(ctx, strerror(errno));
    }

    std::vector<std::vector<std::string>> keys;
    for (size_t i = 1; i < reply_length; i++) {   // Starting from 1 as first one count
        RedisModuleCallReply *key_reply = RedisModule_CallReplyArrayElement(reply, i);
        if (RedisModule_CallReplyType(key_reply) == REDISMODULE_REPLY_STRING){
            RedisModuleString *response = RedisModule_CreateStringFromCallReply(key_reply);
            const char *response_str = RedisModule_StringPtrLen(response, NULL);
            std::vector<std::string> response_vector = {response_str};
            keys.push_back(response_vector);
        }else if ( RedisModule_CallReplyType(key_reply) == REDISMODULE_REPLY_ARRAY){
            size_t inner_reply_length = RedisModule_CallReplyLength(key_reply);
            std::vector<std::string> inner_keys;
            for (size_t i = 0; i < inner_reply_length; i++) {
                RedisModuleCallReply *inner_key_reply = RedisModule_CallReplyArrayElement(key_reply, i);
                if (RedisModule_CallReplyType(inner_key_reply) == REDISMODULE_REPLY_STRING){
                    RedisModuleString *inner_response = RedisModule_CreateStringFromCallReply(inner_key_reply);
                    const char *inner_response_str = RedisModule_StringPtrLen(inner_response, NULL);
                    inner_keys.push_back(inner_response_str);
                }
            }
            keys.push_back(inner_keys);
        }
    }

    for (const auto& it : keys) {
        if ( it.size() == 1){
            RedisModule_ReplyWithStringBuffer(ctx, it.at(0).c_str(), strlen(it.at(0).c_str()));
        }
        else {
            RedisModule_ReplyWithArray(ctx , 2);
            RedisModule_ReplyWithStringBuffer(ctx, it.at(0).c_str(), strlen(it.at(0).c_str()));
            RedisModule_ReplyWithStringBuffer(ctx, it.at(1).c_str(), strlen(it.at(1).c_str()));
        }
    }

    // Add to scroll after confirming new scroll is succesful
    d_h.scroll_wait_list.push_back(std::make_tuple(query_id, offset, limit));

    Print_Status(ctx , "Scroll_RedisCommand ends");

    return REDISMODULE_OK;
}
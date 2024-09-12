#include "module_utils.h"
#include "logger.h"
#include "module_constants.h"
#include "module_data_handler.h"

std::string Get_Client_Name(RedisModuleCtx *ctx) {
    unsigned long long client_id = RedisModule_GetClientId(ctx);
    return Get_Client_Name_From_ID(ctx, client_id);
}

std::string Get_Client_Name_From_ID(RedisModuleCtx *ctx, unsigned long long client_id) {
    RedisModuleString *client_name = RedisModule_GetClientNameById(ctx, client_id); 
    if ( client_name == NULL){
        return "";
    }
    std::string client_name_str = RedisModule_StringPtrLen(client_name, NULL);
    if ( client_name_str.empty()){
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Get_Client_Name_From_ID : Failed to get client name because client name is not set." );
        return "";
    }
    return client_name_str;
}

void Print_Status(RedisModuleCtx *ctx , std::string optional){
    Data_Handler &d_h = Data_Handler::getInstance();
    
    std::string log_str = "\nSTATUS\n";
    log_str = log_str + "[" + optional + "]\n"; 
    log_str = log_str + "CLIENT_2_QUERY_MAP\n"; 
    for(auto &c2q : d_h.client_2_query){
        log_str = log_str + "Client : " +  c2q.first + "\n"; 
        for(auto &qId : c2q.second){
            log_str = log_str + "Query : " + std::to_string(qId.first) + " , ";
        }
        log_str = log_str + "\n";
    }
    log_str = log_str + "\n";
    log_str = log_str + "ID_2_QUERY_MAP\n"; 
    for(auto &i2q : d_h.id_2_query){
        log_str = log_str + "Query ID : " +  std::to_string(i2q.first) +  " , Query : " + i2q.second + "\n"; 
        log_str = log_str + "\n";
    }
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , log_str );
}


std::string Vector_2_String(std::vector<std::string> vec) {
    std::string str = "";
    for(auto const& e : vec) str += (e + CCT_MODULE_QUERY_DELIMETER);
    if(str.length() > CCT_MODULE_QUERY_DELIMETER.length() ) {
        str.erase(str.length() - CCT_MODULE_QUERY_DELIMETER.length());
    }
    return str;
}

void Print_Response(redisReply *reply) {
    if(reply->type == REDIS_REPLY_ARRAY){
        for (std::size_t i = 0; i < reply->elements; i++) {
            if(reply->element[i]->type == REDIS_REPLY_ARRAY) {
                Print_Response(reply->element[i]);
            } else if(reply->element[i]->type == REDIS_REPLY_STRING){
                printf("%s\n", reply->element[i]->str);
            } else if(reply->element[i]->type == REDIS_REPLY_INTEGER){
                printf("%lld\n", reply->element[i]->integer);
            } else if(reply->element[i]->type == REDIS_REPLY_DOUBLE){
                printf("%f\n", reply->element[i]->dval);
            }
        }
    } else if(reply->type == REDIS_REPLY_STRING){
        printf("%s\n", reply->str);
    } else if(reply->type == REDIS_REPLY_INTEGER){
        printf("%lld\n", reply->integer);
    } else if(reply->type == REDIS_REPLY_DOUBLE){
        printf("%f\n", reply->dval);
    } else if(reply->type == REDIS_REPLY_ERROR){
        printf("%s\n", reply->str);
    }
}

void  Get_Keys_From_Array(RedisModuleCtx *ctx, RedisModuleCallReply *array , std::vector<std::string> &keys, std::string kn , std::string inner_key_arg_prefix) {
    size_t array_length = RedisModule_CallReplyLength(array);
    for (size_t i = 0; i < array_length; i++) { 
        RedisModuleCallReply *key_name = RedisModule_CallReplyArrayElement(array, i);
        if ( RedisModule_CallReplyType(key_name) != REDISMODULE_REPLY_STRING ) {
            continue;
        }
        RedisModuleString *key_name_rstr = RedisModule_CreateStringFromCallReply(key_name);
        std::string key_name_str = RedisModule_StringPtrLen(key_name_rstr, NULL);
        //printf("Key name : %s \n", key_name_str.c_str());
        if(key_name_str == kn && (i+1) < array_length){
            RedisModuleCallReply *key_value = RedisModule_CallReplyArrayElement(array, i + 1);
            if ( RedisModule_CallReplyType(key_value) != REDISMODULE_REPLY_STRING ) {
                continue;
            }
            RedisModuleString *key_value_rstr = RedisModule_CreateStringFromCallReply(key_value);
            std::string key_value_str = RedisModule_StringPtrLen(key_value_rstr, NULL);
            //printf("Key value : %s \n", key_value_str.c_str());
            keys.push_back((inner_key_arg_prefix + key_value_str));
        }
    }
}

void Remove_Outer_Paranthesis(std::string &input) {
    if (input.size() > 0 && input[0] == '[' && input[input.size() - 1] == ']') {
        input = input.substr(1, input.size() - 2);
    }    
}
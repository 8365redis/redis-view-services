#include <string>
#include <set>
#include <thread>
#include "view_search_command.h"
#include "logger.h"
#include "module_constants.h"
#include "module_utils.h"

using json = nlohmann::json;

class View_Diff {
public:
    long long int index;
    std::string key;
    std::string operation;
    nlohmann::json old_value;
    nlohmann::json new_value;
    std::string diff;
};

int View_Search_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    RedisModule_AutoMemory(ctx);

    std::vector<RedisModuleString*> arguments_redis_string_vector(argv+1, argv + argc);
    std::vector<std::string> arguments_string_vector;
    for(RedisModuleString* arg : arguments_redis_string_vector) {
        arguments_string_vector.push_back(RedisModule_StringPtrLen(arg, NULL));
    }

    std::string arguments_string = "";
    for(auto const& e : arguments_string_vector) arguments_string += (e + CCT_MODULE_QUERY_DELIMETER);
    if(arguments_string.length() > CCT_MODULE_QUERY_DELIMETER.length() ) {
        arguments_string.erase(arguments_string.length() - CCT_MODULE_QUERY_DELIMETER.length());
    }

    std::string client_name = Get_Client_Name(ctx);

    if( CLIENT_2_QUERY_MAP.count(client_name) == 0) { 
        std::unordered_map<long long int, std::vector<std::string>> first_entry_dict;
        first_entry_dict[LAST_VIEW_SEARCH_IDENTIFIER] = arguments_string_vector;
        CLIENT_2_QUERY_MAP[client_name] = first_entry_dict;
    }else {
        CLIENT_2_QUERY_MAP[client_name][LAST_VIEW_SEARCH_IDENTIFIER] = arguments_string_vector;
    }
    
    printf("Client : %s , Query : %s \n" , client_name.c_str(), arguments_string.c_str());
    std::unordered_map<std::string, nlohmann::json> empty;
    QUERY_2_VALUE_MAP[LAST_VIEW_SEARCH_IDENTIFIER] = empty;
    ID_2_QUERY_MAP[LAST_VIEW_SEARCH_IDENTIFIER] = arguments_string;

    
    // Forward Search
    RedisModuleCallReply *reply = RedisModule_Call(ctx, "FT.SEARCH", "v", argv + 1, argc - 1);
    if (RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_ARRAY) {
        return RedisModule_ReplyWithError(ctx, strerror(errno));
    }

    // Parse Search Result
    const size_t reply_length = RedisModule_CallReplyLength(reply);
    RedisModule_ReplyWithArray(ctx , reply_length + 1); // +1 because we are adding the query id 

    // Add the new query ID 
    RedisModule_ReplyWithLongLong(ctx, LAST_VIEW_SEARCH_IDENTIFIER);

    RedisModuleCallReply *key_int_reply = RedisModule_CallReplyArrayElement(reply, 0);
    if (RedisModule_CallReplyType(key_int_reply) == REDISMODULE_REPLY_INTEGER){
        long long size = RedisModule_CallReplyInteger(key_int_reply);
        RedisModule_ReplyWithLongLong(ctx, size);
    }else {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "View_Search_RedisCommand failed to get reply size." );
        return REDISMODULE_ERR;
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
            size_t inner_reply_length = RedisModule_CallReplyLength(reply);
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

    LAST_VIEW_SEARCH_IDENTIFIER = LAST_VIEW_SEARCH_IDENTIFIER + 1;
    return REDISMODULE_OK;
}

void Start_View_Search_Handler(RedisModuleCtx *ctx) {
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Start_Main_Search_Handler called." );
    std::thread view_search_thread(View_Search_Handler, ctx, std::ref(QUERY_2_VALUE_MAP), std::ref(CLIENT_2_QUERY_MAP));
    view_search_thread.detach();
}


void View_Search_Handler(RedisModuleCtx *ctx, std::unordered_map<long long int, std::unordered_map<std::string, json>> &query_2_value ,
                             std::unordered_map<std::string, std::unordered_map<long long int, std::vector<std::string>>> &client_2_query) {

    while(true) {
        //LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "View_Search_Handler called." );
        RedisModule_ThreadSafeContextLock(ctx);

        for (auto &client_2_query_item : client_2_query) {
            std::string current_client = client_2_query_item.first;
            for (auto &id_to_query_item : client_2_query_item.second) {
                //LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "View_Search_Handler iterate for: " + client_2_query_item.first );
                long long int current_query_id = id_to_query_item.first;

                std::vector<RedisModuleString*> arguments;
                for(std::string arg : id_to_query_item.second){
                    arguments.push_back(RedisModule_CreateString(ctx, arg.c_str(), arg.length()));
                    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "View_Search_Handler: Arguments : " + arg );
                }
                
                // Forward Search
                RedisModuleCallReply *reply = RedisModule_Call(ctx, "FT.SEARCH", "v", arguments.begin(), arguments.size());
                if (RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_ARRAY) {
                    LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "View_Search_Handler FT.SEARCH failed." );
                }
                // Parse Search Result
                const size_t reply_length = RedisModule_CallReplyLength(reply);
                std::vector<std::vector<std::string>> keys;
                for (size_t i = 1; i < reply_length; i++) {   // Starting from 1 as first one count
                    RedisModuleCallReply *key_reply = RedisModule_CallReplyArrayElement(reply, i);
                    if (RedisModule_CallReplyType(key_reply) == REDISMODULE_REPLY_STRING) {
                        RedisModuleString *response = RedisModule_CreateStringFromCallReply(key_reply);
                        const char *response_str = RedisModule_StringPtrLen(response, NULL);
                        std::vector<std::string> response_vector = {response_str};
                        keys.push_back(response_vector);
                    }else if ( RedisModule_CallReplyType(key_reply) == REDISMODULE_REPLY_ARRAY) {
                        size_t inner_reply_length = RedisModule_CallReplyLength(reply);
                        std::vector<std::string> inner_keys;
                        for (size_t i = 0; i < inner_reply_length; i++) {
                            RedisModuleCallReply *inner_key_reply = RedisModule_CallReplyArrayElement(key_reply, i);
                            if (RedisModule_CallReplyType(inner_key_reply) == REDISMODULE_REPLY_STRING) {
                                RedisModuleString *inner_response = RedisModule_CreateStringFromCallReply(inner_key_reply);
                                const char *inner_response_str = RedisModule_StringPtrLen(inner_response, NULL);
                                inner_keys.push_back(inner_response_str);
                            }
                        }
                        keys.push_back(inner_keys);
                    }
                }
                
                std::unordered_map<std::string, nlohmann::json> old_values = query_2_value[current_query_id];
                //{key_id : json_value}
                std::unordered_map<std::string, nlohmann::json> new_values;
                std::string key = "";
                for(auto vec : keys){
                    if(vec.size() == 1){
                        key = vec.at(0);
                    } else {
                        if(key.empty()){
                            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "View_Search_Handler Key is empty." );
                            continue;
                        }
                        std::string value = vec.at(1);
                        if (!json::accept(value)){
                            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "View_Search_Handler JSON is not valid." );
                        } else {
                            new_values[key] = json::parse(value);
                        }
                    }
                }
            

                //Compare previous values
                std::vector<View_Diff> diff_values;
                
                std::vector<std::string> view_old_keys;
                for(auto &kv : query_2_value[current_query_id]) {
                    view_old_keys.push_back(kv.first);
                    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "View_Search_Handler: Old key : " + kv.first );
                }
                std::vector<std::string> view_new_keys;
                for(auto &kv : new_values) {
                    view_new_keys.push_back(kv.first);
                    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "View_Search_Handler: New key : " + kv.first );
                }

                // Iterate new keys from old keys
                int diff_index = 0;
                while (diff_index < view_old_keys.size()){
                    std::string old_key = view_old_keys[diff_index];
                    std::string new_key = "";
                    if (diff_index < view_new_keys.size()) {
                        new_key = view_new_keys[diff_index];
                    }
                    nlohmann::json old_value =  old_values[old_key];
                    nlohmann::json new_value;
                    if(!new_key.empty()) {
                        new_value = new_values[new_key];
                    }
                    if(!new_key.empty() && !old_key.empty()) {
                        if(new_value != old_value) {
                            View_Diff current_diff;
                            current_diff.index = diff_index;
                            current_diff.key = old_key;
                            current_diff.old_value = old_value;
                            current_diff.new_value = new_value;
                            current_diff.operation = "UPDATE";
                            current_diff.diff = "";
                            diff_values.push_back(current_diff);
                        }
                    } else if( new_key.empty() && !old_key.empty()) { // Some keys are deleted
                        View_Diff current_diff;
                        current_diff.index = diff_index;
                        current_diff.key = old_key;
                        current_diff.old_value = old_value;
                        current_diff.new_value = new_value;
                        current_diff.operation = "DELETE";
                        current_diff.diff = "";
                        diff_values.push_back(current_diff);
                    }
                    diff_index++;
                }

                // If the new key list is bigger than old key list it means we have new keys
                while (diff_index < view_new_keys.size()) {
                    std::string new_key = view_new_keys[diff_index];
                    nlohmann::json new_value = new_values[new_key];
                    View_Diff current_diff;
                    current_diff.index = diff_index;
                    current_diff.key = new_key;
                    current_diff.new_value = new_value;
                    current_diff.operation = "NEW";
                    current_diff.diff = "";
                    diff_values.push_back(current_diff);                    
                    diff_index++;
                }

                // Write new values 
                query_2_value[current_query_id] = new_values;

                // Write to stream
                int stream_write_size = diff_values.size();
                if( stream_write_size != 0 ) { 
                    int stream_write_size_total = (stream_write_size * 2) + 2;
                    RedisModuleString* client_name = RedisModule_CreateString(ctx, current_client.c_str(), current_client.length());
                    RedisModuleKey *stream_key = RedisModule_OpenKey(ctx, client_name, REDISMODULE_WRITE);
                    RedisModuleString **xadd_params = (RedisModuleString **) RedisModule_Alloc(sizeof(RedisModuleString *) * stream_write_size_total);
                    xadd_params[0] = RedisModule_CreateString(ctx,"QUERY", strlen("QUERY"));
                    xadd_params[1] = RedisModule_CreateString(ctx, ID_2_QUERY_MAP[current_query_id].c_str(), ID_2_QUERY_MAP[current_query_id].length());
                    int i = 2;
                    for(auto&value : diff_values){
                        if(i > stream_write_size_total){
                            assert("Stream writing pass limit ???");
                        }
                        xadd_params[i] = RedisModule_CreateString(ctx, value.first.c_str(), value.first.length());
                        xadd_params[i+1] = RedisModule_CreateString(ctx, value.second.dump().c_str(), value.second.dump().length());
                        i += 2;
                    }
                    int stream_add_resp = RedisModule_StreamAdd( stream_key, REDISMODULE_STREAM_ADD_AUTOID, NULL, xadd_params, stream_write_size_total/2);
                    if (stream_add_resp != REDISMODULE_OK) {
                        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Register_RedisCommand failed to create the stream." );
                    }
                }
            }     
        }

        // Unsubscribe queries
        for (auto &unsub_id : UNSUBSCRIBE_WAITING_LIST) {
            // client2query
            for (auto &client_2_query_item : client_2_query) {
                client_2_query_item.second.erase(unsub_id);
            }
            // query2value
            query_2_value.erase(unsub_id);
            // id2query
            ID_2_QUERY_MAP.erase(unsub_id);
        }
        UNSUBSCRIBE_WAITING_LIST.clear();

        RedisModule_ThreadSafeContextUnlock(ctx);
        std::this_thread::sleep_for(std::chrono::milliseconds(1000)); // Check every second
    }
}
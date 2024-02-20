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
    long long int index = 0;
    std::string old_key = "";
    std::string new_key = "";
    std::string operation = "";
    nlohmann::json old_value;
    nlohmann::json new_value;
    nlohmann::json diff;
    nlohmann::json to_json()  {
        nlohmann::json view_diff_str;
        view_diff_str["index"] = index;
        view_diff_str["old_key"] = old_key;
        view_diff_str["new_key"] = new_key;
        view_diff_str["operation"] = operation;
        view_diff_str["old_value"] = old_value;
        view_diff_str["new_value"] = new_value;
        view_diff_str["diff"] = diff;
        return view_diff_str;
    };
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

    std::string client_name_str = Get_Client_Name(ctx);

    if( CLIENT_2_QUERY_MAP.count(client_name_str) == 0) { 
        std::unordered_map<long long int, std::vector<std::string>> first_entry_dict;
        first_entry_dict[LAST_VIEW_SEARCH_IDENTIFIER] = arguments_string_vector;
        CLIENT_2_QUERY_MAP[client_name_str] = first_entry_dict;
    }else {
        CLIENT_2_QUERY_MAP[client_name_str][LAST_VIEW_SEARCH_IDENTIFIER] = arguments_string_vector;
    }
    
    printf("Client : %s , Query : %s \n" , client_name_str.c_str(), arguments_string.c_str());
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
            //RedisModule_ReplyWithStringBuffer(ctx, it.at(2).c_str(), strlen(it.at(2).c_str()));
            //RedisModule_ReplyWithStringBuffer(ctx, it.at(3).c_str(), strlen(it.at(3).c_str()));
        }
    }

    //{key_id : json_value}
    std::unordered_map<std::string, nlohmann::json> new_values;
    std::vector<std::string> view_new_keys;
    std::string key = "";
    for(auto vec : keys){
        if(vec.size() == 1){ // Only key
            key = vec.at(0);
            view_new_keys.push_back(key);
        } else {
            if(key.empty()){
                LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "View_Search_Handler Key is empty." );
                continue;
            }
            std::string value = vec.at(1); // It is hardcoded value for value itself
            if (!json::accept(value)){
                LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "View_Search_Handler JSON is not valid." );
            } else {
                new_values[key] = json::parse(value);
            }
        }
    }
    //Compare previous values (it is all new)
    std::vector<View_Diff> diff_values;

    int index = 0;
    for (auto &key : view_new_keys) {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "View_Search_Handler: Starting to check and add new keys" );
        nlohmann::json new_value = new_values[key];
        View_Diff current_diff;
        current_diff.index = index;
        current_diff.new_key = key;
        current_diff.new_value = new_value;
        current_diff.operation = "NEW";
        diff_values.push_back(current_diff);                    
        index++;
    }

    // Write to stream
    int diff_size = diff_values.size();
    json diff_values_json_array = json::array();
    for(auto &diff : diff_values){
        printf("Index : %s and key : %s" , std::to_string(diff.index).c_str() , diff.new_key.c_str());
        diff_values_json_array.push_back(diff.to_json());
    }

    if( diff_size != 0 ) { 
        int stream_write_size_total = 2 + 2;
        RedisModuleString* client_name = RedisModule_CreateString(ctx, client_name_str.c_str(), client_name_str.length());
        RedisModuleKey *stream_key = RedisModule_OpenKey(ctx, client_name, REDISMODULE_WRITE);
        RedisModuleString **xadd_params = (RedisModuleString **) RedisModule_Alloc(sizeof(RedisModuleString *) * stream_write_size_total);
        xadd_params[0] = RedisModule_CreateString(ctx,"QUERY", strlen("QUERY"));
        xadd_params[1] = RedisModule_CreateString(ctx, ID_2_QUERY_MAP[LAST_VIEW_SEARCH_IDENTIFIER].c_str(), ID_2_QUERY_MAP[LAST_VIEW_SEARCH_IDENTIFIER].length());
        

        xadd_params[2] = RedisModule_CreateString(ctx, "DIFF", strlen("DIFF"));
        xadd_params[3] = RedisModule_CreateString(ctx, diff_values_json_array.dump().c_str(), diff_values_json_array.dump().length());

        int stream_add_resp = RedisModule_StreamAdd( stream_key, REDISMODULE_STREAM_ADD_AUTOID, NULL, xadd_params, stream_write_size_total/2);
        if (stream_add_resp != REDISMODULE_OK) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "View_Search_Handler failed to add to the stream." );
        }
    }


    // Write new values 
    QUERY_2_VALUE_MAP[LAST_VIEW_SEARCH_IDENTIFIER] = new_values;
    // Write new order 
    QUERY_2_INDEX_MAP[LAST_VIEW_SEARCH_IDENTIFIER] = view_new_keys;



    LAST_VIEW_SEARCH_IDENTIFIER = LAST_VIEW_SEARCH_IDENTIFIER + 1;
    return REDISMODULE_OK;
}

void Start_View_Search_Handler(RedisModuleCtx *ctx) {
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Start_Main_Search_Handler called." );
    std::thread view_search_thread(View_Search_Handler, ctx, std::ref(QUERY_2_VALUE_MAP), std::ref(CLIENT_2_QUERY_MAP), std::ref(QUERY_2_INDEX_MAP));
    view_search_thread.detach();
}


void View_Search_Handler(RedisModuleCtx *ctx, std::unordered_map<long long int, std::unordered_map<std::string, json>> &query_2_value ,
                             std::unordered_map<std::string, std::unordered_map<long long int, std::vector<std::string>>> &client_2_query, 
                             std::unordered_map<long long int, std::vector<std::string>> &query_2_index) {

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
                    //LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "View_Search_Handler: Arguments : " + arg );
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
                        size_t inner_reply_length = RedisModule_CallReplyLength(key_reply);
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

                std::unordered_map<std::string, nlohmann::json> old_values;
                if(query_2_value.count(current_query_id) > 0 ) {
                    old_values = query_2_value[current_query_id];
                }
                std::vector<std::string> view_old_keys;
                if(query_2_index.count(current_query_id) > 0 ) {
                    view_old_keys = query_2_index[current_query_id];
                }
                
                //{key_id : json_value}
                std::unordered_map<std::string, nlohmann::json> new_values;
                std::vector<std::string> view_new_keys;
                std::string key = "";
                for(auto vec : keys){
                    if(vec.size() == 1){ // Only key
                        key = vec.at(0);
                        view_new_keys.push_back(key);
                    } else {
                        if(key.empty()){
                            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "View_Search_Handler Key is empty." );
                            continue;
                        }
                        std::string value = vec.at(1); // It is hardcoded value for value itself
                        if (!json::accept(value)){
                            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "View_Search_Handler JSON is not valid." );
                        } else {
                            new_values[key] = json::parse(value);
                        }
                    }
                }
                //Compare previous values
                std::vector<View_Diff> diff_values;

                // Iterate new keys from old keys
                long unsigned int diff_index = 0;
                while (diff_index < view_old_keys.size()){
                    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "View_Search_Handler: Starting to check old keys" );
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
                    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "View_Search_Handler: Old key loop : Old key : " + old_key + " , and new key : "  + new_key);
                    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "View_Search_Handler: Old key loop : Old value : " + old_value.dump() + " , and new value : "  + new_value.dump());
                    if(!new_key.empty() && !old_key.empty()) {
                        if(new_value != old_value) {
                            LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "View_Search_Handler: Keys are same values are different , key: " + new_key );
                            View_Diff current_diff;
                            current_diff.index = diff_index;
                            current_diff.old_key = old_key;
                            current_diff.new_key = new_key;
                            current_diff.old_value = old_value;
                            current_diff.new_value = new_value;
                            current_diff.operation = "UPDATE";
                            current_diff.diff = json::diff(old_value, new_value);
                            diff_values.push_back(current_diff);
                        }
                    } else if( new_key.empty() && !old_key.empty()) { // Some keys are deleted
                        View_Diff current_diff;
                        current_diff.index = diff_index;
                        current_diff.old_key = old_key;
                        current_diff.new_key = new_key;
                        current_diff.old_value = old_value;
                        current_diff.new_value = new_value;
                        current_diff.operation = "DELETE";
                        diff_values.push_back(current_diff);
                    }
                    diff_index++;
                }

                // If the new key list is bigger than old key list it means we have new keys
                while (diff_index < view_new_keys.size()) {
                    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "View_Search_Handler: Starting to check and add new keys" );
                    std::string new_key = view_new_keys[diff_index];
                    nlohmann::json new_value = new_values[new_key];
                    View_Diff current_diff;
                    current_diff.index = diff_index;
                    current_diff.new_key = new_key;
                    current_diff.new_value = new_value;
                    current_diff.operation = "NEW";
                    diff_values.push_back(current_diff);                    
                    diff_index++;
                }

                // Write new values 
                query_2_value[current_query_id] = new_values;
                // Write new order 
                query_2_index[current_query_id] = view_new_keys;

                // Write to stream
                int diff_size = diff_values.size();
                json diff_values_json_array = json::array();
                for(auto &diff : diff_values){
                    printf("Index : %s and key : %s" , std::to_string(diff.index).c_str() , diff.new_key.c_str());
                    diff_values_json_array.push_back(diff.to_json());
                }

                if( diff_size != 0 ) { 
                    int stream_write_size_total = 2 + 2;
                    RedisModuleString* client_name = RedisModule_CreateString(ctx, current_client.c_str(), current_client.length());
                    RedisModuleKey *stream_key = RedisModule_OpenKey(ctx, client_name, REDISMODULE_WRITE);
                    RedisModuleString **xadd_params = (RedisModuleString **) RedisModule_Alloc(sizeof(RedisModuleString *) * stream_write_size_total);
                    xadd_params[0] = RedisModule_CreateString(ctx,"QUERY", strlen("QUERY"));
                    xadd_params[1] = RedisModule_CreateString(ctx, ID_2_QUERY_MAP[current_query_id].c_str(), ID_2_QUERY_MAP[current_query_id].length());
                    

                    xadd_params[2] = RedisModule_CreateString(ctx, "DIFF", strlen("DIFF"));
                    xadd_params[3] = RedisModule_CreateString(ctx, diff_values_json_array.dump().c_str(), diff_values_json_array.dump().length());

                    int stream_add_resp = RedisModule_StreamAdd( stream_key, REDISMODULE_STREAM_ADD_AUTOID, NULL, xadd_params, stream_write_size_total/2);
                    if (stream_add_resp != REDISMODULE_OK) {
                        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "View_Search_Handler failed to add to the stream." );
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
#include "view_count_command.h"

#include <string>
#include <thread>
#include <vector>
#include <hiredis/hiredis.h>
#include "logger.h"
#include "module_utils.h"
#include "module_constants.h"
#include "module_monitoring.h"
#include "replication_node_tracker.h"

static unsigned long LAST_VIEW_COUNT_IDENTIFIER = 1;

int Search_Count_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    auto latency_metric_start = Get_Start_Time();
    auto latency_metric_start_epoch = Get_Epoch_Time();
    RedisModule_AutoMemory(ctx);
    Data_Handler &d_h = Data_Handler::getInstance();

    if(d_h.registered_clients.count(Get_Client_Name(ctx)) == 0 ) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Search_Count_RedisCommand failed , client is not registered." );
        return RedisModule_ReplyWithError(ctx, "Not registered client");       
    }

    std::vector<RedisModuleString*> arguments(argv+1, argv + argc);
    arguments.push_back(RedisModule_CreateString(ctx, "LIMIT", 5 ));
    arguments.push_back(RedisModule_CreateString(ctx, "0", 1 ));
    arguments.push_back(RedisModule_CreateString(ctx, "0", 1 ));

    std::vector<std::string> arguments_arg;
    for(RedisModuleString* arg : arguments){
        arguments_arg.push_back(RedisModule_StringPtrLen(arg, NULL));
    }

    long long int current_count = 0;
    RedisModuleCallReply *reply = RedisModule_Call(ctx,"FT.SEARCH", "v", arguments.begin(), arguments.size());
    if (RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_ARRAY) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Search_Count_RedisCommand failed." );
        RedisModule_ReplyWithSimpleString(ctx, "Forwarded search failed");
        return REDISMODULE_ERR;
    } else {
        RedisModuleCallReply *key_int_reply = RedisModule_CallReplyArrayElement(reply, 0);
        if (RedisModule_CallReplyType(key_int_reply) == REDISMODULE_REPLY_INTEGER){
            current_count = RedisModule_CallReplyInteger(key_int_reply);
            LOG(ctx, REDISMODULE_LOGLEVEL_VERBOSE , "Search_Count_RedisCommand get size : " + std::to_string(current_count));
        }else {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Search_Count_RedisCommand failed to get reply current_count.");
            RedisModule_ReplyWithSimpleString(ctx, "Reply doesnt have count");
            return REDISMODULE_ERR;
        }
    }

    // Reply array sizing
    RedisModule_ReplyWithArray(ctx , 6);  // id , count , result's   

    // Reply Add the new query ID  
    RedisModule_ReplyWithCString(ctx, COUNT_QUERY_ID_STR.c_str());
    RedisModule_ReplyWithLongLong(ctx, LAST_VIEW_COUNT_IDENTIFIER);

    // Reply Add the query 
    std::string index_and_query = arguments_arg[0] + " " + arguments_arg[1]; 
    RedisModule_ReplyWithCString(ctx, COUNT_QUERY_STR.c_str());
    RedisModule_ReplyWithCString(ctx, index_and_query.c_str());

    // Reply count  
    RedisModule_ReplyWithCString(ctx, COUNT_STR.c_str());
    RedisModule_ReplyWithLongLong(ctx, current_count);

    std::string client_name = Get_Client_Name(ctx);
    Client_Size_Info info(client_name, arguments_arg, current_count , LAST_VIEW_COUNT_IDENTIFIER);
    
    d_h.client_size_info[client_name].insert(std::make_pair(LAST_VIEW_COUNT_IDENTIFIER, info));

    LAST_VIEW_COUNT_IDENTIFIER++;

    std::string client_name_str = Get_Client_Name(ctx);
    auto latency_metric = Get_Delta_Time(latency_metric_start);
    Add_Metric(ctx, latency_metric_start_epoch, (VIEW_COUNT_LATENCY_METRIC_STREAM_PREFIX + client_name_str + MONITORING_TS_DELIMETER + index_and_query), std::to_string(latency_metric), Create_Latency_Labels(client_name_str,LATENCY_TYPE_VIEW_COUNT));
    Add_Count_Metric(ctx, (VIEW_COUNT_COUNT_METRIC_STREAM_PREFIX + client_name_str), Create_Count_Labels(client_name_str, COUNT_TYPE_VIEW_COUNT));
    return REDISMODULE_OK;
}

void Start_Search_Count_Handler(RedisModuleCtx *ctx) {
    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Start_Search_Count_Handler called." );
    std::thread main_search_thread(Search_Count_Handler, ctx);
    main_search_thread.detach();
}


void Search_Count_Handler(RedisModuleCtx *ctx) {
    Data_Handler &d_h = Data_Handler::getInstance();
    std::unordered_map<std::string, IP_Address_Port> replication_nodes_count;
    while(true) {
        auto latency_metric_total_handler_start = Get_Start_Time();
        auto latency_metric_total_handler_start_epoch = Get_Epoch_Time();
        // First handle unregistered clients
        for (auto &unreg_count_client : d_h.unreg_count_wait_list) {
            if(d_h.client_size_info.count(unreg_count_client) == 0){
                LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Search_Count_Handler Unreg for count not found for client : " + unreg_count_client);
                continue;
            } else {
                Reset_Count(ctx, VIEW_COUNT_COUNT_METRIC_STREAM_PREFIX + unreg_count_client); 
                d_h.client_size_info.erase(unreg_count_client);
            }
        }
        d_h.unreg_count_wait_list.clear();

        // Second unsub the count
        for(const auto &unsub_count_info : d_h.unsub_count_wait_list){
            if(d_h.client_size_info.count(unsub_count_info.stream_name) == 0){
                LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Search_Count_Handler Unsub for count not found for client : " + unsub_count_info.stream_name);
                continue;
            } else {
                for (auto it = d_h.client_size_info[unsub_count_info.stream_name].begin(); it != d_h.client_size_info[unsub_count_info.stream_name].end(); ) {
                    if ( (*it).first == unsub_count_info.id) {
                        Add_Count_Metric(ctx , (VIEW_COUNT_COUNT_METRIC_STREAM_PREFIX+unsub_count_info.stream_name), Create_Count_Labels(unsub_count_info.stream_name, COUNT_TYPE_VIEW_COUNT), false); // Decrease its count
                        it = d_h.client_size_info[unsub_count_info.stream_name].erase(it); // erase returns the next iterator
                    } else {
                        ++it; // only increment if not erased
                    }
                }
            }
        }
        d_h.unsub_count_wait_list.clear();

        //LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Search_Count_Handler called." );
        RedisModule_ThreadSafeContextLock(ctx);

        //Get_Replication_Nodes(ctx,replication_nodes_count);

        for(auto &clients_info : d_h.client_size_info){
            for (auto& key2info : clients_info.second){
                auto latency_metric_current_query_start = Get_Start_Time();
                auto latency_metric_current_query_start_epoch = Get_Epoch_Time();
                auto &info = key2info.second;
                //LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Search_Count_Handler handling : " + info.stream_name );
                long long int current_count = 0;
                if(info.arguments.size() < 2){
                    LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Search_Count_Handler client size info argument size less than 2.");
                    continue;
                }
                std::string index_and_query = info.arguments[0] + " " + info.arguments[1];

                std::vector<RedisModuleString*> arguments;
                for(std::string arg : info.arguments){
                    arguments.push_back(RedisModule_CreateString(ctx, arg.c_str(), arg.length()));
                }

                LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Search_Count_Handler calling FT.SEARCH with args : " + Vector_2_String(info.arguments) );
                if(replication_nodes_count.size() > 0) {
                    //LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "View_Search_Handler will forward FT.AGGREGATE to replication node" );
                    Forward_Request_To_Replication_Node(ctx, replication_nodes_count, info);
                    continue;
                }
                LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Search_Count_Handler will handle FT.SEARCH in master" );
                
                RedisModuleCallReply *reply = RedisModule_Call(ctx,"FT.SEARCH", "v", arguments.begin(), arguments.size());
                //printf("Search_Count_Handler reply : %d\n",RedisModule_CallReplyType(reply));
                if (RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_ARRAY) {
                    LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Search_Count_RedisCommand Handler failed." );
                } else {
                    RedisModuleCallReply *key_int_reply = RedisModule_CallReplyArrayElement(reply, 0);
                    if (RedisModule_CallReplyType(key_int_reply) == REDISMODULE_REPLY_INTEGER){
                        current_count = RedisModule_CallReplyInteger(key_int_reply);
                        LOG(ctx, REDISMODULE_LOGLEVEL_VERBOSE , "Search_Count_RedisCommand Handler get size : " + std::to_string(current_count)  + " for query : " + index_and_query);
                    }else {
                        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Search_Count_RedisCommand Handler failed to get reply current_count." );
                    }
                }

                const int stream_write_size = 6; // id , query, count ; per key and value
                const int stream_write_field_num = stream_write_size / 2 ;
                if(info.count != current_count) {
                    RedisModuleString* client_name = RedisModule_CreateString(ctx, info.stream_name.c_str(), info.stream_name.length());
                    RedisModuleKey *stream_key = RedisModule_OpenKey(ctx, client_name, REDISMODULE_WRITE);
                    RedisModuleString **xadd_params = (RedisModuleString **) RedisModule_Alloc(sizeof(RedisModuleString *) * stream_write_size);

                    xadd_params[0] = RedisModule_CreateString(ctx, COUNT_QUERY_ID_STR.c_str(), COUNT_QUERY_ID_STR.length());
                    std::string id_str = std::to_string(info.id);
                    xadd_params[1] = RedisModule_CreateString(ctx, id_str.c_str(), id_str.length());

                    xadd_params[2] = RedisModule_CreateString(ctx, COUNT_QUERY_STR.c_str(), COUNT_QUERY_STR.length());
                    xadd_params[3] = RedisModule_CreateString(ctx, index_and_query.c_str(), index_and_query.length());

                    std::string size_str = std::to_string(current_count);
                    xadd_params[4] = RedisModule_CreateString(ctx, COUNT_STR.c_str(), COUNT_STR.length());
                    xadd_params[5] = RedisModule_CreateString(ctx, size_str.c_str(), size_str.length());

                    int stream_add_resp = RedisModule_StreamAdd( stream_key, REDISMODULE_STREAM_ADD_AUTOID, NULL, xadd_params, stream_write_field_num);
                    if (stream_add_resp != REDISMODULE_OK) {
                        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Search_Count_RedisCommand failed to create the stream." );
                    }
                }
                info.count = current_count;
                auto latency_metric_current_query = Get_Delta_Time(latency_metric_current_query_start);
                Add_Metric(ctx, latency_metric_current_query_start_epoch, (VIEW_COUNT_HANDLER_LATENCY_METRIC_STREAM_PREFIX + info.stream_name + MONITORING_TS_DELIMETER + index_and_query), 
                                std::to_string(latency_metric_current_query), Create_Latency_Labels(info.stream_name,LATENCY_TYPE_VIEW_COUNT_HANDLER));
            }
        }
        auto latency_metric_total_handler = Get_Delta_Time(latency_metric_total_handler_start);
        Add_Metric(ctx, latency_metric_total_handler_start_epoch, (VIEW_COUNT_HANDLER_TOTAL_LATENCY_METRIC_STREAM_PREFIX), std::to_string(latency_metric_total_handler), Create_Handler_Latency_Label()); 

        RedisModule_ThreadSafeContextUnlock(ctx);
        
        unsigned long long total_time_spend_in_handler_in_ms = (unsigned long) latency_metric_total_handler / NANO_2_MILI; 
        long handler_frequency_delta = COUNT_HANDLER_DEFAULT_FREQUENCY - total_time_spend_in_handler_in_ms;
        unsigned long long handler_frequency = COUNT_HANDLER_MIN_FREQUENCY;
        if(handler_frequency_delta > COUNT_HANDLER_MIN_FREQUENCY) {
            handler_frequency = handler_frequency_delta;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(handler_frequency)); 
    }
}


void Forward_Request_To_Replication_Node(RedisModuleCtx *ctx, std::unordered_map<std::string, IP_Address_Port> nodes, 
                                            Client_Size_Info &info) {
    static unsigned long long int distributer_count = 0;
    int total_replication_node = nodes.size();
    std::vector<std::string> keys;
    for (const auto& node : nodes) {
        keys.push_back(node.first);
    }
    int current_picked_node_index = distributer_count % total_replication_node;
    LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Forward_Request_To_Replication_Node (Count) picked node:" + keys[current_picked_node_index] );
    IP_Address_Port node = nodes[keys[current_picked_node_index]];
    std::thread t(Handler_Forward_Request, ctx, node, std::ref(info));
    t.detach(); // Detach the thread to allow it to run independently
    distributer_count++;
}

void Handler_Forward_Request(RedisModuleCtx *ctx, IP_Address_Port node, Client_Size_Info &info) {
    auto latency_metric_current_query_start = Get_Start_Time();
    auto latency_metric_current_query_start_epoch = Get_Epoch_Time();

    redisContext *hiredis_replica_context;
    hiredis_replica_context = redisConnect(node.get_IP().c_str(), node.get_port());
    if (hiredis_replica_context == NULL || hiredis_replica_context->err) {
        std::string error(hiredis_replica_context->errstr);
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Handler_Forward_Request (Count) connection error:" + error );
        redisFree(hiredis_replica_context);
        return;
    }

    std::string arguments_str = Vector_2_String(info.arguments);

    redisReply *search_reply;
    std::string command = "FT.SEARCH " + arguments_str;
    search_reply = (redisReply*) redisCommand(hiredis_replica_context, command.c_str());
    if (search_reply == NULL || hiredis_replica_context->err) {
        std::string error(hiredis_replica_context->errstr);
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Handler_Forward_Request (Count) request error at FT.SEARCH:" + error );
        freeReplyObject(search_reply);
        redisFree(hiredis_replica_context);
        return;
    }
    
    //Print_Response(search_reply);

    if(search_reply->type != REDIS_REPLY_ARRAY || search_reply->elements < 1  || search_reply->element[0]->type != REDIS_REPLY_INTEGER){
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Handler_Forward_Request (Count) response error at FT.SEARCH" );
        freeReplyObject(search_reply);
        redisFree(hiredis_replica_context);
        return;
    }

    int count = search_reply->element[0]->integer;
    std::string count_str = std::to_string(count);

    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Handler_Forward_Request (Count) got result at FT.SEARCH:" + count_str );

    freeReplyObject(search_reply);
    redisFree(hiredis_replica_context);

    std::string index_and_query = info.arguments[0] + " " + info.arguments[1];

    if(info.count == count) {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Handler_Forward_Request (Count) got same count ignore:" + count_str );
        auto latency_metric_current_query = Get_Delta_Time(latency_metric_current_query_start);
        Add_Replication_Metric(ctx, latency_metric_current_query_start_epoch, (VIEW_COUNT_HANDLER_REPLICA_LATENCY_METRIC_STREAM_PREFIX + node.get_key() + 
                    MONITORING_TS_DELIMETER + info.stream_name + MONITORING_TS_DELIMETER + index_and_query),
                    std::to_string(latency_metric_current_query), Create_Replica_Latency_Labels(info.stream_name, LATENCY_TYPE_VIEW_COUNT_HANDLER, node.get_key()));
        return;
    }
    info.count = count;

    Data_Handler &d_h = Data_Handler::getInstance();
    redisContext *hiredis_master_context;
    hiredis_master_context = redisConnect(d_h.master_node.get_IP().c_str(), d_h.master_node.get_port());
    if (hiredis_master_context == NULL || hiredis_master_context->err) {
        std::string error(hiredis_master_context->errstr);
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Handler_Forward_Request (Count) connection error:" + error );
        redisFree(hiredis_master_context);
        return;
    }

    LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "Handler_Forward_Request (Count) got different count , write to stream:" + count_str );
    std::string id_str = std::to_string(info.id);
    redisReply *stream_reply;
    stream_reply = (redisReply*) redisCommand(hiredis_master_context, "XADD %s * %s %s %s %s %s %s " , info.stream_name.c_str(), COUNT_QUERY_ID_STR.c_str(), id_str.c_str(), 
                                                COUNT_QUERY_STR.c_str(), index_and_query.c_str(), COUNT_STR.c_str(), count_str.c_str() );

    if (stream_reply == NULL || hiredis_master_context->err) {
        std::string error(hiredis_master_context->errstr);
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Handler_Forward_Request request error at XADD : " + error );
        redisFree(hiredis_master_context);
        freeReplyObject(stream_reply);
        return;
    }

    auto latency_metric_current_query = Get_Delta_Time(latency_metric_current_query_start);
    Add_Replication_Metric(ctx, latency_metric_current_query_start_epoch, (VIEW_COUNT_HANDLER_REPLICA_LATENCY_METRIC_STREAM_PREFIX + node.get_key() + 
                MONITORING_TS_DELIMETER + info.stream_name + MONITORING_TS_DELIMETER + index_and_query),
                std::to_string(latency_metric_current_query), Create_Replica_Latency_Labels(info.stream_name, LATENCY_TYPE_VIEW_COUNT_HANDLER, node.get_key()));

}
#include "heartbeat_command.h"
#include "logger.h"
#include "module_data_handler.h"
#include "module_utils.h"

int Heartbeat_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    
    if (argc > 2 ) {
        return RedisModule_WrongArity(ctx);
    }

    std::string client_name = Get_Client_Name(ctx);

    Data_Handler &d_h = Data_Handler::getInstance();

    if (client_name.empty() || d_h.registered_clients.count(client_name) == 0) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Heartbeat_RedisCommand failed : Client is not registered : " + client_name );
        return RedisModule_ReplyWithError(ctx, "Not registered client");
    }

    //UPDATE GLOBAL LAST SEEN
    auto now = std::chrono::system_clock::now();
    auto ms  = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch());
    unsigned long long ms_value = ms.count();
    d_h.client_2_last_seen[client_name] = ms_value;

    // Check if we have to trim the stream
    if (argc == 2) {
        if ( Trim_From_Stream(ctx, argv[1], client_name) == REDISMODULE_ERR ){
            return RedisModule_ReplyWithError(ctx, "Trim with given Stream ID failed");
        }
    } 
    
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

int Trim_From_Stream(RedisModuleCtx *ctx, RedisModuleString *last_read_id, std::string client_name) {
    RedisModuleStreamID minid;
    if (RedisModule_StringToStreamID(last_read_id, &minid) != REDISMODULE_OK) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Trim_From_Stream:  Provided Stream ID is not valid.");
        return REDISMODULE_ERR;
    }

    minid.seq = minid.seq + 1; // We get last read so delete the last read too 

    RedisModuleString *client_name_r = RedisModule_CreateString(ctx, client_name.c_str(), client_name.length());
    RedisModuleKey *stream_key = RedisModule_OpenKey(ctx, client_name_r, REDISMODULE_WRITE);

    if (RedisModule_StreamTrimByID(stream_key, 0, &minid) < 0) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Trim_From_Stream:  Trim with given Stream ID failed.");
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}
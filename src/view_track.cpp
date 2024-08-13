#include <string>
#include <vector>
#include <set>
#include <unordered_map>
#include <thread>
#include "json/json.hpp"
#include "redis/redismodule.h"
#include "logger.h"
#include "view_search_command.h"
#include "module_constants.h"
#include "module_utils.h"
#include "register_command.h"
#include "unregister_command.h"
#include "view_count_command.h"
#include "unsubscribe_command.h"
#include "unsubscribe_count_command.h"
#include "scroll_command.h"
#include "heartbeat_command.h"
#include "test_command.h"
#include "client_tracker.h"
#include "version.h"
#include "module_config.h"

#ifndef VIEW_MODULE_VERSION
#define VIEW_MODULE_VERSION "unknown"
#endif

using json = nlohmann::json;


#ifdef __cplusplus
extern "C" {
#endif

static RedisModuleCtx *rdts_staticCtx;

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    int version_int = Get_Module_Version(VIEW_MODULE_VERSION);
    const char* version_string = { VIEW_MODULE_VERSION " compiled at " __TIME__ " "  __DATE__  };
    LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "VIEW_MODULE_VERSION : " + std::string(version_string));

    #ifdef _DEBUG
    LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "THIS IS A DEBUG BUILD." );
    #endif
    #ifdef NDEBUG
    LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "THIS IS A RELEASE BUILD." );
    #endif 

    if (RedisModule_Init(ctx, "VIEW.SEARCH", version_int, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    Module_Config &module_config = Module_Config::getInstance();
    std::string config_file_path_str = "";
    if (argc > 0) {
        config_file_path_str = RedisModule_StringPtrLen(argv[0], NULL);
    }
    #ifdef _DEBUG
    config_file_path_str = "non-existing-file";  // DEBUG version will always use hardcoded default DEBUG values
    #endif
    module_config.Read_Module_Config(ctx, config_file_path_str);

    rdts_staticCtx = RedisModule_GetDetachedThreadSafeContext(ctx);

    if (RedisModule_CreateCommand(ctx,"VIEW.REGISTER", Register_RedisCommand , "write", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    } else {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "VIEW.REGISTER command created successfully.");
    }

    if (RedisModule_CreateCommand(ctx,"VIEW.UNREGISTER", UnRegister_RedisCommand , "write", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    } else {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "VIEW.UNREGISTER command created successfully.");
    }

    if (RedisModule_CreateCommand(ctx,"VIEW.COUNT", Search_Count_RedisCommand , "readonly", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    } else {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "VIEW.COUNT command created successfully.");
    }

    if (RedisModule_CreateCommand(ctx,"VIEW.SEARCH", View_Search_RedisCommand , "readonly", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    } else {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "VIEW.SEARCH command created successfully.");
    }

    if (RedisModule_CreateCommand(ctx, "VIEW.SEARCH.UNSUBSCRIBE", UnSubscribe_RedisCommand , "readonly", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    } else {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "VIEW.SEARCH.UNSUBSCRIBE command created successfully.");
    }

    if (RedisModule_CreateCommand(ctx, "VIEW.SEARCH.SCROLL", Scroll_RedisCommand , "readonly", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    } else {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "VIEW.SEARCH.SCROLL command created successfully.");
    }

    if (RedisModule_CreateCommand(ctx, "VIEW.HEARTBEAT", Heartbeat_RedisCommand , "write", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    } else {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "VIEW.HEARTBEAT command created successfully.");
    }

    if (RedisModule_CreateCommand(ctx, "VIEW.COUNT.UNSUBSCRIBE", UnSubscribe_Count_RedisCommand , "readonly", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    } else {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "VIEW.COUNT.UNSUBSCRIBE command created successfully.");
    }

    if (RedisModule_CreateCommand(ctx, "VIEW.TEST", Test_RedisCommand , "readonly", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    } else {
        LOG(ctx, REDISMODULE_LOGLEVEL_DEBUG , "VIEW.TEST command created successfully.");
    }

    
    Start_Search_Count_Handler(rdts_staticCtx);

    Start_View_Search_Handler(rdts_staticCtx);

    Start_Client_Handler(rdts_staticCtx);
 
    if (RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_ClientChange,
                                             Handle_Client_Event) != REDISMODULE_OK) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "RedisModule_OnLoad failed to SubscribeToServerEvent for RedisModuleEvent_ClientChange." );
        return RedisModule_ReplyWithError(ctx, strerror(errno));
    }

    /*
    RedisModule_GetMyClusterID
    int redis_port;
    if ( RedisModule_GetClusterNodeInfo(ctx,NULL,NULL,NULL,&redis_port,NULL) != REDISMODULE_OK) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "RedisModule_OnLoad failed to get self port." );
        return RedisModule_ReplyWithError(ctx, strerror(errno));
    }
    LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "RedisModule_OnLoad PORT: " + std::to_string(redis_port) );
    */
    return REDISMODULE_OK;
}

#ifdef __cplusplus
}
#endif


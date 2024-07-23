#include <chrono>

#include "test_command.h"

#include <hiredis/hiredis.h>

int Test_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){

    redisReply *reply;
    redisContext *c;

    auto start = std::chrono::high_resolution_clock::now();
    c = redisConnect("127.0.0.1", 6380);
    auto end = std::chrono::high_resolution_clock::now();
    double connect_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    printf("Duration for connect : %lf\n", connect_duration );

    if (c->err) {
        printf("error: %s\n", c->errstr);
        return 1;
    }

    /* PINGs */
    start = std::chrono::high_resolution_clock::now();
    reply = (redisReply*) redisCommand(c,"FT.AGGREGATE test_index * SORTBY 4 @field1 DESC @field4 ASC LIMIT 0 10");
    end = std::chrono::high_resolution_clock::now();
    double command_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    printf("Duration for command : %lf\n", command_duration );
    printf("RESPONSE: %s\n", reply->str);
    freeReplyObject(reply);

    redisFree(c);
    
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;

}
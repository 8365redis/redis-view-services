#ifndef MODULE_CONFIG_H
#define MODULE_CONFIG_H

#include <string>
#include "redis/redismodule.h"

const unsigned long DEFAULT_MONITORING_STREAM_CAP = 1000000; // Default cap 1M

const std::string DEFAULT_CONFIG_FILE_NAME  = "view-server-config.ini";
const std::string DEFAULT_CONFIG_SECTION  = "view-server";

const std::string MONITORING_ENABLED_CONFIG  = "MONITORING_ENABLED";
const std::string MONITORING_STREAM_CAP_CONFIG = "MONITORING_STREAM_CAP";

class Module_Config {
public:
    static Module_Config& getInstance(){
        static Module_Config instance; 
        return instance;
    }
private:
    Module_Config() {}
    bool monitoring_enabled = true;
    unsigned long long monitoring_stream_cap = DEFAULT_MONITORING_STREAM_CAP;
public:
    Module_Config(Module_Config const&) = delete;
    void operator=(Module_Config const&) = delete;
    void Read_Module_Config(RedisModuleCtx *ctx, std::string config_file_path_str);
    bool Get_Monitoring() const { return monitoring_enabled; }
    unsigned long long Get_Monitoring_Stream_Cap() const { return monitoring_stream_cap; }
};
#endif /* MODULE_CONFIG_H */
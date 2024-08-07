#ifndef MODULE_CONFIG_H
#define MODULE_CONFIG_H

#include <string>
#include "redis/redismodule.h"

const unsigned long DEFAULT_RETENTION = 1000 * 60 * 60 * 24 * 7; // Default retention 7 days

const std::string DEFAULT_CONFIG_FILE_NAME  = "view-server-config.ini";
const std::string DEFAULT_CONFIG_SECTION  = "view-server";

const std::string MONITORING_ENABLED_CONFIG  = "MONITORING_ENABLED";
const std::string MONITORING_RETENTION_CONFIG = "MONITORING_RETENTION";

class Module_Config {
public:
    static Module_Config& getInstance(){
        static Module_Config instance; 
        return instance;
    }
private:
    Module_Config() {}
    bool monitoring_enabled = true;
    unsigned long long monitoring_retention = DEFAULT_RETENTION;
public:
    Module_Config(Module_Config const&) = delete;
    void operator=(Module_Config const&) = delete;
    void Read_Module_Config(RedisModuleCtx *ctx, std::string config_file_path_str);
    bool Get_Monitoring() const { return monitoring_enabled; }
    unsigned long long Get_Monitoring_Retention() const { return monitoring_retention; }
};
#endif /* MODULE_CONFIG_H */
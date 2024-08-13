#include <dlfcn.h>
#include "module_config.h"
#include "mini/ini.h"
#include "logger.h"

void Module_Config::Read_Module_Config(RedisModuleCtx *ctx, std::string config_file_path_str) {
    std::string config_file_full_path;
    if(config_file_path_str.empty()) {
        Dl_info dl_info;
        dladdr((void*)&Module_Config::Read_Module_Config, &dl_info);
        std::string  module_lib_path = dl_info.dli_fname;
        size_t found = module_lib_path.find_last_of("/");
        std::string directory_name = module_lib_path.substr(0,found+1);
        config_file_full_path =  directory_name + DEFAULT_CONFIG_FILE_NAME;
    }else {
        config_file_full_path = config_file_path_str;
    }
    mINI::INIFile file(config_file_full_path);
    mINI::INIStructure config;
    bool config_read = file.read(config);

    if(config_read == false) {
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Module config file is not loaded (using default values) : " + config_file_full_path);
    } else {
        if(config_file_path_str.empty()) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Module config file is read from default config file." );
        } else {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Module config file provided as argument." );
        }
        LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Module config values read from file: " + config_file_full_path);

        if(config[DEFAULT_CONFIG_SECTION].size() == 0) {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Module config section " + DEFAULT_CONFIG_SECTION + " is missing. Using default values.");
            return;
        }

        if( !config[DEFAULT_CONFIG_SECTION][MONITORING_ENABLED_CONFIG].empty() ) {

            std::string monitoring_enabled_str = config[DEFAULT_CONFIG_SECTION][MONITORING_ENABLED_CONFIG];
            monitoring_enabled = monitoring_enabled_str == "true" ? true : false;
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Module config value [MONITORING_ENABLED_CONFIG] : " + std::to_string(monitoring_enabled));
        } else {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Module config value [MONITORING_ENABLED_CONFIG] is not found in config file using default.");
        }

        if( !config[DEFAULT_CONFIG_SECTION][MONITORING_RETENTION_CONFIG].empty() ) {
            try{
                int monitoring_retention_hours = std::stoi(config[DEFAULT_CONFIG_SECTION][MONITORING_RETENTION_CONFIG]);
                monitoring_retention = monitoring_retention_hours * 60 * 60 * 1000;
            } catch (std::invalid_argument const& ex) {
                LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Module config value " + MONITORING_RETENTION_CONFIG  + " has failed to read value.");
            }
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Module config value [MONITORING_RETENTION_CONFIG] : " + std::to_string(monitoring_retention));
        } else {
            LOG(ctx, REDISMODULE_LOGLEVEL_WARNING , "Module config value [MONITORING_RETENTION_CONFIG] is not found in config file using default.");
        }


    }

}
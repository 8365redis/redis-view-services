#ifndef MODULE_CONFIG_H
#define MODULE_CONFIG_H

const unsigned long DEFAULT_RETENTION = 1000 * 60 * 60 * 24 * 7; // Default retention 7 days

class Module_Config {
public:
    static Module_Config& getInstance(){
        static Module_Config instance; 
        return instance;
    }
private:
    Module_Config() {}
    bool monitoring_enabled;
    unsigned long long monitoring_retention = DEFAULT_RETENTION;
public:
    Module_Config(Module_Config const&) = delete;
    void operator=(Module_Config const&) = delete;
    void Set_Monitoring(bool enabled) { monitoring_enabled = enabled; }
    bool Get_Monitoring() { return monitoring_enabled; }
    void Set_Monitoring_Retention(unsigned long long retention) { monitoring_retention = retention; }
    unsigned long long Get_Monitoring_Retention() { return monitoring_retention; }
};
#endif /* MODULE_CONFIG_H */
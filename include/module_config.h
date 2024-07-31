#ifndef MODULE_CONFIG_H
#define MODULE_CONFIG_H

class Module_Config {
public:
    static Module_Config& getInstance(){
        static Module_Config instance; 
        return instance;
    }
private:
    Module_Config() {}
    bool monitoring_enabled; 
public:
    Module_Config(Module_Config const&) = delete;
    void operator=(Module_Config const&) = delete;
    void Set_Monitoring(bool enabled) { monitoring_enabled = enabled; }
    bool Get_Monitoring() { return monitoring_enabled; }
    
};
#endif /* MODULE_CONFIG_H */
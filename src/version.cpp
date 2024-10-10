#include "version.h"

int VersionManager::version = 0;  // Initialize the static member
std::string VersionManager::version_str = "";  // Initialize the static member

void VersionManager::Set_Module_Version(const char* version_string) {
    if (version == 0) {  // Only set the version once
        int major = 0;
        int minor = 0;
        int patch = 0;
        sscanf(version_string, "%d.%d.%d", &major, &minor, &patch);
        version = (10000 * major + 100 * minor + patch);
        version_str = version_string;
    }
}

int VersionManager::Get_Module_Version() {
    return version;
}

std::string VersionManager::Get_Module_Version_Str() {
    return version_str;
}

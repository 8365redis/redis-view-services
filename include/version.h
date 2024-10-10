#ifndef VIEW_VERSION_H
#define VIEW_VERSION_H

#include <string>

class VersionManager {
public:
    // Set the version using a string like "1.2.3"
    static void Set_Module_Version(const char* version_string);

    // Get the version as an integer (e.g., 10203 for "1.2.3")
    static int Get_Module_Version();

    // Get the version as an string (e.g. "1.2.3")
    static std::string Get_Module_Version_Str();

private:
    static int version;
    static std::string version_str;
};

#endif /* VIEW_VERSION_H */

//
// Created by Jorge Imperial-Sosa on 1/25/21.
//

#ifndef FTDCPARSER_CONFIGURATION_H
#define FTDCPARSER_CONFIGURATION_H

#include <filesystem>
#include "yaml-cpp/yaml.h"

class Configuration {
public:
    explicit Configuration(std::string path) { this->path = std::filesystem::absolute(path); }
    ~Configuration() = default;;

    int WriteDefaultConfiguration();

    static std::vector<std::string> AliasesToPrefixes(const std::string &all_aliases);

public:
    int read();
    int write();


private:
    std::string path;
    std::map<std::string, std::string> aliases;
    YAML::Node root;
};



#endif //FTDCPARSER_CONFIGURATION_H
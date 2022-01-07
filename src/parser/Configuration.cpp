//
// Created by jorge on 8/14/21.
//

#include "Configuration.h"
//
// Created by Jorge Imperial-Sosa on 1/26/21.
//
//
// Created by Jorge Imperial on 11/17/20.
//

#include <yaml-cpp/yaml.h>
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/algorithm/string.hpp>
#include <fstream>
#include <Configuration.h>


int Configuration::read() {

    try {
        BOOST_LOG_TRIVIAL(info) << "Alias file in  " << this->path;
        root = YAML::LoadFile(this->path);
        YAML::Node aliases_node = root["aliases"];

        for(YAML::const_iterator it=aliases_node.begin();it != aliases_node.end();++it) {
            auto key = it->first.as<std::string>();  // <- key
            auto value = it->second.as<std::string>();    // <- value
            this->aliases.emplace(key,value);
            BOOST_LOG_TRIVIAL(info) << "Alias: " << value;
        }
        return 0;
    }

    catch (YAML::ParserException &pe) {
        BOOST_LOG_TRIVIAL(fatal) << "Configuration.read() " << this->path << " " << pe.msg;
        return 1;
    }

    catch(YAML::Exception &e ) {
        BOOST_LOG_TRIVIAL(fatal) << "Configuration.read() " << this->path << " " << e.msg;
        return 1;
    }
}



int Configuration::write() {

    YAML::Emitter out;
    out << YAML::BeginMap;
    out << YAML::Key << "aliases";
    out << YAML::BeginMap;

    for (auto &a : this->aliases) {
        out << YAML::Key << a.first;
        out << YAML::Value << a.second;
    }
    out << YAML::EndMap;
    out << YAML::EndMap;

    try {
        std::ofstream out_file_name;
        out_file_name.open(this->path, std::ios::out);
        out_file_name << out.c_str();
        out_file_name.close();
    }
    catch ( YAML::Exception &e) {
        BOOST_LOG_TRIVIAL(fatal) << e.msg;
        return 1;
    }
    return 0;
}


int Configuration::WriteDefaultConfiguration() {
    YAML::Emitter out;

    // Useful defaults
    std::map <std::string, std::string> alias_test = std::map <std::string, std::string>();
    alias_test.emplace("find_failed", "serverStatus.metrics.commands.find.ailed");
    alias_test.emplace("find_total",  "serverStatus.metrics.commands.find.total");
    alias_test.emplace("find_modify_failed", "serverStatus.metrics.commands.findAndModify.failed");
    alias_test.emplace("find_modify_total", "serverStatus.metrics.commands.findAndModify.total");
    alias_test.emplace("update_failed",  "serverStatus.metrics.commands.update.failed");
    alias_test.emplace("update_total", "serverStatus.metrics.commands.update.total");
    alias_test.emplace("delete_failed", "serverStatus.metrics.commands.delete.failed");
    alias_test.emplace("delete_total", "serverStatus.metrics.commands.delete.total");
    alias_test.emplace("insert_failed", "serverStatus.metrics.commands.insert.failed");
    alias_test.emplace("insert_total", "serverStatus.metrics.commands.insert.total");
    alias_test.emplace("ops_command", "serverStatus.opcounters.command");
    alias_test.emplace("ops_insert", "serverStatus.opcounters.insert");
    alias_test.emplace("ops_query", "serverStatus.opcounters.query");
    alias_test.emplace("ops_update", "serverStatus.opcounters.update");
    alias_test.emplace("ops_getmore", "serverStatus.opcounters.getmore");
    alias_test.emplace("ops_delete", "serverStatus.opcounters.delete");
    alias_test.emplace("rs_state_0", "replSetGetStatus.members.0.state");
    alias_test.emplace("rs_state_1", "replSetGetStatus.members.1.state");
    alias_test.emplace("rs_state_2", "replSetGetStatus.members.2.state");

    out << YAML::BeginMap;
    out << YAML::Key << "aliases";

    out << YAML::BeginMap;
    for (auto &a : alias_test) {
        out << YAML::Key << a.first;
        out << YAML::Value << a.second;
    }
    out << YAML::EndMap;
    out << YAML::EndMap;

    try {
        std::ofstream out_file_name;
        out_file_name.open(this->path, std::ios::out);
        out_file_name << out.c_str();
        out_file_name.close();
    }

    catch ( YAML::Exception &e) {
        BOOST_LOG_TRIVIAL(fatal) << "Configuration.WriteDefaultConfiguration() " << e.msg;
        return 1;
    }

    return 0;
}


std::vector<std::string>
Configuration::AliasesToPrefixes(const std::string &all_aliases) {

    // break comma separated
    std::vector<std::string> strs;
    boost::split(strs, all_aliases, boost::is_any_of(","));
    return strs;
}

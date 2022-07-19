//
// Created by jorge on 4/27/22.
//

#ifndef PYFTDC_METRICSTOWTMAP_H
#define PYFTDC_METRICSTOWTMAP_H


#include <map>
#include <vector>

class ColumnGroup {
public:
    ColumnGroup() { columns.clear(); }
    ColumnGroup(std::string name) { colgroupName = name; }

    void setName(std::string groupName ) { colgroupName = groupName; }
    void addColumn(std::string column) ;

    std::string getColumns();

private:
    std::string colgroupName;
    std::vector<std::string> columns;
};


class MetricsToWTMap {
public:
    int add(std::string metric);
    bool parseMetricName(std::string metricName,
                         std::string *collectionName, std::string *groupName, std::string *columnName);

    std::string getTableName(int i);

    size_t getTableCount();

    int getTableIndexByName(std::string name);

    std::string getColumnsForTable(std::string name);

private:
    std::map<std::string, ColumnGroup*> tables;

};


#endif //PYFTDC_METRICSTOWTMAP_H

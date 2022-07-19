//
// Created by jorge on 4/27/22.
//

#include <sstream>
#include "MetricsToWTMap.h"
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>

void
ColumnGroup::addColumn(std::string column) {
    if (!column.empty())
       columns.emplace_back(column);
}


bool
MetricsToWTMap::parseMetricName(std::string metricName,
                         std::string  *collectionName,
                         std::string  *groupName,
                         std::string  *columnName ) {
    // Metric names are separated by dots
    std::istringstream iss(metricName);

    std::string token;
    int tokenCount = 0;

    while (std::getline(iss, token, '.')) {
        if (!token.empty()) {
            switch (tokenCount) {
                case 0:
                    *collectionName = token;
                    break;
                case 1:
                    *groupName = token;
                    break;
                case 2:
                    columnName->append(token);
                    break;
                default:
                    columnName->append("_");
                    columnName->append(token);
            }
            ++tokenCount;
        }
    }

    std::replace(columnName->begin(), columnName->end(), ' ', '-');

    return true;
}

int
MetricsToWTMap::add(std::string metricName) {
    std::string table;
    std::string group;
    std::string column;


    if (parseMetricName(metricName, &table, &group, &column)) {

        BOOST_LOG_TRIVIAL(info) << "Table:" << table << "  Group:" << group << " Column:" << column;

        ColumnGroup *colGroup;
        auto thisTable = tables.find(table);

        if (thisTable != tables.end())
            colGroup = thisTable->second;
        else {
            colGroup = new ColumnGroup(group);
            tables.emplace(table, colGroup);
        }

        colGroup->addColumn(column);
    }

    return 0;
}

std::string
MetricsToWTMap::getTableName(int n) {
    int i = 0;
    for (auto it : tables) {
        if (n == i) return it.first;
        ++i;
    }
    return std::string();
}

size_t
MetricsToWTMap::getTableCount() {
    return tables.size();
}

int MetricsToWTMap::getTableIndexByName(std::string name) {
    size_t i = 0;
    for (auto it : tables) {
        if (name == it.first) return i;
        ++i;
    }

    return -1;
}

std::string MetricsToWTMap::getColumnsForTable(std::string name) {
    auto t = tables[name];

    if (t)
        return t->getColumns();
    else return "";

}

std::string ColumnGroup::getColumns() {
    auto cols = this->columns;
    std::string columnList;
    bool firstOne = true;
    for (auto & c : cols) {
        if (!firstOne)  columnList += ",";
        firstOne = false;
        columnList += c;
    }

    return columnList;
}

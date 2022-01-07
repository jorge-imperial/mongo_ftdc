//
// Created by jorge on 11/25/20.
//

#ifndef FTDCPARSER_METRIC_H
#define FTDCPARSER_METRIC_H

#include <string>
#include <vector>
#include <bson/bson.h>

class ChunkMetric {

public:

    ChunkMetric(std::string name, bson_type_t param, int64_t init_value);
    ChunkMetric(std::string name);

    static const int MAX_SAMPLES = 300;
public:
    std::string name;
    bson_type_t  type;
    uint64_t  values[MAX_SAMPLES];  // All metrics fit in a 64 bit integer so....

};


#endif //FTDCPARSER_METRIC_H

//
// Created by jorge on 11/25/20.
//
#include "include/ChunkMetric.h"


ChunkMetric::ChunkMetric(std::string name, bson_type_t type, int64_t initialValue) {
    this->name = std::move(name);
    this->type = type;
    values[0] = initialValue;
}

ChunkMetric::ChunkMetric(std::string name) {
    this->name = std::move(name);
}

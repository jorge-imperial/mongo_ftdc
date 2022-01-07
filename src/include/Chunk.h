//
// Created by jorge on 11/14/20.
//

#ifndef FTDCPARSER_CHUNK_H
#define FTDCPARSER_CHUNK_H

#include <cstdint>
#include <string>
#include <utility>
#include <vector>
#include <map>

#include "ChunkMetric.h"
#include "Timestamp.h"

using namespace ftdcparser;

/**
 *
 *  A chunkVector is a document that has _id, type and data or doc depending if this is a INFO or DATA chunkVector.
 */
class Chunk {

public:

    static const int CHUNK_MAX_SIZE = 1000000;
    static const int INVALID_CHUNK_NUMBER   = INT_MAX;
    static const int INVALID_TIMESTAMP_POS = INT_MAX;

    Chunk (const uint8_t *data, size_t size, int64_t id, bool logMetricNames);
    ~Chunk() {
        delete [] compressed ;
        delete [] decompressed;
    }
    int Decompress();
    const uint8_t *getUncompressedData() { return decompressed; };
    size_t getUncompressedSize() const { return decompressed_size; };
    int ReadVariableSizedInts();
    int ConstructMetrics(const uint8_t *data );
    int getMetricsCount() { return metrics.size(); }


    ChunkMetric *getMetric(int metricNumber) { return metrics[metricNumber]; };
    size_t getMetric(const std::string metricName, uint64_t *data)  {
        for ( auto &m : metrics) {
            if (m->name == metricName) {
                memcpy(data, m->values, (deltasInChunk+1)*sizeof(uint64_t));
                return deltasInChunk;
            }
        }
        return 0;
    }
    uint64_t *getMetric(const std::string metricName) {
        for ( auto &m : metrics) {
            if (m->name == metricName)
                return m->values;
        }
        return 0;
    }
    size_t getMetricNames(std::vector<std::string> & metricNames);

    int64_t getId() const { return id; };
    size_t getSamplesCount() const { return 1+deltasInChunk; };
    Timestamp getStart() { return  start; };
    Timestamp getEnd() { return end; };
    void setTimestampLimits();

    bool operator<(Chunk const &other) {  return id < other.id; }

    bool Consume() ;

    std::string getJsonAtPosition(size_t position);
    std::string getJsonFromTimestamp(Timestamp ts);

private:
    int inflate(const void *src, int srcLen, void *dst, int dstLen);

    int64_t id;
    Timestamp start, end;

    std::vector<ChunkMetric*> metrics;

    uint8_t *decompressed{};
    int decompressed_size{};

    uint8_t *compressed{};
    int compressed_size{};

    std::map <bson_type_t, std::string> BSONTypes;

    int32_t metricsInChunk;
    int32_t deltasInChunk;

    bson_t *bsonStruct;  // The initial chunk as a json string
};


#endif //FTDCPARSER_CHUNK_H

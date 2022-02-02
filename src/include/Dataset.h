//
// Created by jorge on 12/16/20.
//

#ifndef FTDCPARSER_DATASET_H
#define FTDCPARSER_DATASET_H

#include <string>
#include <vector>
#include <map>
#include <boost/thread/mutex.hpp>
#include "SampleLocation.h"
#include "Chunk.h"
#include "FileParsedData.h"
#include "Timestamp.h"
#include "Metrics.h"


class Dataset {

public:


    Dataset() : samplesInDataset(0),  metricNames(0), lazyParsing(0) {};

    void addChunk(Chunk *pChunk);
    size_t getChunkCount() { return chunkVector.size(); }
    Chunk *getChunk(size_t n) { return (n < chunkVector.size()) ? chunkVector[n] : nullptr; }
    std::vector<Chunk *> getChunkVector() { return chunkVector; }
    size_t getMetricNames(std::vector< std::string> & metricNames);
    size_t getMetricLength() const { return samplesInDataset; }

    MetricsPtr getMetric(std::string   metricName, Timestamp start, Timestamp end, bool ratedMetric=false);

    uint64_t getMetricValue(std::string metricName, size_t pos);

    std::map<std::string, MetricsPtr> getHashMetrics() { return hashMapMetrics; }
    size_t getMetricNamesCount() { return metricNames.size(); }
    void sortChunks();
    void FileParsed(std::string path, uint64_t start, uint64_t end, size_t samplesInFile);

    bool IsMetricInDataset(const std::string& metric);
    std::vector<FileParsedData*> getParsedFileInfo() {  return this->filesParsed; }
    size_t LoadMetricsNamesFromChunk();

    void setLazyParsingFlag() { lazyParsing = true; }
    [[nodiscard]] bool getLazyParsing() const { return lazyParsing; }

    std::vector<MetricsPtr> getMetrics( std::vector<std::string> metricNames,
                                                 size_t start,  size_t end,
                                                 bool ratedMetrics);
    MetricsPtr getMetricMatrix( std::vector<std::string> metricNames, size_t *stride, Timestamp start,  Timestamp end,
                     bool ratedMetrics);

    Timestamp getStartTimestamp();
    Timestamp getEndTimestamp();

    std::string getJsonFromTimestamp(Timestamp ts);
    std::string getJsonAtPosition(int pos);

    std::string getCsvFromTimestamp(Timestamp ts);

private:
    std::vector<FileParsedData *> filesParsed;
    SampleLocation getLocationInMetric(Timestamp ts, bool fromStart);
    MetricsPtr assembleMetricFromChunks(std::string name,  SampleLocation startLocation, SampleLocation endLocation);
    bool ConvertToRatedMetric(MetricsPtr pVector);

private:
    std::vector<Chunk*> chunkVector;
    size_t samplesInDataset;
    boost::mutex mu;
    std::vector<std::string> metricNames;
    std::map<std::string,  MetricsPtr>  hashMapMetrics;

    bool lazyParsing;
};


#endif //FTDCPARSER_DATASET_H

//
// Created by jorge on 11/2/20.
//

#ifndef FTDCPARSER_FTDCPARSER_H
#define FTDCPARSER_FTDCPARSER_H

#include "iostream"
#include "vector"
#include <Chunk.h>
#include <Dataset.h>
#include <ParserTasksList.h>
#include <string_view>
#include <string>
#include "FileParsedData.h"


// From libbson
#include <bson/bson.h>

#include <boost/program_options.hpp>

class FTDCParser    {
public:
    bson_reader_t* open(std::string file_path);

    int parseFiles(std::vector<std::string> filePaths, bool onlyMetadata=false, bool onlyMetricNames=false, bool lazyParsing=false);
    int parseFiles(std::string filePaths, bool onlyMetadata=false,  bool onlyMetricNames=false, bool lazyParsing=false);

    int parseInfoChunk (const bson_t *bson);
    std::vector<std::string> getMetricsNamesPrefixed(std::string prefix) ;
    std::vector<std::string> getMetricsNames();

    MetricsPtr getMetric(std::string name, Timestamp start=INVALID_TIMESTAMP, Timestamp end=INVALID_TIMESTAMP, bool ratedMetric=false);
    std::vector<MetricsPtr> getMetric( std::vector<std::string> metricNames,
                                                Timestamp start=INVALID_TIMESTAMP, Timestamp end=INVALID_TIMESTAMP, bool ratedMetric=false);
 
    size_t getMetricLength() { return 0; }
    std::vector<Chunk*> getChunks() { return dataSet.getChunkVector(); }

    std::vector<std::string> getMetadata() { return metadata; }

    std::vector<FileParsedData*> getParsedFileInfo() {return dataSet.getParsedFileInfo(); }

    MetricsPtr getMetricMatrix(std::vector<std::string> metricNames,
                                        size_t *length,
                                        Timestamp start=INVALID_TIMESTAMP, Timestamp end=INVALID_TIMESTAMP,
                                        bool ratedMetric=false);

    size_t dumpDocsAsJsonTimestamps( std::string  inputFile,  std::string  outputFile, Timestamp start, Timestamp end);
    size_t dumpDocsAsCsvTimestamps( std::string  inputFile,  std::string  outputFile, Timestamp start, Timestamp end);

    std::string getJsonAtPosition(size_t position);

    void setVerbose(bool verbosity);

    // For testing.
    Dataset* getDataset() { return &dataSet; }

private:
    ParserTasksList parserTasks;
    Dataset dataSet;
    std::vector<std::string> metadata;
    bool verbose = false;
};


#endif //FTDCPARSER_FTDCPARSER_H
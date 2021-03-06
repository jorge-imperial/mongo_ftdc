//
// Created by jorge on 12/16/20.
//
#include "include/Dataset.h"

#include "SampleLocation.h"
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>


namespace logging = boost::log;

size_t
Dataset::LoadMetricsNamesFromChunk() {
    if (chunkVector.size() > 0) {
        if (lazyParsing && (chunkVector[0]->getUncompressedSize() == 0)) {
            // Consume the first chunk, so we have metric names.
            chunkVector[0]->Consume();
        }
        chunkVector[0]->getMetricNames(metricNames);
        return metricNames.size();
    }

    return 0;
}

size_t
Dataset::getMetricNames(std::vector<std::string> & metrics) {

    if (metricNames.empty())
       LoadMetricsNamesFromChunk();

    metrics = metricNames;
    return metrics.size();
}

void
Dataset::addChunk(Chunk *pChunk) {

    // Critical section
    mu.lock();

    //
    chunkVector.emplace_back(pChunk);

    // total size of samplesInDataset
    samplesInDataset += pChunk->getSamplesCount();

    // Append metrics here.

    //
    mu.unlock();
}

void
Dataset::sortChunks() {
    struct {
        bool operator()(Chunk *a, Chunk *b) const { return a->getId() < b->getId(); }
    } compare;
    std::sort(chunkVector.begin(), chunkVector.end(), compare);
}

bool
Dataset::IsMetricInDataset(const std::string& metric) {

    // If lazy parsing,
    if (metricNames.empty())
        LoadMetricsNamesFromChunk();

    for (const auto& m: metricNames)
        if (m.compare((metric)) == 0)
            return true;

    return false;
}

MetricsPtr
Dataset::getMetric(std::string   metricName, const Timestamp start, const Timestamp end, bool ratedMetric)
{

    if (metricName.at(0) == '@')  {
        metricName = metricName.substr(1,metricName.size()-1);
        ratedMetric = true;
    }

    if (!IsMetricInDataset(metricName)) {
        return nullptr;
    }

    auto start_chunk_pos = getLocationInMetric(start, true);
    auto end_chunk_pos = getLocationInMetric(end, false);

    if (lazyParsing) { ;
        // for chunks between start and end, start parser threads
        for (auto chunkNumber = start_chunk_pos.getChunkLoc(); chunkNumber<=end_chunk_pos.getChunkLoc(); ++chunkNumber ) {
            if (chunkVector[chunkNumber]->getUncompressedSize() == 0)
                chunkVector[chunkNumber]->Consume();
        }
    }

    MetricsPtr metrics =  assembleMetricFromChunks(metricName, start_chunk_pos, end_chunk_pos);
    if (ratedMetric)
        ConvertToRatedMetric(metrics);

    return metrics;
}

SampleLocation
Dataset::getLocationInMetric(Timestamp ts, bool fromStart) {

    auto chunkPos = Chunk::INVALID_CHUNK_NUMBER;
    auto samplePos = Chunk::INVALID_TIMESTAMP_POS;

    // Easy cases
    if (ts == INVALID_TIMESTAMP) {
        if (fromStart) { // first ts of first chunk
            chunkPos = 0;
            samplePos = 0;
        }
        else { // last ts of last chunk
            chunkPos = chunkVector.size()-1;
            samplePos =  ChunkMetric::MAX_SAMPLES-1;
        }

        return  SampleLocation(chunkPos, samplePos);
    }

    //
    int chunkNumber = 0;
    for (auto c: chunkVector) {
        if (ts >= c->getStart() && ts <= c->getEnd()) {  // this is the chunk
            chunkPos = chunkNumber;
            // Timestamps 'start' metrics are always the 0-th position
            auto timestamps = c->getChunkMetric(0);
            for (int i = 0; i < ChunkMetric::MAX_SAMPLES; ++i) {
                if (timestamps->getValue(i) >= ts) {
                    samplePos = i;

                    if (i ==0 && !fromStart) {
                        // actually previous chunk
                        samplePos = ChunkMetric::MAX_SAMPLES - 1;
                        --chunkPos;
                    }
                    break;
                }
            }
            break;
        }
        ++chunkNumber;
    }

    return  SampleLocation (chunkPos,samplePos);
}

MetricsPtr
Dataset::assembleMetricFromChunks(const std::string metricName, SampleLocation startLocation, SampleLocation endLocation) {

    // chunks and positions
    auto startChunk = startLocation.getChunkLoc();
    auto startSamplePos =  startLocation.getSampleLoc();

    auto endChunk = endLocation.getChunkLoc();
    auto endSamplePos = endLocation.getSampleLoc();

    MetricsPtr p = new std::vector<uint64_t>;
    if (endChunk == startChunk) {
        auto sample_count = endSamplePos - startSamplePos;
        p->reserve(sample_count);
        auto c = chunkVector[startChunk]->getMetric(metricName);
        p->assign(c + startSamplePos, c + endSamplePos);
    }
    else {
        size_t sampleCount = (endChunk-startChunk)*ChunkMetric::MAX_SAMPLES;
        BOOST_LOG_TRIVIAL(info) << "Metric: '" << metricName << "'. Reserving for " << sampleCount << " samples.";
        p->reserve(sampleCount);

        // first chunk
        auto metric = chunkVector[startChunk]->getMetric(metricName);
        p->assign(metric, metric + (ChunkMetric::MAX_SAMPLES - startSamplePos));
        // Append chunks
        for (int i = startChunk + 1; i < endChunk; ++i) {
            metric = chunkVector[i]->getMetric(metricName);
            if (!metric) {
                BOOST_LOG_TRIVIAL(error) << "Empty metric in chunk: " << i << " name= '" << metricName << "'";
            }
            else {
                p->insert(p->end(), metric, metric + ChunkMetric::MAX_SAMPLES);
            }
        }

        // Append last chunk
        metric = chunkVector[endChunk]->getMetric(metricName);
        if (!metric)
            BOOST_LOG_TRIVIAL(error) << "Empty metric in last chunk: name='" << metricName <<"'";
        else
            p->insert(p->end(), metric, metric + endSamplePos);
    }
    return p;
}

bool
Dataset::ConvertToRatedMetric(MetricsPtr metric) {

    bool goesNegative = false;

    auto it = metric->end();

    for (auto prev = it-1; it != metric->begin(); --it, --prev) {
        *it -= *prev;
        if (*it < 0) goesNegative = true;
    }

    // Remove first element because there is no way of calculating delta (this way avg, min, max are valid!).
    //metric->erase(metric->begin());

    // Or keep element, copying from 1st position.
    metric->at(0) = metric->at(1);

    return !goesNegative;
}

void
Dataset::finishedParsingFile(std::string path, Timestamp start, Timestamp end, size_t samplesInFile) {
#if 0
    int metricsNameLen = 0;
    for (auto chunk : chunkVector) {

        auto currMetricLen = chunk->getMetricsCount();
        if (metricsNameLen != 0  && metricsNameLen!=currMetricLen) {
            BOOST_LOG_TRIVIAL(debug) << "Number of metrics differ from chunk to chunk:" << metricsNameLen << "!= " << currMetricLen;
        }

        if (metricsNameLen!=currMetricLen) {
            metricNames.clear();
            chunk->getMetricNames(metricNames);
            metricsNameLen = currMetricLen;
        }
    }
#endif
    // Not all chunks have all metrics.
    for (auto & chunk : chunkVector) {
        std::vector<std::string> metricsInChunk;
        chunk->getMetricNames(metricsInChunk);
        for ( auto & s : metricsInChunk)
            metricNames.emplace_back(s);
    }

    sort(metricNames.begin(), metricNames.end());
    auto n = metricNames.size();
    metricNames.erase( unique(metricNames.begin(),metricNames.end()), metricNames.end());

    auto fileData = new FileParsedData(path.c_str(), start, end, samplesInFile);
    filesParsed.emplace_back(fileData);

    // WT
    initMongoStorage();
    writeAsCollections();
}

std::vector<MetricsPtr>
Dataset::getMetrics(const std::vector<std::string> metricNames,
                    const size_t start, const size_t end,
                    const bool ratedMetrics) {

    std::vector<MetricsPtr> metricList;

    for(auto name : metricNames) {
        auto element = getMetric(name, start, end, ratedMetrics);
        metricList.emplace_back(element);
    }

    return metricList;
}

MetricsPtr
Dataset::getMetricMatrix(const std::vector<std::string> metricNames, size_t *stride,
                         const Timestamp start, const Timestamp end,  const bool ratedMetrics) {

    //  Get metrics
    auto mm = getMetrics(metricNames, start, end, ratedMetrics);
    // get a length
    size_t len=0;
    for (auto m: mm) {
        if (m && m->size()>0) {
            len = m->size();

            if (stride)
                *stride = len;
            break;
        }
    }

    // Allocate
    MetricsPtr p = new std::vector<uint64_t>;

    p->reserve(len*metricNames.size());

    for (auto m: mm) {
        if (m)
            p->insert(p->end(), m->begin(),m->end());
        else
            p->insert(p->end(), len, 0);
    }

    return p;
}

uint64_t
Dataset::getMetricValue(std::string metricName, size_t pos) {
    int chunkNumber = pos / ChunkMetric::MAX_SAMPLES;
    int posInChunk = pos % ChunkMetric::MAX_SAMPLES;
    auto v = chunkVector[chunkNumber]->getMetric(metricName);

    return v[posInChunk];
}

Timestamp
Dataset::getStartTimestamp() {
    return chunkVector[0]->getStart();
}

Timestamp
Dataset::getEndTimestamp() {
    return chunkVector[chunkVector.size()-1]->getEnd();
}

std::string
Dataset::getJsonFromTimestamp(Timestamp ts) {
    auto location = getLocationInMetric(ts, true);

    if (location.getChunkLoc() == Chunk::INVALID_CHUNK_NUMBER
    || location.getSampleLoc() == Chunk::INVALID_TIMESTAMP_POS)
        return "{}";

    return chunkVector[location.getChunkLoc()]->getJsonAtPosition(location.getSampleLoc());
}

std::string
Dataset::getJsonAtPosition(int pos) {

    int chunkNumber = pos / ChunkMetric::MAX_SAMPLES;
    int posInChunk = pos % ChunkMetric::MAX_SAMPLES;
    return chunkVector[chunkNumber]->getJsonAtPosition(posInChunk);
}

std::string
Dataset::getCsvFromTimestamp(Timestamp ts) {
    auto location = getLocationInMetric(ts, true);

    if (location.getChunkLoc() == Chunk::INVALID_CHUNK_NUMBER
        || location.getSampleLoc() == Chunk::INVALID_TIMESTAMP_POS)
        return "{}";
    return chunkVector[location.getChunkLoc()]->getCsvAtPosition(location.getSampleLoc());
}

/*
 * Create the metrics table. Keys are timestamps.
 *
 * table=hostname_port ; use the host name and port as the name of the table
 * access_pattern_hint=sequential  ; This is the usual case for our application
 * key_format:u  ; Timestamps are keys, and we store them as 64 bit unsigned integers
 * value_format: uuu... ; Because we also store 64 bit unsigned integers
 * columns: timestamp,name,name,...  ; These are the names of the columns
 * colgroups=() ; Not sure if to use them yet
 * block_compressor=lz4 ; minimize space on disk
 */
size_t
Dataset::createMetricsMaps() {

    std::vector<std::string> metricNames;
    for (auto & chunk : chunkVector) {
        std::vector<std::string> metricsInChunk;
        chunk->getMetricNames(metricsInChunk);

        for ( auto & s : metricsInChunk)
            metricNames.emplace_back(s);
    }

    sort(metricNames.begin(), metricNames.end());
    auto n = metricNames.size();
    metricNames.erase( unique(metricNames.begin(),metricNames.end()), metricNames.end());

    // Separate metrics in collections and groups
    for (auto &metricName : metricNames) {
        MetricsMap.add(metricName);
    }

    return 0;
}


int 
Dataset::initMongoStorage() {

    createMetricsMaps();

    mongoDBPath = std::filesystem::current_path();
    // create directory
    if (!std::filesystem::exists("dbpath"))
        std::filesystem::create_directory("dbpath");

    // Open a connection to the database, creating it if necessary.
    mongoDBPath += std::filesystem::path().preferred_separator;
    mongoDBPath += "dbpath";

    return 0;
}


int
Dataset::writeAsCollections() {

    return 0;
}


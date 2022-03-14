//
// Created by jorge on 3/13/22.
//

#include <boost/test/included/unit_test.hpp>
#include <boost/test/parameterized_test.hpp>

#include <boost/format.hpp>

#include <boost/algorithm/string/classification.hpp>

#include <FTDCParser.h>
#include <Chunk.h>
#include <iostream>
#include <boost/thread.hpp>

#include <filesystem>

static const char *DATA_FILE_NAMES[] = { "./cpp_tests/metrics.3.6.17.ftdc",
                                         "./cpp_tests/metrics.4.0.26.ftdc",
                                         "./cpp_tests/metrics.4.2.14.ftdc",
                                         "./cpp_tests/metrics.4.4.12.ftdc",
                                         "./cpp_tests/metrics.5.0.2.ftdc",
                                         NULL };
static const char *CSV_FILE_NAMES[] = { "./cpp_tests/metrics.3.6.17.csv",
                                        "./cpp_tests/metrics.4.0.26.csv",
                                        "./cpp_tests/metrics.4.2.14.csv",
                                        "./cpp_tests/metrics.4.4.12.csv",
                                        "./cpp_tests/metrics.5.0.2.csv",
                                        NULL };

int ParserTaskConsumerThread(ParserTasksList *parserTasks, Dataset *dataSet);


size_t readMetricsFromCSV(std::string csvTestFile, std::vector<ChunkMetric *> * metrics) {

    metrics->clear();
    metrics->reserve(2000);

    //
    std::ifstream f;
    f.open(csvTestFile, std::ios::in);
    std::string line;
    while (getline(f, line)) { //read data from file object and put it into string.

        std::stringstream ss(line);
        std::string token;
        ChunkMetric *pM = nullptr;
        int field = 0;
        do {
            std::getline(ss, token, ';');
            if (!pM) {
                pM = new ChunkMetric(token, BSON_TYPE_INT64, 0);
            } else {
                auto val = pM->getValues();
                val[field++] = atol(token.c_str());
            }

        } while (field < 300);
        metrics->emplace_back(pM);
    }
    f.close();

    return metrics->size();
}

BOOST_AUTO_TEST_SUITE(ftdc_parsing_suite)

    void OpenFile( const char *file ) {
        // Create parser
        auto *parser = new FTDCParser();

        auto file_path = std::filesystem::current_path();
        file_path.append(file);
        BOOST_TEST_MESSAGE(file_path);

        auto reader = parser->open(file_path);
        BOOST_CHECK_NE(reader, nullptr);

        bson_reader_destroy(reader);

        delete parser;
    }

    void ReadInfoChunk(const char *file) {
        // Create parser
        auto *parser = new FTDCParser();

            auto file_path = std::filesystem::current_path();
            file_path.append(DATA_FILE_NAMES[0]);
            BOOST_TEST_MESSAGE(file_path);
            auto reader = parser->open(file_path);
        BOOST_CHECK(reader);

        auto pBsonChunk = bson_reader_read(reader, 0);

        parser->parseInfoChunk(pBsonChunk);

        bson_reader_destroy(reader);
        delete parser;
        BOOST_CHECK_EQUAL(1, 1);
    }

    void ReadDataChunk(const char *file) {

        // Create parser
        auto parser = new FTDCParser();

        auto reader = parser->open(file);
        BOOST_CHECK(reader);

        auto pBsonChunk = bson_reader_read(reader, 0);

        parser->parseInfoChunk(pBsonChunk);

        pBsonChunk = bson_reader_read(reader, 0);

        Dataset dataset;
        ParserTasksList parserTasks;
        int64_t id;
        bson_iter_t iter;
        if (bson_iter_init(&iter, pBsonChunk)) {
            while (bson_iter_next(&iter)) {

                if (BSON_ITER_HOLDS_BINARY(&iter)) {
                    bson_subtype_t subtype;
                    uint32_t bin_size;
                    const uint8_t *data;
                    bson_iter_binary(&iter, &subtype, &bin_size, reinterpret_cast<const uint8_t **>(&data));

                    // the memory pointed to by data is managed internally. Better make a copy
                    uint8_t *bin_data = new uint8_t[bin_size];
                    memcpy(bin_data, data, bin_size);
                    parserTasks.push(bin_data, bin_size, id);

                    break; // <-- only one!

                } else if (BSON_ITER_HOLDS_DATE_TIME(&iter)) {
                    id = bson_iter_date_time(&iter);
                }
            }
        }

        size_t numThreads = boost::thread::hardware_concurrency();
        boost::thread_group threads;
        for (size_t i = 0; i < numThreads; ++i)
            threads.add_thread(new boost::thread(ParserTaskConsumerThread, &parserTasks, &dataset));

        // Wait for threads to finish
        threads.join_all();

        BOOST_CHECK_EQUAL(dataset.getChunkCount(), 1);

        auto pChunk = dataset.getChunk(0);
        BOOST_CHECK(pChunk);

        bson_reader_destroy(reader);

        delete parser;
        BOOST_CHECK_EQUAL(1, 1);
    }


    void CheckMetricNames(const char *file) {
        // Create parser
        auto parser = new FTDCParser();

        auto reader = parser->open(file);
        BOOST_CHECK(reader);

        auto pBsonChunk = bson_reader_read(reader, 0);

        parser->parseInfoChunk(pBsonChunk);

        // - - - - - - - - - -
        pBsonChunk = bson_reader_read(reader, 0);

        Dataset dataset;
        ParserTasksList parserTasks;
        int64_t id;
        bson_iter_t iter;
        if (bson_iter_init(&iter, pBsonChunk)) {
            while (bson_iter_next(&iter)) {

                if (BSON_ITER_HOLDS_BINARY(&iter)) {
                    bson_subtype_t subtype;
                    uint32_t bin_size;
                    const uint8_t *data;
                    bson_iter_binary(&iter, &subtype, &bin_size, reinterpret_cast<const uint8_t **>(&data));

                    // the memory pointed to by data is managed internally. Better make a copy
                    uint8_t *bin_data = new uint8_t[bin_size];
                    memcpy(bin_data, data, bin_size);
                    parserTasks.push(bin_data, bin_size, id);

                    break;

                } else if (BSON_ITER_HOLDS_DATE_TIME(&iter)) {
                    id = bson_iter_date_time(&iter);
                }
            }
        }

        size_t numThreads = boost::thread::hardware_concurrency();
        boost::thread_group threads;
        for (size_t i = 0; i < numThreads; ++i)
            threads.add_thread(new boost::thread(ParserTaskConsumerThread, &parserTasks, &dataset));

        // Wait for threads to finish
        threads.join_all();

        BOOST_CHECK_EQUAL(dataset.getChunkCount(), 1);

        auto pChunk = dataset.getChunk(0);

        BOOST_CHECK(pChunk);

        auto decomp_result = pChunk->Decompress();
        auto data = pChunk->getUncompressedData();
        BOOST_CHECK(data);
        auto dataSize = pChunk->getUncompressedSize();
        BOOST_CHECK(dataSize);

        // We construct the metrics. This are name and first value only since deltas have not been read.
        int metrics = pChunk->ConstructMetrics(data);
        BOOST_CHECK_EQUAL(metrics, pChunk->getMetricsCount());

        // Get names
        std::vector<std::string> metricsNames;
        pChunk->getMetricNames(metricsNames);

        // Compare first 1000 names
        for (int m = 0; m < 1000; ++m) {

            auto pMetric = pChunk->getMetric(m);
            BOOST_CHECK(pMetric);

            BOOST_CHECK_EQUAL(pMetric->getName(), metricsNames[m]);
        }

        //
        bson_reader_destroy(reader);

        delete parser;
    }


    void CompareMetrics(const char *dataFile, const char *csvFile) {
        // Create parser
        auto parser = new FTDCParser();

        auto reader = parser->open(DATA_FILE_NAMES[0]);
        BOOST_CHECK(reader);

        auto pBsonChunk = bson_reader_read(reader, 0);


        parser->parseInfoChunk(pBsonChunk);

        // - - - - - - - - - -
        pBsonChunk = bson_reader_read(reader, 0);

        Dataset dataset;
        ParserTasksList parserTasks;
        int64_t id;
        bson_iter_t iter;
        if (bson_iter_init(&iter, pBsonChunk)) {
            while (bson_iter_next(&iter)) {

                if (BSON_ITER_HOLDS_BINARY(&iter)) {
                    bson_subtype_t subtype;
                    uint32_t bin_size;
                    const uint8_t *data;
                    bson_iter_binary(&iter, &subtype, &bin_size, reinterpret_cast<const uint8_t **>(&data));

                    // the memory pointed to by data is managed internally. Better make a copy
                    uint8_t *bin_data = new uint8_t[bin_size];
                    memcpy(bin_data, data, bin_size);
                    parserTasks.push(bin_data, bin_size, id);

                    break;

                } else if (BSON_ITER_HOLDS_DATE_TIME(&iter)) {
                    id = bson_iter_date_time(&iter);
                }
            }
        }

        size_t numThreads = boost::thread::hardware_concurrency();
        boost::thread_group threads;
        for (size_t i = 0; i < numThreads; ++i)
            threads.add_thread(new boost::thread(ParserTaskConsumerThread, &parserTasks, &dataset));

        // Wait for threads to finish
        threads.join_all();

        BOOST_CHECK_EQUAL(dataset.getChunkCount(), 1);

        auto pChunk = dataset.getChunk(0);
        BOOST_CHECK(pChunk);

        auto data = pChunk->getUncompressedData();
        BOOST_CHECK(data);
        auto dataSize = pChunk->getUncompressedSize();
        BOOST_CHECK(dataSize);

        // We construct the metrics. These are name and first value only since deltas have not been read.
        int metrics = pChunk->ConstructMetrics(data);
        BOOST_CHECK_EQUAL(metrics, pChunk->getMetricsCount());

        std::vector<ChunkMetric*> readMetrics;
        auto metricsCount = readMetricsFromCSV(CSV_FILE_NAMES[0], &readMetrics);

        int brokenMetrics = 0;
        for (int i = 0; i < readMetrics.size(); ++i) {
            auto pMetric = pChunk->getMetric(i);
            BOOST_TEST_MESSAGE(str(boost::format("metric %1% name: %2%") % i % pMetric->getName() ));
            auto pMetricsFromCSV = readMetrics[i];

            for (int j = 0; j < metricsCount; ++j) {

                if (pMetric->getValue(j) != pMetricsFromCSV->getValue(j)) {

                    uint64_t diff = pMetric->getValue(j) - pMetricsFromCSV->getValue(j);

                    auto errs = str(
                            boost::format("ChunkMetric %1% %2% diverges index %3%  abs(%4%)  Is %5%  should be %6%")
                            % pMetric->getName().c_str() % i % j % std::abs((long) (diff)) % pMetric->getValue(j) %
                            pMetricsFromCSV->getValue(j));

                    ++brokenMetrics;
                    BOOST_TEST_MESSAGE(errs);
                    break;
                }
            }
        }

        BOOST_CHECK_EQUAL(brokenMetrics, 0);
        //
        bson_reader_destroy(reader);
    }


    BOOST_AUTO_TEST_CASE(test_OpenFile) {

        for (int n=0; DATA_FILE_NAMES[n] != NULL; ++n)
           OpenFile(DATA_FILE_NAMES[n]);
    }

    BOOST_AUTO_TEST_CASE(test_ReadInfoChunk) {
        for (int n=0;  DATA_FILE_NAMES[n] != NULL; ++n)
            ReadInfoChunk(DATA_FILE_NAMES[n]);
    }


    BOOST_AUTO_TEST_CASE(test_ReadDataChunk) {
        for (int n=0;  DATA_FILE_NAMES[n] != NULL; ++n)
            ReadDataChunk(DATA_FILE_NAMES[n]);
    }

    BOOST_AUTO_TEST_CASE(test_checkMetricNames) {
        for (int n=0;  DATA_FILE_NAMES[n] != NULL; ++n)
            CheckMetricNames(DATA_FILE_NAMES[n]);
    }


    BOOST_AUTO_TEST_CASE(test_compareMetrics) {
        for (int n=0;  DATA_FILE_NAMES[n] != NULL; ++n)
            CompareMetrics(DATA_FILE_NAMES[n], CSV_FILE_NAMES[n]);
    }
}
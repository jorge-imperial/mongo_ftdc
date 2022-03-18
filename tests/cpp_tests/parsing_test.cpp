//
// Created by jorge on 3/13/22.
//

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test_suite.hpp>
#include <boost/test/unit_test.hpp>

#include <boost/format.hpp>
#include <boost/thread.hpp>

#include <FTDCParser.h>
#include <Chunk.h>
#include <iostream>

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
size_t ReadMetricsFromCSV(std::string csvTestFile, std::vector<ChunkMetric *> * metrics);


struct TestFixtureParsing {

    TestFixtureParsing() {
        ;
    }

    ~TestFixtureParsing() {
        ;
    }

    void TestFixtureSetup() {
        BOOST_CHECK(true);
    }

};

BOOST_FIXTURE_TEST_SUITE(ParsingSuite, TestFixtureParsing);

    BOOST_AUTO_TEST_SUITE(ParsingSuite)

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

        size_t numThreads = 1; //  boost::thread::hardware_concurrency();
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
            if (!pMetric) BOOST_CHECK(pMetric);

            if (pMetric->getName().compare(metricsNames[m]))
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
        auto metricsCount = ReadMetricsFromCSV(CSV_FILE_NAMES[0], &readMetrics);

        int brokenMetrics = 0;
        for (int i = 0; i < readMetrics.size(); ++i) {
            auto pMetric = pChunk->getMetric(i);
            auto pMetricsFromCSV = readMetrics[i];

            // BOOST_TEST_MESSAGE(str(boost::format("metric %1% name: %2%  (%4%) %3%")  % i % pMetric->getName() % pMetricsFromCSV->getName() % dataFile ));


            for (int j = 0; j < metricsCount; ++j) {

                if (pMetric->getValue(j) != pMetricsFromCSV->getValue(j)) {

                    auto errs = str(
                            boost::format("%1%: ChunkMetric %3%[%2%] is %4%. File %5%[%2%] is %6%")
                            % i % j
                            % pMetric->getName().c_str() % pMetric->getValue(j)
                            % pMetricsFromCSV->getName().c_str() % pMetricsFromCSV->getValue(j)  );


                    BOOST_TEST_MESSAGE(errs);
                    ++brokenMetrics;
                    break;
                }
            }
        }

        if (brokenMetrics) {
            BOOST_TEST_MESSAGE("-----------------------------------------------Some metrics diverged.");
        }
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

BOOST_AUTO_TEST_SUITE_END();
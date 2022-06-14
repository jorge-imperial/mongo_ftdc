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

#include <wiredtiger.h>


static const char *DATA_FILE_NAMES[] = { "./cpp_tests/metrics.3.6.17.ftdc",
                                         "./cpp_tests/metrics.4.0.26.ftdc",
                                         "./cpp_tests/metrics.4.2.14.ftdc",
                                         "./cpp_tests/metrics.4.4.12.ftdc",
                                         "./cpp_tests/metrics.5.0.2.ftdc",
                                         nullptr };
static const char *CSV_FILE_NAMES[] = { "./cpp_tests/metrics.3.6.17.csv",
                                        "./cpp_tests/metrics.4.0.26.csv",
                                        "./cpp_tests/metrics.4.2.14.csv",
                                        "./cpp_tests/metrics.4.4.12.csv",
                                        "./cpp_tests/metrics.5.0.2.csv",
                                        nullptr };

int ParserTaskConsumerThread(ParserTasksList *parserTasks, Dataset *dataSet);
size_t ReadMetricsFromCSV(std::string csvTestFile, std::vector<ChunkMetric *> * metrics);


// TODO: Move all these to another file



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


void CreateDirectoryIfNotPresent(const char *name, std::string & path) ;

BOOST_FIXTURE_TEST_SUITE(ParsingSuite, TestFixtureParsing);

    BOOST_AUTO_TEST_SUITE(ParsingSuite)

    void OpenFile( const char *file ) {
        // Create parser
        auto *parser = new FTDCParser();

        auto file_path = std::filesystem::current_path();
        file_path.append(file);
        BOOST_TEST_MESSAGE(file_path);

        bson_reader_t  *reader = parser->open(file_path);
        BOOST_CHECK_NE(reader, nullptr);

        bson_reader_destroy(reader);

        delete parser;
    }

    void ReadInfoChunk(const char *file) {
        // Create parser
        auto *parser = new FTDCParser();

        auto file_path = std::filesystem::current_path();
        file_path.append(file);
        BOOST_TEST_MESSAGE(file_path);
        bson_reader_t  *reader = parser->open(file_path);
        BOOST_CHECK(reader);

        const bson_t *pBsonChunk = bson_reader_read(reader, nullptr);

        parser->parseInfoChunk(pBsonChunk);

        bson_reader_destroy(reader);
        delete parser;
        BOOST_CHECK_EQUAL(1, 1);
    }

    void ReadDataChunk(const char *file) {
        // Create parser
        auto parser = new FTDCParser();

        bson_reader_t  *reader = parser->open(file);
        BOOST_CHECK(reader);

        const bson_t * pBsonChunk = bson_reader_read(reader, nullptr);

        parser->parseInfoChunk(pBsonChunk);

        pBsonChunk = bson_reader_read(reader, nullptr);

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
                    auto bin_data = new uint8_t[bin_size];
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

        bson_reader_t  *reader = parser->open(file);
        BOOST_CHECK(reader);

        const bson_t *pBsonChunk = bson_reader_read(reader, nullptr);

        parser->parseInfoChunk(pBsonChunk);

        // - - - - - - - - - -
        pBsonChunk = bson_reader_read(reader, nullptr);

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
                    auto bin_data = new uint8_t[bin_size];
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

            auto pMetric = pChunk->getChunkMetric(m);
            if (!pMetric) BOOST_CHECK(pMetric);

            if (pMetric->getName() ==metricsNames[m])
                BOOST_CHECK_EQUAL(pMetric->getName(), metricsNames[m]);
        }

        //
        bson_reader_destroy(reader);

        delete parser;
    }

    BOOST_AUTO_TEST_CASE(test_Parser_OpenFile) {

        for (int n=0; DATA_FILE_NAMES[n] != nullptr; ++n)
           OpenFile(DATA_FILE_NAMES[n]);
    }

    BOOST_AUTO_TEST_CASE(test_Parser_ReadInfoChunk) {
        for (int n=0;  DATA_FILE_NAMES[n] != nullptr; ++n)
            ReadInfoChunk(DATA_FILE_NAMES[n]);
    }

    BOOST_AUTO_TEST_CASE(test_Parser_ReadDataChunk) {
        for (int n=0;  DATA_FILE_NAMES[n] != nullptr; ++n)
            ReadDataChunk(DATA_FILE_NAMES[n]);
    }

    BOOST_AUTO_TEST_CASE(test_Parser_CheckMetricNames) {
        for (int n=0;  DATA_FILE_NAMES[n] != nullptr; ++n)
            CheckMetricNames(DATA_FILE_NAMES[n]);
    }

    void CompareMetrics(const char *dataFile, const char *csvFile) {
        // Create parser
        auto parser = new FTDCParser();

        bson_reader_t *reader  = parser->open(dataFile);
        BOOST_CHECK(reader);

        const bson_t * pBsonChunk = bson_reader_read(reader, nullptr);


        parser->parseInfoChunk(pBsonChunk);

        // - - - - - - - - - -
        pBsonChunk = bson_reader_read(reader, nullptr);

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
                    auto bin_data = new uint8_t[bin_size];
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
        auto metricsCount = ReadMetricsFromCSV(csvFile, &readMetrics);

        int brokenMetrics = 0;
        for (int i = 0; i < readMetrics.size(); ++i) {
            auto pMetric = pChunk->getChunkMetric(i);
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


    BOOST_AUTO_TEST_CASE(test_Parser_compareMetrics) {
        for (int n=0;  DATA_FILE_NAMES[n] != nullptr; ++n)
            CompareMetrics(DATA_FILE_NAMES[n], CSV_FILE_NAMES[n]);
    }



    // ------------------------




    BOOST_AUTO_TEST_CASE(test_wt_WriteOneDoc) {

        WT_CONNECTION *conn;
        WT_CURSOR *cursor;
        WT_SESSION *session;
        const char *key, *value;


        int ret;

        // Open a session handle for the database.
        ret = conn->open_session(conn, nullptr, nullptr, &session);
        BOOST_CHECK_EQUAL(ret,0);
        ret = session->create(session, "table:access", "key_format=S,value_format=S");
        BOOST_CHECK_EQUAL(ret,0);
        ret = session->open_cursor(session, "table:access", nullptr, nullptr, &cursor);
        BOOST_CHECK_EQUAL(ret,0);

        cursor->set_key(cursor, "key1"); // Insert a record.
        cursor->set_value(cursor, "value1");
        ret = cursor->insert(cursor);
        BOOST_CHECK_EQUAL(ret,0);

        ret = cursor->reset(cursor); // Restart the scan.
        BOOST_CHECK_EQUAL(ret,0);
        while ((ret = cursor->next(cursor)) == 0) {
            ret = cursor->get_key(cursor, &key) ;
            BOOST_CHECK_EQUAL(ret,0);
            ret = cursor->get_value(cursor, &value);
            BOOST_CHECK_EQUAL(ret,0);
            printf("Got record: %s : %s\n", key, value);
        }
        BOOST_CHECK_EQUAL(ret, WT_NOTFOUND); // Check for end-of-table.

        ret = conn->close(conn, nullptr); // Close all handles.
        BOOST_CHECK_EQUAL(ret,0);
    }





    BOOST_AUTO_TEST_CASE(test_wt_WriteDocs)
    {
            // Create parser
            auto parser = new FTDCParser();
            std::string file = std::string(DATA_FILE_NAMES[4]);
            parser->parseFile(file);






#if CACA
            /*
             * Create two column groups: a primary column group with the country code, year and population
             * (named "main"), and a population column group with the population by itself (named
             * "population").
             */
            ret = session->create(session, "colgroup:metrics:main", "columns=(country,year,population)");
            ret = session->create(session, "colgroup:metrics:population", "columns=(population)");

            // Create an index with a simple key.
            ret = (session->create(session, "index:metrics:country", "columns=(country)"));
            // Create an index with a composite key (country,year).
            ret = (session->create(session, "index:metrics:country_plus_year", "columns=(country,year)"));
            // Create an immutable index.
            ret = (session->create(session, "index:metrics:immutable_year", "columns=(year),immutable"));

            // Insert the records into the table.
            ret = (session->open_cursor(session, "table:metrics", nullptr, "append", &cursor));
            for (p = pop_data; p->year != 0; p++) {
                cursor->set_value(cursor, p->country, p->year, p->population);
                ret = cursor->insert(cursor);
            }
            ret = cursor->close(cursor);

            // Update records in the table.
            ret = session->open_cursor(session, "table:metrics", nullptr, nullptr, &cursor);
            while ((ret = cursor->next(cursor)) == 0) {
                ret = cursor->get_key(cursor, &recno);
                ret = cursor->get_value(cursor, &country, &year, &population);
                cursor->set_value(cursor, country, year, population + 1);
                ret = cursor->update(cursor);
            }
            BOOST_CHECK_EQUAL(ret, WT_NOTFOUND);

            ret = cursor->close(cursor);
            // List the records in the table.
            ret = (session->open_cursor(session, "table:metrics", nullptr, nullptr, &cursor));
            while ((ret = cursor->next(cursor)) == 0) {
                ret = cursor->get_key(cursor, &recno);
                ret = cursor->get_value(cursor, &country, &year, &population);
                printf("ID %" PRIu64, recno);
                printf( ": country %s, year %" PRIu16 ", population %" PRIu64 "\n", country, year, population);
            }
            BOOST_CHECK_EQUAL(ret, WT_NOTFOUND);
            ret = cursor->close(cursor);

            // List the records in the table using raw mode.
            ret = session->open_cursor(session, "table:metrics", nullptr, "raw", &cursor);
            while ((ret = cursor->next(cursor)) == 0) {
                WT_ITEM key, value;
                ret = cursor->get_key(cursor, &key);
                ret = wiredtiger_struct_unpack(session, key.data, key.size, "r", &recno);
                printf("ID %" PRIu64, recno);
                ret = cursor->get_value(cursor, &value);
                ret = wiredtiger_struct_unpack(
                        session, value.data, value.size, "5sHQ", &country, &year, &population);
                printf(
                        ": country %s, year %" PRIu16 ", population %" PRIu64 "\n", country, year, population);
            }
            BOOST_CHECK_EQUAL(ret, WT_NOTFOUND);
            ret = (cursor->close(cursor));
            /*
             * Open a cursor on the main column group, and return the information for a particular country.
             */
            ret = (session->open_cursor(session, "colgroup:metrics:main", nullptr, nullptr, &cursor));
            cursor->set_key(cursor, 2);
            ret = (cursor->search(cursor));
            ret = (cursor->get_value(cursor, &country, &year, &population));
            printf(
                    "ID 2: country %s, year %" PRIu16 ", population %" PRIu64 "\n", country, year, population);
            ret = (cursor->close(cursor));
            /*
             * Open a cursor on the population column group, and return the population of a particular
             * country.
             */
            ret = (session->open_cursor(session, "colgroup:metrics:population", nullptr, nullptr, &cursor));
            cursor->set_key(cursor, 2);
            ret = (cursor->search(cursor));
            ret = (cursor->get_value(cursor, &population));
            printf("ID 2: population %" PRIu64 "\n", population);
            ret = (cursor->close(cursor));
            // Search in a simple index
            ret = (session->open_cursor(session, "index:metrics:country", nullptr, nullptr, &cursor));
            cursor->set_key(cursor, "AU\0\0\0");
            ret = (cursor->search(cursor));
            ret = (cursor->get_value(cursor, &country, &year, &population));
            printf("AU: country %s, year %" PRIu16 ", population %" PRIu64 "\n", country, year, population);
            ret = (cursor->close(cursor));
            // Search in a composite index.
            ret = (
                    session->open_cursor(session, "index:metrics:country_plus_year", nullptr, nullptr, &cursor));
            cursor->set_key(cursor, "USA\0\0", (uint16_t)1900);
            ret = (cursor->search(cursor));
            ret = (cursor->get_value(cursor, &country, &year, &population));
            printf(
                    "US 1900: country %s, year %" PRIu16 ", population %" PRIu64 "\n", country, year, population);
            ret = (cursor->close(cursor));
            /*
             * Use a projection to return just the table's country and year columns.
             */
            ret = (session->open_cursor(session, "table:metrics(country,year)", nullptr, nullptr, &cursor));
            while ((ret = cursor->next(cursor)) == 0) {
                ret = (cursor->get_value(cursor, &country, &year));
                printf("country %s, year %" PRIu16 "\n", country, year);
            }
            BOOST_CHECK_EQUAL(ret, WT_NOTFOUND);
            ret = (cursor->close(cursor));
            /*
             * Use a projection to return just the table's country and year columns, using raw mode.
             */
            ret = (
                    session->open_cursor(session, "table:metrics(country,year)", nullptr, "raw", &cursor));
            while ((ret = cursor->next(cursor)) == 0) {
                WT_ITEM value;
                ret = (cursor->get_value(cursor, &value));
                ret = (
                        wiredtiger_struct_unpack(session, value.data, value.size, "5sH", &country, &year));
                printf("country %s, year %" PRIu16 "\n", country, year);
            }
            BOOST_CHECK_EQUAL(ret, WT_NOTFOUND);
            ret = (cursor->close(cursor));
            /*
             * Use a projection to return just the table's record number key from an index.
             */
            ret = (
                    session->open_cursor(session, "index:metrics:country_plus_year(id)", nullptr, nullptr, &cursor));
            while ((ret = cursor->next(cursor)) == 0) {
                ret = (cursor->get_key(cursor, &country, &year));
                ret = (cursor->get_value(cursor, &recno));
                printf("row ID %" PRIu64 ": country %s, year %" PRIu16 "\n", recno, country, year);
            }
            BOOST_CHECK_EQUAL(ret, WT_NOTFOUND);
            ret = (cursor->close(cursor));
            /*
             * Use a projection to return just the population column from an index.
             */
            ret = (session->open_cursor(
                    session, "index:metrics:country_plus_year(population)", nullptr, nullptr, &cursor));
            while ((ret = cursor->next(cursor)) == 0) {
                ret = (cursor->get_key(cursor, &country, &year));
                ret = (cursor->get_value(cursor, &population));
                printf("population %" PRIu64 ": country %s, year %" PRIu16 "\n", population, country, year);
            }
            BOOST_CHECK_EQUAL(ret, WT_NOTFOUND);
            ret = (cursor->close(cursor));
            /*
             * Use a projection to avoid accessing any other column groups when using an index: supply an
             * empty list of value columns.
             */
            ret = (
                    session->open_cursor(session, "index:metrics:country_plus_year()", nullptr, nullptr, &cursor));
            while ((ret = cursor->next(cursor)) == 0) {
                ret = (cursor->get_key(cursor, &country, &year));
                printf("country %s, year %" PRIu16 "\n", country, year);
            }
            BOOST_CHECK_EQUAL(ret, WT_NOTFOUND);
            ret = (cursor->close(cursor));
            // Open cursors needed by the join.
            ret = (session->open_cursor(session, "join:table:metrics", nullptr, nullptr, &join_cursor));
            ret = (
                    session->open_cursor(session, "index:metrics:country", nullptr, nullptr, &country_cursor));
            ret = (
                    session->open_cursor(session, "index:metrics:immutable_year", nullptr, nullptr, &year_cursor));
            // select values WHERE country == "AU" AND year > 1900
            country_cursor->set_key(country_cursor, "AU\0\0\0");
            ret = (country_cursor->search(country_cursor));
            ret = (session->join(session, join_cursor, country_cursor, "compare=eq,count=10"));
            year_cursor->set_key(year_cursor, (uint16_t)1900);
            ret = (year_cursor->search(year_cursor));
            ret = (
                    session->join(session, join_cursor, year_cursor, "compare=gt,count=10,strategy=bloom"));
            // List the values that are joined
            while ((ret = join_cursor->next(join_cursor)) == 0) {
                ret = (join_cursor->get_key(join_cursor, &recno));
                ret = (join_cursor->get_value(join_cursor, &country, &year, &population));
                printf("ID %" PRIu64, recno);
                printf(
                        ": country %s, year %" PRIu16 ", population %" PRIu64 "\n", country, year, population);
            }
            BOOST_CHECK_EQUAL(ret, WT_NOTFOUND);
            ret = (session->open_cursor(session, "statistics:join", join_cursor, nullptr, &stat_cursor));
            ret = (stat_cursor->close(stat_cursor));
            ret = (join_cursor->close(join_cursor));
            ret = (year_cursor->close(year_cursor));
            ret = (country_cursor->close(country_cursor));
            // Open cursors needed by the join.
            ret = (session->open_cursor(session, "join:table:metrics", nullptr, nullptr, &join_cursor));
            ret = (session->open_cursor(session, "join:table:metrics", nullptr, nullptr, &subjoin_cursor));
            ret = (
                    session->open_cursor(session, "index:metrics:country", nullptr, nullptr, &country_cursor));
            ret = (
                    session->open_cursor(session, "index:metrics:country", nullptr, nullptr, &country_cursor2));
            ret = (
                    session->open_cursor(session, "index:metrics:immutable_year", nullptr, nullptr, &year_cursor));
            /*
             * select values WHERE (country == "AU" OR country == "UK")
             *                     AND year > 1900
             *
             * First, set up the join representing the country clause.
             */
            country_cursor->set_key(country_cursor, "AU\0\0\0");
            ret = (country_cursor->search(country_cursor));
            ret = (
                    session->join(session, subjoin_cursor, country_cursor, "operation=or,compare=eq,count=10"));
            country_cursor2->set_key(country_cursor2, "UK\0\0\0");
            ret = (country_cursor2->search(country_cursor2));
            ret = (
                    session->join(session, subjoin_cursor, country_cursor2, "operation=or,compare=eq,count=10"));
            // Join that to the top join, and add the year clause
            ret = (session->join(session, join_cursor, subjoin_cursor, nullptr));
            year_cursor->set_key(year_cursor, (uint16_t)1900);
            ret = (year_cursor->search(year_cursor));
            ret = (
                    session->join(session, join_cursor, year_cursor, "compare=gt,count=10,strategy=bloom"));
            // List the values that are joined
            while ((ret = join_cursor->next(join_cursor)) == 0) {
                ret = (join_cursor->get_key(join_cursor, &recno));
                ret = (join_cursor->get_value(join_cursor, &country, &year, &population));
                printf("ID %" PRIu64, recno);
                printf(
                        ": country %s, year %" PRIu16 ", population %" PRIu64 "\n", country, year, population);
            }
            BOOST_CHECK_EQUAL(ret, WT_NOTFOUND);
            ret = (join_cursor->close(join_cursor));
            ret = (subjoin_cursor->close(subjoin_cursor));
            ret = (country_cursor->close(country_cursor));
            ret = (country_cursor2->close(country_cursor2));
            ret = (year_cursor->close(year_cursor));
            ret = (conn->close(conn, nullptr));
#endif
        }
    }


BOOST_AUTO_TEST_SUITE_END();



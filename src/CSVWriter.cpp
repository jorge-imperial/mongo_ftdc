//
// Created by Jorge Imperial-Sosa on 1/16/21.
//

#include "CSVWriter.h"

#include <boost/thread.hpp>
#include <boost/thread/detail/thread_group.hpp>
#include <fstream>
#include <boost/log/trivial.hpp>
#include "WriterTaskList.h"
#include "WriterTask.h"


static size_t counter = 0;

static boost::mutex output_mu;

//TODO: use only one consumer thread function for both json and csv output.
int
CsvWriterConsumerThread(WriterTaskList *writerTasks,
                         Dataset *dataSet,
                         std::ofstream *out) {

    while (!writerTasks->isEmpty()) {
        auto task = writerTasks->get();

        auto csv = dataSet->getCsvFromTimestamp(task.getTimestamp());

        output_mu.lock();
        *out << counter++ << "," << csv << std::endl;
        output_mu.unlock();
    }
    return 0;
}

size_t
CSVWriter::dumpCSVTimestamps(Dataset *pDataset, std::string outputPath, Timestamp start, Timestamp end, bool rated) {

    // get metrics
    std::map<std::string, MetricsPtr> hashedMetrics;

    std::ofstream csvFileStream;
    csvFileStream.open(outputPath); // opens the file
    if (!csvFileStream) { // file couldn't be opened
        BOOST_LOG_TRIVIAL(error) << "Could not open " << outputPath;
        return 1;
    }

    if (start == INVALID_TIMESTAMP)  start = pDataset->getStartTimestamp();
    if (end == INVALID_TIMESTAMP) end = pDataset->getEndTimestamp();


    auto ts = pDataset->getMetric("start", start, end,false);
    WriterTaskList csvTasks(start, end, ts->size());
    size_t i = 0;
    for (auto t : *ts)
        csvTasks.setTimestamp(i++, t);


    // Write metric names in first row
    std::vector<std::string> metricNames ;
    auto n = pDataset->getMetricNames(metricNames);
    csvFileStream << "#line,";
    for (size_t i=0;i<metricNames.size();++i)
        csvFileStream << "\"" << metricNames[i] << "\",";
    BOOST_LOG_TRIVIAL(debug) << "WriterTasks: From " << start << " to " << end << ". Metrics size " << ts->size();
    // Thread pool
    size_t numThreads = boost::thread::hardware_concurrency() - 1;
    boost::thread_group threads;

    for (size_t i = 0; i < numThreads; ++i)
        threads.add_thread(
                new boost::thread(CsvWriterConsumerThread, &csvTasks, pDataset,   &csvFileStream));

    // Wait for threads to finish
    threads.join_all();

    return 0;
}


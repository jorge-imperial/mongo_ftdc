#include <pybind11/pybind11.h>
#include <pybind11/stl_bind.h>
#include <pybind11/stl.h>
#include <pybind11/numpy.h>

#include <FTDCParser.h>
#include <vector>
#include <filesystem>
#include <boost/format.hpp>


namespace py = pybind11;

using namespace py::literals;
using namespace ftdcparser;

typedef std::vector<std::string> MetricNames;


#define FALSE 0

#define STRINGIFY(x) #x
#define MACRO_STRINGIFY(x) STRINGIFY(x)

//TODO: Check for memory leaks.



// helper function to avoid making a copy when returning a py::array_t
// author: https://github.com/YannickJadoul
// source: https://github.com/pybind/pybind11/issues/1042#issuecomment-642215028
template <typename Sequence>


inline py::array_t<typename Sequence::value_type>
as_pyarray(Sequence &&seq) {
    auto size = seq.size();
    auto data = seq.data();
    std::unique_ptr<Sequence> seq_ptr = std::make_unique<Sequence>(std::move(seq));
    auto capsule = py::capsule(seq_ptr.get(), [](void *p) { std::unique_ptr<Sequence>(reinterpret_cast<Sequence*>(p)); });
    seq_ptr.release();
    return py::array(size, data, capsule);
}

inline py::array_t<uint64_t >
as_pyarray(MetricsPtr m) {
    auto size = m->size();
    auto data = m->data();
    std::unique_ptr<MetricsPtr> seq_ptr = std::make_unique<MetricsPtr>(std::move(m));
    auto capsule = py::capsule(seq_ptr.get(),
                               [](void *p) { std::unique_ptr<Metrics>(reinterpret_cast<Metrics *>(p)); });
    seq_ptr.release();
    return py::array(size, data, capsule);
}


struct ParserClass {
    FTDCParser *pParser;
    std::vector<std::string> metadata;
    std::vector<std::string> fileList;
    MetricNames metric_names;
    py::array_t<unsigned long> emptyArray;
    MetricsPtr emptyMetrics;

    explicit ParserClass() {
        pParser = new FTDCParser();

    };

    int parseFile(std::string file, bool lazy=true) {

        fileList.emplace_back(file);
        int n = pParser->parseFiles(&fileList, false, false, lazy);

        if (n == 0) {
            // Timestamps, metric names, and metadata as fields in python
            metric_names = pParser->getMetricsNames();
            metadata = pParser->getMetadata();

        }
        return n;
    }

    int parseDir(std::string dir, bool lazy=true) {
        // if it exists, and it a directory, pop and push contents
        if (std::filesystem::exists(dir) && std::filesystem::is_directory(dir)) {

            for (auto&& fileInPath : std::filesystem::directory_iterator(dir))
                fileList.push_back(fileInPath.path().string());

            // Not really necessary.
            std::sort(fileList.begin(), fileList.end());
            int n = pParser->parseFiles(&fileList, false, false, lazy);

            if (n == 0) {
                // metric names and metadata as fields in python
                metric_names = pParser->getMetricsNames();
                metadata = pParser->getMetadata();
            }
            return n;

        }
        return -1;
    }

    int dumpFileAsJson(std::string input, std::string output) {
        return pParser->dumpDocsAsJsonTimestamps(input, output, INVALID_TIMESTAMP, INVALID_TIMESTAMP);
    }

    int dumpFileAsCsv(std::string input, std::string output) {
        return pParser->dumpDocsAsCsvTimestamps(input, output, INVALID_TIMESTAMP, INVALID_TIMESTAMP);
    }

    py::list
    getParsedFileInfo() {
        auto fi = pParser->getParsedFileInfo();

        py::list parsedFiles;
        for (auto f : fi) {

            auto msg = str(boost::format("{ \"abs_path\" : %1%, \"samples\" : %2%, \"start\" : %3%, \"end\" : %4% }") %
                           f->getFileAbsolute() %
                           f->getSamplesCount() %
                           f->getStart() %
                           f->getEnd()
            );

            auto fileInfo = py::dict("abs_path"_a=f->getFileAbsolute(),
                                     "samples"_a=f->getSamplesCount(),
                                     "start"_a=f->getStart(),
                                     "end"_a=f->getEnd()
                                     );

            parsedFiles.append(fileInfo);
        }

        return parsedFiles;
    }

    MetricsPtr  get_timestamps(size_t start =INVALID_TIMESTAMP, size_t end = INVALID_TIMESTAMP) {
        return pParser->getMetric( "start", start, end);
    }

    MetricsPtr  getMetric(std::string metric_name,
                       size_t start = INVALID_TIMESTAMP, size_t end =INVALID_TIMESTAMP,
                       bool rated_metric = false) {
        return pParser->getMetric(metric_name, start, end, rated_metric);
    }

    std::vector<MetricsPtr> getMetricList(const std::vector<std::string> metric_names,
                                                                      size_t start = INVALID_TIMESTAMP,
                                                                      size_t end = INVALID_TIMESTAMP,
                                                                      bool rated_metric = false) {

        auto m = pParser->getMetric(std::move(metric_names), start, end, rated_metric);

        std::vector<MetricsPtr> metricList;
        for(int i=0;i<m.size();++i) {

            // How to handle metric names that are not found?
            if (m[i] != nullptr) {

                metricList.emplace_back(m[i]);
            }
            else
                metricList.emplace_back(emptyMetrics);

        }
        return metricList;
    }

    uint64_t getMetricSampleCount() { //TODO: may or may not be valid in lazy parsing mode
        return pParser->getMetricLength();
    }

    py::array_t<unsigned long>  getMetricAsNumpyArray(std::string metric_name,
                                                      size_t start = INVALID_TIMESTAMP,
                                                      size_t end = INVALID_TIMESTAMP,
                                                      bool rated_metric = false) {
        auto m = pParser->getMetric(metric_name, start, end, rated_metric);
        return as_pyarray(m);
    }

    std::vector<py::array_t<unsigned long>> getMetricListAsNumpyArray(const std::vector<std::string> metric_names,
                                                                      size_t start = INVALID_TIMESTAMP,
                                                                      size_t end = INVALID_TIMESTAMP,
                                                                      bool rated_metric = false) {

        auto m = pParser->getMetric(std::move(metric_names), start, end, rated_metric);

        std::vector<py::array_t<unsigned long>> metricList;
        for(int i=0;i<m.size();++i) {

            // How to handle metric names that are not found?
            if (m[i] != nullptr) {
                auto element = as_pyarray(m[i]);
                metricList.emplace_back(element);
            }
            else
                metricList.emplace_back(emptyArray);

        }
        return metricList;
    }

    py::array_t<uint64_t>  getMetricListAsNumpyMatrix(const std::vector<std::string> metric_names,
                                                      size_t start = INVALID_TIMESTAMP,
                                                      size_t end = INVALID_TIMESTAMP,
                                                      bool rated_metric = false,
                                                      bool transpose = false)  {
        size_t stride = 0;
        auto m = pParser->getMetricMatrix( metric_names, &stride, start, end, rated_metric);

        if (stride !=0) {

            int metricNamesSize = metric_names.size();
            if (!transpose) {
                return py::array_t<uint64_t>({ metricNamesSize, (int)stride}, m->data());
            }
            else {
                py::array_t<uint64_t> a = py::array_t<uint64_t>( {(int)stride, metricNamesSize});

                uint64_t   *p   = m->data();

                auto r =  a.mutable_unchecked();

                //printf("dimensions %zd x %zd\n", r.shape(0), r.shape(1));
                //printf("Element (0,0)=%ld\nElement (%ld,0)=%ld\n", p[0], r.shape(0),  p[r.shape(0)]);

                for (int i=0; i<r.shape(0); ++i) {
                    for (int j=0; j<r.shape(1); ++j)
                        r(i,j) = p[i + (j*r.shape(0))];
                }
                return a;
            }

        }
        else {
            // err
            return py::array_t<uint64_t>({ 0, 0 }, { 4, 8 });
        }

    }
};



PYBIND11_MODULE(_core, m) {

    m.doc() = R"pbdoc(
        MongoDB FTDC files parser library.
        -----------------------

        .. currentmodule:: pyftdc

        .. autosummary::
           :toctree: _generate

           parse_dir
           parse_file
           get_metric
           get_timestamps
           get_metric_sample_count
           get_metric_names
           timestamps
           metadata
           get_metric_numpy
           get_metrics_list_numpy
           get_metrics_list_numpy_matrix
    )pbdoc";


  py::class_<ParserClass>(m, "FTDCParser")
        .def(py::init<>())
        .def("parse_dir", &ParserClass::parseDir,
             "Parses all files in a directory",
             py::arg("dir"),
             py::arg("lazy") = true)
        .def("parse_file", &ParserClass::parseFile,
             "Parses one file",
             py::arg("file"),
             py::arg("lazy") = true)
        .def("get_parsed_file_info", &ParserClass::getParsedFileInfo,
             "Returns information on parsed files")
        .def("dump_file_as_json", &ParserClass::dumpFileAsJson,
             "Dumps a file contents to a file as JSON structures.",
             py::arg("input"),
             py::arg("output"))
        .def("dump_file_as_csv", &ParserClass::dumpFileAsCsv,
               "Dumps a file contents to a file as CSV file.",
               py::arg("input"),
               py::arg("output"))
        .def("get_metric", &ParserClass::getMetric,
             "Returns a list of values from the metrics, using starting and ending timestamps if specified",
             py::arg("metric_name"),
             py::arg("start") = ::INVALID_TIMESTAMP,
             py::arg("end") = ::INVALID_TIMESTAMP,
             py::arg("rated_metric") = false)
        .def("get_metrics_list", &ParserClass::getMetricList,
               "Returns a list of values from the metrics list, using starting and ending timestamps if specified",
               py::arg("metric_name"),
               py::arg("start") = ::INVALID_TIMESTAMP,
               py::arg("end") = ::INVALID_TIMESTAMP,
               py::arg("rated_metric") = false)
        .def("get_timestamps", &ParserClass::get_timestamps,
             "Returns timestamps",
             py::arg("start") = ::INVALID_TIMESTAMP,
             py::arg("end") = ::INVALID_TIMESTAMP)
        .def("get_metric_sample_count", &ParserClass::getMetricSampleCount)
        .def_readonly("metric_names", &ParserClass::metric_names)
        .def_readonly("metadata", &ParserClass::metadata)
        .def("get_metric_numpy", &ParserClass::getMetricAsNumpyArray,
             "Returns a metric as a numpy array.",
             py::arg("metric_name"),
             py::arg("start") = ::INVALID_TIMESTAMP,
             py::arg("end") = ::INVALID_TIMESTAMP,
             py::arg("rated_metric") = false)
        .def("get_metrics_list_numpy", &ParserClass::getMetricListAsNumpyArray,
             "Returns a list of metrics as numpy arrays.",
             py::arg("metric_names"),
             py::arg("start") = ::INVALID_TIMESTAMP,
             py::arg("end") = ::INVALID_TIMESTAMP,
             py::arg("rated_metric") = false)
        .def("get_metrics_list_numpy_matrix", &ParserClass::getMetricListAsNumpyMatrix,
             "Returns a matrix of metrics.",
             py::arg("metric_names"),
             py::arg("start") = ::INVALID_TIMESTAMP,
             py::arg("end") = ::INVALID_TIMESTAMP,
             py::arg("rated_metric") = false,
             py::arg("transpose") = false)
        ;



#ifdef VERSION_INFO
    m.attr("__version__") = MACRO_STRINGIFY(VERSION_INFO);
#else
    m.attr("__version__") = "dev";
#endif
} // PYBIND11_MODULE

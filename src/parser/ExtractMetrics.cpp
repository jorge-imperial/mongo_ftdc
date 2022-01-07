//
// Created by jorge on 8/14/21.
//

#include <vector>

#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
#include <boost/program_options.hpp>
#include <boost/filesystem.hpp>

#include <yaml-cpp/yaml.h>

#include <FTDCParser.h>
#include <CSVWriter.h>
#include <Configuration.h>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>


namespace logging = boost::log;
namespace po = boost::program_options;

void init_logging(bool verbose) {
    if (verbose)
        logging::core::get()->set_filter(logging::trivial::severity >= logging::trivial::debug);
    else
        logging::core::get()->set_filter(logging::trivial::severity >= logging::trivial::info);
}


//
static const char* FILE_ARG = "files";
static const char* OUTPUT_PATH = "out-path";
static const char* METRICS_NAMES_ARG = "metric-names";
static const char* START_ARG = "start";
static const char* END_ARG = "end";
static const char* CONFIG_ARG = "config";
static const char* METRICS_ARG = "metrics";
static const char* METRICS_PREFIX = "prefix";
static const char* VERBOSE_ARG = "verbose";
static const char* SEPARATED_ARG = "separate";
static const char* CHUNK_TXT_ARG = "dump-text";
static const char* METADATA_ARG = "metadata";
static const char* TIMESTAMPS_ARG = "timestamps";



int main(int argc, char *argv[]) {

    po::options_description config("Command line configuration");
    try {
        // Determine home directory for user, and default config.
        auto home = getenv("HOME");
        std::filesystem::path configFile;
        if (home) {
            configFile = std::filesystem::path(home);
            configFile /= ".ftdc_parser";
        }

        // Declare the supported options.
        config.add_options()
                ("version", "Print version string.")
                ("help", "produce help message.")

                (VERBOSE_ARG,po::bool_switch()->default_value(false), "Display every chunkVector initial data.")

                (FILE_ARG, po::value<std::vector<std::string>>(), "FTDC file(s) to parse.")
                (OUTPUT_PATH, po::value<std::string>()->default_value("./output"), "Prefix/path to write output files.")

                (CONFIG_ARG, po::value<std::string>()->default_value(configFile.c_str()), "Configuration file.")
                (METRICS_NAMES_ARG, po::bool_switch()->default_value(false), "Dump key names from metrics in file (false)")

                (SEPARATED_ARG, po::bool_switch()->default_value(false), "Write metrics in separate files.(false)")

                (START_ARG, po::value<std::string>()->default_value(""), "Start date and time (e.g.: 2020/12/31 13:15:21)" )
                (END_ARG, po::value<std::string>()->default_value(""), "End date and time (e.g.: 2020/12/31 13:15:21)" )

                (METRICS_ARG, po::value<std::string>(), "List of metrics to dump to file, separated by comma.")
                (METRICS_PREFIX, po::value<std::string>(), "Dump to file metrics whose prefix matches.")

                (TIMESTAMPS_ARG,  po::bool_switch()->default_value(false), "Output date as timestamps.")

                (METADATA_ARG,  po::bool_switch()->default_value(false), "Output metadata only.")
                ;

        po::variables_map vm;
        po::store(po::parse_command_line(argc, argv, config), vm);
        po::notify(vm);

        init_logging(vm[VERBOSE_ARG].as<bool>());

        if (vm.count("help")) {
            BOOST_LOG_TRIVIAL(info)  << config;
            return 1;
        }

        if (vm.count("version")) {
            BOOST_LOG_TRIVIAL(info)  << "version 0.0.1";
            return 1;
        }

        Configuration configuration(vm[CONFIG_ARG].as<std::string>());

        // Write default config file if none found.
        if (!std::filesystem::exists(vm[CONFIG_ARG].as<std::string>())) {
            BOOST_LOG_TRIVIAL(info) << "Writing default configuration in " << vm[CONFIG_ARG].as<std::string>();
            configuration.WriteDefaultConfiguration();
        }

        if (configuration.read()) {
            if (vm[METRICS_PREFIX].empty() && !vm[CHUNK_TXT_ARG].as<bool>()) {
                BOOST_LOG_TRIVIAL(fatal) << "Ending";
                return 1;
            }
        }

        auto *pParser = new FTDCParser();
        std::vector<std::string> files;
        if (vm.count(FILE_ARG)) {
            files = vm[FILE_ARG].as<std::vector<std::string>>();

            // directories?
            std::vector<boost::filesystem::path> fromDirectory;
            for (auto & file : files) {
                boost::filesystem::path p(file);
                // if it exists, and it is a directory, pop and push contents
                if (boost::filesystem::exists(p) && boost::filesystem::is_directory(p)) {

                    for (auto&& fileInPath : boost::filesystem::directory_iterator(p))
                        fromDirectory.push_back(fileInPath.path());

                    std::sort(fromDirectory.begin(), fromDirectory.end());

                    for (auto&& x : fromDirectory)
                        BOOST_LOG_TRIVIAL(info) << "\t" << x ;
                }
            }
            if (fromDirectory.size()>0) {
                files.clear();
                for ( auto &p : fromDirectory) {
                    files.emplace_back( p.generic_string()  );
                }
            }
        }
        else {
            files = { "./metrics.interim" };
        }

        pParser->parseFiles(&files,   vm[METADATA_ARG].as<bool>(), vm[METRICS_NAMES_ARG].as<bool>());

        // If only metadata, end here.
        if ( vm[METADATA_ARG].as<bool>() || vm[METRICS_NAMES_ARG].as<bool>()) return 0;

        std::vector<std::string> metrics;
        if (!vm[METRICS_PREFIX].empty()) {
            metrics = pParser->getMetricsNamesPrefixed(vm[METRICS_PREFIX].as<std::string>());
        }

        if (!vm[METRICS_ARG].empty()) { // These might be comma separated
            std::vector<std::string> sep_metrics;
            boost::split(sep_metrics, vm[METRICS_ARG].as<std::string>(), boost::is_any_of(",\n"));

            for (auto m : sep_metrics) {
                auto metrics_alias = pParser->getMetricsNamesPrefixed(m);
                for (auto mm : metrics_alias)
                    metrics.emplace_back(mm);
            }
        }

        if (metrics.size() == 0) {
            BOOST_LOG_TRIVIAL(info) << "No metric names match.";
            return 3;
        }
        else {
            std::string outPath = vm[OUTPUT_PATH].as<std::string>();

            if (vm[SEPARATED_ARG].as<bool>()) {
                CSVWriter::OutputMultipleFiles(pParser, outPath, metrics,
                                               vm[TIMESTAMPS_ARG].as<bool>(),
                                               vm[START_ARG].as<std::string>(), vm[END_ARG].as<std::string>());
            }
            else {
                // does it end in CSV?
                auto s = outPath.substr(outPath.length()-4);
                if (s.compare(".csv"))
                    outPath.append(".csv");

                CSVWriter::OutputSingleFile(pParser, outPath, metrics,
                                            vm[TIMESTAMPS_ARG].as<bool>(),
                                            vm[START_ARG].as<std::string>(), vm[END_ARG].as<std::string>());
            }
        }

        BOOST_LOG_TRIVIAL(info) << "Done.";
    }
    catch ( boost::program_options::unknown_option &bo) {
        BOOST_LOG_TRIVIAL(error) << "Could not recognize parameter: " << bo.get_option_name();
        BOOST_LOG_TRIVIAL(info)  << config;
        return 1;
    }
    catch(YAML::Exception &e) {
        BOOST_LOG_TRIVIAL(error) << "Could not open configuration file: " << e.what();
        return 2;
    }

    return 0;
}

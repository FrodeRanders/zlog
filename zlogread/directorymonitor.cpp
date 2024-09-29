//
// Created by Frode Randers on 2024-09-28.
//

#include <iostream>
#include <string>
#include <vector>
#include <memory>
#include <chrono>
#include <ctime>
#include <thread>

#include <boost/log/core.hpp>
#include <boost/process.hpp>
#include <boost/filesystem.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/sinks/text_file_backend.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/sources/logger.hpp>
#include <boost/log/sources/record_ostream.hpp>
#include <boost/log/utility/setup/console.hpp>
#include <boost/log/attributes/named_scope.hpp>
#include <__filesystem/path.h>

namespace fs = boost::filesystem;
namespace logging = boost::log;
namespace keywords = boost::log::keywords;
namespace bp = boost::process;

// Forward declarations
std::string tm_to_string(const std::tm* timeStruct, const std::string& format);
std::tm string_to_tm(const std::string& timeString, const std::string& format);
std::tm* today();
bool dates_differ(const std::tm* t1, const std::tm* t2);
bool differs_from_today(const std::tm*const &then);
std::string get_date_path(std::tm* today);
void proceed_to_next_day(std::tm& date);

//
typedef std::map<std::string, std::tuple<std::string, fs::path, std::string, std::string>> pair_map;

// Function to list and pair files with ".header" and ".payload" suffixes
static pair_map find_pairs(const fs::path& dirPath, pair_map& existingFiles) {

    std::map<std::string /* stem */, std::tuple<std::string /* stem */, fs::path /* directory */, std::string /* header filename */, std::string /* payload filename */>> newEntries;

    if (fs::exists(dirPath) && fs::is_directory(dirPath)) {
        std::map<std::string /* stem */ , fs::path /* directory path */> stems;
        std::map<std::string /* stem */, std::string> headerFiles;
        std::map<std::string /* stem */, std::string> payloadFiles;

        // Scan through directory and classify files by extension
        for (const auto& entry : fs::directory_iterator(dirPath)) {
            if (fs::is_regular_file(entry)) {
                fs::path filePath = entry.path();
                if (filePath.extension() == ".state") {
                    // Ignore state files!
                    continue;
                }

                //
                std::string stem = filePath.stem().string();
                fs::path basePath = filePath.parent_path();
                stems[stem] = filePath;

                if (filePath.extension() == ".header") {
                    headerFiles[stem] = filePath.filename().string();
                } else if (filePath.extension() == ".payload") {
                    payloadFiles[stem] = filePath.filename().string();
                }
            }
        }

        // Match .header and .payload pairs by stem
        for (const auto& _stem : stems) {
            const std::string stem = _stem.first;
            const fs::path path = _stem.second;

            if (headerFiles.find(stem) != headerFiles.end() &&
                payloadFiles.find(stem) != payloadFiles.end()) {
                // We have a pair of header and payload files
                std::tuple<std::string, fs::path, std::string, std::string> entry =
                    std::make_tuple(stem, path, headerFiles[stem], payloadFiles[stem]);

                // Check if the entry already exists in the existing map
                if (existingFiles.find(stem) == existingFiles.end()) {
                    // New entry, add it to both the newEntries and the existingFiles map
                    newEntries[stem] = entry;
                    existingFiles[stem] = entry;
                }
            } else {
                BOOST_LOG_TRIVIAL(error) << ".header and .payload files do not match for " << stem << std::endl;
            }
        }
    } else {
        BOOST_LOG_TRIVIAL(error) << "Directory does not exist or is not accessible: " << dirPath << std::endl;
    }

    // Return the delta map (only new entries)
    return newEntries;
}

// Function to process files and monitor rollover
int monitor_directory(const fs::path& myself, const std::string& basePath, const std::string& dateStr) {
    // Set up file logging
    logging::add_file_log(
        keywords::file_name = "monitor_%N.log",
        keywords::rotation_size = 10 * 1024 * 1024,  // Rotate after 10 MB
        keywords::format = "[%TimeStamp%] [%Severity%] %Message%",
        keywords::auto_flush = true  // Flush to file after each log message
    );

    logging::add_common_attributes();

    // Details for spawning child processes, later on
    std::vector<fs::path> location;
    location.push_back(myself.parent_path());
    std::string executable = myself.filename().string();

    // Determine date of log files
    std::tm initialDate{};
    std::tm* date;
    if (dateStr.empty()) {
        date = today();
    } else {
        initialDate = string_to_tm(dateStr, "%Y-%m-%d");
        date = &initialDate;
    }

    BOOST_LOG_TRIVIAL(debug) << "Will instantiate sub-processes using executable: " << myself << std::endl;

    // Determine path to log files
    fs::path currentPath = basePath;
    currentPath /= get_date_path(date);

    pair_map trackedUnits;

    // Identify log files and spawn child processes for processing header and payload pairs
    while (true) {
        BOOST_LOG_TRIVIAL(info) << "Monitoring directory: " << currentPath << std::endl;

        // Find pairs of files in the current directory
        auto untrackedUnits = find_pairs(currentPath, trackedUnits);
        if (untrackedUnits.empty()) {
            BOOST_LOG_TRIVIAL(error) << "No matching .header and .payload pairs found in directory: " << currentPath << std::endl;
        } else {
            std::vector<std::tuple<std::shared_ptr<bp::child>, std::shared_ptr<bp::ipstream>, unsigned int>> children;

            // Launch a process for each pair of files
            unsigned int shard = 0;
            for (const auto& untrackedUnit : untrackedUnits) {
                // 'logUnit' is pairs of stem and tuples from the 'logUnits' map.
                const std::string& stem = std::get<0>(untrackedUnit.second);
                const fs::path& path = std::get<1>(untrackedUnit.second);
                const std::string& headerFile = std::get<2>(untrackedUnit.second);
                const std::string& payloadFile = std::get<3>(untrackedUnit.second);

                // Pipe for capturing stdout of child process
                auto pipe_stream = std::make_shared<bp::ipstream>();

                // Launch a new child process with stdout redirected to pipe_stream
                try {
                    std::shared_ptr<bp::child> child = std::make_shared<bp::child>(
                        bp::search_path(executable, location),
                        "-p",
                        std::to_string(++shard),
                        basePath,
                        tm_to_string(date, "%Y-%m-%d"),
                        headerFile,
                        payloadFile,
                        bp::std_out > *pipe_stream  // redirect stdout to pipe_stream
                    );
                    children.emplace_back(child, pipe_stream, shard);

                    BOOST_LOG_TRIVIAL(info)
                    << "Processor #" << shard << " (pid=" << child->id() << ") handles "
                    << headerFile << " and "
                    << payloadFile
                    << std::endl;
                }
                catch (const boost::process::v1::process_error& e) {
                    BOOST_LOG_TRIVIAL(error) << "Failed to spawn child process: " << e.what() << std::endl;
                }
            }

            // Wait for all child processes to finish
            while (!children.empty()) {
                for (auto it = children.begin(); it != children.end();) {
                    std::shared_ptr<bp::child> child = std::get<0>(*it);
                    std::shared_ptr<bp::ipstream> pipe_stream = std::get<1>(*it);
                    unsigned int shard = std::get<2>(*it);

                    if (child->running()) {
                        std::string line;
                        // Read from the child's stdout (non-blocking)
                        if (pipe_stream && std::getline(*pipe_stream, line) && !line.empty()) {
                            BOOST_LOG_TRIVIAL(info) << "Processor #" << shard << " (pid=" << child->id() << ") output: " << line;
                        }
                        ++it; // since we are iterating manually (to accommodate the erase (below))
                    } else {
                        child->wait();

                        // Read from the child's stdout (non-blocking)
                        std::string line;
                        if (pipe_stream && std::getline(*pipe_stream, line) && !line.empty()) {
                            BOOST_LOG_TRIVIAL(info) << "Processor #" << shard << "(pid=" << child->id() << ") output: " << line;
                        }

                        int exitCode = child->exit_code();
                        if (exitCode == 0) {
                            BOOST_LOG_TRIVIAL(info) << "Processor #" << shard << " (pid=" << child->id() << ") finished" << std::endl;
                        } else {
                            BOOST_LOG_TRIVIAL(info) << "Processor #" << shard << " (pid=" << child->id() << ") reports error: " << exitCode << std::endl;
                        }

                        it = children.erase(it);
                    }
                }

                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }

        if (dateStr.empty()) {
            // Check if we have rolled over to the next day
            if (differs_from_today(date)) {
                BOOST_LOG_TRIVIAL(info) << "Detected day rollover" << std::endl;

                std::string info = "\nProcessed log files in directory: ";
                info += currentPath.string();
                info += "\n";

                for (const auto& trackedUnit : trackedUnits) {
                    // 'entry' is pairs of stem and tuples from the 'trackedFiles' map.
                    const std::string& headerFile = std::get<2>(trackedUnit.second);
                    const std::string& payloadFile = std::get<3>(trackedUnit.second);

                    info += "   ";
                    info += headerFile + " & " + payloadFile;
                    info += "\n";
                }
                BOOST_LOG_TRIVIAL(info) << info << std::endl;

                trackedUnits.clear();

                date = today();
                currentPath = basePath + "/" + get_date_path(date);

                BOOST_LOG_TRIVIAL(info) << "Switching to new directory: " << currentPath << std::endl;
            }
        } else {
            // TODO: implement
            proceed_to_next_day(*date);

            return 0;
        }
    }
}

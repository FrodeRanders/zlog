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

#include "zlog.h"

namespace fs = boost::filesystem;
namespace logging = boost::log;
namespace keywords = boost::log::keywords;
namespace bp = boost::process;

// Forward declarations
std::string tm_to_string(const std::tm& timeStruct, const std::string& format);
std::tm string_to_tm(const std::string& timeString, const std::string& format);
std::tm today();
bool differs_from_today(const std::tm& then);
std::string get_date_path(const std::tm& today);
void proceed_to_next_day(std::tm& date);

//
typedef std::map<
    std::string /* stem */,
    std::tuple<std::string /* stem */,
        fs::path /* directory */,
        std::string /* header filename */,
        std::string /* payload filename */>
> pair_map;

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
        keywords::open_mode = std::ios_base::app,    // Open in append mode
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
    std::tm date = today();
    if (dateStr.empty()) {
        date = today();
    } else {
        date = string_to_tm(dateStr, DATE_FORMAT);
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
            // Launch a child process for each pair of files
            std::vector<
                std::tuple<std::shared_ptr<bp::child>, std::shared_ptr<bp::ipstream>, unsigned int /* shard */, std::string /* stem */>
            > children;
            {
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
                            tm_to_string(date, DATE_FORMAT),
                            headerFile,
                            payloadFile,
                            bp::std_out > *pipe_stream  // redirect stdout to pipe_stream
                        );
                        children.emplace_back(child, pipe_stream, shard, stem);

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
            }

            // Wait for all child processes to finish
            while (!children.empty()) {
                for (auto cit = children.begin(); cit != children.end();) {
                    std::shared_ptr<bp::child> child = std::get<0>(*cit);
                    std::shared_ptr<bp::ipstream> pipe_stream = std::get<1>(*cit);
                    unsigned int shard = std::get<2>(*cit);
                    std::string stem = std::get<3>(*cit);

                    if (child->running()) {
                        std::string line;
                        // Read from the child's stdout (non-blocking). Note that if we read while
                        // the process is running, we will not be able to read anything when the
                        // child has exited. We accept that, since we want to be able to pick up
                        // possible reports from the child process as they arrive.
                        if (pipe_stream && std::getline(*pipe_stream, line) && !line.empty()) {
                            BOOST_LOG_TRIVIAL(info) << "Processor #" << shard << " (pid=" << child->id() << ") reports: " << line;
                        }
                        ++cit; // since we are iterating manually (to accommodate the erase (below))
                    } else {
                        child->wait();

                        // Read from the child's stdout (non-blocking). Note that this picks
                        // up data from stdout that has not already been read (above). Anyhow,
                        // the child process log entry is duly identified so we will be able to
                        // piece together what happened.
                        std::string line;
                        if (pipe_stream)
                            std::getline(*pipe_stream, line);

                        int exitCode = child->exit_code();

                        if (exitCode > 100) {
                            // 101: Error opening header file
                            // 102: Error opening payload file
                            //
                            std::string info = "Processor #";
                            info += std::to_string(shard);
                            info += " (pid=";
                            info += std::to_string(child->id());
                            info += ") could not load ";
                            if (exitCode == 101) {
                                info += "header file ";
                                info += stem + ".header";
                            } else {
                                info += "payload file ";
                                info += stem + ".payload";
                            }
                            if (!line.empty()) {
                                info += ". It reports: " + line;
                            }

                            // Remove this header and payload file pair from 'trackedUnits', and they will
                            // be picked up again in a little while.
                            //
                            auto tuit = trackedUnits.find(stem);
                            if (tuit != trackedUnits.end()) {
                                trackedUnits.erase(tuit);
                                BOOST_LOG_TRIVIAL(info) << info << " -- Retrying later" << std::endl;
                            } else {
                                BOOST_LOG_TRIVIAL(error) << info << " -- Failed to locate unit among tracked units!" << std::endl;
                            }
                        } else if (exitCode == 10) {
                            std::string info = "Processor #";
                            info += std::to_string(shard);
                            info += " (pid=";
                            info += std::to_string(child->id());
                            info += ") could not process all headers in file ";
                            info += stem + ".header. ";
                            if (!line.empty()) {
                                info += ". It reports: " + line;
                            }
                            BOOST_LOG_TRIVIAL(error) << info << std::endl;

                        } else if (exitCode == 0) {
                            BOOST_LOG_TRIVIAL(info) << "Processor #" << shard << " (pid=" << child->id() << ") finished gracefully with report: " << line << std::endl;
                        } else {
                            BOOST_LOG_TRIVIAL(info) << "Processor #" << shard << " (pid=" << child->id() << ") reports error (" << exitCode << "): " << line << std::endl;
                        }

                        cit = children.erase(cit);
                    }
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }

        if (dateStr.empty()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));

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
                    info += headerFile + " & ";
                    info += payloadFile;
                    info += "\n";
                }
                BOOST_LOG_TRIVIAL(info) << info << std::endl;

                date = today();
                currentPath = basePath;
                currentPath /= get_date_path(date);

                trackedUnits.clear();

                BOOST_LOG_TRIVIAL(info) << "Switching to new directory: " << currentPath << std::endl;
            } else {
                BOOST_LOG_TRIVIAL(info) << "No day rollover detected, but child processes ended?" << std::endl;
                BOOST_LOG_TRIVIAL(info) << "Set on " << tm_to_string(date, DATE_FORMAT)
                << " and today is " << tm_to_string(today(), DATE_FORMAT) << std::endl;
                std::this_thread::sleep_for(std::chrono::seconds(30));
            }
        } else {
            BOOST_LOG_TRIVIAL(info) << "Ending" << std::endl;
            return 0;
        }
    }
}

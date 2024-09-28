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

namespace fs = boost::filesystem;
namespace logging = boost::log;
namespace keywords = boost::log::keywords;
namespace bp = boost::process;

// Forward declarations
std::string tmToString(const std::tm* timeStruct, const std::string& format);
std::tm stringToTm(const std::string& timeString, const std::string& format);
std::tm* today();
bool datesDiffer(const std::tm* t1, const std::tm* t2);
bool differsFromToday(const std::tm*const &then);
std::string getDatePath(std::tm* today);
void increaseByOneDay(std::tm& date);

// Function to list and pair files with ".header" and ".payload" suffixes
static std::map<std::string, std::pair<fs::path, fs::path>> findFilePairs(const fs::path& dirPath) {
    std::map<std::string, std::pair<fs::path, fs::path>> filePairs;

    if (fs::exists(dirPath) && fs::is_directory(dirPath)) {
        std::map<std::string, fs::path> headerFiles;
        std::map<std::string, fs::path> payloadFiles;

        // Scan through directory and classify files by extension
        for (const auto& entry : fs::directory_iterator(dirPath)) {
            if (fs::is_regular_file(entry)) {
                fs::path filePath = entry.path();
                std::string filename = filePath.stem().string(); // File name without extension

                if (filePath.extension() == ".header") {
                    headerFiles[filename] = filePath;
                } else if (filePath.extension() == ".payload") {
                    payloadFiles[filename] = filePath;
                }
            }
        }

        // Match .header and .payload pairs by filename
        for (const auto& headerEntry : headerFiles) {
            const std::string& baseName = headerEntry.first;
            if (payloadFiles.find(baseName) != payloadFiles.end()) {
                filePairs[baseName] = std::make_pair(headerEntry.second, payloadFiles[baseName]);
            }
        }
    } else {
        BOOST_LOG_TRIVIAL(error) << "Directory does not exist or is not accessible: " << dirPath << std::endl;
    }

    return filePairs;
}

// Function to process files and monitor rollover
int monitor(const fs::path& myself, const std::string& basePath, const std::string& dateStr) {
    // Set up file logging
    logging::add_file_log(
        keywords::file_name = "monitor_%N.log",
        keywords::rotation_size = 10 * 1024 * 1024,  // Rotate after 10 MB
        keywords::format = "[%TimeStamp%] [%Severity%] %Message%",
        keywords::auto_flush = true  // Flush to file after each log message
    );

    // Add common attributes, such as time stamps and process IDs
    logging::add_common_attributes();
    BOOST_LOG_TRIVIAL(debug) << "Processor is: " << myself << std::endl;

    // Extract the program's base name (without the directory part)
    std::vector<fs::path> location;
    location.push_back(myself.parent_path());
    std::string executable = myself.filename().string();

    //
    std::tm initialDate{};
    std::tm* date;
    if (dateStr.empty()) {
        date = today();
    } else {
        initialDate = stringToTm(dateStr, "%Y-%m-%d");
        date = &initialDate;
    }

    fs::path currentPath = basePath;
    currentPath /= getDatePath(date);

    while (true) {
        BOOST_LOG_TRIVIAL(info) << "Monitoring directory: " << currentPath << std::endl;

        // Find pairs of files in the current directory
        auto filePairs = findFilePairs(currentPath);
        if (filePairs.empty()) {
            BOOST_LOG_TRIVIAL(error) << "No matching .header and .payload pairs found in directory: " << currentPath << std::endl;
        } else {
            std::vector<std::pair<std::shared_ptr<bp::child>, std::shared_ptr<bp::ipstream>>> children;

            // Launch a process for each pair of files
            unsigned int id = 0;
            for (const auto& pair : filePairs) {
                const fs::path& headerFile = pair.second.first;
                const fs::path& payloadFile = pair.second.second;

                // Pipe for capturing the stdout of the child process
                auto pipe_stream = std::make_shared<bp::ipstream>();

                // Launch a new child process with stdout redirected to pipe_stream
                try {
                    std::shared_ptr<bp::child> child = std::make_shared<bp::child>(
                        bp::search_path(executable, location),
                        "-p",
                        std::to_string(++id),
                        basePath,
                        tmToString(date, "%Y-%m-%d"),
                        headerFile.filename().string(),
                        payloadFile.filename().string(),
                        bp::std_out > *pipe_stream  // redirect stdout to pipe_stream
                    );

                    BOOST_LOG_TRIVIAL(info)
                    << "Processor #" << id << " (pid=" << child->id() << ") handles "
                    << headerFile.string() << " and "
                    << payloadFile.string()
                    << std::endl;

                    children.emplace_back(child, pipe_stream);
                }
                catch (const boost::process::v1::process_error& e) {
                    BOOST_LOG_TRIVIAL(error) << "Failed to start processor: " << e.what() << std::endl;
                }
            }

            // Wait for all child processes to finish
            while (!children.empty()) {
                for (auto it = children.begin(); it != children.end();) {
                    std::shared_ptr<bp::child> child = it->first;
                    std::shared_ptr<bp::ipstream> pipe_stream = it->second;

                    if (child->running()) {
                        std::string line;
                        // Read from the child's stdout (non-blocking)
                        if (pipe_stream && std::getline(*pipe_stream, line) && !line.empty()) {
                            BOOST_LOG_TRIVIAL(info) << "Processor pid=" << child->id() << " output: " << line;
                        }
                        ++it; // since we are iterating manually and need to accommodate the erase (below)
                    } else {
                        // Process has finished, capture the exit code
                        child->wait();

                        std::string line;
                        // Read from the child's stdout (non-blocking)
                        if (pipe_stream && std::getline(*pipe_stream, line) && !line.empty()) {
                            BOOST_LOG_TRIVIAL(info) << "Processor pid=" << child->id() << " output: " << line;
                        }

                        int exitCode = child->exit_code();
                        if (exitCode == 0) {
                            // OBSERVE!! This is reverse behaviour to normal!
                            BOOST_LOG_TRIVIAL(warning) << "A processor reports failure" << std::endl;
                        } else {
                            BOOST_LOG_TRIVIAL(info) << "Processor #" << exitCode << " (pid=" << child->id() << ") finished" << std::endl;
                        }

                        it = children.erase(it);
                    }
                }

                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }

        if (dateStr.empty()) {
            // Check if we have rolled over to the next day
            if (differsFromToday(date)) {
                date = today();
                currentPath = basePath + "/" + getDatePath(date);
                BOOST_LOG_TRIVIAL(info) << "Detected day rollover. Switching to new directory: " << currentPath << std::endl;
            }
        } else {
            increaseByOneDay(*date);
            // if equal to today?

            return 0;
        }
    }
}

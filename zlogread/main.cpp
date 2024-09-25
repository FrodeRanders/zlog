#include <boost/filesystem.hpp>
#include <boost/process.hpp>
#include <iostream>
#include <vector>
#include <string>
#include <chrono>
#include <ctime>
#include <thread>

#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/sinks/text_file_backend.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/sources/logger.hpp>
#include <boost/log/sources/record_ostream.hpp>
#include <boost/log/utility/setup/console.hpp>
#include <boost/log/attributes/timer.hpp>
#include <boost/log/attributes/named_scope.hpp>

#if defined(__APPLE__) && defined(__MACH__)
    #include <mach-o/dyld.h>  // For _NSGetExecutablePath
    #include <limits.h>
#elif defined(__linux__)
    #include <unistd.h>       // For readlink
    #include <limits.h>
#endif

namespace fs = boost::filesystem;
namespace bp = boost::process;

namespace logging = boost::log;
namespace keywords = boost::log::keywords;


// Function to list and pair files with ".header" and ".payload" suffixes
static std::map<std::string, std::pair<fs::path, fs::path>> findFilePairs(const std::string& dirPath) {
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

// Function to simulate detecting if the day has rolled over
static bool detectDayRollover(const std::string& currentPath) {
    std::time_t now = std::time(nullptr);
    std::tm* nowTm = std::localtime(&now);

    // Format the current date directory (e.g., /year/month/day)
    std::string nextDayPath = std::to_string(1900 + nowTm->tm_year) + "/" +
                              std::to_string(nowTm->tm_mon) + "/" +
                              std::to_string(nowTm->tm_mday + 1);

    return (currentPath != nextDayPath);
}

static std::string getCurrentPath(const std::string& basePath) {
    std::time_t now = std::time(nullptr);
    std::tm* nowTm = std::localtime(&now);

    std::string path = basePath + "/" +
        std::to_string(1900 + nowTm->tm_year) + "/" +
        std::to_string(nowTm->tm_mon + 1) + "/" +
        std::to_string(nowTm->tm_mday);

    return path;
}

// Function to process files and monitor rollover
void monitorDirectoryAndProcess(const fs::path& myself, const std::string& basePath) {
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

    // Start at current day
    std::string currentPath = getCurrentPath(basePath);

    while (true) {
        BOOST_LOG_TRIVIAL(info) << "Monitoring directory: " << currentPath << std::endl;

        // Find pairs of files in the current directory
        auto filePairs = findFilePairs(currentPath);
        if (filePairs.empty()) {
            BOOST_LOG_TRIVIAL(error) << "No matching .header and .payload pairs found in directory: " << currentPath << std::endl;
        } else {
            std::vector<std::shared_ptr<bp::child>> children;

            // Launch a process for each pair of files
            unsigned int id = 0;
            for (const auto& pair : filePairs) {
                const fs::path& headerFile = pair.second.first;
                const fs::path& payloadFile = pair.second.second;

                // Launch a new child process using Boost.Process
                try {
                    std::shared_ptr<bp::child> child = std::make_shared<bp::child>(
                        bp::search_path(executable, location),
                        "-p",
                        std::to_string(++id),
                        headerFile.string(),
                        payloadFile.string());

                    BOOST_LOG_TRIVIAL(debug)
                    << "Processor: " << id << " "
                    << headerFile.string() << " "
                    << payloadFile.string()
                    << std::endl;

                    children.push_back(child);
                }
                catch (const boost::process::v1::process_error& e) {
                    BOOST_LOG_TRIVIAL(error) << "Failed to start processor: " << e.what() << std::endl;
                }
            }

            // Wait for all child processes to finish
            for (auto& child : children) {
                child->wait();
                BOOST_LOG_TRIVIAL(info) << "Header and payload processor finished with exit code: " << child->exit_code() << std::endl;
            }
        }

        // Simulate polling for directory changes and rollover detection
        std::this_thread::sleep_for(std::chrono::seconds(5));  // Simulate periodic check

        // Check if we have rolled over to the next day
        if (detectDayRollover(currentPath)) {
            currentPath = getCurrentPath(basePath);
            BOOST_LOG_TRIVIAL(info) << "Detected day rollover. Switching to new directory: " << currentPath << std::endl;
        }
    }
}

// Forward declarations
int processPair(unsigned long id, const std::string& headerFile, const std::string& payloadFile);
void printStackTrace();

//
int main(int argc, char* argv[]) {
    try {
        if (argc < 2) {
            std::cerr << "Usage: " << argv[0] << " <base-directory>" << std::endl;
            return 1;
        }

        // Set up console logging
        logging::add_console_log(
            std::clog,
            keywords::format = "[%TimeStamp%] [%Severity%] %Message%"
        );

        if (std::strcmp(argv[1], "-p") == 0 && argc == 5) {
            unsigned long id = std::stoul(argv[2]);
            return processPair(id, argv[3], argv[4]);
        }

        monitorDirectoryAndProcess(argv[0], argv[1]);
    }
    catch (const std::invalid_argument& ia) {
        std::cerr << "Invalid argument: " << ia.what() << std::endl;
        return 1;
    }
    catch (std::exception& e) {
        std::cerr << "Failed to process logs: " << e.what() << std::endl;
        printStackTrace();
        return 1;
    }
    return 0;
}

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
std::map<std::string, std::pair<fs::path, fs::path>> findFilePairs(const fs::path& dirPath) {
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
bool detectDayRollover(const std::string& currentDayPath) {
    std::time_t now = std::time(nullptr);
    std::tm* nowTm = std::localtime(&now);

    // Format the current date directory (e.g., /year/month/day)
    std::string nextDayPath = std::to_string(1900 + nowTm->tm_year) + "/" +
                              std::to_string(nowTm->tm_mon + 1) + "/" +
                              std::to_string(nowTm->tm_mday);

    return (currentDayPath != nextDayPath);
}

// Function to process files and monitor rollover
void monitorDirectoryAndProcess(const std::string& baseDir, const std::string& executable) {
    // Set up file logging
    logging::add_file_log(
        keywords::file_name = "monitor_%N.log",
        keywords::rotation_size = 10 * 1024 * 1024,  // Rotate after 10 MB
        keywords::format = "[%TimeStamp%] [%Severity%] %Message%",
        keywords::auto_flush = true  // Flush to file after each log message
    );

    // Add common attributes, such as time stamps and process IDs
    logging::add_common_attributes();
    BOOST_LOG_TRIVIAL(debug) << "Header and payload processor is: " << executable << std::endl;

    //
    std::string currentDayPath = baseDir + "/" + "2024/09/25";  // TODO

    while (true) {
        BOOST_LOG_TRIVIAL(info) << "Monitoring directory: " << currentDayPath << std::endl;

        // Find pairs of files in the current directory
        auto filePairs = findFilePairs(currentDayPath);
        if (filePairs.empty()) {
            BOOST_LOG_TRIVIAL(error) << "No matching .header and .payload pairs found in directory: " << currentDayPath << std::endl;
        } else {
            std::vector<std::shared_ptr<bp::child>> children;

            // Launch a process for each pair of files
            unsigned int id = 0;
            for (const auto& pair : filePairs) {
                const fs::path& headerFile = pair.second.first;
                const fs::path& payloadFile = pair.second.second;

                // Launch a new child process using Boost.Process
                std::shared_ptr<bp::child> child = std::make_shared<bp::child>(
                    bp::search_path(executable),
                    "-p",
                    std::to_string(++id),
                    headerFile.string(),
                    payloadFile.string());

                children.push_back(child);
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
        if (detectDayRollover(currentDayPath)) {
            std::time_t now = std::time(nullptr);
            std::tm* nowTm = std::localtime(&now);

            // Update the current day directory to the new one
            currentDayPath = baseDir + "/" + std::to_string(1900 + nowTm->tm_year) + "/" +
                             std::to_string(nowTm->tm_mon + 1) + "/" +
                             std::to_string(nowTm->tm_mday);

            BOOST_LOG_TRIVIAL(info) << "Detected day rollover. Switching to new directory: " << currentDayPath << std::endl;
        }
    }
}

int processPair(unsigned long id, const std::string& headerFile, const std::string& payloadFile);

int main(int argc, char* argv[]) {
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

    std::string baseDir = argv[1];

    std::string executable;
    char path[PATH_MAX];

#if defined(__APPLE__) && defined(__MACH__)
    // macOS specific code
    uint32_t size = sizeof(path);
    if (_NSGetExecutablePath(path, &size) == 0) {
        executable = path;
    } else {
        std::cerr << "Buffer too small; need size: " << size << std::endl;
        std::exit(1);
    }
#elif defined(__linux__)
    // Linux specific code
    ssize_t count = readlink("/proc/self/exe", path, PATH_MAX);
    if (count != -1) {
        path[count] = '\0';  // Null-terminate the path
        executable = path;
    } else {
        std::cerr << "Error determining the executable path on Linux." << std::endl;
    }
#else
#error "Unsupported platform"
#endif

    monitorDirectoryAndProcess(baseDir, executable);
    return 0;
}

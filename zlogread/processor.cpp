//
// Created by Frode Randers on 2024-09-25.
//
#include <iostream>
#include <fstream>
#include <string>
#include <unistd.h>   // For sleep()
#include <sys/stat.h> // For file size checking
#include <vector>
#include <cerrno>
#include <cstring>    // For strerror

#include <boost/log/core.hpp>
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

// Forward declarations
std::string tmToString(const std::tm*const timeStruct, const std::string& format);
std::tm stringToTm(const std::string& timeString, const std::string& format);
std::tm* today();
bool differsFromToday(const std::tm*const &then);
std::string getDatePath(std::tm* today);



static std::vector<std::string> split(const std::string& line, char delimiter) {
    std::vector<std::string> result;
    std::stringstream ss(line);
    std::string item;

    while (std::getline(ss, item, delimiter)) {
        result.push_back(item);
    }

    return result;
}

// Utility function to get file size
static std::streamoff getFileSize(const std::string& path) {
    struct stat stat_buf;
    int rc = stat(path.c_str(), &stat_buf);
    return rc == 0 ? stat_buf.st_size : -1;
}

// Utility function to save the current state (last read positions)
static void saveState(const fs::path path, unsigned long id, std::streamoff lastHeaderPos, std::streamoff lastPayloadPos) {
    std::string name = "processor-" + std::to_string(id) + ".state";
    fs::path statePath = path;
    statePath /= name;

    std::ofstream stateStream(statePath.string(), std::ios::binary | std::ios::out | std::ios::trunc);
    if (stateStream) {
        stateStream << std::to_string(lastHeaderPos) << "," << std::to_string(lastPayloadPos) << std::endl;
        stateStream.close();
    }
    //BOOST_LOG_TRIVIAL(trace) << "Saved offsets[" << id <<"]: header=" << lastHeaderPos << ", payload=" << lastPayloadPos << std::endl;
}

// Utility function to load the saved state (last read positions)
static void loadState(const fs::path path, unsigned long id, std::streamoff &lastHeaderPos, std::streamoff &lastPayloadPos) {
    std::string name = "processor-" + std::to_string(id) + ".state";
    fs::path statePath = path;
    statePath /= name;

    std::ifstream stateFile(statePath.string(), std::ios::binary | std::ios::in);
    if (stateFile) {
        std::string line;
        if (std::getline(stateFile, line)) {
            std::vector<std::string> data = split(line, ',');
            if (data.size() != 2) {
                BOOST_LOG_TRIVIAL(error) << "Corrupt state: " << line << " (" << name << ")" << std::endl;
            } else {
                lastHeaderPos = static_cast<std::streamoff>(std::stoul(data[0]));
                lastPayloadPos = static_cast<std::streamoff>(std::stoul(data[1]));
                BOOST_LOG_TRIVIAL(trace) << "Loaded offsets[" << id <<"]: header=" << lastHeaderPos << ", payload=" << lastPayloadPos << std::endl;
            }
        } else {
            BOOST_LOG_TRIVIAL(debug) << "Empty file: " << name << std::endl;
        }
        stateFile.close();
    }
}

// Simulate processing input/output data
static void processInputOutput(const std::vector<char>& input, const std::vector<char>& output) {
    std::string _input = std::string(input.begin(), input.end());
    std::string _output = std::string(output.begin(), output.end());

    if (!_input.starts_with("Input") && _input.ends_with("Input")) {
        BOOST_LOG_TRIVIAL(error) << "Corrupt Input: " << _input << std::endl;
        throw std::underflow_error("Corrupt Input: " + _input);
    }

    if (!_output.starts_with("Output") && _output.ends_with("Output")) {
        BOOST_LOG_TRIVIAL(error) << "Corrupt Output: " << _output << std::endl;
        throw std::underflow_error("Corrupt Output: " + _output);
    }
}

int process(
    int id,
    const std::string& baseDir,
    const std::string& dateStr,
    const std::string& headerFile,
    const std::string& payloadFile
 ) {
    std::string logFileName = "processor_";
    logFileName += std::to_string(id);
    logFileName += "_%N.log";

    // Set up file logging
    logging::add_file_log(
        keywords::file_name = logFileName,
        keywords::rotation_size = 10 * 1024 * 1024,  // Rotate after 10 MB
        keywords::format = "[%TimeStamp%] [%Severity%] %Message%",
        keywords::auto_flush = true  // Flush to file after each log message
    );

    // Add common attributes, such as time stamps and process IDs
    logging::add_common_attributes();

    std::streamoff lastPayloadPos = 0;  // Position in the payload file
    std::streamoff lastHeaderPos = 0;   // Position in the header file

    std::tm initialDate = stringToTm(dateStr, "%Y-%m-%d");
    std::tm* date = &initialDate;

    fs::path headerFilePath = baseDir;
    headerFilePath /= getDatePath(date);
    fs::path payloadFilePath = headerFilePath; // shared so far
    fs::path stateDir = headerFilePath; // shared so far

    headerFilePath /= headerFile; // unique
    payloadFilePath /= payloadFile; // unique


    // Load the previous state (if any)
    loadState(stateDir, id, lastHeaderPos, lastPayloadPos);

    BOOST_LOG_TRIVIAL(info) << "Processor #" << id << " starting at position " << lastHeaderPos << " in " << headerFilePath.string() << std::endl;

    // Open both files and keep them open
    std::ifstream payloadStream(payloadFilePath.string(), std::ios::binary | std::ios::in);
    std::ifstream headerStream(headerFilePath.string(), std::ios::binary | std::ios::in);

    // Check for file open errors
    if (!payloadStream.is_open()) {
        BOOST_LOG_TRIVIAL(error) << "Error opening payload file: " << strerror(errno) << std::endl;
        return 0; // signalling an error in this context
    }

    if (!headerStream.is_open()) {
        BOOST_LOG_TRIVIAL(error) << "Error opening header file: " << strerror(errno) << std::endl;
        return 0; // signalling an error in this context
    }

    unsigned long counter = 0L;
    while (true) {
        try {
            if (getFileSize(headerFilePath.string()) > lastHeaderPos) {
                // Seek to the last known position in the header file
                headerStream.clear(); // clears EOF flag if set
                headerStream.seekg(lastHeaderPos);

                // Read header entries
                std::string line;
                while (std::getline(headerStream, line)) {
                    //BOOST_LOG_TRIVIAL(trace) << "Read: " << line << std::endl;

                    std::vector<std::string> data = split(line, ',');

                    if (data.size() != 10) {
                        BOOST_LOG_TRIVIAL(info) << "Header not ready: " << headerFile << std::endl;
                        break; // try again later
                    }

                    auto inputSize = static_cast<std::streamsize>(std::stoul(data[7]));
                    auto outputSize = static_cast<std::streamsize>(std::stoul(data[8]));
                    auto offset = static_cast<std::streamoff>(std::stoul(data[9]));

                    // Check if the corresponding payload data is fully written
                    std::streamoff expectedPayloadSize = offset + inputSize + outputSize;

                    // Get the current payload file size
                    if (getFileSize(payloadFilePath.string()) >= expectedPayloadSize) {
                        // Payload data is available. Seek to the last read position in the payload file
                        payloadStream.clear(); // clears EOF flag if set
                        payloadStream.seekg(offset);

                        // Read input and output from the payload file
                        std::vector<char> inputBuffer(inputSize);
                        std::vector<char> outputBuffer(outputSize);

                        payloadStream.read(inputBuffer.data(), inputSize);
                        payloadStream.read(outputBuffer.data(), outputSize);

                        // Process input/output
                        processInputOutput(inputBuffer, outputBuffer);
                        counter++;

                        // Update the last read position in both the header and payload files
                        lastPayloadPos = expectedPayloadSize;
                        lastHeaderPos = headerStream.tellg();

                        // Persist the current read positions
                        saveState(stateDir, id, lastHeaderPos, lastPayloadPos);

                    } else {
                        break; // try again later
                    }
                }
            }
        } catch (const std::exception& e) {
            BOOST_LOG_TRIVIAL(error) << "Error: " << e.what() << std::endl;

            throw;
        }

        // Sleep before polling again to avoid busy waiting
        sleep(1); // seconds!

        // Check if we have rolled over to the next day
        if (differsFromToday(date)) {
            BOOST_LOG_TRIVIAL(info) << "Detected date rollover from " << tmToString(date, "%Y-%m-%d") << " to " << tmToString(today(), "%Y-%m-%d") << std::endl;

            payloadStream.close();
            headerStream.close();

            std::cout << "Processed " << counter << " entries" << std::endl;
            return id;
        }
    }
}

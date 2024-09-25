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
#include <boost/log/attributes/timer.hpp>
#include <boost/log/attributes/named_scope.hpp>

namespace fs = boost::filesystem;
namespace logging = boost::log;
namespace keywords = boost::log::keywords;

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
static void saveState(unsigned long id, std::streamoff lastHeaderPos, std::streamoff lastPayloadPos) {
    std::string name = "processor-" + std::to_string(id) + ".state";
    std::ofstream stateStream(name, std::ios::binary | std::ios::out | std::ios::trunc);
    if (stateStream) {
        stateStream << std::to_string(lastHeaderPos) << "," << std::to_string(lastPayloadPos) << std::endl;
        stateStream.close();
    }
    BOOST_LOG_TRIVIAL(trace) << "Saved offsets: header=" << lastHeaderPos << ", payload=" << lastPayloadPos << std::endl;
}

// Utility function to load the saved state (last read positions)
static void loadState(unsigned long id, std::streamoff &lastHeaderPos, std::streamoff &lastPayloadPos) {
    std::string name = "processor-" + std::to_string(id) + ".state";
    std::ifstream stateFile(name, std::ios::binary | std::ios::in);
    if (stateFile) {
        std::string line;
        if (std::getline(stateFile, line)) {
            std::vector<std::string> data = split(line, ',');
            if (data.size() != 2) {
                BOOST_LOG_TRIVIAL(error) << "Corrupt state: " << line << " (" << name << ")" << std::endl;
            } else {
                lastHeaderPos = static_cast<std::streamoff>(std::stoul(data[0]));
                lastPayloadPos = static_cast<std::streamoff>(std::stoul(data[1]));
                BOOST_LOG_TRIVIAL(trace) << "Loaded offsets: header=" << lastHeaderPos << ", payload=" << lastPayloadPos << std::endl;
            }
        } else {
            BOOST_LOG_TRIVIAL(debug) << "Empty file: " << name << std::endl;
        }
        stateFile.close();
    }
}

// Simulate processing input/output data
static void processInputOutput(const std::vector<char>& input, const std::vector<char>& output) {
    BOOST_LOG_TRIVIAL(debug) << "Processing Input: " << std::string(input.begin(), input.end()) << std::endl;
    BOOST_LOG_TRIVIAL(debug) << "Processing Output: " << std::string(output.begin(), output.end()) << std::endl;
}

int processPair(unsigned long id, const std::string& headerFile, const std::string& payloadFile) {
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

    // Load the previous state (if any)
    loadState(id, lastHeaderPos, lastPayloadPos);

    // Open both files and keep them open
    std::ifstream payloadStream(payloadFile, std::ios::binary | std::ios::in);
    std::ifstream headerStream(headerFile, std::ios::binary | std::ios::in);
    unsigned long counter = 0L;

    // Check for file open errors
    if (!payloadStream.is_open()) {
        BOOST_LOG_TRIVIAL(error) << "Error opening payload file: " << strerror(errno) << std::endl;
        return 1;
    }

    if (!headerStream.is_open()) {
        BOOST_LOG_TRIVIAL(error) << "Error opening header file: " << strerror(errno) << std::endl;
        return 1;
    }

    while (true) {
        try {
            if (getFileSize(headerFile) > lastHeaderPos) {
                // Seek to the last read position in the header file
                headerStream.clear(); // Clear EOF flag if set
                headerStream.seekg(lastHeaderPos); // Move to last read position in header

                // Read header entries
                std::string line;
                while (std::getline(headerStream, line)) {
                    std::vector<std::string> data = split(line, ',');

                    if (data.size() != 10) {
                        BOOST_LOG_TRIVIAL(info) << "Header not ready? " << line << " (" << headerFile << ")" << std::endl;
                        break; // try again later
                    }

                    auto inputSize = static_cast<std::streamsize>(std::stoul(data[7]));
                    auto outputSize = static_cast<std::streamsize>(std::stoul(data[8]));
                    auto offset = static_cast<std::streamoff>(std::stoul(data[9]));

                    /*
                    std::string header;
                    header += std::to_string(++counter);
                    header += ": ";

                    for (int i = 0; i < 7; ++i) {
                        header += data[i];
                        header += " | ";
                    }

                    header += std::to_string(inputSize);
                    header += " | ";
                    header += std::to_string(outputSize);
                    header += " | ";
                    header += std::to_string(offset);

                    BOOST_LOG_TRIVIAL(trace) << header << std::endl;
                    */

                    // Check if the corresponding payload data is fully written
                    std::streamoff expectedPayloadSize = offset + inputSize + outputSize;

                    // Get the current payload file size
                    if (getFileSize(payloadFile) >= expectedPayloadSize) {
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

                        // Update the last read position in both the header and payload files
                        lastPayloadPos = expectedPayloadSize;
                        lastHeaderPos = headerStream.tellg();

                        // Persist the current read positions
                        saveState(id, lastHeaderPos, lastPayloadPos);

                    } else {
                        break; // try again later
                    }
                }
            }
        } catch (const std::exception& e) {
            BOOST_LOG_TRIVIAL(error) << "Error: " << e.what() << std::endl;
            throw;
        }
        // BOOST_LOG_TRIVIAL(trace) << "Waiting for " << headerFile << std::endl;

        // Sleep before polling again to avoid busy waiting
        sleep(5); // seconds!

        /*
        // Handle file rotation: Check if files have been rotated
        if (getFileSize(payloadFile) == -1 || getFileSize(headerFile) == -1) {
            // Reopen files after rotation or if an error occurs
            payloadStream.close();
            headerStream.close();

            payloadStream.open(payloadFile, std::ios::binary | std::ios::in);
            headerStream.open(headerFile, std::ios::binary | std::ios::in);

            if (!payloadStream.is_open() || !headerStream.is_open()) {
                BOOST_LOG_TRIVIAL(error) << "Error reopening files after rotation: " << strerror(errno) << std::endl;
                return 1; // Exit if reopening fails
            }

            // Reset file positions to the last known state
            payloadStream.seekg(lastPayloadPos);
            headerStream.seekg(lastHeaderPos);
        }
        */
    }

    // Close the file streams when done (in case of graceful shutdown)
    payloadStream.close();
    headerStream.close();

    return 0;
}

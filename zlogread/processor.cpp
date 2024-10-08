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
#include <boost/log/attributes/named_scope.hpp>

namespace fs = boost::filesystem;
namespace logging = boost::log;
namespace keywords = boost::log::keywords;

// Forward declarations
std::string tm_to_string(const std::tm& timeStruct, const std::string& format);
std::tm string_to_tm(const std::string& timeString, const std::string& format);
std::tm today();
bool differs_from_today(const std::tm& then);
std::string get_date_path(const std::tm& today);



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
static std::streamoff get_filesize(const std::string& path) {
    struct stat stat_buf;
    int rc = stat(path.c_str(), &stat_buf);
    return rc == 0 ? stat_buf.st_size : -1;
}

// Utility function to save the current state (last read positions)
static void save_state(const fs::path path, unsigned long id, std::streamoff lastHeaderPos, std::streamoff lastPayloadPos) {
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
static void load_state(const fs::path path, unsigned long id, std::streamoff &lastHeaderPos, std::streamoff &lastPayloadPos) {
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

static void process_header_and_payload(
    const std::vector<std::string>& headerData,
    const std::streamsize inputSize,
    const std::streamsize outputSize,
    std::ifstream& payloadStream,
    unsigned long& size, unsigned long& count
) {
    //--------------------------------------------------------------------------
    // Here you have the individual header fields (in 'headerData'),
    // payload data: input (in 'input') and output (in 'output').
    //--------------------------------------------------------------------------

    // For debugging purposes, we read the data and makes some checks based on
    // knowledge of what zloggen (z-log generator, i.e. a test application) is writing...
    std::vector<char> inputBuffer(inputSize);
    std::vector<char> outputBuffer(outputSize);

    payloadStream.read(inputBuffer.data(), inputSize);
    payloadStream.read(outputBuffer.data(), outputSize);

    std::string input = std::string(inputBuffer.begin(), inputBuffer.end());
    std::string output = std::string(outputBuffer.begin(), outputBuffer.end());

    if (!input.starts_with("Input") && input.ends_with("Input")) {
        BOOST_LOG_TRIVIAL(error) << "Corrupt input: " << input << std::endl;
        throw std::underflow_error("Corrupt input: " + input);
    }

    if (!output.starts_with("Output") && output.ends_with("Output")) {
        BOOST_LOG_TRIVIAL(error) << "Corrupt output: " << output << std::endl;
        throw std::underflow_error("Corrupt output: " + output);
    }

    size += inputSize + outputSize;
    ++count;

    if (size > 5000L || count > 5000L) { // Arbitrary values, really
        BOOST_LOG_TRIVIAL(info) << "Reached limit (on accumulated size=" << size << " or count=" << count << ")" << std::endl;

        // Break up things and reset accumulators
        size = 0L;
        count = 0L;
    }
}

int process(
    int shard,
    const std::string& baseDir,
    const std::string& dateStr,
    const std::string& headerFile,
    const std::string& payloadFile
) {
    std::string logFileName = "processor_";
    logFileName += std::to_string(shard);
    logFileName += "_%N.log";

    // Set up file logging
    logging::add_file_log(
        keywords::file_name = logFileName,
        keywords::open_mode = std::ios_base::app,    // Open in append mode
        keywords::rotation_size = 10 * 1024 * 1024,  // Rotate after 10 MB
        keywords::format = "[%TimeStamp%] [%Severity%] %Message%",
        keywords::auto_flush = true  // Flush to file after each log message
    );

    logging::add_common_attributes();

    //
    std::streamoff lastPayloadPos = 0;
    std::streamoff lastHeaderPos = 0;

    std::tm date = string_to_tm(dateStr, "%Y-%m-%d");

    fs::path headerFilePath = baseDir;
    headerFilePath /= get_date_path(date);
    fs::path payloadFilePath = headerFilePath; // shared so far
    fs::path stateDir = headerFilePath; // shared so far

    headerFilePath /= headerFile; // unique
    payloadFilePath /= payloadFile; // unique


    // Load the previous state (if any)
    load_state(stateDir, shard, lastHeaderPos, lastPayloadPos);

    BOOST_LOG_TRIVIAL(info) << "Processor #" << shard << " starting at position " << lastHeaderPos << " in " << headerFilePath.string() << std::endl;


    // Open both files and keep them open
    std::ifstream headerStream(headerFilePath.string(), std::ios::binary | std::ios::in);
    std::ifstream payloadStream(payloadFilePath.string(), std::ios::binary | std::ios::in);

    if (!headerStream.is_open()) {
        std::string info = "Error opening header file (";
        info += strerror(errno);
        info += "): " + headerFilePath.string();
        BOOST_LOG_TRIVIAL(error) << info << std::endl;
        std::cout << info << std::endl;

        return 101;
    }

    // Check for file open errors
    if (!payloadStream.is_open()) {
        std::string info = "Error opening payload file (";
        info += strerror(errno);
        info += "): " + payloadFilePath.string();
        BOOST_LOG_TRIVIAL(error) << info << std::endl;
        std::cout << info << std::endl;

        headerStream.close();
        return 102;
    }

    // Accumulators
    unsigned long accSize = 0L;
    unsigned long accCount = 0L;

    //
    unsigned long processedEntries = 0L;
    signed int remainingReadAttempts = 0;
    while (true) {
        try {
            if (get_filesize(headerFilePath.string()) > lastHeaderPos) {
                // Seek to the last known position in the header file
                headerStream.clear(); // clears EOF flag if set
                headerStream.seekg(lastHeaderPos);

                // Read header entries
                std::string line;
                while (std::getline(headerStream, line)) {
                    //BOOST_LOG_TRIVIAL(trace) << "Read: " << line << std::endl;
                    std::vector<std::string> headerData = split(line, ',');

                    if (headerData.size() != 10) {
                        if (remainingReadAttempts == 0) {
                            remainingReadAttempts = 10;
                        } else {
                            --remainingReadAttempts;
                        }
                        BOOST_LOG_TRIVIAL(info) << "Header not ready: " << headerFile << " -- Remaining attempts: " << remainingReadAttempts << std::endl;
                        break; // try again later
                    }

                    auto inputSize = static_cast<std::streamsize>(std::stoul(headerData[7]));
                    auto outputSize = static_cast<std::streamsize>(std::stoul(headerData[8]));
                    auto offset = static_cast<std::streamoff>(std::stoul(headerData[9]));

                    // Check if the corresponding payload data is fully written
                    std::streamoff expectedPayloadSize = offset + inputSize + outputSize;

                    // Get the current payload file size
                    if (get_filesize(payloadFilePath.string()) >= expectedPayloadSize) {
                        // Payload data is available. Seek to the last read position in the payload file
                        payloadStream.clear(); // clears EOF flag if set
                        payloadStream.seekg(offset);

                        // Process input/output
                        process_header_and_payload(headerData, inputSize, outputSize, payloadStream, accSize, accCount);
                        processedEntries++;

                        // Update the last read position in both the header and payload files
                        lastPayloadPos = expectedPayloadSize;
                        lastHeaderPos = headerStream.tellg();

                        // Persist the current read positions
                        save_state(stateDir, shard, lastHeaderPos, lastPayloadPos);
                        remainingReadAttempts = 0;
                    } else {
                        break; // try again later
                    }
                }
            }
        } catch (const std::exception& e) {
            std::string info = "Aborting processing of ";
            info += headerFilePath.string();
            info += " and corresponding ";
            info += payloadFilePath.string();
            info += ": ";
            info += e.what();
            BOOST_LOG_TRIVIAL(error) << info << std::endl;
            throw;
        }

        std::this_thread::sleep_for(std::chrono::seconds(10));

        // Check if we have rolled over to the next day
        if (differs_from_today(date) && remainingReadAttempts == 0) {
            BOOST_LOG_TRIVIAL(info) << "I'm done, since I detected date rollover to " << tm_to_string(today(), "%Y-%m-%d")
            << " and I could not read more data from " << tm_to_string(date, "%Y-%m-%d") << std::endl;

            headerStream.close();
            payloadStream.close();

            std::cout << "Processed " << processedEntries << " entries" << std::endl;
            return 0;
        }

        if (remainingReadAttempts > 0) {
            if (remainingReadAttempts == 1) {
                // We have tried many times, so we will give up now
                BOOST_LOG_TRIVIAL(error) << "I'm done, since I detected date rollover to "
                         << tm_to_string(today(), "%Y-%m-%d")
                         << ", but I failed repeatedly to read from header file " << headerFile
                         << " at offset " << lastHeaderPos << " in log for "
                         << tm_to_string(date, "%Y-%m-%d") << std::endl;

                headerStream.close();
                payloadStream.close();

                std::cout << "Successfully processed " << processedEntries
                          << " entries, but repeatedly failed reading header file " << headerFile
                          << " at offset " << lastHeaderPos << " in log for "
                          << tm_to_string(date, "%Y-%m-%d") << std::endl;
                return 10;

            }

            // BOOST_LOG_TRIVIAL(trace) << "Re-attempting header read (" << remainingReadAttempts << " times)..." << std::endl;
        }
    }
}

//
// Created by Frode Randers on 2024-09-25.
//
#include <iostream>
#include <fstream>
#include <string>
#include <sys/stat.h>
#include <vector>
#include <cerrno>
#include <cstring>
#include <thread>
#include <chrono>
#include <sstream>
#include <stdexcept>

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

#include "zlog.h"
#include "processor.h"
#include "processoraction.h"
#include "date_utils.h"

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

static std::streamoff get_filesize(const std::string& path) {
    struct stat stat_buf{};
    int rc = stat(path.c_str(), &stat_buf);
    return rc == 0 ? stat_buf.st_size : -1;
}

bool HeaderParser::try_parse(const std::string& line, HeaderEntry& entry) const {
    entry.fields = split(line, ',');
    if (entry.fields.size() != NUMBER_HEADER_FIELDS) {
        return false;
    }

    entry.input_size = static_cast<std::streamsize>(std::stoul(entry.fields[7]));
    entry.output_size = static_cast<std::streamsize>(std::stoul(entry.fields[8]));
    entry.offset = static_cast<std::streamoff>(std::stoul(entry.fields[9]));
    return true;
}

StateStore::StateStore(const fs::path& dir, unsigned long id) {
    std::string name = "processor-" + std::to_string(id) + ".state";
    path_ = dir / name;
}

bool StateStore::load(std::streamoff& lastHeaderPos, std::streamoff& lastPayloadPos, unsigned long& size, unsigned long& count) const {
    std::ifstream stateFile(path_.string(), std::ios::binary | std::ios::in);
    if (!stateFile) {
        return false;
    }

    std::string line;
    if (std::getline(stateFile, line)) {
        std::vector<std::string> data = split(line, ',');
        if (data.size() != 4) {
            BOOST_LOG_TRIVIAL(error) << "Corrupt state: " << line << " (" << path_.filename().string() << ")" << std::endl;
            return false;
        }

        lastHeaderPos = static_cast<std::streamoff>(std::stoul(data[0]));
        lastPayloadPos = static_cast<std::streamoff>(std::stoul(data[1]));
        size = static_cast<std::streamoff>(std::stoul(data[2]));
        count = static_cast<std::streamoff>(std::stoul(data[3]));
        BOOST_LOG_TRIVIAL(trace) << "Loaded state: header=" << lastHeaderPos << ", payload=" << lastPayloadPos
                                << ", size=" << size << ", count=" << count << std::endl;
        return true;
    }

    BOOST_LOG_TRIVIAL(debug) << "Empty file: " << path_.filename().string() << std::endl;
    return false;
}

void StateStore::save(std::streamoff lastHeaderPos, std::streamoff lastPayloadPos, unsigned long size, unsigned long count) const {
    std::ofstream stateStream(path_.string(), std::ios::binary | std::ios::out | std::ios::trunc);
    if (!stateStream) {
        return;
    }

    stateStream
        << std::to_string(lastHeaderPos) << ","
        << std::to_string(lastPayloadPos) << ","
        << std::to_string(size) << ","
        << std::to_string(count) << std::endl;
}

void BatchAccumulator::add(std::streamsize delta) {
    size_ += static_cast<unsigned long>(delta);
    ++count_;
}

bool BatchAccumulator::should_flush() const {
    return size_ > NOMINAL_BATCH_SIZE || count_ > NOMINAL_BATCH_COUNT;
}

void BatchAccumulator::reset() {
    size_ = 0L;
    count_ = 0L;
}

void BatchAccumulator::set(unsigned long size, unsigned long count) {
    size_ = size;
    count_ = count;
}

unsigned long BatchAccumulator::size() const {
    return size_;
}

unsigned long BatchAccumulator::count() const {
    return count_;
}

PayloadReader::PayloadReader(std::ifstream& payloadStream)
    : payloadStream_(payloadStream) {}

void PayloadReader::read(const HeaderEntry& entry, std::string& input, std::string& output) {
    std::vector<char> inputBuffer(entry.input_size);
    std::vector<char> outputBuffer(entry.output_size);

    payloadStream_.read(inputBuffer.data(), entry.input_size);
    payloadStream_.read(outputBuffer.data(), entry.output_size);

    input.assign(inputBuffer.begin(), inputBuffer.end());
    output.assign(outputBuffer.begin(), outputBuffer.end());
}

Processor::Processor(
    unsigned long shard,
    const std::string& baseDir,
    const std::string& dateStr,
    const std::string& headerFile,
    const std::string& payloadFile,
    std::unique_ptr<ProcessorAction> action)
    : shard_(shard),
      baseDir_(baseDir),
      dateStr_(dateStr),
      headerFile_(headerFile),
      payloadFile_(payloadFile),
      action_(std::move(action)) {}

static void validate_payload(const std::string& input, const std::string& output) {
    if (!input.starts_with("Input") && input.ends_with("Input")) {
        BOOST_LOG_TRIVIAL(error) << "Corrupt input: " << input << std::endl;
        throw std::underflow_error("Corrupt input: " + input);
    }

    if (!output.starts_with("Output") && output.ends_with("Output")) {
        BOOST_LOG_TRIVIAL(error) << "Corrupt output: " << output << std::endl;
        throw std::underflow_error("Corrupt output: " + output);
    }
}

int Processor::run() {
    std::string logFileName = "processor_" + std::to_string(shard_) + "_%N.log";

    logging::add_file_log(
        keywords::file_name = logFileName,
        keywords::open_mode = std::ios_base::app,
        keywords::rotation_size = 10 * 1024 * 1024,
        keywords::format = "[%TimeStamp%] [%Severity%] %Message%",
        keywords::auto_flush = true
    );

    logging::add_common_attributes();

    std::streamoff lastPayloadPos = 0;
    std::streamoff lastHeaderPos = 0;

    std::tm date = string_to_tm(dateStr_, DATE_FORMAT);

    fs::path headerFilePath = baseDir_;
    headerFilePath /= get_date_path(date);
    fs::path payloadFilePath = headerFilePath;
    fs::path stateDir = headerFilePath;

    headerFilePath /= headerFile_;
    payloadFilePath /= payloadFile_;

    BatchAccumulator accumulator;
    StateStore stateStore(stateDir, shard_);

    unsigned long loadedSize = 0L;
    unsigned long loadedCount = 0L;
    stateStore.load(lastHeaderPos, lastPayloadPos, loadedSize, loadedCount);
    accumulator.set(loadedSize, loadedCount);
    if (accumulator.should_flush()) {
        action_->flush("Reached limit: size=" + std::to_string(accumulator.size()) +
                       " count=" + std::to_string(accumulator.count()));
        accumulator.reset();
    }

    BOOST_LOG_TRIVIAL(info) << "Processor #" << shard_ << " starting at position " << lastHeaderPos
                            << " in " << headerFilePath.string() << std::endl;

    std::ifstream headerStream(headerFilePath.string(), std::ios::binary | std::ios::in);
    std::ifstream payloadStream(payloadFilePath.string(), std::ios::binary | std::ios::in);

    if (!headerStream.is_open()) {
        std::string info = "Error opening header file (";
        info += strerror(errno);
        info += "): " + headerFilePath.string();
        BOOST_LOG_TRIVIAL(error) << info << std::endl;
        std::cout << info << std::endl;
        return STATUS_COULD_NOT_OPEN_HEADER_FILE;
    }

    if (!payloadStream.is_open()) {
        std::string info = "Error opening payload file (";
        info += strerror(errno);
        info += "): " + payloadFilePath.string();
        BOOST_LOG_TRIVIAL(error) << info << std::endl;
        std::cout << info << std::endl;
        headerStream.close();
        return STATUS_COULD_NOT_OPEN_PAYLOAD_FILE;
    }

    unsigned long processedEntries = 0L;
    signed int remainingReadAttempts = 0;
    HeaderParser parser;
    PayloadReader payloadReader(payloadStream);

    while (true) {
        try {
            if (get_filesize(headerFilePath.string()) > lastHeaderPos) {
                headerStream.clear();
                headerStream.seekg(lastHeaderPos);

                std::string line;
                while (std::getline(headerStream, line)) {
                    HeaderEntry entry;
                    if (!parser.try_parse(line, entry)) {
                        if (remainingReadAttempts == 0) {
                            remainingReadAttempts = NUMBER_HEADER_READ_ATTEMPTS;
                        } else {
                            --remainingReadAttempts;
                        }
                        BOOST_LOG_TRIVIAL(info) << "Header not ready: " << headerFile_ << " -- Remaining attempts: "
                                                << remainingReadAttempts << std::endl;
                        break;
                    }

                    std::streamoff expectedPayloadSize = entry.offset + entry.input_size + entry.output_size;
                    if (get_filesize(payloadFilePath.string()) >= expectedPayloadSize) {
                        payloadStream.clear();
                        payloadStream.seekg(entry.offset);

                        std::string input;
                        std::string output;
                        payloadReader.read(entry, input, output);
                        validate_payload(input, output);

                        accumulator.add(entry.input_size + entry.output_size);
                        ++processedEntries;

                        if (accumulator.should_flush()) {
                            action_->flush("Reached limit: size=" + std::to_string(accumulator.size()) +
                                           " count=" + std::to_string(accumulator.count()));
                            accumulator.reset();
                        }

                        lastPayloadPos = expectedPayloadSize;
                        lastHeaderPos = headerStream.tellg();
                        stateStore.save(lastHeaderPos, lastPayloadPos, accumulator.size(), accumulator.count());
                        remainingReadAttempts = 0;
                    } else {
                        break;
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

        if (differs_from_today(date) && remainingReadAttempts == 0) {
            BOOST_LOG_TRIVIAL(info) << "Detected date rollover to " << tm_to_string(today(), DATE_FORMAT)
                                    << ". Can not read more data from " << tm_to_string(date, DATE_FORMAT) << std::endl;

            headerStream.close();
            payloadStream.close();

            action_->flush("Date roll over, clean flush...");

            std::cout << "Processed " << processedEntries << " entries" << std::endl;
            return STATUS_ENDED_SUCCESSFULLY;
        }

        if (remainingReadAttempts > 0 && remainingReadAttempts == 1) {
            BOOST_LOG_TRIVIAL(error) << "Detected date rollover to " << tm_to_string(today(), DATE_FORMAT)
                                     << ". Repeatedly failed to read from header file " << headerFile_
                                     << " at offset " << lastHeaderPos << " for " << tm_to_string(date, DATE_FORMAT)
                                     << std::endl;

            headerStream.close();
            payloadStream.close();

            action_->flush("Date roll over, unclean flush...");

            std::cout << "Successfully processed " << processedEntries
                      << " entries. Repeatedly failed to read header file " << headerFile_
                      << " at offset " << lastHeaderPos << " for " << tm_to_string(date, DATE_FORMAT) << std::endl;

            return STATUS_ENDED_UNSUCCESSFULLY;
        }
    }
}

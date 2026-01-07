//
// Created by Frode Randers on 2024-10-12.
//

#ifndef PROCESSOR_H
#define PROCESSOR_H

#include <fstream>
#include <string>
#include <vector>
#include <memory>

#include <boost/filesystem.hpp>

#include "processoraction.h"

namespace fs = boost::filesystem;

struct HeaderEntry {
    std::vector<std::string> fields;
    std::streamsize input_size = 0;
    std::streamsize output_size = 0;
    std::streamoff offset = 0;
};

class HeaderParser {
public:
    bool try_parse(const std::string& line, HeaderEntry& entry) const;
};

class StateStore {
public:
    StateStore(const fs::path& dir, unsigned long id);
    bool load(std::streamoff& lastHeaderPos, std::streamoff& lastPayloadPos, unsigned long& size, unsigned long& count) const;
    void save(std::streamoff lastHeaderPos, std::streamoff lastPayloadPos, unsigned long size, unsigned long count) const;

private:
    fs::path path_;
};

class BatchAccumulator {
public:
    void add(std::streamsize delta);
    bool should_flush() const;
    void reset();
    void set(unsigned long size, unsigned long count);
    unsigned long size() const;
    unsigned long count() const;

private:
    unsigned long size_ = 0L;
    unsigned long count_ = 0L;
};

class PayloadReader {
public:
    explicit PayloadReader(std::ifstream& payloadStream);
    void read(const HeaderEntry& entry, std::string& input, std::string& output);

private:
    std::ifstream& payloadStream_;
};

class Processor {
public:
    Processor(
        unsigned long shard,
        const std::string& baseDir,
        const std::string& dateStr,
        const std::string& headerFile,
        const std::string& payloadFile,
        std::unique_ptr<ProcessorAction> action);
    int run();

private:
    unsigned long shard_;
    std::string baseDir_;
    std::string dateStr_;
    std::string headerFile_;
    std::string payloadFile_;
    std::unique_ptr<ProcessorAction> action_;
};

int process(int shard, const std::string& baseDir, const std::string& dateStr, const std::string& headerFile, const std::string& payloadFile);

#endif // PROCESSOR_H

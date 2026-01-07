//
// Created by Frode Randers on 2024-10-12.
//

#ifndef LOGWRITER_H
#define LOGWRITER_H

#include <fstream>
#include <random>
#include <string>
#include <vector>

class DelayPolicy {
public:
    void random_delay(int minMs, int maxMs) const;
};

class EntryGenerator {
public:
    EntryGenerator();
    const std::string& input() const;
    const std::string& output() const;
    std::string header_prefix(unsigned long counter) const;
    std::string header_mid(unsigned long counter) const;

private:
    std::vector<std::string> fruits_;
    std::string input_;
    std::string output_;
};

class FilePairSet {
public:
    FilePairSet(const std::string& dirPath, unsigned int numPairs);
    FilePairSet(const FilePairSet&) = delete;
    FilePairSet& operator=(const FilePairSet&) = delete;
    FilePairSet(FilePairSet&&) noexcept = default;
    FilePairSet& operator=(FilePairSet&&) noexcept = default;
    bool open();
    void close();
    std::ofstream& header(unsigned int index);
    std::ofstream& payload(unsigned int index);
    std::vector<std::streamoff>& offsets();

private:
    std::string dirPath_;
    std::vector<std::ofstream> headerFiles_;
    std::vector<std::ofstream> payloadFiles_;
    std::vector<std::streamoff> offsets_;
};

class LogWriter {
public:
    LogWriter();
    void generate_for_day(const std::string& basePath, const std::tm& date, unsigned int numFilePairs, unsigned int numberEntries);
    void generate_continuous(const std::string& basePath);

private:
    void ensure_directory(const std::string& dirPath) const;
    EntryGenerator generator_;
    DelayPolicy delay_;
};

#endif // LOGWRITER_H

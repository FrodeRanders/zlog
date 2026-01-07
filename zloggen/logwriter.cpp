//
// Created by Frode Randers on 2024-10-12.
//

#include "logwriter.h"

#include <boost/filesystem.hpp>
#include <chrono>
#include <iostream>
#include <thread>

#include "date_utils.h"

namespace fs = boost::filesystem;

void DelayPolicy::random_delay(int minMs, int maxMs) const {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(minMs, maxMs);
    std::this_thread::sleep_for(std::chrono::milliseconds(dis(gen)));
}

EntryGenerator::EntryGenerator()
    : fruits_({"Apple", "Banana", "Cherry", "Date", "Elderberry", "Fig", "Grape"}),
      input_("InputInputInputInputInputInputInputInputInputInputInput"),
      output_("OutputOutputOutputOutputOutputOutputOutputOutputOutputOutputOutputOutputOutputOutput") {}

const std::string& EntryGenerator::input() const {
    return input_;
}

const std::string& EntryGenerator::output() const {
    return output_;
}

std::string EntryGenerator::header_prefix(unsigned long counter) const {
    std::string headerLine;
    headerLine += fruits_[counter % fruits_.size()] + ",";
    headerLine += fruits_[(counter + 1) % fruits_.size()] + ",";
    headerLine += "Potato,,Carrot,";
    headerLine += fruits_[(counter + 2) % fruits_.size()] + ",";
    return headerLine;
}

std::string EntryGenerator::header_mid(unsigned long counter) const {
    return fruits_[(counter + 3) % fruits_.size()] + ",";
}

FilePairSet::FilePairSet(const std::string& dirPath, unsigned int numPairs)
    : dirPath_(dirPath),
      headerFiles_(numPairs),
      payloadFiles_(numPairs),
      offsets_(numPairs, 0) {}

bool FilePairSet::open() {
    for (unsigned int i = 0; i < headerFiles_.size(); ++i) {
        std::string headerFilename = dirPath_ + "/file" + std::to_string(i) + ".header";
        std::string payloadFilename = dirPath_ + "/file" + std::to_string(i) + ".payload";

        headerFiles_[i].open(headerFilename, std::ios::out | std::ios::app);
        if (!headerFiles_[i].is_open()) {
            std::cerr << "Error opening header file: " << headerFilename << std::endl;
            return false;
        }

        payloadFiles_[i].open(payloadFilename, std::ios::out | std::ios::app);
        if (!payloadFiles_[i].is_open()) {
            std::cerr << "Error opening payload file: " << payloadFilename << std::endl;
            return false;
        }
    }
    return true;
}

void FilePairSet::close() {
    for (auto& headerFile : headerFiles_) {
        if (headerFile.is_open()) {
            headerFile.close();
        }
    }
    for (auto& payloadFile : payloadFiles_) {
        if (payloadFile.is_open()) {
            payloadFile.close();
        }
    }
}

std::ofstream& FilePairSet::header(unsigned int index) {
    return headerFiles_.at(index);
}

std::ofstream& FilePairSet::payload(unsigned int index) {
    return payloadFiles_.at(index);
}

std::vector<std::streamoff>& FilePairSet::offsets() {
    return offsets_;
}

LogWriter::LogWriter() = default;

void LogWriter::ensure_directory(const std::string& dirPath) const {
    if (!fs::exists(dirPath)) {
        fs::create_directories(dirPath);
    }
}

void LogWriter::generate_for_day(const std::string& basePath, const std::tm& date, unsigned int numFilePairs, unsigned int numberEntries) {
    std::cout << "Generating test data for " << (1900 + date.tm_year)
              << "-" << (date.tm_mon + 1) << "-" << date.tm_mday << " " << std::flush;

    std::string dirPath = basePath + "/" + get_date_path(date);
    ensure_directory(dirPath);

    FilePairSet files(dirPath, numFilePairs);
    if (!files.open()) {
        return;
    }

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> fileSelector(0, static_cast<int>(numFilePairs) - 1);
    std::uniform_int_distribution<> delayedHeaderWrite(0, 100);

    auto& offsets = files.offsets();

    for (unsigned int entryIndex = 0; entryIndex < numberEntries; ++entryIndex) {
        int fileIndex = fileSelector(gen);

        std::string headerLine = generator_.header_prefix(entryIndex);
        if (delayedHeaderWrite(gen) > 10) {
            headerLine += generator_.header_mid(entryIndex);
        } else {
            files.header(fileIndex) << headerLine << std::flush;
            delay_.random_delay(1, 100);
            headerLine = generator_.header_mid(entryIndex);
        }

        headerLine += std::to_string(generator_.input().size()) + ",";
        headerLine += std::to_string(generator_.output().size()) + ",";
        headerLine += std::to_string(offsets[fileIndex]) + "\n";
        files.header(fileIndex) << headerLine;

        files.payload(fileIndex) << generator_.input() << generator_.output();

        offsets[fileIndex] += static_cast<std::streamoff>(generator_.input().size() + generator_.output().size());

        delay_.random_delay(10, 50);
        if (entryIndex % 5 == 0) {
            files.header(fileIndex).flush();
            delay_.random_delay(10, 50);
            files.payload(fileIndex).flush();
        } else if (entryIndex % 7 == 0) {
            files.payload(fileIndex).flush();
            delay_.random_delay(10, 50);
            files.header(fileIndex).flush();
        }

        delay_.random_delay(1, 10);
    }

    files.close();
    std::cout << "-- completed" << std::endl;
}

void LogWriter::generate_continuous(const std::string& basePath) {
    constexpr unsigned int numFilePairs = 10;
    std::tm date = today();

    std::cout << "Generating test data for " << (1900 + date.tm_year)
              << "-" << (date.tm_mon + 1) << "-" << date.tm_mday << " " << std::flush;

    std::string dirPath = basePath + "/" + get_date_path(date);
    ensure_directory(dirPath);

    FilePairSet files(dirPath, numFilePairs);
    if (!files.open()) {
        return;
    }

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> fileSelector(0, numFilePairs - 1);
    std::uniform_int_distribution<> delayedHeaderWrite(0, 100);

    auto& offsets = files.offsets();
    unsigned long counter = 0L;

    while (true) {
        int fileIndex = fileSelector(gen);

        std::string headerLine = generator_.header_prefix(counter);
        if (delayedHeaderWrite(gen) > 10) {
            headerLine += generator_.header_mid(counter);
        } else {
            files.header(fileIndex) << headerLine << std::flush;
            delay_.random_delay(1, 100);
            headerLine = generator_.header_mid(counter);
        }

        headerLine += std::to_string(generator_.input().size()) + ",";
        headerLine += std::to_string(generator_.output().size()) + ",";
        headerLine += std::to_string(offsets[fileIndex]) + "\n";
        files.header(fileIndex) << headerLine;

        files.payload(fileIndex) << generator_.input() << generator_.output();

        offsets[fileIndex] += static_cast<std::streamoff>(generator_.input().size() + generator_.output().size());

        delay_.random_delay(0, 5);
        if (counter % 5 == 0) {
            files.header(fileIndex).flush();
            delay_.random_delay(1, 50);
            files.payload(fileIndex).flush();
        } else if (counter % 7 == 0) {
            files.payload(fileIndex).flush();
            delay_.random_delay(1, 50);
            files.header(fileIndex).flush();
        }
        ++counter;

        delay_.random_delay(0, 5);

        if (differs_from_today(date)) {
            std::cout << std::flush << std::endl << "Detected day rollover" << std::endl;
            files.close();

            date = today();
            dirPath = basePath + "/" + get_date_path(date);
            ensure_directory(dirPath);

            std::cout << "Generating test data for " << (1900 + date.tm_year)
                      << "-" << (date.tm_mon + 1) << "-" << date.tm_mday << " " << std::endl << std::flush;

            files = FilePairSet(dirPath, numFilePairs);
            if (!files.open()) {
                return;
            }
        } else {
            std::cout << "." << std::flush;
        }
    }
}

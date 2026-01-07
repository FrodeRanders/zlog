//
// Created by Frode Randers on 2024-09-28.
//

#include <iostream>
#include <string>
#include <vector>
#include <chrono>
#include <ctime>
#include <thread>

#include <boost/log/core.hpp>
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

#include "zlog.h"
#include "directorymonitor.h"
#include "date_utils.h"

namespace fs = boost::filesystem;
namespace logging = boost::log;
namespace keywords = boost::log::keywords;
namespace bp = boost::process::v2;
namespace asio = boost::asio;

ChildProcess::ChildProcess(bp::popen&& process, unsigned int shard, std::string stem)
    : process(std::move(process)),
      buffer(std::make_unique<asio::streambuf>()),
      shard(shard),
      stem(std::move(stem)) {}

PairMap FilePairScanner::find_new_pairs(const fs::path& dirPath, PairMap& existingFiles) const {
    PairMap newEntries;

    if (fs::exists(dirPath) && fs::is_directory(dirPath)) {
        std::map<std::string, fs::path> stems;
        std::map<std::string, std::string> headerFiles;
        std::map<std::string, std::string> payloadFiles;

        for (const auto& entry : fs::directory_iterator(dirPath)) {
            if (!fs::is_regular_file(entry)) {
                continue;
            }

            fs::path filePath = entry.path();
            if (filePath.extension() == ".state") {
                continue;
            }

            std::string stem = filePath.stem().string();
            stems[stem] = filePath;

            if (filePath.extension() == ".header") {
                headerFiles[stem] = filePath.filename().string();
            } else if (filePath.extension() == ".payload") {
                payloadFiles[stem] = filePath.filename().string();
            }
        }

        for (const auto& stemEntry : stems) {
            const std::string& stem = stemEntry.first;
            const fs::path& path = stemEntry.second;

            if (headerFiles.find(stem) != headerFiles.end() &&
                payloadFiles.find(stem) != payloadFiles.end()) {
                if (existingFiles.find(stem) == existingFiles.end()) {
                    FilePair entry{stem, path, headerFiles[stem], payloadFiles[stem]};
                    newEntries[stem] = entry;
                    existingFiles[stem] = entry;
                }
            } else {
                BOOST_LOG_TRIVIAL(error) << ".header and .payload files do not match for " << stem << std::endl;
            }
        }
    } else {
        BOOST_LOG_TRIVIAL(error) << "Directory does not exist or is not accessible: " << dirPath << std::endl;
    }

    return newEntries;
}

std::vector<ChildProcess> ProcessSupervisor::launch_children(
    asio::io_context& io,
    const PairMap& newPairs,
    const bp::filesystem::path& executable,
    const std::string& basePath,
    const std::string& dateStr) const {
    std::vector<ChildProcess> children;

    unsigned int shard = 0;
    for (const auto& newPair : newPairs) {
        const FilePair& pair = newPair.second;

        std::vector<std::string> args{
            "-p",
            std::to_string(++shard),
            basePath,
            dateStr,
            pair.header_file,
            pair.payload_file
        };

        try {
            bp::popen process(io.get_executor(), executable, args);
            children.emplace_back(std::move(process), shard, pair.stem);

            BOOST_LOG_TRIVIAL(info)
                << "Processor #" << shard << " (pid=" << children.back().process.id() << ") handles "
                << pair.header_file << " and " << pair.payload_file << std::endl;
        }
        catch (const std::exception& e) {
            BOOST_LOG_TRIVIAL(error) << "Failed to spawn child process: " << e.what() << std::endl;
        }
    }

    return children;
}

void ProcessSupervisor::start_read(ChildProcess& child) {
    if (!child.buffer) {
        return;
    }

    asio::async_read_until(
        child.process.get_stdout(),
        *child.buffer,
        '\n',
        [&child](const boost::system::error_code& ec, std::size_t) {
            if (ec) {
                return;
            }

            if (!child.buffer) {
                return;
            }
            std::istream is(child.buffer.get());
            std::string line;
            std::getline(is, line);
            if (!line.empty()) {
                child.last_line = line;
                BOOST_LOG_TRIVIAL(info) << "Processor #" << child.shard << " (pid=" << child.process.id() << ") reports: " << line;
            }
            start_read(child);
        }
    );
}

static void report_exited_child(const ChildProcess& child, PairMap& trackedUnits) {
    int exitCode = child.exit_code;
    const std::string& line = child.last_line;

    if (exitCode > FILE_READ_RELATED_ERRORS) {
        std::string info = "Processor #" + std::to_string(child.shard) + " (pid=" +
                           std::to_string(child.process.id()) + ") could not load ";
        if (exitCode == STATUS_COULD_NOT_OPEN_HEADER_FILE) {
            info += "header file " + child.stem + ".header";
        } else if (exitCode == STATUS_COULD_NOT_OPEN_PAYLOAD_FILE) {
            info += "payload file " + child.stem + ".payload";
        } else {
            info += "some file??";
        }
        if (!line.empty()) {
            info += ". It reports: " + line;
        }

        auto tuit = trackedUnits.find(child.stem);
        if (tuit != trackedUnits.end()) {
            trackedUnits.erase(tuit);
            BOOST_LOG_TRIVIAL(info) << info << " -- Retrying later" << std::endl;
        } else {
            BOOST_LOG_TRIVIAL(error) << info << " -- Failed to locate unit among tracked units!" << std::endl;
        }
    } else if (exitCode == STATUS_ENDED_UNSUCCESSFULLY) {
        std::string info = "Processor #" + std::to_string(child.shard) + " (pid=" +
                           std::to_string(child.process.id()) + ") could not process all headers in file " +
                           child.stem + ".header.";
        if (!line.empty()) {
            info += " It reports: " + line;
        }
        BOOST_LOG_TRIVIAL(error) << info << std::endl;

    } else if (exitCode == STATUS_ENDED_SUCCESSFULLY) {
        BOOST_LOG_TRIVIAL(info) << "Processor #" << child.shard << " (pid=" << child.process.id()
                                << ") finished gracefully with report: " << line << std::endl;
    } else {
        BOOST_LOG_TRIVIAL(info) << "Processor #" << child.shard << " (pid=" << child.process.id()
                                << ") reports error (" << exitCode << "): " << line << std::endl;
    }
}

void ProcessSupervisor::monitor_children(asio::io_context& io, std::vector<ChildProcess>& children, PairMap& trackedUnits) const {
    for (auto& child : children) {
        start_read(child);
        child.process.async_wait([&child](const boost::system::error_code& ec, int exitCode) {
            if (!ec) {
                child.exit_code = exitCode;
            }
            child.exited = true;
        });
    }

    while (true) {
        io.restart();
        io.run_for(std::chrono::milliseconds(100));

        bool all_exited = true;
        for (const auto& child : children) {
            if (!child.exited) {
                all_exited = false;
                break;
            }
        }
        if (all_exited) {
            break;
        }
    }

    for (const auto& child : children) {
        report_exited_child(child, trackedUnits);
    }

    children.clear();
}

DirectoryMonitor::DirectoryMonitor(const fs::path& myself, const std::string& basePath, const std::string& dateStr)
    : myself_(myself),
      basePath_(basePath),
      dateStr_(dateStr) {}

int DirectoryMonitor::run() {
    logging::add_file_log(
        keywords::file_name = "monitor_%N.log",
        keywords::open_mode = std::ios_base::app,
        keywords::rotation_size = 10 * 1024 * 1024,
        keywords::format = "[%TimeStamp%] [%Severity%] %Message%",
        keywords::auto_flush = true
    );

    logging::add_common_attributes();

    bp::filesystem::path executable = fs::absolute(myself_).string();

    std::tm date = dateStr_.empty() ? today() : string_to_tm(dateStr_, DATE_FORMAT);

    BOOST_LOG_TRIVIAL(debug) << "Will instantiate sub-processes using executable: " << executable << std::endl;

    fs::path currentPath = basePath_;
    currentPath /= get_date_path(date);

    PairMap trackedUnits;

    while (true) {
        BOOST_LOG_TRIVIAL(info) << "Monitoring directory: " << currentPath << std::endl;

        auto untrackedUnits = scanner_.find_new_pairs(currentPath, trackedUnits);
        if (untrackedUnits.empty()) {
            BOOST_LOG_TRIVIAL(error) << "No matching .header and .payload pairs found in directory: "
                                     << currentPath << std::endl;
        } else {
            asio::io_context io;
            auto children = supervisor_.launch_children(io, untrackedUnits, executable, basePath_,
                                                        tm_to_string(date, DATE_FORMAT));
            supervisor_.monitor_children(io, children, trackedUnits);
        }

        if (dateStr_.empty()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));

            if (differs_from_today(date)) {
                BOOST_LOG_TRIVIAL(info) << "Detected day rollover" << std::endl;

                std::string info = "\nProcessed log files in directory: " + currentPath.string() + "\n";
                for (const auto& trackedUnit : trackedUnits) {
                    info += "   " + trackedUnit.second.header_file + " & " + trackedUnit.second.payload_file + "\n";
                }
                BOOST_LOG_TRIVIAL(info) << info << std::endl;

                date = today();
                currentPath = basePath_;
                currentPath /= get_date_path(date);
                trackedUnits.clear();

                BOOST_LOG_TRIVIAL(info) << "Switching to new directory: " << currentPath << std::endl;
            } else {
                BOOST_LOG_TRIVIAL(info) << "No day rollover detected, but child processes ended?" << std::endl;
                BOOST_LOG_TRIVIAL(info) << "Set on " << tm_to_string(date, DATE_FORMAT)
                                        << " and today is " << tm_to_string(today(), DATE_FORMAT) << std::endl;
                std::this_thread::sleep_for(std::chrono::seconds(30));
            }
        } else {
            BOOST_LOG_TRIVIAL(info) << "Ending" << std::endl;
            return STATUS_ENDED_SUCCESSFULLY;
        }
    }
}

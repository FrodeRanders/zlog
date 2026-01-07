//
// Created by Frode Randers on 2024-10-12.
//

#ifndef DIRECTORYMONITOR_H
#define DIRECTORYMONITOR_H

#include <map>
#include <memory>
#include <string>
#include <vector>

#include <boost/filesystem.hpp>
#include <boost/asio.hpp>
#include <boost/process/v2/popen.hpp>

namespace fs = boost::filesystem;
namespace bp = boost::process::v2;
namespace asio = boost::asio;

struct FilePair {
    std::string stem;
    fs::path directory;
    std::string header_file;
    std::string payload_file;
};

using PairMap = std::map<std::string, FilePair>;

class FilePairScanner {
public:
    PairMap find_new_pairs(const fs::path& dirPath, PairMap& existingFiles) const;
};

struct ChildProcess {
    ChildProcess(bp::popen&& process, unsigned int shard, std::string stem);

    bp::popen process;
    std::unique_ptr<asio::streambuf> buffer;
    unsigned int shard = 0;
    std::string stem;
    bool exited = false;
    int exit_code = 0;
    std::string last_line;
};

class ProcessSupervisor {
public:
    std::vector<ChildProcess> launch_children(
        asio::io_context& io,
        const PairMap& newPairs,
        const bp::filesystem::path& executable,
        const std::string& basePath,
        const std::string& dateStr) const;
    void monitor_children(asio::io_context& io, std::vector<ChildProcess>& children, PairMap& trackedUnits) const;

private:
    static void start_read(ChildProcess& child);
};

class DirectoryMonitor {
public:
    DirectoryMonitor(const fs::path& myself, const std::string& basePath, const std::string& dateStr);
    int run();

private:
    fs::path myself_;
    std::string basePath_;
    std::string dateStr_;
    FilePairScanner scanner_;
    ProcessSupervisor supervisor_;
};

int monitor_directory(const fs::path& myself, const std::string& basePath, const std::string& dateStr);

#endif // DIRECTORYMONITOR_H

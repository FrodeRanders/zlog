#include <iostream>
#include <string>
#include <cstring>

#include <boost/log/expressions.hpp>
#include <boost/log/sinks/text_file_backend.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <boost/log/utility/setup/console.hpp>

#include "zlog.h"
#include "processor.h"
#include "directorymonitor.h"

namespace logging = boost::log;
namespace keywords = boost::log::keywords;

int main(int argc, char* argv[]) {
    try {
        if (argc < 2) {
            std::cerr << "Usage: " << argv[0] << " <base-directory> [<date>]" << std::endl;
            return STATUS_ARGUMENTS_MISSING;
        }

        logging::add_console_log(
            std::clog,
            keywords::format = "[%TimeStamp%] [%Severity%] %Message%"
        );

        if (std::strcmp(argv[1], "-p") == 0 && argc == 7) {
            int id = std::stoi(argv[2]);
            return process(id, argv[3], argv[4], argv[5], argv[6]);
        }

        std::string dateStr;
        if (argc >= 3) {
            dateStr = argv[2];
        }
        return monitor_directory(argv[0], argv[1], dateStr);
    }
    catch (const std::invalid_argument& ia) {
        std::cout << "Invalid argument: " << ia.what() << std::endl;
        return STATUS_INVALID_ARGUMENT;
    }
    catch (std::exception& e) {
        std::cout << "Failed to process logs: " << e.what() << std::endl;
        return STATUS_GENERAL_FAILURE;
    }
}

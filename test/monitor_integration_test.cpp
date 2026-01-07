// End-to-end DirectoryMonitor test using a helper executable.

#include <fstream>
#include <iostream>
#include <string>

#include <boost/filesystem.hpp>

#include "directorymonitor.h"
#include "date_utils.h"
#include "zlog.h"

namespace fs = boost::filesystem;

static int fail(const std::string& message) {
    std::cerr << message << std::endl;
    return 1;
}

int main() {
    fs::path base_dir = fs::temp_directory_path() / fs::unique_path("zlog-monitor-%%%%-%%%%");
    fs::create_directories(base_dir);

    std::tm date = today();
    std::string date_str = tm_to_string(date, DATE_FORMAT);

    fs::path day_dir = base_dir / get_date_path(date);
    fs::create_directories(day_dir);

    fs::path header_path = day_dir / "file0.header";
    fs::path payload_path = day_dir / "file0.payload";

    {
        std::ofstream header(header_path.string(), std::ios::binary | std::ios::out);
        std::ofstream payload(payload_path.string(), std::ios::binary | std::ios::out);
        if (!header || !payload) {
            fs::remove_all(base_dir);
            return fail("Failed to create test header/payload files");
        }
        header << "dummy" << std::endl;
        payload << "data";
    }

    fs::path helper_path = MONITOR_HELPER_PATH;
    if (!fs::exists(helper_path)) {
        fs::remove_all(base_dir);
        return fail("monitor_helper executable not found");
    }

    DirectoryMonitor monitor(helper_path, base_dir.string(), date_str);
    int rc = monitor.run();

    if (rc != STATUS_ENDED_SUCCESSFULLY) {
        fs::remove_all(base_dir);
        return fail("Expected DirectoryMonitor to exit successfully");
    }

    fs::path marker = base_dir / "helper_ran";
    if (!fs::exists(marker)) {
        fs::remove_all(base_dir);
        return fail("Expected helper to create marker file");
    }

    fs::remove_all(base_dir);
    std::cout << "monitor_integration_test passed" << std::endl;
    return 0;
}

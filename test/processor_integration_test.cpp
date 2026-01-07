// End-to-end processor test using a small header/payload pair.

#include <cstdlib>
#include <fstream>
#include <iostream>

#include <boost/filesystem.hpp>

#include "processor.h"
#include "processoraction.h"
#include "date_utils.h"
#include "zlog.h"

namespace fs = boost::filesystem;

class TestAction final : public ProcessorAction {
public:
    void flush(const std::string& reason) override {
        last_reason = reason;
        ++flush_count;
    }

    int flush_count = 0;
    std::string last_reason;
};

static int fail(const std::string& message) {
    std::cerr << message << std::endl;
    return 1;
}

static void set_sleep_env() {
#if defined(_WIN32)
    _putenv_s("ZLOG_PROCESSOR_SLEEP_MS", "0");
#else
    setenv("ZLOG_PROCESSOR_SLEEP_MS", "0", 1);
#endif
}

int main() {
    set_sleep_env();

    std::tm date = today();
    date.tm_mday -= 1;
    std::mktime(&date);
    std::string date_str = tm_to_string(date, DATE_FORMAT);

    fs::path base_dir = fs::temp_directory_path() / fs::unique_path("zlog-processor-%%%%-%%%%");
    fs::create_directories(base_dir);

    fs::path day_dir = base_dir / get_date_path(date);
    fs::create_directories(day_dir);

    fs::path header_path = day_dir / "file0.header";
    fs::path payload_path = day_dir / "file0.payload";

    std::string input = "Input123";
    std::string output = "Output1234";

    {
        std::ofstream header(header_path.string(), std::ios::binary | std::ios::out);
        std::ofstream payload(payload_path.string(), std::ios::binary | std::ios::out);
        if (!header || !payload) {
            fs::remove_all(base_dir);
            return fail("Failed to create test header/payload files");
        }

        header << "a,b,c,d,e,f,g," << input.size() << "," << output.size() << ",0\n";
        payload << input << output;
    }

    auto action = std::make_unique<TestAction>();
    TestAction* action_ptr = action.get();

    Processor processor(1, base_dir.string(), date_str, "file0.header", "file0.payload", std::move(action));
    int rc = processor.run();

    if (rc != STATUS_ENDED_SUCCESSFULLY) {
        fs::remove_all(base_dir);
        return fail("Expected processor to exit successfully on date rollover");
    }

    fs::path state_path = day_dir / "processor-1.state";
    if (!fs::exists(state_path)) {
        fs::remove_all(base_dir);
        return fail("Expected processor to write state file");
    }

    if (action_ptr->flush_count < 1) {
        fs::remove_all(base_dir);
        return fail("Expected processor to flush at least once");
    }

    fs::remove_all(base_dir);
    std::cout << "processor_integration_test passed" << std::endl;
    return 0;
}

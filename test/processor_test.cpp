// Tests for processor helpers (HeaderParser, BatchAccumulator, StateStore).

#include <iostream>
#include <string>

#include <boost/filesystem.hpp>

#include "processor.h"
#include "zlog.h"

namespace fs = boost::filesystem;

static int fail(const std::string& message) {
    std::cerr << message << std::endl;
    return 1;
}

static int test_header_parser() {
    HeaderParser parser;
    HeaderEntry entry;

    if (!parser.try_parse("a,b,c,d,e,f,g,11,22,33", entry)) {
        return fail("Expected header parser to accept valid input");
    }

    if (entry.fields.size() != NUMBER_HEADER_FIELDS) {
        return fail("Expected header parser to capture 10 fields");
    }

    if (entry.input_size != 11 || entry.output_size != 22 || entry.offset != 33) {
        return fail("Expected header parser to parse size/offset fields");
    }

    HeaderEntry invalid;
    if (parser.try_parse("a,b,c,d,e,f,g,11,22", invalid)) {
        return fail("Expected header parser to reject short input");
    }

    return 0;
}

static int test_batch_accumulator() {
    BatchAccumulator accumulator;

    accumulator.add(NOMINAL_BATCH_SIZE);
    if (accumulator.should_flush()) {
        return fail("Expected batch size at threshold to not flush");
    }

    accumulator.add(1);
    if (!accumulator.should_flush()) {
        return fail("Expected batch size above threshold to flush");
    }

    accumulator.reset();
    for (int i = 0; i < NOMINAL_BATCH_COUNT; ++i) {
        accumulator.add(1);
    }
    if (accumulator.should_flush()) {
        return fail("Expected batch count at threshold to not flush");
    }

    accumulator.add(1);
    if (!accumulator.should_flush()) {
        return fail("Expected batch count above threshold to flush");
    }

    return 0;
}

static int test_state_store() {
    fs::path temp_dir = fs::temp_directory_path() / fs::unique_path("zlog-test-%%%%-%%%%-%%%%");
    fs::create_directories(temp_dir);

    StateStore store(temp_dir, 7);

    std::streamoff last_header = 10;
    std::streamoff last_payload = 20;
    unsigned long size = 30;
    unsigned long count = 40;

    store.save(last_header, last_payload, size, count);

    std::streamoff loaded_header = 0;
    std::streamoff loaded_payload = 0;
    unsigned long loaded_size = 0;
    unsigned long loaded_count = 0;

    if (!store.load(loaded_header, loaded_payload, loaded_size, loaded_count)) {
        return fail("Expected state store to load after saving");
    }

    if (loaded_header != last_header || loaded_payload != last_payload ||
        loaded_size != size || loaded_count != count) {
        return fail("Expected state store to roundtrip values");
    }

    fs::remove_all(temp_dir);
    return 0;
}

int main() {
    if (int rc = test_header_parser(); rc != 0) return rc;
    if (int rc = test_batch_accumulator(); rc != 0) return rc;
    if (int rc = test_state_store(); rc != 0) return rc;

    std::cout << "processor_test passed" << std::endl;
    return 0;
}

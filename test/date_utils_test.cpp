#include <cassert>
#include <ctime>
#include <iostream>

#include "date_utils.h"

static int fail(const std::string& message) {
    std::cerr << message << std::endl;
    return 1;
}

static int test_differs_from_today() {
    std::tm now = today();

    if (differs_from_today(now)) {
        return fail("Expected today() to not differ from today");
    }

    std::tm yesterday = now;
    yesterday.tm_mday -= 1;
    std::mktime(&yesterday);

    if (!differs_from_today(yesterday)) {
        return fail("Expected yesterday to differ from today");
    }

    std::tm tomorrow = now;
    tomorrow.tm_mday += 1;
    std::mktime(&tomorrow);

    if (!differs_from_today(tomorrow)) {
        return fail("Expected tomorrow to differ from today");
    }

    return 0;
}

static int test_dates_differ() {
    std::tm t1 = {};
    t1.tm_year = 124;
    t1.tm_mon = 0;
    t1.tm_mday = 15;

    std::tm t2 = t1;
    if (dates_differ(t1, t2)) {
        return fail("Expected identical dates to not differ");
    }

    t2.tm_mday += 1;
    std::mktime(&t2);
    if (!dates_differ(t1, t2)) {
        return fail("Expected differing dates to differ");
    }

    return 0;
}

static int test_tm_roundtrip() {
    std::tm t = {};
    t.tm_year = 124;
    t.tm_mon = 6;
    t.tm_mday = 4;

    std::string formatted = tm_to_string(t, "%Y-%m-%d");
    if (formatted != "2024-07-04") {
        return fail("Expected tm_to_string to format 2024-07-04");
    }

    std::tm parsed = string_to_tm(formatted, "%Y-%m-%d");
    if (parsed.tm_year != t.tm_year || parsed.tm_mon != t.tm_mon || parsed.tm_mday != t.tm_mday) {
        return fail("Expected string_to_tm to roundtrip date");
    }

    return 0;
}

static int test_get_date_path() {
    std::tm t = {};
    t.tm_year = 124;
    t.tm_mon = 9;
    t.tm_mday = 30;

    std::string path = get_date_path(t);
    if (path != "2024/10/30") {
        return fail("Expected get_date_path to return 2024/10/30");
    }

    return 0;
}

static int test_string_to_tm_invalid() {
    try {
        string_to_tm("2024-13-01", "%Y-%m-%d");
    } catch (const std::exception&) {
        return 0;
    }
    return fail("Expected string_to_tm to throw on invalid input");
}

int main() {
    if (int rc = test_differs_from_today(); rc != 0) return rc;
    if (int rc = test_dates_differ(); rc != 0) return rc;
    if (int rc = test_tm_roundtrip(); rc != 0) return rc;
    if (int rc = test_get_date_path(); rc != 0) return rc;
    if (int rc = test_string_to_tm_invalid(); rc != 0) return rc;

    std::cout << "date_utils_test passed" << std::endl;
    return 0;
}

//
// Created by Frode Randers on 2024-09-28.
//

#include "date_utils.h"

#include <sstream>
#include <iomanip>
#include <stdexcept>

std::string tm_to_string(const std::tm& timeStruct, const std::string& format) {
    std::ostringstream oss;
    oss << std::put_time(&timeStruct, format.c_str());
    return oss.str();
}

std::tm string_to_tm(const std::string& timeString, const std::string& format) {
    std::tm timeStruct = {};
    std::istringstream iss(timeString);
    iss >> std::get_time(&timeStruct, format.c_str());
    if (iss.fail()) {
        throw std::runtime_error("Failed to parse time string \"" + timeString + "\". Format should be: " + format);
    }
    return timeStruct;
}

std::tm today() {
    std::time_t now = std::time(nullptr);
    std::tm local_tm = *std::localtime(&now);
    return local_tm;
}

bool dates_differ(const std::tm& t1, const std::tm& t2) {
    return (t1.tm_year != t2.tm_year ||
            t1.tm_mon != t2.tm_mon ||
            t1.tm_mday != t2.tm_mday);
}

bool differs_from_today(const std::tm& then) {
    return dates_differ(then, today());
}

std::string get_date_path(const std::tm& date) {
    return std::to_string(1900 + date.tm_year) + "/" +
        std::to_string(date.tm_mon + 1) + "/" +
        std::to_string(date.tm_mday);
}

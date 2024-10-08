//
// Created by Frode Randers on 2024-09-28.
//

#include <iostream>
#include <sstream>
#include <iomanip>
#include <ctime>

// Convert std::tm* to std::string
std::string tm_to_string(const std::tm& timeStruct, const std::string& format) {
    std::ostringstream oss;
    oss << std::put_time(&timeStruct, format.c_str());
    return oss.str();
}

// Convert std::string to std::tm
std::tm string_to_tm(const std::string& timeString, const std::string& format) {
    std::tm timeStruct = {};
    std::istringstream iss(timeString);
    iss >> std::get_time(&timeStruct, format.c_str());
    if (iss.fail()) {
        std::string info = "Failed to parse time string \"" + timeString + "\". Format should be: " + format;
        throw std::runtime_error(info);
    }
    return timeStruct;
}

void proceed_to_next_day(std::tm& date) {
    // Increment day
    date.tm_mday += 1;

    // Normalize
    std::mktime(&date);
}

// Function to return the current date (deep copy of std::tm)
std::tm today() {
    std::time_t now = std::time(nullptr);
    std::tm local_tm = *std::localtime(&now); // Deep copy of the std::tm struct
    return local_tm;
}

// Function to check if two tm structs represent different days
bool dates_differ(const std::tm& t1, const std::tm& t2) {
    return (t1.tm_year != t2.tm_year ||
            t1.tm_mon != t2.tm_mon ||
            t1.tm_mday != t2.tm_mday);
}

bool differs_from_today(const std::tm& then) {
    return dates_differ(then, today());
}

std::string get_date_path(const std::tm& today) {
    std::string path =
        std::to_string(1900 + today.tm_year) + "/" +
        std::to_string(today.tm_mon + 1) + "/" +
        std::to_string(today.tm_mday);

    return path;
}

//
// Created by Frode Randers on 2024-09-28.
//

#ifndef DATE_UTILS_H
#define DATE_UTILS_H

#include <string>
#include <ctime>

std::string tm_to_string(const std::tm& timeStruct, const std::string& format);
std::tm string_to_tm(const std::string& timeString, const std::string& format);
std::tm today();
bool dates_differ(const std::tm& t1, const std::tm& t2);
bool differs_from_today(const std::tm& then);
std::string get_date_path(const std::tm& date);

#endif // DATE_UTILS_H

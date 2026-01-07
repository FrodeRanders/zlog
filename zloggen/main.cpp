#include <iostream>
#include <string>
#include <ctime>

#include "date_utils.h"
#include "logwriter.h"

int main(int argc, char* argv[]) {
    try {
        if (argc < 2) {
            std::cerr << "Usage: " << argv[0] << " <base-directory> <number_of_days> <number_of_file_pairs> <number_of_entries>" << std::endl;
            return 1;
        }

        unsigned int numberOfDays = 0;
        unsigned int numberOfFilePairs = 0;
        unsigned int numberOfEntries = 0;

        unsigned int idx = 1;
        std::string basePath = argv[idx++];

        if (argc > 2) {
            numberOfDays = std::stoul(argv[idx++]);
            if (numberOfDays == 0) {
                std::cerr << "Provide number of days" << std::endl;
                return 1;
            }
        }

        if (argc > 3) {
            numberOfFilePairs = std::stoul(argv[idx++]);
            if (numberOfFilePairs == 0) {
                std::cerr << "Provide number of file pairs" << std::endl;
                return 1;
            }
        }

        if (argc > 4) {
            numberOfEntries = std::stoul(argv[idx++]);
            if (numberOfEntries == 0) {
                std::cerr << "Provide number of entries" << std::endl;
                return 1;
            }
        }

        LogWriter writer;
        std::tm date = today();

        if (argc == 5) {
            for (unsigned int day = 0; day < numberOfDays; ++day) {
                writer.generate_for_day(basePath, date, numberOfFilePairs, numberOfEntries);
                date.tm_mday += 1;
                std::mktime(&date);
            }
        } else {
            writer.generate_continuous(basePath);
        }
    }
    catch (const std::invalid_argument& ia) {
        std::cerr << "Invalid argument: " << ia.what() << std::endl;
        return 1;
    }
    catch (const std::exception& e) {
        std::cerr << "Failed to generate data: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}

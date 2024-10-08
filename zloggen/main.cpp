#include <boost/filesystem.hpp>
#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <chrono>
#include <thread>
#include <random>
#include <ctime>


namespace fs = boost::filesystem;

// Forward declarations
std::string tm_to_string(const std::tm& timeStruct, const std::string& format);
std::tm string_to_tm(const std::string& timeString, const std::string& format);
std::tm today();
bool differs_from_today(const std::tm& then);
std::string get_date_path(const std::tm& today);
void proceed_to_next_day(std::tm& date);


// Generate a random delay between min and max milliseconds
void random_delay(int minMs, int maxMs) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(minMs, maxMs);
    std::this_thread::sleep_for(std::chrono::milliseconds(dis(gen)));
}

// Simulate writing entries into header/payload paired files for a given date
void generate_test_data_for_day(const std::string& basePath, const std::tm& date, const unsigned int numFilePairs, const unsigned int numberEntries) {
    std::cout << "Generating test data for " << (1900 + date.tm_year)
              << "-" << (date.tm_mon + 1) << "-" << date.tm_mday << " " << std::flush;

    std::vector<std::string> fruits = {"Apple", "Banana", "Cherry", "Date", "Elderberry", "Fig", "Grape"};
    std::string inputString = "InputInputInputInputInputInputInputInputInputInputInput";
    std::string outputString = "OutputOutputOutputOutputOutputOutputOutputOutputOutputOutputOutputOutputOutputOutput";

    std::string dirPath = basePath + "/" + get_date_path(date);

    // Create the directory structure if it doesn't exist
    if (!fs::exists(dirPath)) {
        fs::create_directories(dirPath);
    }

    // Open header and payload file pairs
    std::vector<std::ofstream> headerFiles(numFilePairs);
    std::vector<std::ofstream> payloadFiles(numFilePairs);
    for (int i = 0; i < numFilePairs; ++i) {
        std::string headerFilename = dirPath + "/file" + std::to_string(i) + ".header";
        std::string payloadFilename = dirPath + "/file" + std::to_string(i) + ".payload";

        headerFiles[i].open(headerFilename, std::ios::out | std::ios::app);
        payloadFiles[i].open(payloadFilename, std::ios::out | std::ios::app);
    }

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> fileSelector(0, static_cast<signed>(numFilePairs) - 1);

    std::uniform_int_distribution<> delayedHeaderWrite(0, 100);


    // Write entries into the files
    std::vector<std::streamoff> currentOffset(numFilePairs, 0);

    for (int entryIndex = 0; entryIndex < numberEntries; ++entryIndex) {
        int fileIndex = fileSelector(gen);  // Randomly select one of the file pairs

        // Generate header entry
        std::string headerLine;
        headerLine += fruits[entryIndex % fruits.size()] + ",";
        headerLine += fruits[(entryIndex + 1) % fruits.size()] + ",";
        headerLine += "Potato,,Carrot,";
        headerLine += fruits[(entryIndex + 2) % fruits.size()] + ",";

        if (delayedHeaderWrite(gen) > 10) {
            // No flush
            headerLine += fruits[(entryIndex + 3) % fruits.size()] + ",";

        } else {
            // Simulate (on the client side) reading half written headers
            // partial write of header entry
            headerFiles[fileIndex] << headerLine << std::flush;
            random_delay(1, 100);

            headerLine = fruits[(entryIndex + 3) % fruits.size()] + ",";
        }

        headerLine += std::to_string(inputString.size()) + ",";
        headerLine += std::to_string(outputString.size()) + ",";
        headerLine += std::to_string(currentOffset[fileIndex]) + "\n";
        headerFiles[fileIndex] << headerLine;

        // Generate payload data
        payloadFiles[fileIndex] << inputString << outputString;

        // Update the current offset for the next entry
        currentOffset[fileIndex] += static_cast<std::streamoff>(inputString.size() + outputString.size());

        // Stochastically flush buffers
        random_delay(10, 50);
        if (entryIndex % 5 == 0) {
            headerFiles[fileIndex].flush();   // Flush header file first
            random_delay(10, 50);  // Delay between flushing header and payload
            payloadFiles[fileIndex].flush();  // Then flush payload file
        } else if (entryIndex % 7 == 0) {
            payloadFiles[fileIndex].flush();  // Flush payload file first
            random_delay(10, 50);  // Delay between flushing payload and header
            headerFiles[fileIndex].flush();   // Then flush header file
        }

        // Random delay between entries to simulate realistic file writing
        random_delay(1, 10);
    }

    // Close all files
    for (int i = 0; i < numFilePairs; ++i) {
        headerFiles[i].close();
        payloadFiles[i].close();
    }

    std::cout << "-- completed" << std::endl;
}

// Function to increment the date by one day
void increment_date(std::tm& dateTm) {
    std::time_t timeSinceEpoch = std::mktime(&dateTm);
    timeSinceEpoch += 24 * 60 * 60;  // Add 24 hours
    dateTm = *std::localtime(&timeSinceEpoch);
}

[[noreturn]] void generate_continuous_test_data(const std::string& basePath) {
    constexpr unsigned int numFilePairs = 10;
    std::tm date = today();

    std::cout << "Generating test data for " << (1900 + date.tm_year)
              << "-" << (date.tm_mon + 1) << "-" << date.tm_mday << " " << std::flush;

    std::vector<std::string> fruits = {"Apple", "Banana", "Cherry", "Date", "Elderberry", "Fig", "Grape"};
    std::string inputString = "InputInputInputInputInputInputInputInputInputInputInput";
    std::string outputString = "OutputOutputOutputOutputOutputOutputOutputOutputOutputOutputOutputOutputOutputOutput";

    std::string dirPath = basePath + "/" + get_date_path(date);

    // Create the directory structure if it doesn't exist
    if (!fs::exists(dirPath)) {
        fs::create_directories(dirPath);
    }

    // Open header and payload file pairs
    std::vector<std::ofstream> headerFiles(numFilePairs);
    std::vector<std::ofstream> payloadFiles(numFilePairs);
    // Open new files
    for (int i = 0; i < numFilePairs; ++i) {
        std::string headerPath = dirPath + "/file" + std::to_string(i) + ".header";
        std::string payloadPath = dirPath + "/file" + std::to_string(i) + ".payload";

        headerFiles[i].open(headerPath, std::ios::out | std::ios::app);
        if (!headerFiles[i].is_open()) {
            std::cerr << "Error opening header file: " << headerPath << std::endl;
        }

        payloadFiles[i].open(payloadPath, std::ios::out | std::ios::app);
        if (!payloadFiles[i].is_open()) {
            std::cerr << "Error opening payload file: " << payloadPath << std::endl;
        }
    }

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> fileSelector(0, numFilePairs - 1);

    std::uniform_int_distribution<> delayedHeaderWrite(0, 100);

    //
    std::vector<std::streamoff> currentPayloadOffset(numFilePairs, 0);

    //
    unsigned long counter = 0L;
    while (true) {
        int fileIndex = fileSelector(gen);  // Randomly select one of the file pairs

        // Generate header entry
        std::string headerLine;
        headerLine += fruits[counter % fruits.size()] + ",";
        headerLine += fruits[(counter + 1) % fruits.size()] + ",";
        headerLine += "Potato,,Carrot,";
        headerLine += fruits[(counter + 2) % fruits.size()] + ",";

        if (delayedHeaderWrite(gen) > 10) {
            // No flush
            headerLine += fruits[(counter + 3) % fruits.size()] + ",";

        } else {
            // Simulate (on the client side) reading half written headers
            // partial write of header entry
            headerFiles[fileIndex] << headerLine << std::flush;
            random_delay(1, 100);

            headerLine = fruits[(counter + 3) % fruits.size()] + ",";
        }

        headerLine += std::to_string(inputString.size()) + ",";
        headerLine += std::to_string(outputString.size()) + ",";
        headerLine += std::to_string(currentPayloadOffset[fileIndex]) + "\n";
        headerFiles[fileIndex] << headerLine;

        // Generate payload data
        payloadFiles[fileIndex] << inputString << outputString;

        // Update the current offset for the next entry
        currentPayloadOffset[fileIndex] += static_cast<std::streamoff>(inputString.size() + outputString.size());

        // Stochastically flush buffers
        random_delay(0, 5);
        if (counter % 5 == 0) {
            headerFiles[fileIndex].flush();   // Flush header file first
            random_delay(1, 50);  // Delay between flushing header and payload
            payloadFiles[fileIndex].flush();  // Then flush payload file
        } else if (counter % 7 == 0) {
            payloadFiles[fileIndex].flush();  // Flush payload file first
            random_delay(1, 50);  // Delay between flushing payload and header
            headerFiles[fileIndex].flush();   // Then flush header file
        }
        ++counter;

        // Random delay between entries to simulate realistic file writing
        random_delay(0, 5);

        // Check if we have passed into a new day
        if (differs_from_today(date)) {
            std::cout << std::flush << std::endl << "Detected day rollover" << std::endl;

            // Close all open files
            for (int i = 0; i < numFilePairs; ++i) {
                if (headerFiles[i].is_open()) {
                    headerFiles[i].close();
                }
                if (payloadFiles[i].is_open()) {
                    payloadFiles[i].close();
                }
            }

            date = today();
            dirPath = basePath + "/" + get_date_path(date);

            if (!fs::exists(dirPath)) {
                fs::create_directories(dirPath);
            }

            std::cout << "Generating test data for " << (1900 + date.tm_year)
                     << "-" << (date.tm_mon + 1) << "-" << date.tm_mday << " " << std::endl << std::flush;

            // Re-open new files and reset payload offset
            for (int i = 0; i < numFilePairs; ++i) {
                currentPayloadOffset[i] = 0;

                std::string headerPath = dirPath + "/file" + std::to_string(i) + ".header";
                std::string payloadPath = dirPath + "/file" + std::to_string(i) + ".payload";

                headerFiles[i].open(headerPath, std::ios::out | std::ios::app);
                if (!headerFiles[i].is_open()) {
                    std::cerr << "Error opening header file: " << headerPath << std::endl;
                }

                payloadFiles[i].open(payloadPath, std::ios::out | std::ios::app);
                if (!payloadFiles[i].is_open()) {
                    std::cerr << "Error opening payload file: " << payloadPath << std::endl;
                }
            }
        } else {
            std::cout << "." << std::flush;
        }
    }
}

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
            }
        }

        if (argc > 4) {
            numberOfEntries = std::stoul(argv[idx++]);
            if (numberOfEntries == 0) {
                std::cerr << "Provide number of entries" << std::endl;
            }
        }

        // Start date (today)
        std::tm date = today();

        // Simulate writing data for the specified number of days
        if (argc == 5) {
            for (int day = 0; day < numberOfDays; ++day) {
                generate_test_data_for_day(basePath, date, numberOfFilePairs, numberOfEntries);
                increment_date(date);  // Move to the next day
            }
        } else {
            generate_continuous_test_data(basePath);
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

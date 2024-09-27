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

// Function to get the date directory path (e.g., /year/month/day)
std::string getDateDirectory(const std::string& baseDir, std::tm* dateTm) {
    // Format the directory as /year/month/day
    std::string dirPath = baseDir + "/" +
                          std::to_string(1900 + dateTm->tm_year) + "/" +
                          std::to_string(dateTm->tm_mon + 1) + "/" +
                          std::to_string(dateTm->tm_mday);
    return dirPath;
}

// Generate a random delay between min and max milliseconds
void randomDelay(int minMs, int maxMs) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(minMs, maxMs);
    std::this_thread::sleep_for(std::chrono::milliseconds(dis(gen)));
}

// Simulate writing entries into header/payload paired files for a given date
void generateTestDataForDay(const std::string& basePath, std::tm* dateTm, unsigned int numFilePairs, unsigned int numberEntries) {
    std::cout << "Starting test data generation for day: " << (1900 + dateTm->tm_year)
              << "/" << (dateTm->tm_mon + 1) << "/" << dateTm->tm_mday << " " << std::flush;

    std::vector<std::string> fruits = {"Apple", "Banana", "Cherry", "Date", "Elderberry", "Fig", "Grape"};
    std::string inputString = "InputInputInputInputInputInputInputInputInputInputInput";
    std::string outputString = "OutputOutputOutputOutputOutputOutputOutputOutputOutputOutputOutputOutputOutputOutput";

    std::string dirPath = getDateDirectory(basePath, dateTm);

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
            randomDelay(1, 100);

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
        randomDelay(10, 50);
        if (entryIndex % 5 == 0) {
            headerFiles[fileIndex].flush();   // Flush header file first
            randomDelay(10, 50);  // Delay between flushing header and payload
            payloadFiles[fileIndex].flush();  // Then flush payload file
        } else if (entryIndex % 7 == 0) {
            payloadFiles[fileIndex].flush();  // Flush payload file first
            randomDelay(10, 50);  // Delay between flushing payload and header
            headerFiles[fileIndex].flush();   // Then flush header file
        }

        // Random delay between entries to simulate realistic file writing
        randomDelay(10, 100);
    }

    // Close all files
    for (int i = 0; i < numFilePairs; ++i) {
        headerFiles[i].close();
        payloadFiles[i].close();
    }

    std::cout << "-- completed" << std::endl;
}

// Function to increment the date by one day
void incrementDate(std::tm* dateTm) {
    std::time_t timeSinceEpoch = std::mktime(dateTm);
    timeSinceEpoch += 24 * 60 * 60;  // Add 24 hours
    *dateTm = *std::localtime(&timeSinceEpoch);
}

int main(int argc, char* argv[]) {
    try {
        if (argc != 5) {
            std::cerr << "Usage: " << argv[0] << " <base-directory> <number_of_days> <number_of_file_pairs> <number_of_entries>" << std::endl;
            return 1;
        }

        unsigned int idx = 1;
        std::string basePath = argv[idx++];

        unsigned int numberOfDays = std::stoul(argv[idx++]);
        if (numberOfDays == 0) {
            std::cerr << "Provide number of days" << std::endl;
            return 1;
        }

        unsigned int numberOfFilePairs = std::stoul(argv[idx++]);
        if (numberOfFilePairs == 0) {
            std::cerr << "Provide number of file pairs" << std::endl;
        }

        unsigned int numberOfEntries = std::stoul(argv[idx++]);
        if (numberOfEntries == 0) {
            std::cerr << "Provide number of entries" << std::endl;
        }

        // Start date (today)
        std::time_t now = std::time(nullptr);
        std::tm dateTm = *std::localtime(&now);

        // Simulate writing data for the specified number of days
        for (int day = 0; day < numberOfDays; ++day) {
            generateTestDataForDay(basePath, &dateTm, numberOfFilePairs, numberOfEntries);
            incrementDate(&dateTm);  // Move to the next day
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

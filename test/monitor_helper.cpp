// Helper executable for DirectoryMonitor integration tests.

#include <fstream>
#include <iostream>
#include <string>

int main(int argc, char* argv[]) {
    if (argc < 4) {
        std::cerr << "Usage: " << argv[0] << " -p <shard> <base-dir> <date> <header> <payload>" << std::endl;
        return 1;
    }

    std::string base_dir = argv[3];
    std::ofstream marker(base_dir + "/helper_ran", std::ios::out | std::ios::trunc);
    marker << "ok" << std::endl;
    marker.close();

    std::cout << "Helper processed" << std::endl;
    return 0;
}

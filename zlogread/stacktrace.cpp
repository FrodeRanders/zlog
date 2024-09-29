//
// Created by Frode Randers on 2024-09-25.
//
#include <iostream>
#include <stdexcept>
#include <execinfo.h>
#include <cstdlib>

void print_stacktrace() {
    const int max_frames = 100;
    void* addrlist[max_frames + 1];

    // Retrieve current stack addresses
    int addrlen = backtrace(addrlist, sizeof(addrlist) / sizeof(void*));

    if (addrlen == 0) {
        std::cerr << "No stack trace available." << std::endl;
        return;
    }

    // Print the stack trace
    char** symbollist = backtrace_symbols(addrlist, addrlen);

    for (int i = 0; i < addrlen; i++) {
        std::cerr << symbollist[i] << std::endl;
    }

    free(symbollist);
}

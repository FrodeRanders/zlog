//
// Created by Frode Randers on 2024-09-25.
//
#include <iostream>
#include <fstream>

#include <boost/log/trivial.hpp>

#include "zlog.h"

namespace logging = boost::log;


void write_to_object_store(const std::string& reason) {
        BOOST_LOG_TRIVIAL(debug) << "Wrap up and save to ObjectStore: " << reason << std::endl;
}

void process_header_and_payload(
    const std::vector<std::string>& headerData,
    const std::streamsize inputSize,
    const std::streamsize outputSize,
    std::ifstream& payloadStream,
    unsigned long& size, unsigned long& count
) {
    //--------------------------------------------------------------------------
    // Here you have the individual header fields (in 'headerData'),
    // payload data: input (in 'input') and output (in 'output').
    //--------------------------------------------------------------------------

    // For debugging purposes, we read the data and makes some checks based on
    // knowledge of what zloggen (z-log generator, i.e. a test application) is writing...
    std::vector<char> inputBuffer(inputSize);
    std::vector<char> outputBuffer(outputSize);

    payloadStream.read(inputBuffer.data(), inputSize);
    payloadStream.read(outputBuffer.data(), outputSize);

    std::string input = std::string(inputBuffer.begin(), inputBuffer.end());
    std::string output = std::string(outputBuffer.begin(), outputBuffer.end());

    if (!input.starts_with("Input") && input.ends_with("Input")) {
        BOOST_LOG_TRIVIAL(error) << "Corrupt input: " << input << std::endl;
        throw std::underflow_error("Corrupt input: " + input);
    }

    if (!output.starts_with("Output") && output.ends_with("Output")) {
        BOOST_LOG_TRIVIAL(error) << "Corrupt output: " << output << std::endl;
        throw std::underflow_error("Corrupt output: " + output);
    }

    size += inputSize + outputSize;
    ++count;

    if (size > NOMINAL_BATCH_SIZE || count > NOMINAL_BATCH_COUNT) { // Arbitrary values, really
        write_to_object_store("Reached limit: size=" + std::to_string(size) + " count=" + std::to_string(count));

        // Reset accumulators
        size = 0L;
        count = 0L;
    }
}


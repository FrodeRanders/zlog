//
// Created by Frode Randers on 2024-09-25.
//
#include <boost/log/trivial.hpp>

#include "processoraction.h"

namespace logging = boost::log;

void ObjectStoreAction::flush(const std::string& reason) {
    BOOST_LOG_TRIVIAL(debug) << "Wrap up and save to ObjectStore: " << reason << std::endl;
}

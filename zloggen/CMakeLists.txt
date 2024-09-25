cmake_minimum_required(VERSION 3.29)
project(zloggen VERSION 1.0)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)

set(BOOST_ROOT /opt/local)
set(BOOST_INCLUDEDIR /opt/local/include)
set(BOOST_LIBRARYDIR /opt/local/lib)

find_package(Boost 1.76 REQUIRED COMPONENTS filesystem system)

if(Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
    message(STATUS "Boost include dirs: ${Boost_INCLUDE_DIRS}")
    message(STATUS "Boost libraries: ${Boost_LIBRARY_DIRS}")
else()
    message(FATAL_ERROR "Could not find Boost!")
endif()

add_executable(zloggen main.cpp)

target_link_libraries(zloggen ${Boost_LIBRARIES})

set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

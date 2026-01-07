cmake_minimum_required(VERSION 3.29)
project(zloggen VERSION 1.0)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)

set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

set(Boost_ROOT /usr/local/boost-1.90.0)
set(BOOST_ROOT /usr/local/boost-1.90.0)
set(BOOST_INCLUDEDIR /usr/local/boost-1.90.0/include)
set(BOOST_LIBRARYDIR /usr/local/boost-1.90.0/lib)
set(Boost_NO_SYSTEM_PATHS ON)

set(TARGET_NAME zloggen)

add_executable(${TARGET_NAME}
        main.cpp
        logwriter.cpp
        ../common/date_utils.cpp
)

find_package(Boost 1.90 CONFIG REQUIRED COMPONENTS
        filesystem
)

if(Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
    target_include_directories(${TARGET_NAME} PRIVATE ${CMAKE_CURRENT_LIST_DIR}/../common)
    message(STATUS "Boost include dirs: ${Boost_INCLUDE_DIRS}")
    message(STATUS "Boost libraries: ${Boost_LIBRARY_DIRS}")
    target_link_libraries(${TARGET_NAME} ${Boost_LIBRARIES})
else()
    message(FATAL_ERROR "Could not find Boost!")
endif()

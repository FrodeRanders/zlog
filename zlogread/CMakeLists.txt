cmake_minimum_required(VERSION 3.29)
project(zlogread VERSION 1.0)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
set(BOOST_ROOT /opt/local)
set(BOOST_INCLUDEDIR /opt/local/include)
set(BOOST_LIBRARYDIR /opt/local/lib)
set(TARGET_NAME zlogread)

add_executable(${TARGET_NAME}
        main.cpp
        processor.cpp
)

target_compile_definitions(${TARGET_NAME}
        PRIVATE
        BOOST_ASIO_HAS_STD_INVOKE_RESULT=1
)

find_package(Boost 1.76 REQUIRED COMPONENTS
        log
        log_setup
        filesystem
        system
        thread
        chrono
        date_time
        atomic
        regex)

if(Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
    message(STATUS "Boost include dirs: ${Boost_INCLUDE_DIRS}")
    message(STATUS "Boost libraries: ${Boost_LIBRARY_DIRS}")
    target_link_libraries(${TARGET_NAME} ${Boost_LIBRARIES})
else()
    message(FATAL_ERROR "Could not find Boost!")
endif()




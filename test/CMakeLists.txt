cmake_minimum_required(VERSION 3.29)
project(zlog_tests)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)

enable_testing()

add_executable(date_utils_test
    date_utils_test.cpp
    ../common/date_utils.cpp
)

target_include_directories(date_utils_test PRIVATE ${CMAKE_CURRENT_LIST_DIR}/../common)
add_test(NAME date_utils_test COMMAND date_utils_test)

set(Boost_ROOT /usr/local/boost-1.90.0)
set(BOOST_ROOT /usr/local/boost-1.90.0)
set(BOOST_INCLUDEDIR /usr/local/boost-1.90.0/include)
set(BOOST_LIBRARYDIR /usr/local/boost-1.90.0/lib)
set(Boost_NO_SYSTEM_PATHS ON)

find_package(Boost 1.90 CONFIG REQUIRED COMPONENTS
    log
    log_setup
    filesystem
    process
    thread
    chrono
    date_time
    atomic
    regex
)

add_executable(processor_test
    processor_test.cpp
    ../zlogread/processor.cpp
    ../common/date_utils.cpp
)

target_include_directories(processor_test PRIVATE
    ${CMAKE_CURRENT_LIST_DIR}/../common
    ${CMAKE_CURRENT_LIST_DIR}/../zlogread
)

target_link_libraries(processor_test ${Boost_LIBRARIES})
add_test(NAME processor_test COMMAND processor_test)

add_executable(processor_integration_test
    processor_integration_test.cpp
    ../zlogread/processor.cpp
    ../common/date_utils.cpp
    ../zlogread/processoraction.cpp
)

target_include_directories(processor_integration_test PRIVATE
    ${CMAKE_CURRENT_LIST_DIR}/../common
    ${CMAKE_CURRENT_LIST_DIR}/../zlogread
)

target_link_libraries(processor_integration_test ${Boost_LIBRARIES})
add_test(NAME processor_integration_test COMMAND processor_integration_test)

add_executable(monitor_helper
    monitor_helper.cpp
)

add_executable(monitor_integration_test
    monitor_integration_test.cpp
    ../zlogread/directorymonitor.cpp
    ../common/date_utils.cpp
)

target_include_directories(monitor_integration_test PRIVATE
    ${CMAKE_CURRENT_LIST_DIR}/../common
    ${CMAKE_CURRENT_LIST_DIR}/../zlogread
)

target_compile_definitions(monitor_integration_test PRIVATE
    MONITOR_HELPER_PATH=\"$<TARGET_FILE:monitor_helper>\"
)

target_link_libraries(monitor_integration_test ${Boost_LIBRARIES})
add_dependencies(monitor_integration_test monitor_helper)
add_test(NAME monitor_integration_test COMMAND monitor_integration_test)

add_custom_target(run_tests
    COMMAND ${CMAKE_CTEST_COMMAND} --test-dir ${CMAKE_CURRENT_BINARY_DIR}
    DEPENDS date_utils_test processor_test processor_integration_test monitor_integration_test monitor_helper
)

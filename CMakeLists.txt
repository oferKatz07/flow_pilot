cmake_minimum_required(VERSION 3.16)
project(flow_pilot VERSION 0.1 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Debug symbols for debugging - disable all optimizations
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -fno-omit-frame-pointer")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O0 -fno-omit-frame-pointer")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -g")

find_package(Boost REQUIRED COMPONENTS system)
if(NOT Boost_FOUND)
    message(FATAL_ERROR "Boost libraries not found. Install Boost and retry.")
endif()

find_package(nlohmann_json 3.2.0 REQUIRED)
find_package(spdlog REQUIRED)
find_package(fmt REQUIRED)

# Find Google Test
find_package(GTest REQUIRED)

# Find sqlite3 (library)
find_library(SQLITE3_LIB sqlite3)
if(SQLITE3_LIB)
    set(SQLITE3_LIBS ${SQLITE3_LIB})
else()
    message(FATAL_ERROR "sqlite3 library not found")
endif()

find_library(NLOHMANN_JSON_SCHEMA_VALIDATOR_LIB
    NAMES nlohmann_json_schema_validator
    PATHS /usr/local/lib /usr/lib
)
if(NOT NLOHMANN_JSON_SCHEMA_VALIDATOR_LIB)
    message(FATAL_ERROR "nlohmann json schema validator library not found")
endif()

set(FLOW_PILOT_SOURCES
    src/logger.cpp
    src/redis_db_async.cpp
    src/sqlite_db.cpp
    src/db_factory.cpp
    src/async_db.cpp
    src/client_sqlite_config_manager.cpp
    src/client_config_manager_factory.cpp
    src/request_handlers.cpp
    src/http_server.cpp
    src/workflow_service.cpp
)

# Build a library from all project sources (exclude main.cpp)
add_library(flow_pilot_lib STATIC ${FLOW_PILOT_SOURCES})
target_include_directories(flow_pilot_lib PUBLIC include)
target_compile_features(flow_pilot_lib PRIVATE cxx_std_20)
target_link_libraries(flow_pilot_lib PRIVATE
    Boost::system
    nlohmann_json::nlohmann_json
    spdlog::spdlog
    fmt::fmt
    ${NLOHMANN_JSON_SCHEMA_VALIDATOR_LIB}
)

# Provide schema path to sources that use WORKFLOW_SCHEMA_PATH
target_compile_definitions(flow_pilot_lib PRIVATE WORKFLOW_SCHEMA_PATH="${CMAKE_SOURCE_DIR}/schema/workflow_request.schema.json")

# Link sqlite3 into the library if found
if(DEFINED SQLITE3_LIBS)
    target_link_libraries(flow_pilot_lib PRIVATE ${SQLITE3_LIBS})
endif()

# Main executable (keeps only main.cpp)
add_executable(flow_pilot src/main.cpp)
target_link_libraries(flow_pilot PRIVATE flow_pilot_lib)
target_compile_definitions(flow_pilot PRIVATE WORKFLOW_SCHEMA_PATH="${CMAKE_SOURCE_DIR}/schema/workflow_request.schema.json")

# Test executable
add_executable(flow_pilot_tests
    tests/workflow_validator_test.cpp
    tests/async_sqlite_db_test.cpp
)

target_include_directories(flow_pilot_tests PRIVATE include)
target_compile_features(flow_pilot_tests PRIVATE cxx_std_20)
target_link_libraries(flow_pilot_tests PRIVATE
    GTest::gtest_main
    flow_pilot_lib
)

target_compile_definitions(flow_pilot_tests PRIVATE WORKFLOW_SCHEMA_PATH="${CMAKE_SOURCE_DIR}/schema/workflow_request.schema.json")

# Enable testing
enable_testing()
add_test(NAME workflow_validator_tests COMMAND flow_pilot_tests)

# Add a custom target to run tests
add_custom_target(run_tests
    COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure
    DEPENDS flow_pilot_tests
    COMMENT "Running all tests"
)

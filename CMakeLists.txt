cmake_minimum_required(VERSION 3.20)
project(mini-kafka LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)  # C++ version 20
set(CMAKE_CXX_STANDARD_REQUIRED ON)  # C++ version 20 is required
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)  # Export compile compile_commands.json for tools like clangd

# ─── Compiler warnings ───────────────────────────────────────────────
include(FetchContent)

# Tắt các config test của crc32c để tránh lỗi build thư viện con
set(CRC32C_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(CRC32C_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
set(CRC32C_USE_GLOG OFF CACHE BOOL "" FORCE)

FetchContent_Declare(
  crc32c
  URL https://github.com/google/crc32c/archive/refs/tags/1.1.2.zip
)
# Bắt buộc phải có hàm này để CMake tải và cài đặt crc32c
FetchContent_MakeAvailable(crc32c)

# ─── Compiler warnings ───────────────────────────────────────────────
add_compile_options(-Wall -Wextra -Wpedantic)  # Enable all warnings

# ─── Common library ────────────────────────────────────────────────────
add_library(kafka_common STATIC
    src/common/serialize.cpp
    src/common/record.cpp
    src/common/batch.cpp
    src/common/message.cpp
)
target_include_directories(kafka_common PUBLIC src)
# Phải link thư viện crc32c vào kafka_common thì batch.cpp mới thấy được header
target_link_libraries(kafka_common PUBLIC crc32c)

# ─── Client library (shared between producer & consumer) ─────────────
add_library(kafka_client STATIC
    src/client/client.cpp
    src/client/connection.cpp
)
target_include_directories(kafka_client PUBLIC src)
target_link_libraries(kafka_client PUBLIC kafka_common)

# ─── Broker executable ───────────────────────────────────────────────
add_executable(broker
    src/broker/main.cpp
    src/broker/server.cpp
    src/broker/log_manager.cpp
    src/broker/connection_handler.cpp
)   
target_include_directories(broker PRIVATE src)
# pthread is not standard library (iostream, vector,...) -> link it manually
target_link_libraries(broker PRIVATE pthread kafka_common)

# ─── Producer CLI ─────────────────────────────────────────────────────
add_executable(producer
    src/producer/main.cpp
)
target_link_libraries(producer PRIVATE kafka_client)

# ─── Consumer CLI ─────────────────────────────────────────────────────
add_executable(consumer
    src/consumer/main.cpp
)
target_link_libraries(consumer PRIVATE kafka_client)

# ─── Unit Tests ───────────────────────────────────────────────────────
include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
)
# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

enable_testing()

add_executable(unit_tests
    tests/test_serialize.cpp
    tests/test_record.cpp
    tests/test_batch.cpp
    tests/test_message.cpp
)
target_include_directories(unit_tests PRIVATE src)
target_link_libraries(unit_tests PRIVATE GTest::gtest_main kafka_common)

include(GoogleTest)
gtest_discover_tests(unit_tests)

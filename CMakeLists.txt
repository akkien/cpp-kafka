cmake_minimum_required(VERSION 3.20)
project(mini-kafka LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)  # C++ version 20
set(CMAKE_CXX_STANDARD_REQUIRED ON)  # C++ version 20 is required
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)  # Export compile compile_commands.json for tools like clangd

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
    src/broker/protocol_parser.cpp
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

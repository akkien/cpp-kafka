# Mini Kafka — C++ Implementation

A single-node message broker inspired by Apache Kafka.

## Build

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build
```

## Run

```bash
# Start the broker (default port 9092)
./build/broker

# Produce a message
./build/producer orders "hello world"

# Consume from offset 0
./build/consumer orders 0
```

## Test with netcat

```bash
echo -e "PRODUCE test 5\nhello" | nc localhost 9092
echo -e "CONSUME test 0 1024"   | nc localhost 9092
```

## Project Structure

```
src/
  common/             Shared types & constants
  broker/             TCP server + log storage
  client/             Reusable client library
  producer/           Producer CLI tool
  consumer/           Consumer CLI tool
```

# Mini Kafka — C++ Implementation

A single-node message broker inspired by Apache Kafka.

- Kafka design: https://kafka.apache.org/42/design/protocol/

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

## Protocol

This broker uses a custom **text-based protocol** (not compatible with Apache Kafka's binary protocol).
It is designed to be simple, debuggable, and testable with `netcat`.

### Producer → Broker Flow

```
Producer                          Broker
   │                                │
   │── TCP connect ────────────────▶│
   │                                │
   │── PRODUCE orders 11\n ────────▶│
   │── hello world ────────────────▶│  ← payload (11 bytes)
   │                                │── append to orders.log
   │                                │── offset = byte position
   │◀── OK 0\n ────────────────────│  ← returns offset
   │                                │
   │── PRODUCE orders 3\n ─────────▶│
   │── foo ────────────────────────▶│
   │◀── OK 23\n ───────────────────│  ← next offset (0 + 8B + 4B + 11B = 23)
   │                                │
   │── close connection ───────────▶│
```

**Request format:**
```
PRODUCE <topic> <payload_length>\n
<payload_bytes>
```

**Response format:**
```
OK <offset>\n
```

### Consumer → Broker Flow

```
Consumer                          Broker
   │                                │
   │── TCP connect ────────────────▶│
   │                                │
   │── CONSUME orders 0 1024\n ────▶│
   │                                │── lseek(fd, 0) — jump to offset 0
   │                                │── read messages up to 1024 bytes
   │◀── MESSAGE 0 11\n ────────────│
   │◀── hello world ───────────────│
   │◀── MESSAGE 23 3\n ────────────│
   │◀── foo ───────────────────────│
   │◀── END\n ─────────────────────│
   │                                │
   │── close connection ───────────▶│
```

**Request format:**
```
CONSUME <topic> <offset> <max_bytes>\n
```

**Response format:**
```
MESSAGE <offset> <payload_length>\n
<payload_bytes>
MESSAGE <offset> <payload_length>\n
<payload_bytes>
END\n
```

### Keep-alive

One TCP connection supports multiple requests. The client does not need
to reconnect for each message:

```cpp
kafka::Client client("127.0.0.1", 9092);
client.connect();                           // 1 connection
client.produce("orders", "message 1");      // reuse
client.produce("orders", "message 2");      // reuse
auto msgs = client.consume("orders", 0);    // reuse
// handle messages from broker
for (const auto& msg : msgs) {
    std::cout << "[offset=" << msg.offset
              << " size=" << msg.size << "] "
              << msg.payload << "\n";
}

// connection closed when client is destroyed
```

### Continuous Polling (like real Kafka consumer)

```cpp
kafka::Client client("127.0.0.1", 9092);
client.connect();

uint64_t offset = 0;
while (true) {
    auto msgs = client.consume("orders", offset, 4096);
    for (const auto& msg : msgs) {
        // process each message
        std::cout << msg.payload << "\n";

        // advance offset past this message
        // 8 bytes (offset) + 4 bytes (size) + payload size
        offset = msg.offset + 12 + msg.size;
    }
    sleep(1);  // poll every second
}
```

### On-disk Format

Each topic is stored as a single append-only `.log` file under `data/`:

```
data/
  orders.log
  payments.log
```

Binary message format inside the file:

```
| offset (8 bytes) | size (4 bytes) | payload (N bytes) |
```

Offset is the **byte position** in the file, enabling O(1) random access via `lseek()`.

Example

File `data/fruit.log` (35 bytes, little-endian trên macOS):

```
Byte   Hex                                ASCII
─────────────────────────────────────────────────
       ┌── Message 1: "durian" ──────────┐
 0-7   00 00 00 00 00 00 00 00           offset = 0
 8-11  06 00 00 00                       size = 6
12-17  64 75 72 69 61 6E                 "durian"
       └─────────────────────────────────┘

       ┌── Message 2: "mango" ───────────┐
18-25  12 00 00 00 00 00 00 00           offset = 18 (0x12)
26-29  05 00 00 00                       size = 5
30-34  6D 61 6E 67 6F                   "mango"
       └─────────────────────────────────┘
```

Tổng: `18 + 8 + 4 + 5 = 35 bytes`. Message tiếp theo sẽ ở offset 35.

## Project Structure

```
src/
  common/             Shared types & constants
  broker/             TCP server + log storage
  client/             Reusable client library
  producer/           Producer CLI tool
  consumer/           Consumer CLI tool
```

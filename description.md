# Mini Kafka C++ Implementation

A production-minded, single-node message broker implementation in C++ inspired by Apache Kafka.

---

## 0. Scope (Realistic Constraints)

* **Single-node only** (no cluster)
* Multiple topics support
* Append-only log storage
* TCP-based protocol
* At-least-once delivery semantics
* No replication or clustering

---

## 1. High-level Architecture

```
Producer → Broker (TCP server) → Log (disk)
                                    ↓
                                Consumer
```

- **Broker** = C++ TCP server
- **Storage** = append-only binary log files
- **Consumers** read by offset

---

## 1.5 Program Structure

```
src/
  broker/          # Main server implementation
    - server.cpp
    - log_manager.cpp
    - connection_handler.cpp
    - protocol_parser.cpp

  client/          # Shared client library (IMPORTANT)
    - client.h
    - client.cpp
    - connection.h
    - connection.cpp

  producer/        # Producer CLI tool
    - main.cpp
    - uses: client library

  consumer/        # Consumer CLI tool
    - main.cpp
    - uses: client library
```

**Key Points:**
- **client/** is a reusable library for any client implementation
- **producer/** and **consumer/** are CLI tools that use the client library
- **broker/** is the server-side TCP handler and storage manager

---

## 2. Protocol (Text-based, Simple & Debuggable)

### 2.1 Produce

**Request:**
```
PRODUCE <topic> <len>\n
<payload>
```

**Example:**
```
PRODUCE orders 11
hello world
```

**Response:**
```
OK <offset>\n
```

---

### 2.2 Consume

**Request:**
```
CONSUME <topic> <offset> <max_bytes>\n
```

**Example:**
```
CONSUME orders 0 1024
```

**Response:**
```
MESSAGE <offset> <len>\n
<payload>
MESSAGE <offset> <len>\n
<payload>
END
```

---

### 2.3 Metadata Query (Optional)

**Request:**
```
LIST_TOPICS
```

---

## 3. Data Model

### Topic
A logical stream of messages (e.g., `orders`, `logs`, `events`)

### Message Structure
```cpp
struct Message {
    uint64_t offset;      // Byte offset in log file
    uint32_t size;        // Payload size in bytes
    std::string payload;  // Message data
};
```

---

## 4. File Format (CRITICAL)

### 4.1 Storage Layout

```
data/
  orders.log
  payments.log
  events.log
```

Each topic gets its own `.log` file.

### 4.2 Binary Message Format

```
| offset (8B) | size (4B) | payload (N bytes) |
```

**Example:**
```
[00000001][0000000B][hello world]
```

### 4.3 Append Logic

```cpp
write(fd, &offset, 8);     // Write offset as 8-byte integer
write(fd, &size, 4);       // Write size as 4-byte integer
write(fd, payload.data(), size);  // Write payload
```

---

## 5. Offset Design (VERY IMPORTANT)

### ❌ Option 1: Message Index (0, 1, 2, 3...)
- Requires scanning entire file
- O(n) access time

### ✅ Option 2: Byte Offset (RECOMMENDED)
- Use `lseek()` to jump to byte position
- O(1) random access
- More practical for real-world use

**Recommendation:** Use byte offset with `lseek()`

---

## 6. Producer Flow

```
Client sends PRODUCE request
           ↓
Broker appends to log file
           ↓
Broker returns offset
           ↓
Client receives confirmation
```

---

## 7. Consumer Flow

```
Client sends CONSUME(topic, offset, max_bytes)
           ↓
Broker seeks to offset in log file
           ↓
Broker reads messages up to max_bytes
           ↓
Broker returns batch of messages
```

---

## 8. Connection Model

* One TCP connection per client
* Multiple requests per connection (keep-alive style)
* No connection pooling (simple single-threaded per connection)

---

## 9. Concurrency Model

### Phase 1 (Simple)
- Thread pool for handling clients
- Mutex per topic/file for write safety

### Phase 2 (Better Performance)
- One dedicated worker thread per topic
- Internal queue per topic for requests
- Reduces lock contention

---

## 10. In-memory State

```cpp
struct TopicState {
    int fd;              // File descriptor
    uint64_t next_offset; // Next available byte offset
};

std::unordered_map<std::string, TopicState> topics;
```

---

## 11. Batching (Performance Optimization)

* Producer can send multiple messages in one request
* Broker writes in batches to reduce syscall overhead
* Significantly improves throughput

---

## 12. Error Handling Strategy

- **Unknown topic** → Auto-create with empty log file
- **Invalid offset** → Return `END` marker
- **Malformed request** → Close connection and log error
- **File I/O errors** → Return error code to client

---

## 13. Backpressure Management

* Limit internal queue size
* Reject or throttle fast producers
* Prevents memory exhaustion

---

## 14. Testing (CLI Tools)

### Test Producer
```bash
echo -e "PRODUCE test 5\nhello" | nc localhost 9092
```

### Test Consumer
```bash
echo -e "CONSUME test 0 1024" | nc localhost 9092
```

---

## 15. Implementation Milestones

### Phase 1: TCP Server + Producer
- Implement TCP server that accepts connections
- Parse PRODUCE requests
- Append to in-memory buffer

### Phase 2: Persistence
- Implement file I/O for log storage
- Write messages to disk
- Track offsets correctly

### Phase 3: Consumer
- Implement CONSUME request parsing
- Seek to correct offset
- Read and return messages in batches

### Phase 4: Multi-client Concurrency
- Add thread pool for concurrent clients
- Implement thread-safe log access
- Handle multiple producers/consumers

### Phase 5: Optimization
- Implement batching
- Consider mmap for large files
- Performance profiling and tuning

---

## 16. Learning Objectives

This project teaches concepts beyond simple HTTP servers:

| Concept | HTTP Server | Mini Kafka |
|---------|------------|-----------|
| State | Stateless | Stateful |
| I/O Pattern | Request/Response | Streaming |
| Persistence | Usually external | Built-in disk I/O |
| Complexity | Simple | System-level |


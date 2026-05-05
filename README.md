# Mini Kafka — C++ Implementation

A high-performance, single-node message broker built in C++ that is fundamentally inspired by the architecture and binary protocol of Apache Kafka.

- **Kafka Design**: [Apache Kafka Protocol Guide](https://kafka.apache.org/42/design/protocol/)
- **Kafka Storage**: [Internal Storage and Log Retention](https://www.conduktor.io/blog/understanding-kafka-s-internal-storage-and-log-retention)

## 🚀 Key Features & Architecture

This project is not just a toy broker; it implements core distributed system techniques to achieve extreme performance and concurrency:

1. **Kafka Binary Protocol Compatibility**: 
   Speaks the official binary Kafka protocol (V3/V4) for `PRODUCE`, `FETCH`, `API_VERSIONS`, and `METADATA`. It is fully interoperable with standard, production-ready clients like `kafkajs`.
2. **Reactive Non-blocking Network Layer**: 
   Utilizes an Event-Driven architecture powered by `kqueue` (macOS). The main thread strictly monitors network events (socket readiness) and buffers incoming data asynchronously. It never blocks on slow clients or disk I/O, allowing it to handle tens of thousands of concurrent connections.
3. **Decoupled I/O Thread Pool**: 
   Parsed requests are pushed into a thread-safe `ConcurrentQueue`. A pool of fixed worker threads pops these tasks, processes the business logic, and interacts with the disk. This 3-layer architecture prevents disk bottlenecks from stalling the network layer.
4. **Zero-Copy Data Transfer**: 
   Uses the `sendfile` system call to stream partition data directly from the kernel's page cache to the network socket. This completely bypasses user-space memory, vastly reducing CPU overhead during high-volume `FETCH` requests.
5. **RecordBatch & Sparse Indexing**: 
   Data is serialized into standard Kafka `RecordBatch` formats. To support fast reads, the broker maintains both a `.log` file and a `.index` file. The sparse index maps logical message offsets to physical byte positions, enabling `O(1)` random access lookups via binary search.

### Concurrency Model

kqueue/epoll is single-threaded, listening to multiple connection requests. If any client send data, it will trigger an event, then main thread will read data from socket, parse it, and push it to a thread-safe queue. Then I/O thread pool will pop tasks from the queue and process them. Finally, I/O threads will push responses to another thread-safe queue, and main thread will pop responses from the queue and send them to clients.

## 📦 Build

Require `cmake` and a C++17 compatible compiler.

```bash
# Generate build files
cmake -B build -DCMAKE_BUILD_TYPE=Debug

# Compile the project
cmake --build build
```

## 🏃 Run

Start the broker (listens on `127.0.0.1:9092` by default). Data will be stored in the `./data` directory.

```bash
./build/broker 9092
```

In separate terminal windows, you can run the provided C++ CLI clients to test the system:

```bash
# Produce messages to topic 'animal' (creates topic automatically)
# Keys and values are separated by ':', multiple messages can be sent.
./build/producer "animal" "lion:tiger:bear"

# Consume from topic 'animal' starting at logical offset 0, reading up to 4096 bytes
./build/consumer "animal" 0 4096
```

## 🧪 Testing with Standard Kafka Clients (Node.js)

Because the broker speaks the official binary protocol, you can use real-world Kafka clients. We have provided a test script using `kafkajs`.

1. Install Node dependencies:
```bash
cd rw-client
npm install kafkajs
```

2. Run the producer & consumer:
```bash
node producer.js
node consumer.js
```

## 📂 Project Structure

```
├── CMakeLists.txt
├── rw-client/                # Node.js standard Kafka client testing scripts
├── src/
│   ├── broker/               # Core Broker Implementation
│   │   ├── server.cpp        # kqueue Event Loop & Non-blocking Network I/O
│   │   ├── thread_pool.cpp   # Worker pool for I/O operations
│   │   ├── request_handler.cpp # Logic processing & Protocol validation
│   │   ├── log_manager.cpp   # Disk I/O, Zero-copy sendfile, and Indexing
│   │   └── topic_state.cpp   # Concurrency management per partition
│   ├── client/               # C++ Client library implementation
│   ├── common/               # Shared Data Structures (RecordBatch, Serialization)
│   ├── consumer/             # C++ Consumer CLI entrypoint
│   └── producer/             # C++ Producer CLI entrypoint
└── tests/                    # Unit tests
```

## 💡 Technical Learnings & Details

- **Endianness**: The Kafka Protocol enforces **Big-Endian** (Network Byte Order). C++ standard library structures and OS architectures (x86/ARM) are generally Little-Endian, requiring strict translation (`htonl`, `ntohl`) at the boundaries.
- **Fragmentation**: TCP is a stream protocol. The `kqueue` network layer manually buffers partial packets (handling network fragmentation) until a full Kafka Message Frame (4 bytes size + payload) is received before dispatching.
- **Locking**: Fine-grained locking is implemented at the `TopicState` level (using `std::shared_mutex` for Readers-Writer optimization) rather than global locks, ensuring Producers to one topic do not block Consumers or Producers on another topic.

# Things I learned from this project

## C++
- function using template must be in .h, not .cpp or it will cause linker error
- std::move helps avoid copying
- declarative way to cast string to int: uint64_t batch_offset = *reinterpret_cast<uint64_t*>(batch_offset_buf.data());

## Kafka 
- use binary protocol to speed up
- use varint to save space
- use sendfile to transfer file, no need to copy to user space
- offset in Kafka is logical order of a message, not byte offset. Hence, besides .log file, kafka also use index file (.index) to speed up random access. .index file store pairs of (offset, position in .log)
- use fine-grained locking, lock on each topic; use R/W lock to optimize lock time

## OS
- recv: use different flag
  - MSG_WAITALL: wait until all data is received


## Encode varint 

300 to binary
```
300 = 0b 1_0010110 0  (binary, 9 bits)

Step 1: value=300 >= 128 → take the lowest 7 bits
  300 & 0x7F = 0010 1100 = 0x2C
  set MSB=1  → 1010 1100 = 0xAC  ← byte 1
  value >>= 7 → value = 2

Step 2: value=2 < 128 → final byte
  0000 0010 = 0x02  ← byte 2, MSB=0 (terminate)

Result: [AC 02]  (2 bytes instead of 4 bytes for int32)
```

## Decode varint

binary to 300
```
Byte 1: AC = 1010 1100
  MSB=1 → continue
  7 bits: 010 1100
  value = 010 1100 (shift 0)

Byte 2: 02 = 0000 0010
  MSB=0 → terminate
  7 bits: 000 0010
  value = 000 0010 | 010 1100 (shift 7)
        = 1 0010 1100
        = 300 ✅
```

### Is it necessary to create separate decode_varint32/64?

`static_cast` between integer types **costs 0 cycles**. It does not generate any instructions on the CPU — it is just a directive for the compiler to "treat this value as another type".

```cpp
// Both of these lines generate the EXACT SAME assembly:
int32_t a = val;                        // implicit conversion
int32_t b = static_cast<int32_t>(val);  // explicit cast
```

Compiler output (x86):

```asm
mov eax, edx    ; only 1 instruction, copy lower 32-bits
```

Therefore, **there is no need to create separate decode_varint32/64** — writing more functions just adds code without making it faster. Keeping 1 `decode_varint` + `static_cast` is the right approach.

**Things that actually affect performance** in this project: syscalls (`recv`, `send`, `read`, `write`), disk I/O, memory allocation — each takes thousands of CPU cycles. `static_cast` takes 0 cycles.


## Message format

April 18, 2026
- kafka use binary protocol, use varint to save space

April 20, 2026
- add size prior to message transfered between producer and broker, and broker and consumer
- must use std::move in many situations to avoid copying
  - c++: move(),
  - other language:reference, smart pointer, garbage collection, ownership, borrowing, thread safe
  
- low level function in C++ is almost identical to operating system call => need to manually check for different OSArchitecture to ensure cross platform compatibility. 

### Fields of record use varint but header use fixed size?
Historically, everything is Fixed-size Big Endian. 
After that, they use varint to save space. However, only fields of record use varint. Other fields remain fixed-size so that old version of application can still parse the message.

## Unique pointer

- Log manager holds one log: log whole send/append for all topics. 
  - Log manager is singleton to be thread safe (must remove copy constructor, assignment operator)
  - use std::map: map<string, TopicState> topic_states, each key is topic name.

=> Want to do fine grained locking, lock on each topic.
- Create TopicState class
  - Cannot be Singleton. If so, we cannot create multiple TopicState objects, one for each topic.
  => But we want to prevent multiple objects for one topic.
    - still remove copy constructor, assignment operator
    - use unique_ptr:  - std::unordered_map<std::string, std::unique_ptr<TopicState>> topics_;

  - Now we want to lock only topics_, which is shared between threads?

```cpp
    TopicState& state = get_or_create(topic)
    {
        std::lock_guard lock(mu_);
        state = get_or_create(topic);
    }
```
  
## Coordinator 

The Group Coordinator is not a separate master node — it is a normal broker designated to manage a specific consumer group.

How Kafka chooses the Group Coordinator:
hash(group_id) % number_of_partitions_of___consumer_offsets_topic
→ partition P
→ which broker is the leader of partition P → that is the Group Coordinator

Kafka has a special internal topic called __consumer_offsets (default 50 partitions). The Group Coordinator is the broker currently holding the leader role for the partition corresponding to the group_id's hash.
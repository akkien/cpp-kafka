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




## Encode varint 

300 to binary
```
300 = 0b 1_0010110 0  (binary, 9 bits)

Bước 1: value=300 >= 128 → lấy 7 bit thấp
  300 & 0x7F = 0010 1100 = 0x2C
  set MSB=1  → 1010 1100 = 0xAC  ← byte 1
  value >>= 7 → value = 2

Bước 2: value=2 < 128 → byte cuối
  0000 0010 = 0x02  ← byte 2, MSB=0 (kết thúc)

Kết quả: [AC 02]  (2 bytes thay vì 4 bytes cho int32)
```

## Decode varint

binary to 300
```
Byte 1: AC = 1010 1100
  MSB=1 → còn tiếp
  7 bit: 010 1100
  value = 010 1100 (shift 0)

Byte 2: 02 = 0000 0010
  MSB=0 → kết thúc
  7 bit: 000 0010
  value = 000 0010 | 010 1100 (shift 7)
        = 1 0010 1100
        = 300 ✅
```

### Có cần tạo riêng decode_varint32/64 không ?

`static_cast` giữa integer types **chi phí = 0**. Nó không tạo ra instruction nào trên CPU — chỉ là lệnh cho compiler
biết "hãy coi giá trị này là kiểu khác".

```cpp
// Cả 2 dòng này tạo ra CÙNG MỘT assembly:
int32_t a = val;                        // implicit conversion
int32_t b = static_cast<int32_t>(val);  // explicit cast
```

Compiler output (x86):

```asm
mov eax, edx    ; chỉ 1 instruction, copy 32-bit thấp
```

Nên **không cần tạo decode_varint32/64** — viết nhiều hàm chỉ thêm code mà không nhanh hơn. Giữ 1 hàm `decode_varint` +
`static_cast` là cách đúng.

**Những thứ thực sự ảnh hưởng performance** trong project này: syscall (`recv`, `send`, `read`, `write`), disk I/O,
memory allocation — mỗi cái tốn hàng ngàn CPU cycles. `static_cast` tốn 0 cycles.


## Message format
### Produce Request
```
┌─ Produce Request ─────────────────────────────────────┐
│                                                        │
│  Request Header (chung cho mọi loại request)           │
│  ├── api_key          (2B)  = 0 (Produce)              │
│  ├── api_version      (2B)                             │
│  ├── correlation_id   (4B)  để match response          │
│  └── client_id        (string)                         │
│                                                        │
│  Produce Request Body                                  │
│  ├── acks             (2B)  -1/0/1                     │
│  ├── timeout_ms       (4B)                             │
│  └── topics[]                                          │
│      ├── topic_name   (string)                         │
│      └── partitions[]                                  │
│          ├── partition_id  (4B)                        │
│          └── record_batch  ← BATCH NẰM Ở ĐÂY           │
│                                                        │
└────────────────────────────────────────────────────────┘
```

### RecordBatch Format
```
┌─ RecordBatch ────────────────────────────────────────┐
│                                                        │
│  magic                (1B)  = 0 hoặc 1                │
│  attributes           (1B)  nén, timestamp, invalid   │
│  last_offset_delta    (4B)  offset của record cuối     │
│  first_timestamp      (8B)  của record đầu             │
│  max_timestamp        (8B)  của record cuối             │
│  producer_id          (8B)  -1 nếu là compacted         │
│  producer_epoch       (2B)  0-32767                    │
│  base_sequence        (4B)  start sequence             │
│  records[]                                             │
│      ┌─ Record ─────────────────────────────────────┐ │
│      │                                                │ │
│      │  length              (4B)  length của record  │ │
│      │  attributes          (1B)  nén, timestamp     │ │
│      │  timestamp_delta     (varint) delta from base │ │
│      │  offset_delta        (varint) delta from prev │ │
│      │  key                 (bytes) key              │ │
│      │  value               (bytes) value            │ │
│      │  headers             (array) optional         │ │
│      │                                                │ │
│      └────────────────────────────────────────────────┘ │
│                                                        │
└────────────────────────────────────────────────────────┘
```

### Produce Response Format
```
┌─ Produce Response ──────────────────────────────────┐
│                                                        │
│  Response Header                                       │
│  ├── correlation_id   (4B)  phải match request.correlation_id  │
│  └── body              ← payload nằm ở đây             │
│                                                        │
│  Produce Response Body                                 │
│  ├── topics[]                                          │
│  │   ├── topic_name     (string)                        │
│  │   └── partitions[]                                  │
│  │       ├── partition_id   (4B)                       │
│  │       ├── base_offset    (8B) 实际写入的第一个 offset │
│  │       └── error_code   (2B) = 0 nếu thành công       │
│  │                                                      │
│  └─ throttle_time_ms (4B)                               │
│                                                        │
└────────────────────────────────────────────────────────┘
```
### Fetch Request Format
```
┌─ Fetch Request ─────────────────────────────────────┐
│                                                        │
│  Request Header (chung)                               │
│  ├── api_key          (2B)  = 1 (Fetch)               │
│  ├── api_version      (2B)                            │
│  ├── correlation_id   (4B)                            │
│  └── client_id        (string)                        │
│                                                        │
│  Fetch Request Body                                    │
│  ├── max_wait_ms      (4B)  broker giữ request tối đa  │
│  ├── min_bytes        (4B)  byte tối thiểu cần tích     │
│  ├── topics[]                                          │
│  │   ├── topic_name     (string)                        │
│  │   └── partitions[]                                  │
│  │       ├── partition_id (4B)                        │
│  │       ├── fetch_offset (8B) đọc từ đây           │
│  │       └── max_bytes    (4B) mỗi partition đọc tối đa│
│  │                                                      │
│  └─ isolation_level    (1B)  0=read_uncommitted       │
│                                                        │
└────────────────────────────────────────────────────────┘
```
### Fetch Response Format
```
┌─ Fetch Response ────────────────────────────────────┐
│                                                        │
│  Response Header                                       │
│  ├── correlation_id   (4B)  trả về đúng ID gửi         │
│  └── body              ← payload nằm ở đây             │
│                                                        │
│  Fetch Response Body                                   │
│  ├── throttle_time_ms (4B)                            │
│  ├── topics[]                                          │
│  │   ├── topic_name     (string)                        │
│  │   └── partitions[]                                  │
│  │       ├── partition_id (4B)                        │
│  │       ├── error_code   (2B) = 0 nếu OK              │
│  │       ├── high_watermark (8B) offset tiếp theo      │
│  │       ├── last_stable_offset (8B) có thể đọc         │
│  │       ├── log_start_offset (8B)                   │
│  │       ├── records           ← log entries ở đây    │
│  │       └── aborted_blocks    (optional) lỗi log       │
│  │                                                      │
│  └─ [body_end]                                         │
│                                                        │
└────────────────────────────────────────────────────────┘
```

April 18, 2026
- kakfa use binary protocol, use varint to save space
- 

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

- Log mamager hold one log: log whole send/append for all topic. 
  - Log manager is singleton to be thread safe (must remove copy constructor, assignment operator)
  - use std::map: map<string, TopicState> topic_states, each key is topic name.

=> Want to do fine grained locking, lock on each topic.
- Create TopicState class
  - Cannot be Singleton. if so, we cannot create multiple TopicState objects, one for each topic.
  => But we want to prevent multiple object for one topic.
    - still remove copy constructor, assignment operator
    - use unique_ptr:  - std::unordered_map<std::string, std::unique_ptr<TopicState>> topics_;

  - Now we want to lock only topics_, which is share between thread?

```cpp
    TopicState& state = get_or_create(topic)
    {
        std::lock_guard lock(mu_);
        state = get_or_create(topic);
    }
```
  
    
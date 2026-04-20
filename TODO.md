# TODO

- use epoll in broker 
- page cache ?

## Optimization

Đúng hướng, nhưng Kafka thật chia thành **3 layer**, không chỉ 2:

```
                    ┌─────────────────────────┐
 Clients ──TCP───▶  │  Network threads (NIO)   │  ← event loop, nhận/gửi bytes
                    │  (num.network.threads=3) │
                    └──────────┬──────────────┘
                               │ request queue
                    ┌──────────▼──────────────┐
                    │  I/O threads             │  ← xử lý logic + đọc/ghi disk
                    │  (num.io.threads=8)      │
                    └──────────┬──────────────┘
                               │
                    ┌──────────▼──────────────┐
                    │  OS Page Cache + Disk    │
                    └─────────────────────────┘
```

| Layer | Việc làm | Blocking? |
|-------|---------|-----------|
| **Network threads** | Nhận bytes từ socket, parse request, gửi response | Không block (NIO/epoll) |
| **Request queue** | Buffer giữa network và I/O | Thread-safe queue |
| **I/O threads** | Thực hiện read/write log file | Có thể block trên disk |

### So sánh với project này

```
Mini Kafka (hiện tại):
Client ──TCP──▶ 1 thread làm TẤT CẢ (nhận request + parse + ghi disk + gửi response)

Kafka thật:
Client ──TCP──▶ Network thread (nhận) ──queue──▶ I/O thread (ghi disk) ──queue──▶ Network thread (gửi)
```

Lợi ích của việc tách: network thread không bao giờ bị chặn bởi disk I/O chậm, nên nó luôn sẵn sàng nhận request mới từ client khác.


## Message format
- https://kafka.apache.org/42/implementation/message-format/#record-batch

broker trust producer on the format of message.
- just do some check on batch header
- add `base_offset` and `partition_leader_epoch`
- write log

Batch Format:
```sh
base_offset            8B    offset đầu tiên trong batch
batch_length           4B    kích thước batch (không tính 12B đầu)
partition_leader_epoch 4B
magic                  1B    version = 2
CRC                    4B    checksum
attributes             2B    compression type, timestamp type
last_offset_delta      4B
first_timestamp        8B
max_timestamp          8B
producer_id            8B    cho exactly-once delivery
producer_epoch         2B
base_sequence          4B
records_count          4B
records                ...   variable-length records
```

Record Format:
```sh
length          varint    kích thước record
attributes      1B
timestamp_delta varint    delta so với first_timestamp
offset_delta    varint    delta so với base_offset
key_length      varint
key             N bytes
value_length    varint
value           N bytes
headers_count   varint
headers         N bytes   (key-value pairs)
```

- store batch into log file
- create batch, not single record
- sendfile()
- read message in client 



- post: how different language handle memory
  - c++: move(),
  - reference, smart pointer, garbage collection, ownership, borrowing, thread safe
  
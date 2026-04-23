# TODO

- use epoll in broker 
- page cache ?
- add more fields to message

- learn C++ concurrency primitive: https://dev.to/min38/thread-pool-3ema

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

## Lock 
3. Kafka thật giải quyết tranh chấp (Race Condition) như thế nào?
Kafka thật sẽ tối ưu khóa (Lock Optimization) cực kỳ tinh vi:

Khóa theo từng Topic/Partition (Fine-Grained Locking): Thay vì dùng 1 biến mu_ cho toàn bộ LogManager, người ta đặt biến std::mutex vào bên trong cấu trúc TopicState. Ai tương tác với file của Topic nào thì chỉ khóa Topic đó. topic_A và topic_B hoàn toàn không block lẫn nhau.
Read không cần Lock (Lock-free Reads): Hàm sendfile (trên Mac) và hàm pread có khả năng đọc dữ liệu từ một offset cụ thể mà không làm thay đổi con trỏ của file descriptor. Do đó, hàng chục Consumer có thể gọi sendfile đọc cùng 1 file log cùng một lúc mà không hề xảy ra xung đột.
Khóa Đọc-Ghi (Readers-Writer Lock): Trong C++17 có std::shared_mutex. Nhiều luồng Consumer có thể cùng cầm khóa Read (đọc song song thoải mái), nhưng luồng Producer khi muốn ghi (append) thì sẽ xin khóa Write (chặn tất cả lại để ghi cho an toàn).
(Tạm thời ở phiên bản Mini Kafka này, cấu trúc dùng 1 lock mu_ của bạn là hoàn toàn hợp lý để chạy cho đúng đã. Tối ưu khóa là bài toán cho phase sau!)

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



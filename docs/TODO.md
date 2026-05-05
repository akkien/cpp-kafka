# TODO

- page cache ?
- num.network.threads=3
- response queue between i/o threads and network threads
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


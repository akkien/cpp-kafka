
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
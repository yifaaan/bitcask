# Bitcask 磁盘格式文档

本文档详细定义了所有磁盘文件的二进制格式，确保新旧版本兼容。

## 1. LogRecord 格式

每条日志记录（写入数据）的二进制布局：

```
┌────────────┬──────────┬─────────────┬───────────────┬────────────┬──────────────┐
│  CRC32C    │   Type   │   KeySize   │   ValueSize   │    Key     │    Value     │
│  (4 bytes) │ (1 byte) │  (varint)   │   (varint)    │  (bytes)   │   (bytes)    │
└────────────┴──────────┴─────────────┴───────────────┴────────────┴──────────────┘
```

### 1.1 字段详解

| 字段 | 大小 | 编码 | 说明 |
|------|------|------|------|
| CRC32C | 4 bytes | Little-Endian uint32 | Castagnoli 多项式 CRC，覆盖 Type + KeySize + ValueSize + Key + Value |
| Type | 1 byte | uint8 | 记录类型：0=Normal, 1=Deleted, 2=TxnFinished |
| KeySize | varint | LEB128 | Key 字节数，最大支持 64-bit |
| ValueSize | varint | LEB128 | Value 字节数，最大支持 64-bit |
| Key | N bytes | Raw | 键内容 |
| Value | M bytes | Raw | 值内容 |

### 1.2 CRC32C 计算

```cpp
// CRC 计算范围：从 Type 开始（跳过 CRC 自身 4 字节）
uint32_t ComputeCRC32C(const LogRecord& record, const byte* header_after_crc) {
    // header_after_crc = {Type, KeySize_varint, ValueSize_varint}
    uint32_t crc = 0;
    crc = CRC32C(crc, header_after_crc, header_size - 4);  // header不含CRC
    crc = CRC32C(crc, record.key.data(), record.key.size());
    crc = CRC32C(crc, record.value.data(), record.value.size());
    return crc;
}
```

**CRC32C 多项式**: 0x82F63B78 (Castagnoli)

**硬件支持**:
- x86: `_mm_crc32_u8`, `_mm_crc32_u32`, `_mm_crc32_u64` (SSE4.2+)
- ARM: `__crc32cb`, `__crc32cd` (ARMv8+)

### 1.3 Varint 编码 (LEB128)

与 Protobuf 相同的编码格式：

```
每个字节:
┌───────────┬─────────────────────────────────────────┐
│ Continue  │           Data Bits (7 bits)            │
│ Bit (1)   │                                         │
└───────────┴─────────────────────────────────────────┘

Continue Bit:
  0 = 最后一个字节
  1 = 还有后续字节

编码示例:
  值 1    → 0x01 (单字节)
  值 127  → 0x7F (单字节)
  值 128  → 0x80 0x01 (两字节)
  值 300  → 0xAC 0x02 (两字节: 0xAC = 0b10101100, 低7位=44, 高位继续; 0x02=2, 组合=44+2*128=300)
```

**编码函数**:

```cpp
// 编码 - 返回写入的字节数
int PutVarint(std::span<std::byte> buf, uint64_t value) {
    int n = 0;
    while (value >= 0x80) {
        buf[n++] = static_cast<std::byte>((value & 0x7F) | 0x80);
        value >>= 7;
    }
    buf[n++] = static_cast<std::byte>(value);
    return n;
}

// 解码 - 返回 {值, 读取字节数}
std::pair<uint64_t, int> GetVarint(std::span<const std::byte> buf) {
    uint64_t result = 0;
    int shift = 0, n = 0;
    while (n < (int)buf.size() && shift < 64) {
        uint8_t byte = static_cast<uint8_t>(buf[n++]);
        result |= static_cast<uint64_t>(byte & 0x7F) << shift;
        if (!(byte & 0x80)) return {result, n};
        shift += 7;
    }
    return {0, 0};  // 错误：不完整或溢出
}
```

### 1.4 LogRecordType 枚举

```cpp
enum class LogRecordType : uint8_t {
    kNormal = 0,      // 正常数据记录
    kDeleted = 1,     // 删除标记（墓碑）
    kTxnFinished = 2, // 事务完成标记
};
```

### 1.5 Header 最大大小

```cpp
constexpr size_t kMaxLogRecordHeaderSize = 1 + 2 * 10;  // Type(1) + 2个varint(最多各10字节)
// 实际计算: Type(1) + KeySize(varint最多5字节for int32) + ValueSize(varint最多5字节)
constexpr size_t kMaxLogRecordHeaderSize = 1 + 2 * 5;   // = 11 bytes
```

对于 int64 类型 varint，最多 10 字节（每字节 7 位有效，64/7 ≈ 10）。

---

## 2. LogRecordPos 编码

索引项存储的位置信息，用于 hint 文件和内部索引：

```
┌─────────────────┬─────────────────┬─────────────────┐
│      FID        │     Offset      │      Size       │
│    (varint)     │    (varint)     │    (varint)     │
└─────────────────┴─────────────────┴─────────────────┘
```

| 字段 | 类型 | 编码 | 说明 |
|------|------|------|------|
| FID | uint32 | varint | 数据文件 ID |
| Offset | int64 | varint | 记录起始偏移 |
| Size | int64 | varint | 记录总大小 |

**最大编码大小**: 30 bytes (3 × 10)

**编码函数**:

```cpp
std::pair<std::vector<std::byte>, int64_t> EncodeLogRecordPos(const LogRecordPos& pos) {
    std::vector<std::byte> buf(30);
    int n = 0;
    n += PutVarint(std::span(buf).subspan(n), pos.fid);
    n += PutVarint(std::span(buf).subspan(n), pos.offset);
    n += PutVarint(std::span(buf).subspan(n), pos.size);
    buf.resize(n);
    return {buf, n};
}
```

---

## 3. 数据文件格式

### 3.1 文件命名

```
{FID:09d}.data

示例:
000000001.data  (FID = 1)
000000002.data  (FID = 2)
...
```

FID 是 9 位十进制数字，零填充，单调递增。

### 3.2 文件内容

数据文件是一系列 LogRecord 的连续拼接：

```
┌─────────────────────────────────────────────────────────────────────┐
│ LogRecord 1 │ LogRecord 2 │ LogRecord 3 │ ... │ LogRecord N │ EOF │
└─────────────────────────────────────────────────────────────────────┘
```

- 文件末尾可能有不完整记录（写入中断），读取时通过 CRC 校验检测
- 每个 record 后可能有 padding（取决于 bytes_per_sync 配置）

### 3.3 Active vs Older Files

```
活跃文件 (active_file_):
  - FID 最大的文件
  - 当前所有写入都追加到此文件
  - write_offset_ 记录当前写入位置

历史文件 (older_files_):
  - FID 较小的文件
  - 只读，用于读取旧数据
  - 当活跃文件超过 max_data_file_size 时，转为历史文件
```

---

## 4. Hint 索引文件格式

Merge 后生成的快速恢复文件：

```
文件名: hint-index
内容: 一系列 {Key, LogRecordPos} 记录
```

每条记录格式：

```
┌─────────────────────────────────────────────────────────────────────┐
│  LogRecord (Normal type)                                            │
│  [CRC][Type=0][KeySize][ValueSize][Key][Value=EncodedLogRecordPos]  │
└─────────────────────────────────────────────────────────────────────┘
```

**本质**: Hint 文件是一系列特殊的 LogRecord，其中:
- Type = kNormal (0)
- Key = 用户键
- Value = EncodeLogRecordPos(pos)

**启动恢复流程**:
1. 优先读取 hint-index，直接重建索引（快）
2. 如果 hint-index 不存在或损坏，回退到扫描所有 .data 文件（慢）

---

## 5. Merge Finished 文件

标记合并完成，重启时执行 swap：

```
文件名: merge-finished
内容: 一条 LogRecord
      Type = kTxnFinished (2)
      Key = "merge-finished"
      Value = EncodeLogRecordPos(起始FID)
```

**记录的 Value** 包含合并后数据文件的起始 FID，用于：
- 区分哪些是合并前的旧文件（需删除）
- 哪些是合并后的新文件（需保留）

---

## 6. LOCK 文件

跨进程独占锁：

```
文件名: LOCK
内容: 空（仅用于文件锁）
```

**锁定方式**:
- Windows: `CreateFile` + `LOCKFILE_EXCLUSIVE_LOCK`
- POSIX: `flock(fd, LOCK_EX | LOCK_NB)`

**生命周期**:
- DB::Open() 时创建并锁定
- DB::Close() 时释放并删除

---

## 7. Redis 数据类型元数据编码

### 7.1 ValueMetadata 布局

所有 Redis 类型值的元数据头部：

```
┌────────────┬─────────────────┬─────────────────┬─────────────────┬─────────────────┐
│    Type    │     Expiry      │    Version      │     Size        │  (Type-specific)│
│  (1 byte)  │   (8 bytes)     │   (8 bytes)     │   (8 bytes)     │                 │
└────────────┴─────────────────┴─────────────────┴─────────────────┴─────────────────┘
```

| 字段 | 大小 | 编码 | 说明 |
|------|------|------|------|
| Type | 1 byte | uint8 | RedisDataType 枚举 |
| Expiry | 8 bytes | LE int64 | 过期时间（steady_clock 纳秒），0=永不过期 |
| Version | 8 bytes | LE uint64 | 集合版本号（用于键编码） |
| Size | 8 bytes | LE uint64 | 集合元素数 |

**Type-specific 字段**（仅 List 类型）:
```
┌─────────────────┬─────────────────┐
│      Head       │      Tail       │
│   (8 bytes)     │   (8 bytes)     │
└─────────────────┴─────────────────┘
```

### 7.2 RedisDataType 枚举

```cpp
enum class RedisDataType : uint8_t {
    kString = 0,
    kHash = 1,
    kList = 2,
    kSet = 3,
    kZSet = 4,
};
```

### 7.3 子键编码

每种集合类型有特定的子键编码格式。

#### HashDataKey

```
{Key} || {Version(8 bytes LE)} || {Field}
```

#### SetDataKey

```
{Key} || {Version(8 bytes LE)} || {Member} || {MemberSize(4 bytes LE)}
```

MemberSize 用于区分相同前缀的 Member（避免前缀冲突）。

#### ListDataKey

```
{Key} || {Version(8 bytes LE)} || {Index(8 bytes LE)}
```

Index 是逻辑索引（0, 1, 2, ...），不是物理位置。

#### ZSetMemberDataKey (member → score)

```
{Key} || {Version(8 bytes LE)} || {Member}
```

#### ZSetScoreDataKey (score → member, for range queries)

```
{Key} || {Version(8 bytes LE)} || {Score(8 bytes BE)} || {Member} || {MemberSize(4 bytes LE)}
```

**Score 使用 Big-Endian**: 为了字典序排列时 score 也能有序。

### 7.4 Double 排序编码

用于 ZSet score 的有序存储：

```cpp
uint64_t OrderedDoubleBits(double score) {
    uint64_t bits = std::bit_cast<uint64_t>(score);  // IEEE 754 bits
    if (bits & 0x8000000000000000) {  // 航数
        return ~bits;  // 航数取反，使其在字典序中排在正数前面
    } else {
        return bits ^ 0x8000000000000000;  // 正数翻转符号位，保持正数内部有序
    }
}

double DoubleFromOrderedBits(uint64_t ordered) {
    uint64_t bits;
    if (ordered & 0x8000000000000000) {
        bits = ordered ^ 0x8000000000000000;  // 恢复正数
    } else {
        bits = ~ordered;  // 恢复航数
    }
    return std::bit_cast<double>(bits);
}
```

**关键点**:
- 航数区间 [-∞, 0] → 映射到 [0x000..., 0x7FF...]
- 正数区间 [0, +∞] → 映射到 [0x800..., 0xFFF...]
- 字典序排列时，航数 < 正数，且内部有序

---

## 8. 文件系统布局示例

```
./bitcask_data/
├── LOCK                    # 锁文件（打开时创建）
├── 000000001.data          # 第一个数据文件（已转为历史）
├── 000000002.data          # 第二个数据文件（已转为历史）
├── 000000003.data          # 当前活跃文件
├── hint-index              # (merge 后生成)
├── merge-finished          # (merge 后生成)
└── ./bitcask_data-merge/   # merge 临时目录（merge 进行中）
    ├── 000000004.data
    ├── hint-index
    └── merge-finished
```

**Merge 完成后**:
- 删除 `000000001.data`, `000000002.data`
- 移动 `bitcask_data-merge/*` 到 `bitcask_data/`
- 删除 `bitcask_data-merge/` 目录

---

## 9. 兼容性验证要点

新 C++23 版本必须与原 C++20 版本的以下方面兼容：

| 项目 | 验证方法 |
|------|----------|
| CRC32C | 用原版本数据文件，新版本读取并验证 CRC |
| Varint | 编码相同数值，比较字节序列是否一致 |
| LogRecord 布局 | 原版本写入，新版本读取，比较 key/value |
| Hint 文件 | 原版本 merge，新版本启动恢复 |
| Redis 元数据 | 原版本写入 Hash/Set/ZSet，新版本读取 |

**关键测试**:
```cpp
// 1. CRC32C 一致性
uint32_t expected = 0x12345678;  // 原版本计算值
uint32_t actual = ComputeCRC32C_HW(data);  // 新版本硬件计算
ASSERT_EQ(expected, actual);

// 2. Varint 一致性
std::vector<std::byte> encoded = EncodeVarint_CPP20(value);  // 原版本 protobuf
std::vector<std::byte> encoded_new = EncodeVarint_CPP23(value);  // 新版本手写
ASSERT_EQ(encoded, encoded_new);
```
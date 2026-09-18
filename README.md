# 短链接服务（shortener-api）

基于 go-zero 框架的短链接服务：将长链接转为短链接，访问短链接时 302 重定向回原始长链接，并通过 RabbitMQ 异步统计每个短链的访问次数。

技术栈：Go 1.23 · go-zero v1.8 · MySQL · Redis · RabbitMQ

## 功能特性

- **转链**：长链接 → 短链接（6 位左右 base62 字符串），同一长链接通过 MD5 判重，重复转链直接返回已有短链
- **跳转**：访问短链接 302 重定向到长链接
- **访问统计**：跳转请求通过 RabbitMQ 异步投递点击事件，消费端计数，解析主流程零阻塞、最终一致
- **发号器**：号段模式分布式发号器（Redis 号段 + 本地双缓冲 + DB checkpoint），热路径无锁、重启恢复绝不重发；另保留 MySQL / Redis 两种简单实现可切换
- **高并发防护**：
  - Redis 布隆过滤器前置拦截不存在的短链，防缓存穿透
  - go-zero sqlc 缓存自带 singleflight，防缓存击穿
- **可靠性**：MQ 生产端 publisher confirm + 失败落盘补偿重发 + 断线自动重连；消费端 Redis 幂等去重、失败重试 3 次后进死信队列

## 环境依赖

| 依赖 | 版本建议 | 用途 |
|------|---------|------|
| Go | ≥ 1.23 | 编译运行 |
| MySQL | 8.x | 长短链映射、跳转计数、发号器 checkpoint |
| Redis | 6.x+ | 发号器号段、布隆过滤器、缓存、计数热数据 |
| RabbitMQ | 3.x | 点击事件异步投递 |
| goctl（可选） | 1.7.x | 重新生成 API/Model 代码时需要 |

## 快速开始

### 1. 建库建表

创建数据库（名称与配置文件一致，本项目为 `shorteren`），然后依次执行根目录与 sequence 目录下的 SQL：

```bash
mysql -uroot -p -e "CREATE DATABASE shorteren DEFAULT CHARSET utf8mb4;"

mysql -uroot -p shorteren < sequence.sql               # 发号器序号表（MySQL 发号器实现用）
mysql -uroot -p shorteren < short_url_map.sql          # 长短链映射表
mysql -uroot -p shorteren < short_url_stats.sql        # 跳转计数表
mysql -uroot -p shorteren < shortener/sequence/segment_checkpoint.sql  # 号段 checkpoint 表
```

### 2. 下载依赖

```bash
go mod tidy
```

### 3. 修改配置

配置文件：`shortener/etc/shortener-api.yaml`，主要配置项：

| 配置项 | 说明 | 默认值 |
|--------|------|--------|
| `Host` / `Port` | HTTP 监听地址 | `0.0.0.0:9000` |
| `SequenceDB.DSN` | 发号器 MySQL 连接串 | `root:123456@tcp(127.0.0.1:3306)/shorteren` |
| `ShortUrlDB.DSN` | 业务 MySQL 连接串 | 同上 |
| `CacheRedis` / `Redis.Host` | Redis 地址 | `127.0.0.1:6379` |
| `RabbitMQ.URL` | AMQP 连接串 | `amqp://guest:guest@127.0.0.1:5672/` |
| `RabbitMQ.FallbackFile` | 发布失败补偿文件路径 | `./data/click_fallback.log` |
| `BaseString` | base62 乱序字符表 | 62 位自定义字符串 |
| `ShortDomain` | 短链域名前缀 | `yang.cn` |
| `ShortUrlBlackList` | 短链黑名单（禁用词） | api、health、convert 等 |
| `SequenceSegment` | 号段发号器：`BizTag` / `Step`(10000) / `Threshold`(80) | — |

### 4. 启动

```bash
cd shortener
go run shortener.go
```

看到以下输出即启动成功（服务同时完成：发号器恢复 + 首段加载、布隆过滤器数据回灌、MQ 消费者启动）：

```
Starting server at 0.0.0.0:9000...
```

## API 使用示例

> 注意：路由挂载在根路径（无 `/api` 前缀）。

**转链**

```bash
curl -X POST http://127.0.0.1:9000/convert \
  -H "Content-Type: application/json" \
  -d '{"longUrl":"https://github.com/zeromicro/go-zero"}'
# {"shortUrl":"yang.cn/W"}
```

转链前置校验：长链接非空（validator required）→ 链接可达性探测 → 非短链本身（防循环转链）→ MD5 判重。

**跳转**

```bash
curl -i http://127.0.0.1:9000/W
# HTTP/1.1 302 Found
# Location: https://github.com/zeromicro/go-zero
```

**查询访问次数**

```bash
curl http://127.0.0.1:9000/stats/W
# {"total":2}
```

## 核心设计

### 号段模式分布式发号器（`shortener/sequence/segment.go`）

| 机制 | 说明 |
|------|------|
| 无锁热路径 | `Next()` 在当前内存号段上 `atomic.AddUint64` 自增，微秒级 |
| 双缓冲预加载 | 当前号段用量达 `Threshold`(默认 80%) 时，后台 goroutine `INCRBY sequence:segment:{bizTag} step` 申请下一号段，CAS 保证单预加载 |
| 无缝切换 | 当前号段耗尽切到备用槽；备用未就绪时短暂等待后同步申请，超时返回 `ErrSegmentNotReady` |
| checkpoint | **申请号段时落盘**（备用槽激活前）：号段终点写 `sequence_checkpoint` 表（GREATEST 幂等只增），指数退避重试 |
| 重启恢复 | 读 DB checkpoint 为安全下界 → 与 Redis 值取大对齐（跳号不重发）→ 同步加载首段，失败拒绝启动 |

**正确性**：号段只有在终点写入 DB 后才激活发放，故 DB `max_id` 永远 ≥ 任何已发放的号——崩溃后从 checkpoint+1 恢复，只跳号、绝不重发。残余风险：INCRBY 成功到 checkpoint 落盘的毫秒级窗口内崩溃且 Redis 数据全丢（生产环境 Redis 需开启 AOF）。

**降级语义**：Redis 不可用 → 号段耗尽后 fail fast（正确性优先）；DB 不可用 → 只阻塞新号段激活，不影响已激活号段发号；启动时 DB 不可用 → 拒绝启动。

多实例安全：Redis INCRBY 是号段分配的唯一协调点，各实例号段天然不重叠。

### RabbitMQ 访问统计

拓扑（均持久化，代码自动声明，幂等）：

| 组件 | 名称 |
|------|------|
| 交换机 | `shortener.stats.exchange`（direct） |
| 队列 | `shortener.stats.click.queue`（配置死信转发） |
| 死信交换机/队列 | `shortener.stats.dlx` / `shortener.stats.click.dlq` |

消息体（JSON）：`msgId`（幂等键）/ `surl` / `clickedAt` / `retry`。

- **生产端**（`pkg/mq` + ShowLogic）：DB 查询成功后异步投递（放 DB 成功之后而非布隆通过之后，避免假阳性误计数）；publisher confirm 保证到达，失败重试 3 次 → 落盘 `FallbackFile` 后台每分钟重发；断线自动重连并重建拓扑
- **消费端**（`internal/consumer`）：Redis `SETNX stats:click:consumed:{msgId}`（TTL 3 天）幂等 → MySQL `INSERT ... ON DUPLICATE KEY UPDATE` 原子累加（真源）→ Redis `INCR stats:click:count:{surl}` 同步热点值（尽力而为）；MySQL 写失败删除幂等标记后重投，3 次耗尽进死信队列；未 ACK 消息由 broker 重投 + 幂等标记防重复计数
- **查询接口**：Redis 计数器 → MySQL 回源（SETNX 回填，避免覆盖消费端新值）

### 关于本地缓存的说明

代码中未启用进程内 L1 本地缓存，缓存均为 Redis 版：go-zero sqlc CachedConn（缓存整个数据行，自带 singleflight 防击穿）+ Redis 布隆过滤器（拦截不存在的短链）。计数链路不受影响。

## 目录结构

```
short-link-system/
├── shortener.api                  # API 定义文件（goctl 生成代码的来源）
├── sequence.sql                   # 发号器序号表（MySQL 发号器实现用）
├── short_url_map.sql              # 长短链映射表
├── short_url_stats.sql            # 跳转计数表
└── shortener/
    ├── shortener.go               # 入口：加载配置、初始化依赖、启动消费者与 HTTP 服务
    ├── etc/
    │   └── shortener-api.yaml     # 配置文件
    ├── internal/
    │   ├── config/config.go       # 配置结构体（与 yaml 必须对齐）
    │   ├── handler/               # HTTP 处理器（goctl 生成）：convert / show / stats
    │   ├── logic/                 # 业务逻辑：转链、跳转、统计查询
    │   ├── consumer/              # RabbitMQ 点击事件消费端
    │   ├── svc/                   # ServiceContext：组装发号器、模型、布隆过滤器、MQ
    │   └── types/types.go         # 请求/响应结构体（含 validator 标签）
    ├── model/                     # goctl 生成的 MySQL model（short_url_map 带 sqlc 缓存）
    ├── pkg/
    │   ├── base62/                # 乱序 base62 编码（号 → 短码）
    │   ├── connect/               # 长链接可达性探测
    │   ├── md5/                   # 长链接 MD5（判重索引）
    │   ├── urltool/               # URL 路径提取（防循环转链）
    │   └── mq/                    # RabbitMQ 生产端封装（拓扑声明/confirm/重试/补偿/重连）
    └── sequence/                  # 发号器：mysql.go / redis.go（简单实现）、segment.go（号段模式，默认）
        └── segment_checkpoint.sql # checkpoint 建表语句
```

## 重新生成代码（可选）

```bash
# API 层
goctl api go -api shortener.api -dir ./shortener --style go_zero

# Model 层（short_url_map 需带 -c 开启缓存）
goctl model mysql datasource -url="root:123456@tcp(127.0.0.1:3306)/shorteren" -table="short_url_map" -dir="./shortener/model" -c
```

> 注意：重新生成后需保留 `types.go` 中的 validator 标签等手写改动；`service_context.go` 中的发号器装配（号段模式）为手写逻辑，不会被生成覆盖。

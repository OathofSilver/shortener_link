# 短链接服务(shortener)

> 基于 go-zero 的短链接服务,完整覆盖**链接生成、短链跳转、访问统计**三大核心链路。
> author: yang
> 代码生成:`goctl api go -api shortener.api -dir ./shortener --style go_zero`

---

## 一、项目定位与背景

1. **短链接(Short Link)**
   通过特定算法将长 URL 压缩为更短、易记的 URL,核心价值包括:
   - **缩短字符**:减少 URL 长度,节省空间(如短信、社交媒体场景)。
   - **提升体验**:便于记忆、分享和传播,避免长 URL 的视觉干扰。
   - **数据分析**:记录访问次数等数据,支持营销效果追踪。

2. **本项目定位**
   一个 Go 语言实现的高并发短链接服务,自增 ID + 乱序 Base62 生成短码,重点关注**取号、缓存与异步计数**三块工程实践:号段模式发号器、布隆过滤器防穿透、RabbitMQ 异步统计链路。

3. **技术选型**

| **组件** | **选型** | **说明** |
|------------------|--------------------------------|--------------------------------------------------------------|
| **Web 框架** | go-zero v1.8.1(Go 1.23) | goctl 生成 handler/logic/types,自带行缓存与 singleflight |
| **存储** | MySQL | 长短链映射、计数真源;取号器独立 DSN(实际开发中应分库) |
| **缓存** | Redis | 布隆过滤器 bitset、行缓存、号段计数、点击计数与幂等标记 |
| **消息队列** | RabbitMQ | 点击事件异步削峰,direct 交换机 + 死信队列拓扑 |
| **发号器** | 号段模式 | Redis 号段 + 本地双缓冲 + DB checkpoint,可切换 MySQL/Redis 实现 |

---

## 二、核心功能

### 1. 链接生成(POST /convert)

输入一个长链接,转为短链接,流程如下:

1. **可达性探测**:用全局连接池的 HTTP Client(2s 超时)请求长链接,状态码非 200 视为无效;连接池复用 TCP 连接,避免高并发下 TIME_WAIT 耗尽本机端口。
2. **MD5 查重**:对长链接求 MD5(32 位十六进制)后按 `md5` 唯一索引查库,已转过的链接直接返回已有短码;用 MD5 建索引而非直接索引长链接,避免长字符串索引耗时。
3. **防循环转链**:提取 URL path 最后一段,若能在 `short_url_map` 中查到,说明输入的已经是短链,拒绝转换。
4. **取号 + 转码**:发号器取一个全局自增号,经**乱序 Base62** 转为短码(字符表在配置文件中打乱,防止短码被猜测遍历);命中黑名单(如 `api`、`health`)则重新取号。
5. **存储映射**:长短链映射写入 MySQL,同时把短码加入布隆过滤器,返回 `短域名/短码`。

### 2. 短链跳转(GET /:shortUrl)

1. **布隆过滤器前置拦截**:短码不存在直接返回 404,不触碰缓存与数据库,**防止缓存穿透**;过滤器基于 Redis bitset(约 2000 万 bit),服务重启后从 MySQL 分页回灌全量短码。
2. **查缓存/数据库**:按 `surl` 查长短链映射,走 go-zero 行缓存,自带 **singleflight**,并发请求同一失效短码时只放一个请求回源,**防止缓存击穿**。
3. **异步计数**:DB 查询成功后,点击事件异步投递 RabbitMQ(不阻塞重定向主流程);投递放在查询成功之后而非过滤器通过之后,避免布隆假阳性/已删除短链被误计数。
4. **302 重定向**:handler 返回 `http.StatusFound` 临时重定向到长链接。

### 3. 访问统计(GET /stats/:shortUrl)

- **读路径**:先查 Redis 计数器 `stats:click:count:{surl}`(消费端维护的热点值),未命中回源 MySQL 计数表(真源),再以 **SETNX** 回填 Redis——不用 SET,避免旧值覆盖消费端刚 INCR 出的新值。
- **写路径**:见「三、异步统计链路」。

### 接口一览

| **功能** | **方法/路径** | **入参** | **出参** |
|--------------|------------------------|------------|------------------------------|
| 转链 | POST /convert | longUrl | shortUrl(短域名/短码) |
| 跳转 | GET /:shortUrl | 短码 | 302 重定向到长链接 |
| 统计 | GET /stats/:shortUrl | 短码 | total(累计点击次数) |

---

## 三、技术架构与关键技术

```
转链链路(POST /convert)
  长链接 → 可达性探测 → MD5 查重 → 防循环转链 → 发号器取号 → Base62+黑名单
        → 写 MySQL(short_url_map)+ 布隆过滤器 → 返回 短域名/短码

跳转链路(GET /:shortUrl)
  短码 → 布隆过滤器(不存在 → 404) → 行缓存/singleflight → MySQL 查长链
        → 302 重定向
        └─(异步)点击事件 → RabbitMQ → 消费者(幂等去重 → MySQL 累加 → Redis INCR)

统计链路(GET /stats/:shortUrl)
  Redis 计数器 → 未命中回源 MySQL → SETNX 回填
```

### 1. 发号器:号段模式(核心)

`sequence` 包提供三种实现,统一实现 `Sequence` 接口,当前默认启用号段模式:

| **实现** | **原理** | **特点** |
|--------------|----------------------------------------|------------------------------------------------|
| MySQL 发号器 | `REPLACE INTO sequence` + LAST_INSERT_ID | 实现最简单,每次取号一次 DB 写入 |
| Redis 发号器 | INCR 原子自增 | 性能好,Redis 数据丢失会重号 |
| **号段模式(默认)** | Redis INCRBY 批量取号段 + 本地双缓冲 + DB checkpoint | 热路径纯内存微秒级,重启只跳号不重发 |

号段模式的关键设计(详见 `sequence/segment.go` 头注释):

1. **热路径无锁**:`Next()` 在当前内存号段上 atomic 自增,纯内存操作,仅在号段边界走慢路径。
2. **双缓冲**:当前号段用量达阈值(默认 80%)时,后台 goroutine 通过 Redis INCRBY 批量申请下一号段(默认 10000 个)填入备用槽;当前号段耗尽后无缝切换。
3. **checkpoint**:申请号段时先把号段终点写 MySQL(`GREATEST` 幂等只增),保证 DB 记录值 ≥ 任何已发放的号;顺序不可颠倒——checkpoint 成功先于号段可用,是"绝不重发"的根本保证。
4. **崩溃恢复**:启动时读 DB checkpoint 作为安全下界,对齐 Redis 计数器(取 `max(dbMax, redisVal)`),只跳号、绝不重发;恢复失败则拒绝启动,发号器不能带病上线。
5. **降级**:Redis 不可用 → 新号段无法申请 → 当前号段耗尽后 fail fast;DB 不可用 → 只阻塞新号段激活,不影响已激活号段发号。
6. **多实例安全**:Redis INCRBY 是号段分配的唯一协调点,各实例号段天然不重叠;checkpoint 乱序到达也不会回退。

### 2. 缓存:布隆过滤器 + singleflight

- **布隆过滤器**:`github.com/zeromicro/go-zero/core/bloom`,bit 存于 Redis(key `bloom_filter`),进程无状态、重启不丢;启动时分页加载 `short_url_map` 全量短码回灌。
- **singleflight**:go-zero 行缓存内置,同一短码缓存失效时合并并发回源请求。
- 计数表 `short_url_stats` 更新极其频繁,**不走行缓存**,消费端直连 MySQL 原子累加。

### 3. 异步统计链路:RabbitMQ

**生产端**(`pkg/mq/rabbitmq.go`,封装于 `mq.RabbitMQ`):

1. 拓扑幂等声明:direct 交换机 + 持久化点击队列 + 死信交换机/队列(承接格式错误或重试耗尽的消息)。
2. **发布确认(publisher confirm)**:等待 broker 落盘回执,未确认视为投递失败。
3. **失败重试 + 本地补偿**:重试多次仍失败时消息按行落盘(`FallbackFile`),后台每分钟扫描重发,成功后移除。
4. **断线自动重连**:连接断开后后台重建连接与拓扑;初始连接失败不报错,期间消息走补偿,保证不丢。

**消费端**(`internal/consumer/click_consumer.go`,单条消息流程):

1. 消息格式校验,解析失败直接进死信队列。
2. Redis `SETNX(msgId)` 幂等校验,重复消息直接 ACK 丢弃(幂等标记 TTL 3 天,覆盖常规重投窗口)。
3. MySQL `INSERT ... ON DUPLICATE KEY UPDATE` 原子累加计数(最终真源)。
4. Redis INCR 同步短链计数值(尽力而为,失败仅记日志不回滚,可由查询接口回源修复)。

可靠性保障:

- **MySQL 写失败**:删除幂等标记 → 重新投递 `retry+1` 的消息 → 3 次耗尽进死信队列。
- **消费实例宕机**:未 ACK 的消息由 broker 重新投递,幂等标记防止重复计数。
- **限流**:prefetch 100,未确认消息超限不再下发,避免消费者被打垮。

### 4. 数据表设计

| **表** | **职责** | **关键设计** |
|-----------------------|--------------------|--------------------------------------------------|
| `short_url_map` | 长短链映射 | `md5`、`surl` 唯一索引;`lurl` 最长 2048 |
| `short_url_stats` | 跳转计数(真源) | `uniq_surl` 唯一键 + ON DUPLICATE KEY UPDATE 原子累加 |
| `sequence` | MySQL 发号器序号表 | REPLACE INTO 后取自增主键 |
| `sequence_checkpoint` | 号段 checkpoint | `biz_tag` 主键,`max_id` GREATEST 只增,作为恢复安全下界 |

---

## 四、使用方式

### 1. 环境准备

依赖:Go 1.23+、MySQL、Redis、RabbitMQ。

依次执行建表 SQL(库名与 DSN 保持一致):

```sql
source short_url_map.sql;        -- 长短链映射表
source short_url_stats.sql;      -- 跳转计数表
source sequence.sql;             -- MySQL 发号器序号表(可选实现)
source shortener/sequence/segment_checkpoint.sql;  -- 号段 checkpoint 表(必须)
```

### 2. 配置说明

配置文件 `shortener/etc/shortener-api.yaml`:

| **配置项** | **示例/默认** | **说明** |
|--------------------|----------------------------------------|--------------------------------------------|
| Host / Port | 0.0.0.0:9000 | 监听地址 |
| SequenceDB / ShortUrlDB | MySQL DSN | 取号器与业务存储分开配置(实际开发应分库) |
| CacheRedis / Redis | 127.0.0.1:6379 | 布隆 bitset、行缓存;号段计数、点击计数与幂等 |
| BaseString | 62 个乱序字符 | 乱序 Base62 字符表,防短码被猜测 |
| ShortUrlBlackList | version、api、health… | 短码黑名单,命中则重新取号 |
| ShortDomain | yang.cn | 拼接返回的短域名 |
| SequenceSegment | BizTag=shortener、Step=10000、Threshold=80 | 号段业务标识、号段长度、预加载阈值(%) |
| RabbitMQ | amqp://… + FallbackFile | 点击事件投递地址与本地补偿文件路径 |

### 3. 启动服务

```bash
cd shortener
go run shortener.go            # 默认读取 etc/shortener-api.yaml
```

启动时会打印配置、初始化布隆过滤器(从 MySQL 回灌全量短码)、加载首个号段(checkpoint 失败会拒绝启动),并以后台协程拉起点击事件消费者。

### 4. 快速体验

```bash
# 转链:长链接 → 短链接
curl -X POST http://127.0.0.1:9000/convert \
  -H "Content-Type: application/json" \
  -d '{"longUrl":"https://github.com/zeromicro/go-zero"}'
# 返回 {"shortUrl":"yang.cn/xxxx"}

# 跳转:访问短码,302 重定向到长链接
curl -i http://127.0.0.1:9000/xxxx

# 统计:查询该短链累计点击次数
curl http://127.0.0.1:9000/stats/xxxx
# 返回 {"total":1}
```

### 5. 运行测试

```bash
go test ./...
```

覆盖 Base62 编解码、MD5、URL 工具、可达性探测,以及号段发号器(基于 miniredis 与 fake checkpoint,验证双缓冲切换、崩溃恢复、故障降级)。

---

以上内容依据项目代码与建表 SQL 整理,可作为快速了解本服务的入口;各模块更细的实现说明见对应包内的注释(推荐从 `sequence/segment.go` 与 `pkg/mq/rabbitmq.go` 读起)。

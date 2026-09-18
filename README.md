# 短链接项目


# 搭建项目的骨架
1. 建库建表
   新建发号器表
````sql
   CREATE TABLE `sequence` (
   `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,
   `stub` VARCHAR(1) NOT NULL,
   `timestamp` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
   PRIMARY KEY (`id`),
   UNIQUE KEY `idx_uniq_stub` (`stub`)
   ) ENGINE=MYISAM DEFAULT CHARSET=utf8 COMMENT = '序号表';
````
新建长链接短链接映射表
````sql
CREATE TABLE `short_url_map` (
`id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
`create_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
`create_by` VARCHAR(64) NOT NULL DEFAULT '' COMMENT '创建者',
`is_del` TINYINT UNSIGNED NOT NULL DEFAULT '0' COMMENT '是否删除：0正常1删除',
`lurl` VARCHAR(2048) DEFAULT NULL COMMENT '长链接',
`md5` CHAR(32) DEFAULT NULL COMMENT '长链接MD5',
`surl` VARCHAR(11) DEFAULT NULL COMMENT '短链接',
PRIMARY KEY (`id`),
INDEX(`is_del`),
UNIQUE(`md5`),
UNIQUE(`surl`)
)ENGINE=INNODB DEFAULT CHARSET=utf8mb4 COMMENT = '长短链映射表';
 ````
2. 搭建go-zero框架骨架
   2.1 编写api文件，使用goctl命令生成代码
````api
syntax = "v1"

info(
    title: "短链接项目"
    desc: "短链接重定向跳转长链接"
    author: "yang"
    email: "@2033231795@qq.com"
    version:"1.0"
)
type ConvertRequest{
    LongURL string `json:"longUrl" validate:"required"`
}
type ConvertResponse{
    ShortURL string  `json:"shortUrl"`
}
type ShowRequest{
    ShortURL string  `json:"shortUrl" validate:"required"`
}
type ShowResponse{
    LongURL string `json:"longUrl"`
}
@server (
    prefix :api
)

service shortener-api{
    @handler ConvertHandler
    post /convert (ConvertRequest)returns(ConvertResponse)
    @handler ShowHandler
    get /:shortUrl (ShowRequest)returns(ShowResponse)
}
````
2.2 根据api文件生成go代码
````bash
goctl api go -api shortener.api  -dir .  -style=goZero
````
3. 根据数据表生成model层代码
````bash
goctl model mysql datasource -url="root:123456@tcp(127.0.0.1:3306)/shorteren" -table="sequence" -dir="./shortener/model"  

goctl model mysql datasource -url="root:123456@tcp(127.0.0.1:3306)/shorteren" -table="short_url_map" -dir="./shortener/model" 
````
4. 下载项目依赖
````bash
go mod tidy
````
5. 运行项目
````bash
go run shortener.go
````
看到如下输出代表项目成功启动
````bash
Starting server at 0.0.0.0:8888...
````
6. 修改配置结构体和配置文件
   注意：两边一定一定要对齐！

# 转链参数检验
1.go-zero使用validator
https://pkg.go.dev/github.com/go-playground/validator/v10
下载依赖：
````bash
go get github.com/go-playground/validator/v10
````
导入依赖：
import "github.com/go-playground/validator/v10"
在api中为结构体添加validat额tag 并添加校验规则

# 查看短链接
# 缓存版
有两种方式
1. 使用自己实现的缓存  surl->lurl 能够节省缓存空间，缓存的数据量小
2. 使用go-zero自带的缓存 surl ->数据行，不需要自己实现，开发量小
   这使用第二种方案：
1. 添加缓存配置
- 配置文件
- 配置config结构体
2. 删除旧的model层代码
   -删除 shorturlmapmodel.go
3. 重新生成model层代码
````bash
goctl model mysql datasource -url="root:123456@tcp(127.0.0.1:3306)/mall" -table="short_url_map" -dir="./model" -c
````
4.修改svc层 ServiceContext.go文件 代码文件

# RabbitMQ 访问量统计

基于消息队列的异步点击计数，解析主流程零阻塞，计数最终一致。

## 数据表
`short_url_stats.sql`：跳转计数表，`surl` 唯一键 + `total_count` 累计值。

## RabbitMQ 拓扑
| 组件 | 名称 | 说明 |
|------|------|------|
| 交换机 | `shortener.stats.exchange` | direct，持久化 |
| 队列 | `shortener.stats.click.queue` | 持久化，配置死信转发 |
| 死信交换机/队列 | `shortener.stats.dlx` / `shortener.stats.click.dlq` | 承接格式错误与重试耗尽的消息 |

消息体(JSON)：`msgId`(幂等去重依据) / `surl` / `clickedAt` / `retry`。

## 生产端（pkg/mq + ShowLogic）
1. 布隆过滤器前置拦截逻辑保持不变；DB 查询成功后**异步**投递点击事件（不阻塞重定向）
2. 发布确认(publisher confirm)保证消息到达 broker；失败重试 3 次
3. 重试仍失败 → 消息落盘 `FallbackFile`，后台每分钟扫描重发（补偿机制，不丢消息）
4. 连接断开自动重连并重建拓扑

## 消费端（internal/consumer）
1. **幂等**：Redis `SETNX stats:click:consumed:{msgId}`（TTL 3 天），重复消息直接 ACK 丢弃
2. **MySQL 计数**：`INSERT ... ON DUPLICATE KEY UPDATE total_count+1` 原子累加（数据真源）
3. **Redis 同步**：`INCR stats:click:count:{surl}`，失败仅记录日志（MySQL 为真源，可由查询接口回源修复）
4. **失败重试**：MySQL 写失败 → 删除幂等标记（允许重试）→ 重新投递 retry+1 的消息 → 3 次耗尽进死信队列
5. **宕机安全**：未 ACK 消息由 broker 重投，幂等标记防止重复计数

## 查询接口
`GET /stats/:shortUrl` → `{"total": 123}`
读取顺序：Redis 计数器 → MySQL 回源（SETNX 回填，避免覆盖消费端新值）。

## 关于本地缓存的说明
代码中从未启用进程内 L1 本地缓存（README 早期"方案1 自实现缓存"未落地），
现缓存均为 Redis 版（go-zero sqlc CachedConn + 布隆过滤器 bitSet），计数链路不受本地缓存影响。

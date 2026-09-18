// Package consumer RabbitMQ 消费端。
//
// 点击事件消费流程(单条消息)：
//  1. 消息格式校验，解析失败直接进死信队列
//  2. Redis SETNX(msgId) 幂等校验，重复消息直接 ACK 丢弃
//  3. MySQL INSERT ... ON DUPLICATE KEY UPDATE 原子累加计数(最终真源)
//  4. Redis INCR 同步短链计数值(尽力而为，失败仅记录日志，不回滚 MySQL)
//
// 可靠性保障：
//   - MySQL 写失败：删除幂等标记 → 重试(重新投递 retry+1 的消息) → 3 次耗尽进死信队列
//   - 消费实例宕机：未 ACK 的消息会被 broker 重新投递，幂等标记防止重复计数
//   - 消息重复：SETNX 天然去重；幂等键 TTL 3 天，覆盖 RabbitMQ 常规重投窗口
package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/zeromicro/go-zero/core/logx"

	"shortener/shortener/internal/svc"
	"shortener/shortener/pkg/mq"
)

const (
	idemKeyPrefix   = "stats:click:consumed:" // 幂等标记 key 前缀
	countKeyPrefix  = "stats:click:count:"    // 计数值 key 前缀
	idemTTLSeconds  = 3 * 24 * 3600           // 幂等标记保留3天
	maxRetry        = 3                       // 单条消息最大处理重试次数
	consumePrefetch = 100                     // 预取数，兼顾吞吐与失败重排队效率
	retryDelay      = 3 * time.Second         // 消费循环异常后的重试间隔
)

type ClickConsumer struct {
	svcCtx *svc.ServiceContext
}

// StartClickConsumer 启动点击事件消费者(阻塞运行，建议 go 协程调用)
func StartClickConsumer(ctx context.Context, svcCtx *svc.ServiceContext) {
	c := &ClickConsumer{svcCtx: svcCtx}
	logx.Info("click stats consumer started")
	c.run(ctx)
}

// run 消费主循环：连接断开或通道异常时自动重建，直到 ctx 取消
func (c *ClickConsumer) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			logx.Info("click stats consumer stopped")
			return
		default:
		}
		if err := c.runOnce(ctx); err != nil {
			logx.Errorw("click consumer loop error, retry later",
				logx.Field("err", err.Error()))
		}
		select {
		case <-ctx.Done():
			logx.Info("click stats consumer stopped")
			return
		case <-time.After(retryDelay):
		}
	}
}

func (c *ClickConsumer) runOnce(ctx context.Context) error {
	cfg := c.svcCtx.Config.RabbitMQ
	conn, err := amqp.Dial(cfg.URL)
	if err != nil {
		return fmt.Errorf("consumer dial: %w", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("consumer open channel: %w", err)
	}
	defer ch.Close()

	if err := mq.DeclareTopology(ch); err != nil { //幂等声明，与生产端保持一致
		return err
	}
	// 限流：未确认消息超过 prefetch 不再下发，避免消费者被打垮
	if err := ch.Qos(consumePrefetch, 0, false); err != nil {
		return fmt.Errorf("consumer qos: %w", err)
	}

	deliveries, err := ch.Consume(mq.QueueClick, "click-stats-consumer",
		false, // 手动 ACK
		false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consumer start: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case d, ok := <-deliveries:
			if !ok { // 通道被 broker 关闭，外层循环重建
				return errors.New("consume channel closed")
			}
			c.handle(ch, d)
		}
	}
}

// handle 处理单条点击事件
func (c *ClickConsumer) handle(ch *amqp.Channel, d amqp.Delivery) {
	var event mq.ClickEvent
	if err := json.Unmarshal(d.Body, &event); err != nil || event.Surl == "" || event.MsgID == "" {
		// 消息格式错误属于无法恢复的错误，重试无意义，直接进死信队列
		logx.Errorw("invalid click event, send to dlq", logx.Field("body", string(d.Body)))
		c.toDeadQueue(ch, d)
		return
	}
	if event.Retry >= maxRetry {
		logx.Errorw("click event retry exhausted, send to dlq",
			logx.Field("msgId", event.MsgID), logx.Field("surl", event.Surl))
		c.toDeadQueue(ch, d)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. Redis 幂等校验：SETNX 成功说明首次处理，失败说明重复消息
	ok, err := c.svcCtx.Redis.SetnxEx(idemKey(event.MsgID), "1", idemTTLSeconds)
	if err != nil {
		// Redis 异常属于临时错误，标记状态未知，先清除再重试(防止误判重复导致消息丢失)
		_, _ = c.svcCtx.Redis.Del(idemKey(event.MsgID))
		c.retry(ch, d, event, fmt.Errorf("redis setnx: %w", err))
		return
	}
	if !ok {
		logx.Infow("duplicate click event, dropped", logx.Field("msgId", event.MsgID))
		_ = d.Ack(false)
		return
	}

	// 2. MySQL 累加计数(数据真源)
	if err := c.svcCtx.StatsModel.IncrClick(ctx, event.Surl); err != nil {
		// 关键补偿：删除幂等标记，让重试消息可以被再次处理
		_, _ = c.svcCtx.Redis.Del(idemKey(event.MsgID))
		c.retry(ch, d, event, fmt.Errorf("mysql incr: %w", err))
		return
	}

	// 3. 同步更新 Redis 计数值(尽力而为)
	// 失败不回滚 MySQL：消息已消费必须 ACK，否则重复计数。
	// Redis 计数仅作为查询接口的热点缓存，即使落后也可由查询接口回源 MySQL 修复
	if _, err := c.svcCtx.Redis.Incr(countKey(event.Surl)); err != nil {
		logx.Errorw("redis incr click count failed, mysql is source of truth",
			logx.Field("err", err.Error()),
			logx.Field("surl", event.Surl))
	}

	_ = d.Ack(false)
}

// retry 重新发布一条 retry+1 的消息到点击队列(排队到队尾，实现延迟重试)，并 ACK 原消息。
// 重试次数耗尽后转入死信队列，等待人工介入。
func (c *ClickConsumer) retry(ch *amqp.Channel, d amqp.Delivery, event mq.ClickEvent, cause error) {
	logx.Errorw("handle click event failed, will retry",
		logx.Field("err", cause.Error()),
		logx.Field("msgId", event.MsgID),
		logx.Field("surl", event.Surl),
		logx.Field("retry", event.Retry+1))

	if event.Retry+1 >= maxRetry {
		c.toDeadQueue(ch, d)
		return
	}
	body, err := json.Marshal(mq.ClickEvent{
		MsgID:     event.MsgID,
		Surl:      event.Surl,
		ClickedAt: event.ClickedAt,
		Retry:     event.Retry + 1,
	})
	if err != nil {
		c.toDeadQueue(ch, d)
		return
	}
	err = ch.PublishWithContext(context.Background(), mq.ExchangeClick, mq.RoutingKeyClick,
		false, false, amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         body,
			Headers:      amqp.Table{mq.HeaderRetryCount: event.Retry + 1},
		})
	if err != nil { // 重试消息发布失败，原消息重新入队
		logx.Errorw("republish retry message failed, requeue original",
			logx.Field("err", err.Error()))
		_ = d.Nack(false, true)
		return
	}
	_ = d.Ack(false)
}

// toDeadQueue 消息转入死信队列(依赖队列的 x-dead-letter-exchange 配置)
func (c *ClickConsumer) toDeadQueue(ch *amqp.Channel, d amqp.Delivery) {
	_ = d.Reject(false) // requeue=false + 死信交换机 → 消息进入 dlq
}

func idemKey(msgID string) string {
	return idemKeyPrefix + msgID
}

func countKey(surl string) string {
	return countKeyPrefix + surl
}

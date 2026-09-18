package main

import (
	"context"
	"flag"
	"fmt"
	"shortener/shortener/pkg/base62"

	"shortener/shortener/internal/config"
	"shortener/shortener/internal/consumer"
	"shortener/shortener/internal/handler"
	"shortener/shortener/internal/svc"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/rest"
)

var configFile = flag.String("f", "etc/shortener-api.yaml", "the config file")

func main() {
	flag.Parse()

	// 打印config
	var c config.Config
	conf.MustLoad(*configFile, &c)
	fmt.Printf("config:%#v\n", c)

	base62.MustInit(c.BaseString)
	server := rest.MustNewServer(c.RestConf)
	defer server.Stop()

	ctx := svc.NewServiceContext(c)

	// 启动点击事件消费者：RabbitMQ -> Redis幂等校验 -> MySQL计数 + Redis计数同步
	go consumer.StartClickConsumer(context.Background(), ctx)

	handler.RegisterHandlers(server, ctx)

	fmt.Printf("Starting server at %s:%d...\n", c.Host, c.Port)
	server.Start()
}

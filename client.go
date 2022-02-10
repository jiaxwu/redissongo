package redissongo

import (
	"github.com/brianvoe/gofakeit/v6"
	"github.com/go-redis/redis/v8"
	"time"
)

type Client struct {
	client    *redis.Client // redis客户端
	leaseTime time.Duration // 锁过期时间
}

type Config struct {
	Addr      string        // redis地址
	LeaseTime time.Duration // 锁过期时间
}

func NewClient(config *Config) *Client {
	if config == nil {
		config = new(Config)
	}
	if config.LeaseTime == 0 {
		config.LeaseTime = time.Second * 30
	}
	if config.Addr == "" {
		config.Addr = "127.0.0.1:6379"
	}
	return &Client{
		leaseTime: config.LeaseTime,
		client: redis.NewClient(&redis.Options{
			Addr: config.Addr,
		}),
	}
}

// GetLock 获取可重入锁
func (c *Client) GetLock(resource string) Lock {
	return NewReentrantLock(c.client, resource, gofakeit.UUID(), c.leaseTime)
}

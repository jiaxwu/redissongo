package redissongo

import (
	"github.com/brianvoe/gofakeit/v6"
	"github.com/go-redis/redis/v8"
	"time"
)

// Client 客户端
type Client struct {
	client    *redis.Client // redis客户端
	leaseTime time.Duration // 锁过期时间
	waitTime  time.Duration // 加锁等待时间
}

// Config 配置
type Config struct {
	Addr      string        // redis地址
	LeaseTime time.Duration // 锁过期时间
	WaitTime  time.Duration // 加锁等待时间
}

func NewClient(config *Config) *Client {
	if config == nil {
		config = new(Config)
	}
	if config.LeaseTime == 0 {
		config.LeaseTime = time.Second * 30
	}
	if config.WaitTime == 0 {
		config.WaitTime = time.Second * 60 * 5
	}
	if config.Addr == "" {
		config.Addr = "127.0.0.1:6379"
	}
	return &Client{
		leaseTime: config.LeaseTime,
		waitTime:  config.WaitTime,
		client: redis.NewClient(&redis.Options{
			Addr: config.Addr,
		}),
	}
}

// GetLock 获取可重入锁
func (c *Client) GetLock(resource string) Lock {
	return c.getLock(NewReentrantLock(c.client, resource, gofakeit.UUID(), c.leaseTime))
}

// GetFairLock 获取公平锁
func (c *Client) GetFairLock(resource string) Lock {
	return c.getLock(NewFairLock(c.client, resource, gofakeit.UUID(), c.leaseTime, c.waitTime))
}

// 获取锁
func (c *Client) getLock(innerLock innerLock) Lock {
	return NewBaseLock(c.client, c.leaseTime, innerLock)
}

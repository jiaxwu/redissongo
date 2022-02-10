package redissongo

import (
	"context"
	"github.com/go-redis/redis/v8"
	"time"
)

type ReentrantLock struct {
	client                *redis.Client      // redis客户端
	resource              string             // 资源
	lockName              string             // 锁名字
	leaseTime             time.Duration      // 锁过期时间
	renewExpirationCancel context.CancelFunc // 重置过期时间关闭函数
}

func NewReentrantLock(client *redis.Client, resource, lockName string, leaseTime time.Duration) *ReentrantLock {
	return &ReentrantLock{
		client:    client,
		resource:  resource,
		lockName:  lockName,
		leaseTime: leaseTime,
	}
}

func (l *ReentrantLock) tryLockInner(ctx context.Context, once bool) (int64, error) {
	script := redis.NewScript(
		"if (redis.call('exists', KEYS[1]) == 0) then " +
			"redis.call('hincrby', KEYS[1], ARGV[2], 1); " +
			"redis.call('pexpire', KEYS[1], ARGV[1]); " +
			"return nil; " +
			"end; " +
			"if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
			"redis.call('hincrby', KEYS[1], ARGV[2], 1); " +
			"redis.call('pexpire', KEYS[1], ARGV[1]); " +
			"return nil; " +
			"end; " +
			"return redis.call('pttl', KEYS[1]);")
	return script.Run(ctx, l.client, []string{l.resource}, l.leaseTime.Milliseconds(), l.lockName).Int64()
}

// 尝试加锁失败的回调函数，用于加锁失败后的清理工作
func (l *ReentrantLock) tryLockFailedInner() {}

func (l *ReentrantLock) renewExpirationInner(ctx context.Context) (bool, error) {
	script := redis.NewScript(
		"if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
			"redis.call('pexpire', KEYS[1], ARGV[1]); " +
			"return 1; " +
			"end; " +
			"return 0;")
	return script.Run(ctx, l.client, []string{l.resource}, l.leaseTime.Milliseconds(), l.lockName).Bool()
}

func (l *ReentrantLock) unlockInner(ctx context.Context) (bool, error) {
	script := redis.NewScript(
		"if (redis.call('hexists', KEYS[1], ARGV[3]) == 0) then " +
			"return nil;" +
			"end; " +
			"local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1); " +
			"if (counter > 0) then " +
			"redis.call('pexpire', KEYS[1], ARGV[2]); " +
			"return 0; " +
			"else " +
			"redis.call('del', KEYS[1]); " +
			"redis.call('publish', KEYS[2], ARGV[1]); " +
			"return 1; " +
			"end; " +
			"return nil;")
	return script.Run(ctx, l.client,
		[]string{l.resource, l.channelName()}, unlockMessage, l.leaseTime.Milliseconds(), l.lockName).Bool()
}

func (l *ReentrantLock) channelName() string {
	return "redissongo_lock_channel:" + l.resource
}

func (l *ReentrantLock) subscribeChannelName() string {
	return l.channelName()
}

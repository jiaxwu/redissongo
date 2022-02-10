package redissongo

import (
	"context"
	"errors"
	"github.com/go-redis/redis/v8"
	"time"
)

type ReentrantLock struct {
	client                *redis.Client      // redis客户端
	resource              string             // 资源
	lockName              string             // 锁名字
	channelName           string             // 等待锁订阅的频道名字
	leaseTime             time.Duration      // 锁过期时间
	renewExpirationCancel context.CancelFunc // 重置过期时间关闭函数
}

func NewReentrantLock(client *redis.Client, resource, lockName string, leaseTime time.Duration) *ReentrantLock {
	return &ReentrantLock{
		client:      client,
		resource:    resource,
		lockName:    lockName,
		channelName: "redissongo_lock_channel:" + resource,
		leaseTime:   leaseTime,
	}
}

// Lock 尝试直到获取锁或被关闭
func (l *ReentrantLock) Lock(ctx context.Context) error {
	_, err := l.tryLock(ctx)
	return err
}

// TryLock 尝试获取锁或直到被关闭
func (l *ReentrantLock) TryLock(ctx context.Context) (bool, error) {
	if ctx.Done() == nil {
		return l.tryLockOnce(ctx)
	}
	return l.tryLock(ctx)
}

// 尝试直到获取锁或被关闭
func (l *ReentrantLock) tryLock(ctx context.Context) (bool, error) {
	_, err := l.tryLockAndRenew(ctx, false)
	if err != nil && !errors.Is(err, redis.Nil) {
		l.tryLockFailedInner()
		return false, err
	}
	if errors.Is(err, redis.Nil) {
		return true, nil
	}

	// 已经done则不再尝试
	select {
	case <-ctx.Done():
		l.tryLockFailedInner()
		return false, nil
	default:
	}

	// 订阅频道等待锁
	channel := l.client.Subscribe(ctx, l.channelName)
	defer channel.Unsubscribe(context.Background(), l.channelName)
	for {
		ttl, err := l.tryLockAndRenew(ctx, false)
		if err != nil && !errors.Is(err, redis.Nil) {
			l.tryLockFailedInner()
			return false, err
		}
		if errors.Is(err, redis.Nil) {
			return true, nil
		}
		_, err = channel.ReceiveTimeout(ctx, time.Duration(ttl)*time.Millisecond)
		if err != nil {
			select {
			case <-ctx.Done():
				l.tryLockFailedInner()
				return false, nil
			default:
			}
		}
	}
}

// 尝试获取锁
func (l *ReentrantLock) tryLockOnce(ctx context.Context) (bool, error) {
	_, err := l.tryLockAndRenew(ctx, true)
	if err != nil && !errors.Is(err, redis.Nil) {
		return false, err
	}
	if errors.Is(err, redis.Nil) {
		return true, nil
	}
	return false, nil
}

func (l *ReentrantLock) tryLockAndRenew(ctx context.Context, once bool) (int64, error) {
	ttl, err := l.tryLockInner(ctx, once)
	if err != nil && !errors.Is(err, redis.Nil) {
		return ttl, err
	}
	if errors.Is(err, redis.Nil) {
		// 启动重置过期时间任务
		ctx, cancel := context.WithCancel(context.Background())
		l.renewExpirationCancel = cancel
		go l.renewExpiration(ctx)
	}
	return ttl, err
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

func (l *ReentrantLock) renewExpiration(ctx context.Context) {
	ticker := time.NewTicker(l.leaseTime / 3)
	for {
		select {
		case <-ticker.C:
			ok, err := l.renewExpirationInner(ctx)
			if err != nil || !ok {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (l *ReentrantLock) renewExpirationInner(ctx context.Context) (bool, error) {
	script := redis.NewScript(
		"if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
			"redis.call('pexpire', KEYS[1], ARGV[1]); " +
			"return 1; " +
			"end; " +
			"return 0;")
	return script.Run(ctx, l.client, []string{l.resource}, l.leaseTime.Milliseconds(), l.lockName).Bool()
}

func (l *ReentrantLock) Unlock(ctx context.Context) error {
	_, err := l.unlockInner(ctx)
	if !errors.Is(err, redis.Nil) {
		return err
	}
	return nil
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
		[]string{l.resource, l.channelName}, unlockMessage, l.leaseTime.Milliseconds(), l.lockName).Bool()
}

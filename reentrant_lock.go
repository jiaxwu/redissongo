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
	channelName           string             // 等待锁订阅的频道名字
	leaseTime             time.Duration      // 锁过期时间
	tryLockScript         *redis.Script      // 加锁脚本
	renewExpirationScript *redis.Script      // 重置过期时间脚本
	renewExpirationCancel context.CancelFunc // 重置过期时间关闭函数
	unlockScript          *redis.Script      // 解锁脚本
}

func NewReentrantLock(client *redis.Client, resource, lockName string, leaseTime time.Duration) Lock {
	return &ReentrantLock{
		client:      client,
		resource:    resource,
		lockName:    lockName,
		channelName: "redissongo_lock_channel:" + resource,
		leaseTime:   leaseTime,
		tryLockScript: redis.NewScript(
			"if (redis.call('exists', KEYS[1]) == 0) then " +
				"redis.call('hincrby', KEYS[1], ARGV[2], 1); " +
				"redis.call('pexpire', KEYS[1], ARGV[1]); " +
				"return 0; " +
				"end; " +
				"if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
				"redis.call('hincrby', KEYS[1], ARGV[2], 1); " +
				"redis.call('pexpire', KEYS[1], ARGV[1]); " +
				"return 0; " +
				"end; " +
				"return redis.call('pttl', KEYS[1]);"),
		renewExpirationScript: redis.NewScript(
			"if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
				"redis.call('pexpire', KEYS[1], ARGV[1]); " +
				"return 1; " +
				"end; " +
				"return 0;"),
		unlockScript: redis.NewScript(
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
				"return nil;"),
	}
}

func (l *ReentrantLock) Lock(ctx context.Context) error {
	ctx, _ = context.WithCancel(ctx)
	_, err := l.TryLock(ctx)
	return err
}

func (l *ReentrantLock) TryLock(ctx context.Context) (bool, error) {
	ttl, err := l.tryLock(ctx)
	if err != nil {
		return false, err
	}
	if ttl == 0 {
		return true, nil
	}
	if ctx.Done() == nil {
		return false, nil
	}

	// 已经done则不再尝试
	select {
	case <-ctx.Done():
		return false, nil
	default:
	}

	// 订阅频道等待锁
	channel := l.client.Subscribe(ctx, l.channelName)
	defer channel.Unsubscribe(context.Background(), l.channelName)
	for {
		ttl, err := l.tryLock(ctx)
		if err != nil {
			return false, err
		}
		if ttl == 0 {
			return true, nil
		}
		_, err = channel.ReceiveMessage(ctx)
		if err != nil {
			return false, err
		}
	}
}

func (l *ReentrantLock) tryLock(ctx context.Context) (int, error) {
	cmd := l.tryLockScript.Run(ctx, l.client, []string{l.resource}, l.leaseTime.Milliseconds(), l.lockName)
	ttl, err := cmd.Int()
	if err != nil {
		return ttl, err
	}
	if ttl == 0 {
		// 启动重置过期时间任务
		ctx, cancel := context.WithCancel(context.Background())
		l.renewExpirationCancel = cancel
		go l.renewExpiration(ctx)
	}
	return ttl, nil
}

func (l *ReentrantLock) renewExpiration(ctx context.Context) {
	ticker := time.NewTicker(l.leaseTime / 3)
	for {
		select {
		case <-ticker.C:
			cmd := l.renewExpirationScript.Run(ctx, l.client,
				[]string{l.resource}, l.leaseTime.Milliseconds(), l.lockName)
			ok, err := cmd.Bool()
			if err != nil || !ok {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (l *ReentrantLock) Unlock(ctx context.Context) error {
	cmd := l.unlockScript.Run(ctx, l.client,
		[]string{l.resource, l.channelName}, unlockMessage, l.leaseTime.Milliseconds(), l.lockName)
	return cmd.Err()
}

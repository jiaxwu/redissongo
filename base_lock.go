package redissongo

import (
	"context"
	"errors"
	"github.com/go-redis/redis/v8"
	"time"
)

// BaseLock 锁的模板
type BaseLock struct {
	client                *redis.Client      // redis客户端
	leaseTime             time.Duration      // 锁过期时间
	renewExpirationCancel context.CancelFunc // 重置过期时间关闭函数
	innerLock             innerLock          // 逻辑复用的关键
}

func NewBaseLock(client *redis.Client, leaseTime time.Duration, innerLock innerLock) *BaseLock {
	return &BaseLock{
		client:    client,
		leaseTime: leaseTime,
		innerLock: innerLock,
	}
}

// Lock 尝试直到获取锁或被关闭
func (l *BaseLock) Lock(ctx context.Context) error {
	_, err := l.tryLock(ctx)
	return err
}

// TryLock 尝试获取锁或直到被关闭
func (l *BaseLock) TryLock(ctx context.Context) (bool, error) {
	if ctx.Done() == nil {
		return l.tryLockOnce(ctx)
	}
	return l.tryLock(ctx)
}

// 尝试直到获取锁或被关闭
func (l *BaseLock) tryLock(ctx context.Context) (bool, error) {
	_, err := l.tryLockAndRenew(ctx, false)
	if err != nil && !errors.Is(err, redis.Nil) {
		l.innerLock.tryLockFailedInner()
		return false, err
	}
	if errors.Is(err, redis.Nil) {
		return true, nil
	}

	// 已经done则不再尝试
	select {
	case <-ctx.Done():
		l.innerLock.tryLockFailedInner()
		return false, nil
	default:
	}

	// 订阅频道等待锁
	channel := l.client.Subscribe(ctx, l.innerLock.subscribeChannelName())
	defer channel.Unsubscribe(context.Background(), l.innerLock.subscribeChannelName())
	for {
		ttl, err := l.tryLockAndRenew(ctx, false)
		if err != nil && !errors.Is(err, redis.Nil) {
			l.innerLock.tryLockFailedInner()
			return false, err
		}
		if errors.Is(err, redis.Nil) {
			return true, nil
		}
		if ttl < 0 {
			_, err = channel.ReceiveMessage(ctx)
		} else {
			subCtx, cancel := context.WithTimeout(ctx, time.Duration(ttl)*time.Millisecond)
			_, err = channel.ReceiveMessage(subCtx)
			cancel()
		}
		if err != nil {
			select {
			case <-ctx.Done():
				l.innerLock.tryLockFailedInner()
				return false, nil
			default:
			}
		}
	}
}

// 尝试获取锁
func (l *BaseLock) tryLockOnce(ctx context.Context) (bool, error) {
	_, err := l.tryLockAndRenew(ctx, true)
	if err != nil && !errors.Is(err, redis.Nil) {
		return false, err
	}
	if errors.Is(err, redis.Nil) {
		return true, nil
	}
	return false, nil
}

func (l *BaseLock) tryLockAndRenew(ctx context.Context, once bool) (int64, error) {
	ttl, err := l.innerLock.tryLockInner(ctx, once)
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

func (l *BaseLock) renewExpiration(ctx context.Context) {
	ticker := time.NewTicker(l.leaseTime / 3)
	for {
		select {
		case <-ticker.C:
			ok, err := l.innerLock.renewExpirationInner(ctx)
			if err != nil || !ok {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (l *BaseLock) Unlock(ctx context.Context) error {
	_, err := l.innerLock.unlockInner(ctx)
	if !errors.Is(err, redis.Nil) {
		return err
	}
	return nil
}

package redissongo

import (
	"context"
)

type Lock interface {
	// Lock 获取锁
	Lock(ctx context.Context) error
	// TryLock 尝试获取锁
	TryLock(ctx context.Context) (bool, error)
	// Unlock 释放锁
	Unlock(ctx context.Context) error
}

type innerLock interface {
	tryLockInner(ctx context.Context, once bool) (int64, error)
	// 尝试加锁失败的回调函数，用于加锁失败后的清理工作
	tryLockFailedInner()
	renewExpirationInner(ctx context.Context) (bool, error)
	unlockInner(ctx context.Context) (bool, error)
	// 被订阅的频道名字
	subscribeChannelName() string
}

const (
	unlockMessage     = 0
	readUnlockMessage = 1
)

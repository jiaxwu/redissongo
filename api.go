package redissongo

import (
	"context"
)

type Lock interface {
	// Lock 获取锁
	Lock(context.Context) error
	// TryLock 尝试获取锁
	TryLock(context.Context) (bool, error)
	// Unlock 释放锁
	Unlock(context.Context) error
}

const (
	unlockMessage     = 0
	readUnlockMessage = 1
)

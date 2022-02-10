package redissongo

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"time"
)

type FairLock struct {
	*ReentrantLock
	waitTime         time.Duration // 等待时间
	threadsQueueName string        // 等待队列
	timeoutSetName   string        // 等待超时时间
}

func NewFairLock(client *redis.Client, resource, lockName string, leaseTime, waitTime time.Duration) *FairLock {
	lock := &FairLock{
		ReentrantLock:    NewReentrantLock(client, resource, lockName, leaseTime),
		waitTime:         waitTime,
		threadsQueueName: "redisson_lock_queue:" + resource,
		timeoutSetName:   "redisson_lock_timeout:" + resource,
	}
	return lock
}

func (l *FairLock) tryLockInner(ctx context.Context, once bool) (int64, error) {
	if once {
		script := redis.NewScript( // remove stale threads
			"while true do " +
				"local firstThreadId2 = redis.call('lindex', KEYS[2], 0);" +
				"if firstThreadId2 == false then " +
				"break;" +
				"end;" +
				"local timeout = tonumber(redis.call('zscore', KEYS[3], firstThreadId2));" +
				"if timeout <= tonumber(ARGV[3]) then " +
				// remove the item from the queue and timeout set
				// NOTE we do not alter any other timeout
				"redis.call('zrem', KEYS[3], firstThreadId2);" +
				"redis.call('lpop', KEYS[2]);" +
				"else " +
				"break;" +
				"end;" +
				"end;" +
				"if (redis.call('exists', KEYS[1]) == 0) " +
				"and ((redis.call('exists', KEYS[2]) == 0) " +
				"or (redis.call('lindex', KEYS[2], 0) == ARGV[2])) then " +
				"redis.call('lpop', KEYS[2]);" +
				"redis.call('zrem', KEYS[3], ARGV[2]);" +

				// decrease timeouts for all waiting in the queue
				"local keys = redis.call('zrange', KEYS[3], 0, -1);" +
				"for i = 1, #keys, 1 do " +
				"redis.call('zincrby', KEYS[3], -tonumber(ARGV[4]), keys[i]);" +
				"end;" +
				"redis.call('hset', KEYS[1], ARGV[2], 1);" +
				"redis.call('pexpire', KEYS[1], ARGV[1]);" +
				"return nil;" +
				"end;" +
				"if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
				"redis.call('hincrby', KEYS[1], ARGV[2], 1);" +
				"redis.call('pexpire', KEYS[1], ARGV[1]);" +
				"return nil;" +
				"end;" +
				"return 1;")
		return script.Run(ctx, l.client, []string{l.resource, l.threadsQueueName, l.timeoutSetName},
			l.leaseTime.Milliseconds(), l.lockName, time.Now().UnixMilli(), l.waitTime.Milliseconds()).Int64()
	}

	script := redis.NewScript(
		// remove stale threads
		"while true do " +
			"local firstThreadId2 = redis.call('lindex', KEYS[2], 0);" +
			"if firstThreadId2 == false then " +
			"break;" +
			"end;" +
			"local timeout = tonumber(redis.call('zscore', KEYS[3], firstThreadId2));" +
			"if timeout <= tonumber(ARGV[4]) then " +
			// remove the item from the queue and timeout set
			// NOTE we do not alter any other timeout
			"redis.call('zrem', KEYS[3], firstThreadId2);" +
			"redis.call('lpop', KEYS[2]);" +
			"else " +
			"break;" +
			"end;" +
			"end;" +

			// check if the lock can be acquired now
			"if (redis.call('exists', KEYS[1]) == 0) " +
			"and ((redis.call('exists', KEYS[2]) == 0) " +
			"or (redis.call('lindex', KEYS[2], 0) == ARGV[2])) then " +

			// remove this thread from the queue and timeout set
			"redis.call('lpop', KEYS[2]);" +
			"redis.call('zrem', KEYS[3], ARGV[2]);" +

			// decrease timeouts for all waiting in the queue
			"local keys = redis.call('zrange', KEYS[3], 0, -1);" +
			"for i = 1, #keys, 1 do " +
			"redis.call('zincrby', KEYS[3], -tonumber(ARGV[3]), keys[i]);" +
			"end;" +

			// acquire the lock and set the TTL for the lease
			"redis.call('hset', KEYS[1], ARGV[2], 1);" +
			"redis.call('pexpire', KEYS[1], ARGV[1]);" +
			"return nil;" +
			"end;" +

			// check if the lock is already held, and this is a re-entry
			"if redis.call('hexists', KEYS[1], ARGV[2]) == 1 then " +
			"redis.call('hincrby', KEYS[1], ARGV[2],1);" +
			"redis.call('pexpire', KEYS[1], ARGV[1]);" +
			"return nil;" +
			"end;" +

			// the lock cannot be acquired
			// check if the thread is already in the queue
			"local timeout = redis.call('zscore', KEYS[3], ARGV[2]);" +
			"if timeout ~= false then " +
			// the real timeout is the timeout of the prior thread
			// in the queue, but this is approximately correct, and
			// avoids having to traverse the queue
			"return timeout - tonumber(ARGV[3]) - tonumber(ARGV[4]);" +
			"end;" +

			// add the thread to the queue at the end, and set its timeout in the timeout set to the timeout of
			// the prior thread in the queue (or the timeout of the lock if the queue is empty) plus the
			// threadWaitTime
			"local lastThreadId = redis.call('lindex', KEYS[2], -1);" +
			"local ttl;" +
			"if lastThreadId ~= false and lastThreadId ~= ARGV[2] then " +
			"ttl = tonumber(redis.call('zscore', KEYS[3], lastThreadId)) - tonumber(ARGV[4]);" +
			"else " +
			"ttl = redis.call('pttl', KEYS[1]);" +
			"end;" +
			"local timeout = ttl + tonumber(ARGV[3]) + tonumber(ARGV[4]);" +
			"if redis.call('zadd', KEYS[3], timeout, ARGV[2]) == 1 then " +
			"redis.call('rpush', KEYS[2], ARGV[2]);" +
			"end;" +
			"return ttl;")
	return script.Run(ctx, l.client, []string{l.resource, l.threadsQueueName, l.timeoutSetName},
		l.leaseTime.Milliseconds(), l.lockName, l.waitTime.Milliseconds(), time.Now().UnixMilli()).Int64()
}

// 尝试加锁失败的回调函数，用于加锁失败后的清理工作
func (l *FairLock) tryLockFailedInner() {
	fmt.Println("xxx")
	go func() {
		script := redis.NewScript(
			// get the existing timeout for the thread to remove
			"local queue = redis.call('lrange', KEYS[1], 0, -1);" +
				// find the location in the queue where the thread is
				"local i = 1;" +
				"while i <= #queue and queue[i] ~= ARGV[1] do " +
				"i = i + 1;" +
				"end;" +
				// go to the next index which will exist after the current thread is removed
				"i = i + 1;" +
				// decrement the timeout for the rest of the queue after the thread being removed
				"while i <= #queue do " +
				"redis.call('zincrby', KEYS[2], -tonumber(ARGV[2]), queue[i]);" +
				"i = i + 1;" +
				"end;" +
				// remove the thread from the queue and timeouts set
				"redis.call('zrem', KEYS[2], ARGV[1]);" +
				"redis.call('lrem', KEYS[1], 0, ARGV[1]);")
		ctx, cancel := context.WithTimeout(context.Background(), l.waitTime)
		defer cancel()
		script.Run(ctx, l.client, []string{l.threadsQueueName, l.timeoutSetName}, l.lockName, l.waitTime.Milliseconds())
	}()
}

func (l *FairLock) unlockInner(ctx context.Context) (bool, error) {
	script := redis.NewScript(
		// remove stale threads
		"while true do " +
			"local firstThreadId2 = redis.call('lindex', KEYS[2], 0);" +
			"if firstThreadId2 == false then " +
			"break;" +
			"end; " +
			"local timeout = tonumber(redis.call('zscore', KEYS[3], firstThreadId2));" +
			"if timeout <= tonumber(ARGV[4]) then " +
			"redis.call('zrem', KEYS[3], firstThreadId2); " +
			"redis.call('lpop', KEYS[2]); " +
			"else " +
			"break;" +
			"end; " +
			"end;" +
			"if (redis.call('exists', KEYS[1]) == 0) then " +
			"local nextThreadId = redis.call('lindex', KEYS[2], 0); " +
			"if nextThreadId ~= false then " +
			"redis.call('publish', KEYS[4] .. ':' .. nextThreadId, ARGV[1]); " +
			"end; " +
			"return 1; " +
			"end;" +
			"if (redis.call('hexists', KEYS[1], ARGV[3]) == 0) then " +
			"return nil;" +
			"end; " +
			"local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1); " +
			"if (counter > 0) then " +
			"redis.call('pexpire', KEYS[1], ARGV[2]); " +
			"return 0; " +
			"end; " +
			"redis.call('del', KEYS[1]); " +
			"local nextThreadId = redis.call('lindex', KEYS[2], 0); " +
			"if nextThreadId ~= false then " +
			"redis.call('publish', KEYS[4] .. ':' .. nextThreadId, ARGV[1]); " +
			"end; " +
			"return 1; ")
	return script.Run(ctx, l.client,
		[]string{l.resource, l.threadsQueueName, l.timeoutSetName, l.channelName()},
		unlockMessage, l.leaseTime.Milliseconds(), l.lockName, time.Now().UnixMilli()).Bool()
}

func (l *FairLock) subscribeChannelName() string {
	return l.channelName() + ":" + l.lockName
}

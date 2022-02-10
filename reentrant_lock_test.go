package redissongo

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"sync"
	"testing"
	"time"
)

func TestNewReentrantLock(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})

	lock1 := NewReentrantLock(client, "counter", "lock1", time.Second*30)
	lock2 := NewReentrantLock(client, "counter", "lock2", time.Second*30)
	count := 100000
	n := 0
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < count; i++ {
			lock1.Lock(context.Background())
			n++
			lock1.Unlock(context.Background())
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < count; i++ {
			lock2.Lock(context.Background())
			n++
			lock2.Unlock(context.Background())
		}
	}()
	wg.Wait()

	if n != count*2 {
		t.Errorf("期望是%d 结果是%d\n", count*2, n)
	}
}

func TestReentrantLock(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})

	lock1 := NewReentrantLock(client, "counter", "lock1", time.Second*30)
	lock1.Lock(context.Background())
	lock1.Lock(context.Background())
	time.Sleep(time.Second * 40)
	lock1.Unlock(context.Background())
	lock1.Unlock(context.Background())
}

func TestReentrantLock2(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})

	lock1 := NewReentrantLock(client, "counter", "lock1", time.Second*30)
	lock2 := NewReentrantLock(client, "counter", "lock2", time.Second*30)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		lock1.Lock(context.Background())
		defer wg.Done()
		defer lock1.Unlock(context.Background())
		fmt.Println("lock1 do something")
		time.Sleep(time.Second * 40)
		fmt.Println("lock1 end")
	}()
	go func() {
		lock2.Lock(context.Background())
		defer wg.Done()
		defer lock2.Unlock(context.Background())
		fmt.Println("lock2 do something")
		time.Sleep(time.Second * 10)
		fmt.Println("lock2 end")
	}()
	wg.Wait()
}

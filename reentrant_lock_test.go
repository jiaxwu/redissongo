package redissongo

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestNewReentrantLock(t *testing.T) {
	client := NewClient(nil)
	lock1 := client.GetLock("counter")
	lock2 := client.GetLock("counter")

	count := 100000
	n := 0
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < count; i++ {
			if lock1.Lock(context.Background()) != nil {
				t.Errorf("lock1 lock failed")
				return
			}
			n++
			lock1.Unlock(context.Background())
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < count; i++ {
			if lock2.Lock(context.Background()) != nil {
				t.Errorf("lock2 lock failed")
				return
			}
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
	client := NewClient(nil)
	lock1 := client.GetLock("counter")

	if lock1.Lock(context.Background()) != nil {
		t.Errorf("lock1 lock failed")
		return
	}
	if lock1.Lock(context.Background()) != nil {
		t.Errorf("lock1 lock failed")
		return
	}
	time.Sleep(time.Second * 40)
	lock1.Unlock(context.Background())
	lock1.Unlock(context.Background())
}

func TestReentrantLock2(t *testing.T) {
	client := NewClient(nil)
	lock1 := client.GetLock("counter")
	lock2 := client.GetLock("counter")

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if lock1.Lock(context.Background()) != nil {
			t.Errorf("lock1 lock failed")
			return
		}
		defer lock1.Unlock(context.Background())
		fmt.Println("lock1 do something")
		time.Sleep(time.Second * 40)
		fmt.Println("lock1 end")
	}()
	go func() {
		defer wg.Done()
		if lock2.Lock(context.Background()) != nil {
			t.Errorf("lock2 lock failed")
			return
		}
		defer lock2.Unlock(context.Background())
		fmt.Println("lock2 do something")
		time.Sleep(time.Second * 10)
		fmt.Println("lock2 end")
	}()
	wg.Wait()
}

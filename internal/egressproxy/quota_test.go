package egressproxy

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestQuota(t *testing.T) {
	q := NewQuotaManager()
	now := time.Unix(1, 0)
	q.now = func() time.Time { return now }
	var rel []func()
	for i := 0; i < 20; i++ {
		r, e := q.Acquire("e", fmt.Sprint("p", i))
		if e != nil {
			t.Fatal(i, e)
		}
		rel = append(rel, r)
	}
	if _, e := q.Acquire("e", "p"); e == nil {
		t.Error("burst exceeded")
	}
	for _, r := range rel {
		r()
		r()
	}
	now = now.Add(time.Second)
	for i := 0; i < 10; i++ {
		r, e := q.Acquire("e", fmt.Sprint("q", i))
		if e != nil {
			t.Fatal(e)
		}
		r()
	}
	if _, e := q.Acquire("e", "q"); e == nil {
		t.Error("rate exceeded")
	}
	q = NewQuotaManager()
	var reservations []*Reservation
	for i := 0; i < 16; i++ {
		r, e := q.Reserve("e", "p")
		if e != nil {
			t.Fatal(e)
		}
		reservations = append(reservations, r)
	}
	if _, e := q.Reserve("e", "p"); e == nil {
		t.Error("pending exceeded")
	}
	for _, r := range reservations {
		r.Release()
	}
}
func TestQuotaCapsAndConcurrency(t *testing.T) {
	q := NewQuotaManager()
	q.now = func() time.Time { return time.Unix(1, 0) }
	// Seed tokens to isolate active cap behavior from rate limiting.
	q.exec["e"] = &quotaEntry{tokens: 100, last: q.now()}
	var rs []func()
	for i := 0; i < 32; i++ {
		q.exec["e"].tokens = 20
		r, e := q.Acquire("e", "p")
		if e != nil {
			t.Fatal(e)
		}
		rs = append(rs, r)
	}
	if _, e := q.Acquire("e", "p"); e == nil {
		t.Error("execution cap exceeded")
	}
	for _, r := range rs {
		r()
	}
	q = NewQuotaManager()
	for i := 0; i < 3000; i++ {
		q.exec[fmt.Sprint("e", i)] = &quotaEntry{tokens: 20, last: q.now()}
	}
	rs = nil
	for i := 0; i < 256; i++ {
		r, e := q.Acquire(fmt.Sprint("e", i), "p")
		if e != nil {
			t.Fatal(e)
		}
		rs = append(rs, r)
	}
	if _, e := q.Acquire("e2999", "p"); e == nil {
		t.Error("project cap exceeded")
	}
	for _, r := range rs {
		r()
	}
	q = NewQuotaManager()
	rs = nil
	for i := 0; i < 2048; i++ {
		r, e := q.Acquire(fmt.Sprint("g", i), fmt.Sprint("gp", i))
		if e != nil {
			t.Fatal("global setup", i, e)
		}
		rs = append(rs, r)
	}
	if _, e := q.Acquire("global-over", "global-over"); e == nil {
		t.Error("global cap exceeded")
	}
	for _, r := range rs {
		r()
	}
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, e := q.Acquire(fmt.Sprint("c", i), fmt.Sprint("cp", i))
			if e == nil {
				r()
				r()
			}
		}(i)
	}
	wg.Wait()
}

func TestReservationCannotReactivateAfterRelease(t *testing.T) {
	q := NewQuotaManager()
	r, err := q.Reserve("e", "p")
	if err != nil {
		t.Fatal(err)
	}
	r.Release()
	if err := r.Activate(); err == nil {
		t.Fatal("released reservation activated")
	}
	if len(q.projects) != 0 {
		t.Fatalf("empty project retained: %#v", q.projects)
	}
}

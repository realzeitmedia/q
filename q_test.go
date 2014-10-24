package q

import (
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func init() {
	rand.Seed(time.Now().Unix())
}

func TestBasic(t *testing.T) {
	q := NewQ("./tmp/", "events")
	defer q.Close()
	q.Enqueue("Event 1")
	q.Enqueue("Event 2")
	q.Enqueue("Event 3")
	for _, want := range []string{
		"Event 1",
		"Event 2",
		"Event 3",
	} {
		if got := q.Dequeue(); want != got {
			t.Errorf("Want %#v, got %#v", want, got)
		}
	}
}

func TestEmpty(t *testing.T) {
	// Read should block until there is something.
	q := NewQ("./tmp/", "events")
	defer q.Close()

	ready := make(chan struct{})

	wg := sync.WaitGroup{}
	for i := 1; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ready <- struct{}{}
			if got := q.Dequeue(); got != "hello world" {
				t.Errorf("Want hello, got %#v", got)
			}
		}()
	}
	for i := 1; i < 10; i++ {
		<-ready
	}

	time.Sleep(2 * time.Millisecond)
	for i := 1; i < 10; i++ {
		q.Enqueue("hello world")
	}

	wg.Wait()
}

func TestBig(t *testing.T) {
	// Tests might run in /tmp/<something>/
	if err := os.Mkdir("./testbig/", 0700); err != nil {
		t.Fatalf("Can't make ./testbig/: %v", err)
	}
	defer os.RemoveAll("./testbig")
	q := NewQ("./testbig/", "events")
	defer q.Close()
	eventCount := 10000
	for i := range make([]struct{}, eventCount) {
		q.Enqueue(fmt.Sprintf("Event %d: %s", i, strings.Repeat("0xDEAFBEEF", 300)))
	}
	for i := range make([]struct{}, eventCount) {
		want := fmt.Sprintf("Event %d: %s", i, strings.Repeat("0xDEAFBEEF", 300))
		if got := q.Dequeue(); want != got {
			t.Fatalf("Want for %d: %#v, got %#v", i, want, got)
		}
	}
}

func TestWriteError(t *testing.T) {
	q := NewQ("/no/such/dir", "events")
	defer q.Close()
	eventCount := 10000
	for i := range make([]struct{}, eventCount) {
		q.Enqueue(fmt.Sprintf("Event %d: %s", i, strings.Repeat("0xDEAFBEEF", 300)))
	}
}

func TestAsync(t *testing.T) {
	// Random sleep readers and writers.
	// Tests might run in /tmp/<something>/
	if err := os.Mkdir("./testasync/", 0700); err != nil {
		t.Fatalf("Can't make ./testasync/: %v", err)
	}
	defer os.RemoveAll("./testasync")
	q := NewQ("./testasync/", "events")
	defer q.Close()
	eventCount := 10000
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range make([]struct{}, eventCount) {
			q.Enqueue(fmt.Sprintf("Event %d: %s", i, strings.Repeat("0xDEAFBEEF", 300)))
			time.Sleep(time.Duration(rand.Intn(100)) * time.Microsecond)
		}
	}()
	// Reader is slower.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range make([]struct{}, eventCount) {
			want := fmt.Sprintf("Event %d: %s", i, strings.Repeat("0xDEAFBEEF", 300))
			if got := q.Dequeue(); want != got {
				t.Fatalf("Want for %d: %#v, got %#v", i, want, got)
			}
			time.Sleep(time.Duration(rand.Intn(150)) * time.Microsecond)
		}
	}()
	wg.Wait()
}

func TestMany(t *testing.T) {
	// Read and write a lot of messages, as fast as possible.
	// Takes less than 3 seconds on my machine.
	eventCount := 1000000
	clients := 10

	// Tests might run in /tmp/<something>/
	if err := os.Mkdir("./testamany/", 0700); err != nil {
		t.Fatalf("Can't make ./testamany/: %v", err)
	}
	defer os.RemoveAll("./testamany")
	q := NewQ("./testamany/", "events")
	defer q.Close()
	wg := sync.WaitGroup{}
	payload := strings.Repeat("0xDEAFBEEF", 30)

	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < eventCount/clients; j++ {
				q.Enqueue(payload)
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range make([]struct{}, eventCount) {
			// The inserts are non-derministic, so we can't have an interesting
			// payload.
			if got := q.Dequeue(); payload != got {
				t.Fatalf("Want for %d: %#v, got %#v", i, payload, got)
			}
		}
	}()
	wg.Wait()
}

package q

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"reflect"
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

func TestBatchSerialize(t *testing.T) {
	b := &batch{}
	b.enqueue("first")
	b.enqueue("second")
	b.enqueue("third")

	// batch to bytes...
	var buf bytes.Buffer
	err := b.serialize(&buf)
	if err != nil {
		t.Fatalf("unexpected serialize error: %v", err)
	}
	want := "QQ\x03\x00\x00\x00\x05\x00\x00\x00first\x06\x00\x00\x00second\x05\x00\x00\x00thirdTheEnd"
	got := buf.String()
	if got != want {
		t.Errorf("serialize error. Got \n%#v, want \n%#v", got, want)
	}

	// ...and back again to batch.
	again, err := deserialize(bytes.NewBufferString(got))
	if err != nil {
		t.Fatalf("unexpected deserialize error: %v", err)
	}
	if !reflect.DeepEqual(again, b) {
		t.Fatalf("deserialize not the same. Want %#v, got %#v", b, again)
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

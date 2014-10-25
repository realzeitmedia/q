package q

import (
	"strings"
	"testing"
)

func BenchmarkSeq(b *testing.B) {
	// First writed a bunch of messages, then read them.
	var (
		eventCount = 100000
		payload    = strings.Repeat("0xDEAFBEEF", 30)
	)

	d := setupDataDir()
	q, err := NewQ(d, "events")
	if err != nil {
		b.Fatal(err)
	}
	defer q.Close()

	for i := 0; i < b.N; i++ {
		for j := 0; j < eventCount; j++ {
			q.Enqueue(payload)
		}
		for j := 0; j < eventCount; j++ {
			if got := <-q.Queue(); payload != got {
				b.Fatalf("Want for %d: %#v, got %#v", i, payload, got)
			}
		}
	}
}

func BenchmarkMulti(b *testing.B) {
	// Read and write at the same time.
	var (
		eventCount = 100000
		clients    = 10
		payload    = strings.Repeat("0xDEAFBEEF", 30)
	)

	d := setupDataDir()
	q, err := NewQ(d, "events")
	if err != nil {
		b.Fatal(err)
	}
	defer q.Close()

	for i := 0; i < b.N; i++ {
		for i := 0; i < clients; i++ {
			go func() {
				for j := 0; j < eventCount/clients; j++ {
					q.Enqueue(payload)
				}
			}()
		}

		for i := range make([]struct{}, eventCount) {
			if got := <-q.Queue(); payload != got {
				b.Fatalf("Want for %d: %#v, got %#v", i, payload, got)
			}
		}
	}
}

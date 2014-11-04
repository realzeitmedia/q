package q_test

import (
	"strings"
	"testing"

	libq "github.com/alicebob/q"
)

func BenchmarkSeq(b *testing.B) {
	// First writed a bunch of messages, then read them.
	var (
		eventCount = 100000
		payload    = strings.Repeat("0xDEAFBEEF", 30)
	)

	d := setupDataDir()
	q, err := libq.NewQ(d, "events", libq.BlockCount(1000))
	if err != nil {
		b.Fatal(err)
	}
	defer q.Close()

	for i := 0; i < b.N; i++ {
		for j := 0; j < eventCount+1; j++ {
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
	// Read and write at the same time. Many writers, the reader can't keep up.
	var (
		eventCount = 100000
		clients    = 10
		payload    = strings.Repeat("0xDEAFBEEF", 30)
	)

	d := setupDataDir()
	q, err := libq.NewQ(d, "events", libq.BlockCount(1000))
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
		q.Enqueue(payload) // flushes the buffer.

		for i := range make([]struct{}, eventCount) {
			if got := <-q.Queue(); payload != got {
				b.Fatalf("Want for %d: %#v, got %#v", i, payload, got)
			}
		}
	}
}

func BenchmarkStarved(b *testing.B) {
	// Reader which can keep up with the writer.
	var (
		eventCount = 100000
		payload    = strings.Repeat("0xDEAFBEEF", 30)
	)

	d := setupDataDir()
	q, err := libq.NewQ(d, "events", libq.BlockCount(500))
	if err != nil {
		b.Fatal(err)
	}
	defer q.Close()

	for i := 0; i < b.N; i++ {
		go func() {
			for j := 0; j < eventCount+1; j++ {
				q.Enqueue(payload)
			}
		}()

		for i := range make([]struct{}, eventCount) {
			if got := <-q.Queue(); payload != got {
				b.Fatalf("Want for %d: %#v, got %#v", i, payload, got)
			}
		}
	}
}

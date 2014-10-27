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

func setupDataDir() string {
	os.RemoveAll("./d")
	if err := os.Mkdir("./d/", 0700); err != nil {
		panic(fmt.Sprintf("Can't make ./d/: %v", err))
	}
	return "./d"
}

func TestBasic(t *testing.T) {
	// Non-file based queueing.
	d := setupDataDir()
	q, err := NewQ(d, "events")
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if got := q.Count(); 0 != got {
		t.Errorf("Want 0, got %#v", got)
	}
	q.Enqueue("Event 1")
	q.Enqueue("Event 2")
	q.Enqueue("")
	q.Enqueue("Event 3")
	if got := q.Count(); 4 != got {
		t.Errorf("Want 4, got %#v", got)
	}

	for _, want := range []string{
		"Event 1",
		"Event 2",
		"",
		"Event 3",
	} {
		if got := <-q.Queue(); want != got {
			t.Errorf("Want %#v, got %#v", want, got)
		}
	}
	if got := q.Count(); 0 != got {
		t.Errorf("Want 0, got %#v", got)
	}
}

func TestBlock(t *testing.T) {
	// Read should block until there is something.
	d := setupDataDir()
	q, err := NewQ(d, "events")
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	ready := make(chan struct{})

	wg := sync.WaitGroup{}
	for i := 1; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ready <- struct{}{}
			if got := <-q.Queue(); got != "hello world" {
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
	// Queue a lot of elements.
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	d := setupDataDir()
	checkFiles := func(want int) {
		if got := fileCount(d); got != want {
			t.Fatalf("Wrong number of files: got %d, want %d", got, want)
		}
	}
	checkFiles(0)

	q, err := NewQ(d, "events")
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	eventCount := 10000
	for i := range make([]struct{}, eventCount) {
		q.Enqueue(fmt.Sprintf("Event %d: %s", i, strings.Repeat("0xDEAFBEEF", 300)))
	}
	// There should be something stored on disk.
	checkFiles(8)
	for i := range make([]struct{}, eventCount) {
		want := fmt.Sprintf("Event %d: %s", i, strings.Repeat("0xDEAFBEEF", 300))
		if got := <-q.Queue(); want != got {
			t.Fatalf("Want for %d: %#v, got %#v", i, want, got)
		}
	}
	// Everything is processed. All files should be gone.
	checkFiles(0)
}

func TestWriteError(t *testing.T) {
	_, err := NewQ("/no/such/dir", "events")
	if err == nil {
		t.Fatalf("Didn't expect to be able to write.")
	}
}

func TestNoPrefix(t *testing.T) {
	// Need a non-nil prefix.
	d := setupDataDir()
	_, err := NewQ(d, "")
	if err == nil {
		t.Fatalf("Didn't expect to be able to write.")
	}
}

func TestAsync(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	// Random sleep readers and writers.
	d := setupDataDir()
	q, err := NewQ(d, "events")
	if err != nil {
		t.Fatal(err)
	}
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
			if got := <-q.Queue(); want != got {
				t.Fatalf("Want for %d: %#v, got %#v", i, want, got)
			}
			time.Sleep(time.Duration(rand.Intn(150)) * time.Microsecond)
		}
	}()
	wg.Wait()
}

func TestMany(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	// Read and write a lot of messages, as fast as possible.
	// Takes less than 3 seconds on my machine.
	eventCount := 1000000
	clients := 10

	d := setupDataDir()
	q, err := NewQ(d, "events")
	if err != nil {
		t.Fatal(err)
	}
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
			if got := <-q.Queue(); payload != got {
				t.Fatalf("Want for %d: %#v, got %#v", i, payload, got)
			}
		}
	}()
	wg.Wait()
}

func TestReopen1(t *testing.T) {
	// Simple reopening.
	d := setupDataDir()
	q, err := NewQ(d, "events")
	if err != nil {
		t.Fatal(err)
	}
	q.Enqueue("Message 1")
	q.Enqueue("Message 2")
	q.Close()

	q, err = NewQ("./d/", "events")
	if err != nil {
		t.Fatal(err)
	}
	if got := q.Count(); got != 2 {
		t.Fatalf("Want 2, got %d msgs", got)
	}
	<-q.Queue()
	<-q.Queue()
	if got := q.Count(); got != 0 {
		t.Fatalf("Want 0, got %d msgs", got)
	}
	q.Close()
}

func TestReopen2(t *testing.T) {
	// Reopening with different read and write batches.
	d := setupDataDir()
	q, err := NewQ(d, "events")
	if err != nil {
		t.Fatal(err)
	}

	// We want at least two files.
	var i int
	for ; fileCount("./d") < 2; i++ {
		q.Enqueue("...the sun shines. Raaain. When the rain comes, they run and hide their heads")
	}
	q.Close()

	q, err = NewQ("./d/", "events")
	if err != nil {
		t.Fatal(err)
	}
	if got := q.Count(); got != uint(i) {
		t.Fatalf("Want %d, got %d msgs", i, got)
	}
	for ; i > 0; i-- {
		<-q.Queue()
	}
	if got := q.Count(); got != 0 {
		t.Fatalf("Want 0, got %d msgs", got)
	}
	// q.Close()
}

func TestNotAString(t *testing.T) {
	// Queue not-a-string.
	d := setupDataDir()
	q, err := NewQ(d, "i")
	if err != nil {
		t.Fatal(err)
	}
	q.Enqueue(1)
	q.Enqueue(42)
	q.Close()

	q, err = NewQ("./d/", "i")
	if err != nil {
		t.Fatal(err)
	}
	if got := q.Count(); got != 2 {
		t.Fatalf("Want 2, got %d msgs", got)
	}
	for _, want := range []int{1, 42} {
		if got := <-q.Queue(); got != want {
			t.Fatalf("Want %v, got %v msgs", want, got)
		}
	}
	if got := q.Count(); got != 0 {
		t.Fatalf("Want 0, got %d msgs", got)
	}
	q.Close()
}

// fileCount is a helper to count files in a directory.
func fileCount(dir string) int {
	fh, _ := os.Open(dir)
	defer fh.Close()
	n, err := fh.Readdirnames(-1)
	if err != nil {
		panic(err)
	}
	return len(n)
}

package q_test

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	libq "github.com/alicebob/q"
)

func init() {
	rand.Seed(time.Now().Unix())
	log.SetOutput(ioutil.Discard)
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
	q, err := libq.NewQ(d, "events", libq.BlockCount(3))
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
	q.Enqueue("Event 3") // Thisone closes the previous block.
	/*
		if got := q.Count(); got != 3 {
			t.Errorf("Want 3, got %#v", got)
		}
	*/

	for _, want := range []string{
		"Event 1",
		"Event 2",
		"",
		// "Event 3",
	} {
		if got := <-q.Queue(); want != got {
			t.Errorf("Want %#v, got %#v", want, got)
		}
	}
	time.Sleep(10 * time.Millisecond)
	if got := q.Count(); 0 != got {
		t.Errorf("Want 0, got %#v", got)
	}
	if got, want := q.DiskUsage(), int64(0); want != got {
		t.Errorf("Want %d, got %d", want, got)
	}
}

func TestBlock(t *testing.T) {
	// Read should block until there is something.
	d := setupDataDir()
	q, err := libq.NewQ(d, "events", libq.BlockCount(1))
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	ready := make(chan struct{})

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		ready <- struct{}{}
		if got := <-q.Queue(); got != "hello world" {
			t.Errorf("Want hello, got %#v", got)
		}
	}()
	<-ready

	time.Sleep(2 * time.Millisecond)
	q.Enqueue("hello world")
	q.Enqueue("flush")

	wg.Wait()
}

func TestBig(t *testing.T) {
	// Queue a lot of elements.
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	d := setupDataDir()

	q, err := libq.NewQ(d, "events", libq.BlockCount(1000))
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	eventCount := 10000
	for i := range make([]struct{}, eventCount+1) {
		q.Enqueue(fmt.Sprintf("Event %d: %s", i, strings.Repeat("0xDEADBEEF", 300)))
	}
	// fmt.Printf("Go read\n")
	// There should be something stored on disk.
	if got, want := fileCount(d), 7; got != want {
		t.Fatalf("Wrong number of files: got %d, want %d", got, want)
	}
	time.Sleep(10 * time.Millisecond) // Enqueue()s are buffered.
	// The wanted size was emperically determined.
	if got, want := q.DiskUsage(), int64(24120200); want != got {
		t.Errorf("Want %d, got %d", want, got)
	}
	for i := range make([]struct{}, eventCount) {
		want := fmt.Sprintf("Event %d: %s", i, strings.Repeat("0xDEADBEEF", 300))
		if got := <-q.Queue(); want != got {
			t.Fatalf("Want for %d: %#v, got %#v", i, want, got)
		}
	}
	// Everything is processed. All files should be gone.
	if got, want := fileCount(d), 0; got != want {
		t.Fatalf("Wrong number of files: got %d, want %d", got, want)
	}
}

func TestWriteError(t *testing.T) {
	_, err := libq.NewQ("/no/such/dir", "events")
	if err == nil {
		t.Fatalf("Didn't expect to be able to write.")
	}
}

func TestInvalidPrefix(t *testing.T) {
	// Need a non-nil prefix.
	d := setupDataDir()
	for prefix, valid := range map[string]bool{
		"":        false,
		"foobar":  true,
		"foo/bar": false,
		"foo-bar": false,
	} {
		_, err := libq.NewQ(d, prefix)
		if (err != nil) == valid {
			t.Fatalf("Invalid invalid prefix: %s", prefix)
		}
	}
}

func TestAsync(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	// Random sleep readers and writers.
	d := setupDataDir()
	q, err := libq.NewQ(d, "events", libq.BlockCount(1000))
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	eventCount := 10000
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range make([]struct{}, eventCount+1) {
			q.Enqueue(fmt.Sprintf("Event %d: %s", i, strings.Repeat("0xDEADBEEF", 300)))
			time.Sleep(time.Duration(rand.Intn(100)) * time.Microsecond)
		}
	}()
	// Reader is a little slower.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range make([]struct{}, eventCount) {
			want := fmt.Sprintf("Event %d: %s", i, strings.Repeat("0xDEADBEEF", 300))
			if got := <-q.Queue(); want != got {
				t.Fatalf("Want for %d: %#v, got %#v", i, want, got)
			}
			time.Sleep(time.Duration(rand.Intn(150)) * time.Microsecond)
		}
	}()
	wg.Wait()
}

func TestMany(t *testing.T) {
	// Read and write a lot of messages, as fast as possible.
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	eventCount := 1000000
	clients := 10

	d := setupDataDir()
	q, err := libq.NewQ(d, "events", libq.BlockCount(1000))
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	wg := sync.WaitGroup{}
	payload := strings.Repeat("0xDEADBEEF", 30)

	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < eventCount/clients; j++ {
				q.Enqueue(payload)
			}
		}()
	}
	wg.Wait()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range make([]struct{}, eventCount-5001) {
			// The inserts are non-derministic, so we can't have an interesting
			// payload.
			if got := <-q.Queue(); payload != got {
				t.Fatalf("Want for %d: %#v, got %#v", i, payload, got)
			}
		}
	}()
	wg.Wait()
}

func TestMany2(t *testing.T) {
	// Test with a few readers. They will be starved.
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	eventCount := 1000000
	clients := 100

	d := setupDataDir()
	q, err := libq.NewQ(d, "events", libq.BlockCount(1000))
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	wg := sync.WaitGroup{}
	payload := strings.Repeat("0xDEADBEEF", 30)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < eventCount+1; j++ {
			q.Enqueue(payload)
		}
	}()

	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < eventCount/clients; i++ {
				if got := <-q.Queue(); payload != got {
					t.Fatalf("Want %#v, got %#v", payload, got)
				}
			}
		}()
	}
	wg.Wait()
}

func TestReopen1(t *testing.T) {
	// Simple reopening.
	d := setupDataDir()
	q, err := libq.NewQ(d, "events")
	if err != nil {
		t.Fatal(err)
	}
	q.Enqueue("Message 1")
	q.Enqueue("Message 2")
	q.Close()

	q, err = libq.NewQ("./d/", "events")
	if err != nil {
		t.Fatal(err)
	}
	/*
		if got := q.Count(); got != 2 {
			t.Fatalf("Want 2, got %d msgs", got)
		}
	*/
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
	q, err := libq.NewQ(d, "events", libq.BlockCount(1000))
	if err != nil {
		t.Fatal(err)
	}

	// We want at least two files.
	for i := 0; i < 2001; i++ {
		q.Enqueue("...the sun shines. Raaain. When the rain comes, they run and hide their heads")
	}
	q.Close()

	q, err = libq.NewQ("./d/", "events")
	if err != nil {
		t.Fatal(err)
	}
	// Counts are not restored on reload.
	if got, want := q.Count(), 0; got != want {
		t.Fatalf("Want %d, got %d msgs", want, got)
	}
	for i := 0; i < 2000; i++ {
		<-q.Queue()
	}
	if got, want := q.Count(), 1; got != want {
		t.Fatalf("Want %d, got %d msgs", want, got)
	}
	q.Close()
}

/*
func TestNotAString(t *testing.T) {
	// Queue not-a-string.
	d := setupDataDir()
	q, err := libq.NewQ(d, "i")
	if err != nil {
		t.Fatal(err)
	}
	q.Enqueue(1)
	q.Enqueue(42)
	q.Close()

	if q, err = libq.NewQ("./d/", "i"); err != nil {
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

*/

func TestReadOnly(t *testing.T) {
	// Only reading doesn't block the close.
	d := setupDataDir()
	q, err := libq.NewQ(d, "i")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-q.Queue():
		t.Fatal("Impossible read")
	default:
	}
	q.Close()
}

func TestEmptyRead(t *testing.T) {
	// Can't read an empty queue after open.
	d := setupDataDir()
	q, err := libq.NewQ(d, "i")
	if err != nil {
		t.Fatal(err)
	}
	q.Enqueue("1")
	q.Enqueue("42")
	q.Close()

	if q, err = libq.NewQ("./d/", "i"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-q.Queue():
		t.Fatal("Impossible read")
	default:
	}
	q.Close()
}

func TestMaxFiles(t *testing.T) {
	// Limit the disk size.

	eventCount := 10000
	// 1000 * ~100bytes =~ 100Kb per block.
	payload := strings.Repeat("0xDEADBEEF", 10)

	d := setupDataDir()
	q, err := libq.NewQ(d, "events", libq.MaxDiskUsage(350*1024), libq.BlockCount(1000))
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	for j := 0; j < eventCount+1; j++ {
		q.Enqueue(fmt.Sprintf("%d: %s", j, payload))
	}
	// There should be just three files left.
	if got, want := fileCount(d), 3; got != want {
		t.Fatalf("Wrong number of files: got %d, want %d", got, want)
	}

	if got, want := q.Count(), 4000; got != want {
		t.Errorf("Want %d, got %d", want, got)
	}

	// Latest entries are discarded.
	for i := 0; i < 4000; i++ {
		want := fmt.Sprintf("%d: %s", i, payload)
		if got := <-q.Queue(); want != got {
			t.Fatalf("Want %#v, got %#v", want, got)
		}
	}
	if got, want := q.Count(), 0; got != want {
		t.Errorf("Want %d, got %d", want, got)
	}
}

func TestMaxFilesOldest(t *testing.T) {
	// Limit the disk size, but keep recent entries.

	eventCount := 10000
	// 1000 * ~100bytes =~ 100Kb per block.
	payload := strings.Repeat("0xDEADBEEF", 10)

	d := setupDataDir()
	q, err := libq.NewQ(d, "events",
		libq.BlockCount(1000), libq.MaxDiskUsage(400*1024), libq.EvictOldest())
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	for j := 0; j < eventCount+1; j++ {
		q.Enqueue(fmt.Sprintf("%d: %s", j, payload))
	}
	// There should be just three files left.
	if got, want := fileCount(d), 3; got != want {
		t.Fatalf("Wrong number of files: got %d, want %d", got, want)
	}

	if got, want := q.Count(), 4000; got != want {
		t.Errorf("Want %d, got %d", want, got)
	}

	// Newest entries are discarded. The first two blocks are not discarded,
	// since they are being read from.
	for i := 0; i < 2000; i++ {
		want := fmt.Sprintf("%d: %s", i, payload)
		if got := <-q.Queue(); want != got {
			t.Fatalf("Want %#v, got %#v", want, got)
		}
	}
	for i := 7000; i < eventCount; i++ {
		want := fmt.Sprintf("%d: %s", i, payload)
		if got := <-q.Queue(); want != got {
			t.Fatalf("Want %#v, got %#v", want, got)
		}
	}
	if got, want := q.Count(), 0; got != want {
		t.Errorf("Want %d, got %d", want, got)
	}
}

func TestTimeout(t *testing.T) {
	// Maxage for a queue chunk
	d := setupDataDir()
	q, err := libq.NewQ(d, "events",
		libq.BlockCount(10000), libq.Timeout(10*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	eventCount := 100
	payload := "readme"
	for j := 0; j < eventCount+1; j++ {
		q.Enqueue(payload)
	}
	for i := 0; i < eventCount; i++ {
		if have, want := <-q.Queue(), payload; have != want {
			t.Fatalf("Have %#v, want %#v", have, want)
		}
	}
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

package q

// TODO: check write permissions on startup.
// TODO: something better with errors. Maybe an error channel?

import (
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	defaultblockCount    = 1024
	defaultEvicionPolicy = evictNewest
	magicNumber          = "QQ"
	fileExtension        = ".q"
)

var (
	// ErrInvalidPrefix is potentially returned by NewQ.
	ErrInvalidPrefix = errors.New("invalid prefix")
)

type queuechunk chan string

// Q is a queue which will use disk storage if it's too bog.
type Q struct {
	dir, prefix    string
	blockElemCount uint
	maxDiskUsage   int64          // in bytes
	evictionPolicy evictionPolicy // for maxDiskUsage
	chunkTimeout   time.Duration
	enqueue        queuechunk
	dequeue        queuechunk
	countReq       chan chan int
	diskusageReq   chan chan int64
	quit           chan chan struct{}
}

// evictionPolicy determines if oldest or youngest files are removed on full
// disk.
type evictionPolicy int

const (
	evictNewest evictionPolicy = iota
	evictOldest                = iota
)

type configcb func(q *Q)

// MaxDiskUsage is an option for NewQ to limit the max disk spaced used (in bytes).
// The default policy is to discard most recent entries, but that can be
// changed with EvictOldest.
func MaxDiskUsage(byteCount int64) configcb {
	return func(q *Q) {
		q.maxDiskUsage = byteCount
	}
}

// BlockCount is an option for NewQ. It is the number of entries the queue can
// have before entries are written to disk. It's also the number of entries per
// file.  The default is 1024.
func BlockCount(count uint) configcb {
	return func(q *Q) {
		q.blockElemCount = count
	}
}

// EvictOldest is an option for NewQ, which modified the MaxDiskUsage
// bahaviour. It makes MaxDiskUsage() remove oldest entries, while the default
// is the opposite.
func EvictOldest() configcb {
	return func(q *Q) {
		q.evictionPolicy = evictOldest
	}
}

// Timeout is an option for NewQ, which gives every element a max time it'll be
// queued.
func Timeout(t time.Duration) configcb {
	return func(q *Q) {
		q.chunkTimeout = t
	}
}

// NewQ makes or opens a Q, with files in and from <dir>/<prefix>-<timestamp>.q .
// `prefix` needs to be a simple, alphanumeric string.
func NewQ(dir, prefix string, configs ...configcb) (*Q, error) {
	if len(prefix) == 0 || strings.ContainsAny(prefix, ":-/") {
		return nil, ErrInvalidPrefix
	}
	q := Q{
		dir:            dir,
		prefix:         prefix,
		blockElemCount: defaultblockCount,
		evictionPolicy: defaultEvicionPolicy,
		enqueue:        make(queuechunk),
		dequeue:        make(queuechunk),
		countReq:       make(chan chan int),
		diskusageReq:   make(chan chan int64),
		quit:           make(chan chan struct{}),
	}
	for _, cb := range configs {
		cb(&q)
	}
	existing, err := q.findExisting()
	if err != nil {
		return nil, err
	}

	enqueuerWg := sync.WaitGroup{}
	loopWg := sync.WaitGroup{}

	incomingChunks := make(chan queuechunk, 1)
	outgoingChunks := make(chan queuechunk)
	queues := q.loadExisting(existing)

	// selectQueueRead is either outgoingChunks or nil. It's is used to disable
	// a select{} case when there is no chunk avialable to read.
	selectQueueRead := outgoingChunks
	// Prepare an empty chunk so the selectQueueRead switch case is triggered.
	readQueue := make(queuechunk)
	close(readQueue)

	// Quit monitor.
	go func() {
		c := <-q.quit
		// stop the enqueuer
		close(q.enqueue)
		enqueuerWg.Wait()

		// The main loop should shut down, so we can close the outgoingChunks
		loopWg.Wait()

		// Save the non-read messages to disk.
		// Note: the reader might still be reading from the channel, and hence
		// some messages might get rearranged.
		{
			batch := newBatch(q.dequeue)
			if batch.len() > 0 {
				if _, err := batch.saveToDisk(q.batchFilename(0)); err != nil {
					log.Printf("error writing batch to disk: %v", err)
				}
			}
		}
		// The dequeuer must be down by now. It closed the dequeue channel.

		c <- struct{}{}
	}()

	loopWg.Add(1)
	go func() {
		defer loopWg.Done()
		for {
			select {
			case queue, ok := <-incomingChunks:
				// The writer deemed the last one full.
				if !ok {
					// incomingChunks is closed. Quit the loop.
					close(outgoingChunks)
					if readQueue != nil {
						// Save the queue which will never be reached anymore.
						batch := newBatch(readQueue)
						if _, err := batch.saveToDisk(q.batchFilename(1)); err != nil {
							log.Printf("error writing batch to disk: %v", err)
						}
					}
					return
				}

				if selectQueueRead == nil {
					// Keep this queue in memory, it'll be used as the next
					// queue.
					selectQueueRead = outgoingChunks
					readQueue = queue
					break
				}

				// It's not being read from. Store it.
				batch := newBatch(queue)
				filename := q.batchFilename(time.Now().UnixNano())
				fileSize, err := batch.saveToDisk(filename)
				if err != nil {
					log.Printf("error writing batch to disk: %v", err)
					break
				}
				queues = append(queues, storedBatch{
					elemCount: batch.len(),
					fileSize:  int64(fileSize),
					filename:  filename,
				})
				queues = q.limitDiskUsage(queues)

			case selectQueueRead <- readQueue:
				// The last complete block is dequeued. See if there is
				// something stored on disk.
			AGAIN:
				if len(queues) > 0 {
					readBatch := queues[0]
					queues = queues[1:]

					var err error
					readQueue, err = openBatch(readBatch.filename)
					if err != nil {
						// Skip this file for this run. Don't delete it.
						log.Printf("open batch %v error: %v", readBatch.filename, err)
						goto AGAIN
					}
					if err = os.Remove(readBatch.filename); err != nil {
						log.Printf("can't remove batch: %v, igoring", err)
						// otherwise we would replay it again next run.
						goto AGAIN
					}
					break
				}
				// Nothing there. Wait for a new chunk.
				readQueue = nil
				selectQueueRead = nil

			case r := <-q.countReq:
				count := len(readQueue)
				for _, b := range queues {
					count += b.elemCount
				}
				r <- count

			case r := <-q.diskusageReq:
				bcount := int64(0)
				for _, b := range queues {
					bcount += b.fileSize
				}
				r <- bcount
			}
		}
	}()

	q.readLoop(&enqueuerWg, incomingChunks, q.chunkTimeout)
	q.writeLoop(outgoingChunks)

	return &q, nil
}

// Close will write all pending queue entries to disk before closing.
func (q *Q) Close() {
	c := make(chan struct{})
	q.quit <- c
	<-c
}

// Enqueue adds a message to the queue. Can be called from different Go
// routines.
func (q *Q) Enqueue(m string) {
	q.enqueue <- m
}

// Queue gives the channel to read queue entries from. Multiple readers of this
// channel is OK. The channel will be closed on shutdown via Close().
func (q *Q) Queue() <-chan string {
	return q.dequeue
}

// Count gives the total number of entries in the queue kept on disk for this
// session, and a few from memory. This method is only for ballpark numbers.
func (q *Q) Count() int {
	r := make(chan int)
	q.countReq <- r
	return <-r
}

// DiskUsage gives the number of bytes used on disk. Entries in memory are not
// counted.
func (q *Q) DiskUsage() int64 {
	r := make(chan int64)
	q.diskusageReq <- r
	return <-r
}

type storedBatch struct {
	filename  string
	elemCount int
	fileSize  int64
}

// findExisting gives the filenames of saved batches.
func (q *Q) findExisting() ([]string, error) {
	f, err := os.Open(q.dir)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var existing []string
	names, err := f.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	for _, name := range names {
		if !strings.HasPrefix(name, q.prefix+"-") || !strings.HasSuffix(name, fileExtension) {
			continue
		}
		existing = append(existing, name)
	}
	sort.Strings(existing)
	return existing, nil
}

// loadExisting makes storedBatch{}es from from stored queues. Used on
// restoreing an old instance.
func (q *Q) loadExisting(existing []string) []storedBatch {
	var batches []storedBatch
	// Put all files in storedBatch objects, with their size. Queue length is
	// too expensive to figure out.
	for _, f := range existing {
		filename := q.dir + "/" + f
		stat, err := os.Stat(filename)
		if err != nil {
			log.Printf("stat batch %v error: %v. Ignoring", f, err)
			continue
		}
		batches = append(batches, storedBatch{
			filename:  filename,
			elemCount: 0,
			fileSize:  stat.Size(),
		})
	}
	return batches
}

// batchFilename generates a filename to save a batch. It's intended to be
// used with a unix nano timestamp.
func (q *Q) batchFilename(id int64) string {
	return fmt.Sprintf("%s/%s-%020d%s", q.dir, q.prefix, id, fileExtension)
}

// limitDiskUsage delete files if too much diskspace is being used.
func (q *Q) limitDiskUsage(batches []storedBatch) []storedBatch {
	if q.maxDiskUsage == 0 {
		// No configured limit.
		return batches
	}
	// Delete most recent ones.
	switch q.evictionPolicy {
	default:
		panic("impossible eviction policy")
	case evictNewest:
		acceptedBatches := make([]storedBatch, 0, len(batches))
		bcount := int64(0)
		for _, b := range batches {
			if bcount+b.fileSize > q.maxDiskUsage {
				log.Printf("removing batch due to disk usage: %s (%d elems)", b.filename, b.elemCount)
				if err := os.Remove(b.filename); err != nil {
					log.Printf("can't remove batch: %v", err)
				}
				continue
			}
			acceptedBatches = append(acceptedBatches, b)
			bcount += b.fileSize
		}
		return acceptedBatches
	case evictOldest:
		// First count the total, then remove from the beginning
		for len(batches) > 0 {
			bcount := int64(0)
			for _, b := range batches {
				bcount += b.fileSize
			}
			if bcount <= q.maxDiskUsage {
				return batches
			}
			b := batches[0]
			log.Printf("removing batch due to disk usage: %s (%d elems)", b.filename, b.elemCount)
			if err := os.Remove(b.filename); err != nil {
				log.Printf("can't remove batch: %v", err)
			}
			batches = batches[1:]
		}
		return batches
	}
}

// readLoop reads messages coming in and batches them in a queuechunk. When the
// chunk is full (or too old) it writes the whole chunk to queuechunk.
func (q *Q) readLoop(wg *sync.WaitGroup, incomingChunks chan queuechunk, chunkTimeout time.Duration) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		queue := make(chan string, q.blockElemCount)

		var timer *time.Timer
		var timerC <-chan time.Time
		if chunkTimeout != 0 {
			timer = time.NewTimer(chunkTimeout)
			timerC = timer.C
		}

		sendDownstream := func() {
			close(queue)
			incomingChunks <- queue
			queue = make(chan string, q.blockElemCount)
			if timer != nil {
				timer.Reset(chunkTimeout)
			}
		}

	OUTER:
		for {
			select {
			case <-timerC:
				// This case is disabled when no timeout is given.
				sendDownstream()
				continue
			case msg, ok := <-q.enqueue:
				if !ok {
					break OUTER
				}
			AGAIN:
				select {
				case queue <- msg:
				default:
					// This chunk is full.
					sendDownstream()
					goto AGAIN
				}
			}
		}
		close(queue)
		incomingChunks <- queue
		close(incomingChunks)
	}()
}

// writeLoop gets a queuechunk from the outgoingChunks chan and writes the
// messages to the external queue.
func (q *Q) writeLoop(outgoingChunks chan queuechunk) {
	go func() {
		defer close(q.dequeue)
		for readQueue := range outgoingChunks {
			for msg := range readQueue {
				q.dequeue <- msg
			}
		}
	}()
}

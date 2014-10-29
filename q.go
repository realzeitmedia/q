package q

// TODO: check write permissions on startup.
// TODO: something better with errors. Maybe an error channel?

import (
	"errors"
	"log"
	"os"
	"sort"
	"strings"
)

const (
	defaultblockCount    = 1024
	defaultEvicionPolicy = evictNewest
	magicNumber          = "QQ"
	fileExtension        = ".q"
)

var (
	errMagicNumber = errors.New("file not a Q file")
	errDataError   = errors.New("invalid file format")
	// ErrInvalidPrefix is potentially returned by NewQ.
	ErrInvalidPrefix = errors.New("invalid prefix")
)

// Q is a queue which will use disk storage if it's too long.
type Q struct {
	dir, prefix    string
	blockElemCount uint
	maxDiskUsage   int64          // in bytes
	evictionPolicy evictionPolicy // for maxDiskUsage
	enqueue        chan interface{}
	dequeue        chan interface{}
	countReq       chan chan uint
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

// BlockCount is the number of entries the queue can have before entries are written
// to disk. It's also the number of entries per file. It's used as an
// option to NewQ. The default is 1024.
func BlockCount(count uint) configcb {
	return func(q *Q) {
		q.blockElemCount = count
	}
}

// EvictOldest makes MaxDiskUsage() remove oldest entries. The default is the
// opposite.
func EvictOldest() configcb {
	return func(q *Q) {
		q.evictionPolicy = evictOldest
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
		enqueue:        make(chan interface{}),
		dequeue:        make(chan interface{}),
		countReq:       make(chan chan uint),
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

	go q.loop(existing)
	return &q, nil
}

// Close will write all pending queue entries to disk before closing.
func (q *Q) Close() {
	c := make(chan struct{})
	q.quit <- c
	<-c
}

// Enqueue adds a message to the queue.
func (q *Q) Enqueue(m interface{}) {
	q.enqueue <- m
}

// Queue gives the channel to read queue entries from.
func (q *Q) Queue() <-chan interface{} {
	return q.dequeue
}

// Count gives the total number of entries in the queue. The reported number
// can be too high if queue files are deleted by something else.
func (q *Q) Count() uint {
	r := make(chan uint)
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
	elemCount uint
	fileSize  int64
}

func (q *Q) loop(existing []string) {
	readBatch, batches := q.loadExisting(existing)
	writeBatch := newBatch(q.prefix)
	if readBatch == nil {
		readBatch = writeBatch
	}
	var out chan interface{}
	var nextUp interface{} // Next element in the queue.
	for {
		// As long as we have no next element we keep `out` on nil. No need to
		// check it in the select loop.
		if out == nil {
			if readBatch.len() > 0 {
				nextUp = readBatch.peek()
				out = q.dequeue
			}
		}
		select {
		case c := <-q.quit:
			close(q.enqueue)
			close(q.dequeue)
			if writeBatch.len() != 0 {
				if _, err := writeBatch.saveToDisk(q.dir); err != nil {
					log.Printf("error batch to disk: %v", err)
				}
			}
			if readBatch != writeBatch && readBatch.len() != 0 {
				if _, err := readBatch.saveToDisk(q.dir); err != nil {
					log.Printf("error batch to disk: %v", err)
				}
			}
			limitDiskUsage(q, batches)
			close(c)
			return
		case r := <-q.countReq:
			count := writeBatch.len()
			if readBatch != writeBatch {
				count += readBatch.len()
			}
			for _, b := range batches {
				count += b.elemCount
			}
			r <- count
		case r := <-q.diskusageReq:
			bcount := int64(0)
			for _, b := range batches {
				bcount += b.fileSize
			}
			r <- bcount
		case in := <-q.enqueue:
			writeBatch.enqueue(in)
			if writeBatch.len() >= q.blockElemCount {
				if readBatch == writeBatch {
					// Don't write to disk, read is already reading from this
					// batch.
					writeBatch = newBatch(q.prefix)
					continue
				}
				fileSize, err := writeBatch.saveToDisk(q.dir)
				if err != nil {
					log.Printf("error writing batch to disk: %v", err)
					writeBatch = newBatch(q.prefix)
					continue
				}
				batches = append(batches, storedBatch{
					filename:  writeBatch.filename,
					elemCount: writeBatch.len(),
					fileSize:  int64(fileSize),
				})
				writeBatch = newBatch(q.prefix)
				batches = limitDiskUsage(q, batches)
			}
		case out <- nextUp:
			// Note: this case is only enabled when the queue is not empty.
			readBatch.dequeue()
			out = nil
			if readBatch.len() == 0 {
			AGAIN:
				// Finished with this batch. Open the next one, if any.
				if len(batches) > 0 {
					var err error
					filename := batches[0].filename
					readBatch, err = openBatch(q.dir + "/" + filename)
					batches = batches[1:]
					if err != nil {
						// Skip this file for this run. Don't delete it.
						log.Printf("open batch %v error: %v", filename, err)
						goto AGAIN
					}
					if err = os.Remove(q.dir + "/" + filename); err != nil {
						log.Printf("can't remove batch: %v", err)
					}
				} else {
					// No batches on disk. Read from the one we're writing to.
					readBatch = writeBatch
				}
			}
		}
	}
}

// findExisting gives the filenames of saved batches.
func (q *Q) findExisting() ([]string, error) {
	f, err := os.Open(q.dir)
	if err != nil {
		return nil, err
	}
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
	f.Close()
	return existing, nil
}

// loadExisting restores the state from disk. Besides all the batches it
// also returns the first batch ready to read from (if any).
func (q *Q) loadExisting(existing []string) (*batch, []storedBatch) {
	var batches []storedBatch

	// Read existing files so we can continue were we left off.
	var readBatch *batch // Point to either the first existing one, or the writeBatch.
	// Check all files. We also like to know their length.
	for _, f := range existing {
		filename := q.dir + "/" + f
		stat, err := os.Stat(filename)
		if err != nil {
			log.Printf("stat batch %v error: %v. Ignoring", f, err)
			continue
		}
		fileSize := stat.Size()

		b, err := openBatch(filename)
		if err != nil {
			log.Printf("open batch %v error: %v. Ignoring", f, err)
			continue
		}
		if b.len() == 0 {
			// Empty batch. Weird.
			if err = os.Remove(q.dir + "/" + f); err != nil {
				log.Printf("can't remove batch: %v", err)
			}
			continue
		}
		if readBatch == nil {
			// readBatch points to the oldest batch...
			readBatch = b
			// ... which can't be on disk anymore.
			if err = os.Remove(q.dir + "/" + f); err != nil {
				log.Printf("can't remove batch: %v", err)
			}
			// We're reading this one. Don't add it to `batches`.
			continue
		}
		batches = append(batches, storedBatch{
			filename:  b.filename,
			elemCount: b.len(),
			fileSize:  fileSize,
		})
	}
	return readBatch, batches
}

// limitDiskUsage delete files if too much diskspace is being used.
func limitDiskUsage(q *Q, batches []storedBatch) []storedBatch {
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
				if err := os.Remove(q.dir + "/" + b.filename); err != nil {
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
			if err := os.Remove(q.dir + "/" + b.filename); err != nil {
				log.Printf("can't remove batch: %v", err)
			}
			batches = batches[1:]
		}
		return batches
	}
}

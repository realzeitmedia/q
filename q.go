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
	// BlockCount is the number of entries the queue can have before entries are written
	// to disk. The system can use maximum twice this number.
	BlockCount    = 1024
	magicNumber   = "QQ"
	fileExtension = ".q"
)

var (
	errMagicNumber   = errors.New("file not a Q file")
	errDataError     = errors.New("invalid file format")
	errInvalidPrefix = errors.New("invalid prefix")
)

// Q is a queue which will use disk storage if it's too long.
type Q struct {
	dir, prefix string
	enqueue     chan interface{}
	dequeue     chan interface{}
	countReq    chan chan uint
	quit        chan chan struct{}
}

// NewQ makes or opens a Q, with files in and from <dir>/<prefix>-1234.q
// prefix needs to be a simple, alphanumeric string.
func NewQ(dir, prefix string) (*Q, error) {
	if len(prefix) == 0 {
		return nil, errInvalidPrefix
	}
	q := Q{
		dir:      dir,
		prefix:   prefix,
		enqueue:  make(chan interface{}),
		dequeue:  make(chan interface{}),
		countReq: make(chan chan uint),
		quit:     make(chan chan struct{}),
	}
	// Look for existing files.
	f, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	var existing []string
	names, err := f.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	for _, name := range names {
		if !strings.HasPrefix(name, prefix+"-") || !strings.HasSuffix(name, fileExtension) {
			continue
		}
		existing = append(existing, name)
	}
	sort.Strings(existing)
	f.Close()
	go q.loop(existing)
	return &q, nil
}

// Close will write all pending queue entries to disk before closing.
func (q *Q) Close() {
	c := make(chan struct{})
	q.quit <- c
	<-c
}

// Enqueue adds a messages to the queue
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

func (q *Q) loop(existing []string) {
	var batches []string // batch file names.
	count := uint(0)     // Total elements in our system.

	// Read existing files so we can continue were we left off.
	var readBatch *batch // Point to either the first existing one, or the writeBatch.
	// Check all files. We also like to know their length.
	for _, f := range existing {
		b, err := openBatch(q.dir + "/" + f)
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
		count += uint(b.len())
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
		batches = append(batches, b.filename)
	}

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
			close(c)
			return
		case r := <-q.countReq:
			r <- count
		case in := <-q.enqueue:
			count++
			writeBatch.enqueue(in)
			if writeBatch.len() > BlockCount {
				if readBatch == writeBatch {
					// Don't write to disk, read is already reading from this
					// batch.
					writeBatch = newBatch(q.prefix)
					continue
				}
				if _, err := writeBatch.saveToDisk(q.dir); err != nil {
					log.Printf("error writing batch to disk: %v", err)
					writeBatch = newBatch(q.prefix)
					continue
				}
				batches = append(batches, writeBatch.filename)
				writeBatch = newBatch(q.prefix)
			}
		case out <- nextUp:
			// Note: this case is only enabled when the queue is not empty.
			readBatch.dequeue()
			count--
			out = nil
			if readBatch.len() == 0 {
			AGAIN:
				// Finished with this batch. Open the next one, if any.
				if len(batches) > 0 {
					var err error
					filename := batches[0]
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

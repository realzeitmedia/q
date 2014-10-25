// TODO: check write permissions on startup.
package q

import (
	"errors"
	"log"
	"os"
	"sort"
	"strings"
)

const (
	minBlockSize  = 2 * 1024 * 1024
	maxMsgSize    = 10 * 1024 * 1024
	magicNumber   = "QQ"
	fileExtension = ".q"
)

var (
	errMagicNumber   = errors.New("file not a Q file.")
	errDataError     = errors.New("invalid file format")
	errInvalidPrefix = errors.New("invalid prefix")
)

type Q struct {
	dir, prefix string
	enqueue     chan string
	dequeue     chan string
	countReq    chan chan uint
	quit        chan chan struct{}
}

func NewQ(dir, prefix string) (*Q, error) {
	if len(prefix) == 0 {
		return nil, errInvalidPrefix
	}
	q := Q{
		dir:      dir,
		prefix:   prefix,
		enqueue:  make(chan string),
		dequeue:  make(chan string),
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
		// fmt.Printf("Consider %s\n", name)
		if !strings.HasPrefix(name, prefix+"-") || !strings.HasSuffix(name, fileExtension) {
			continue
		}
		existing = append(existing, name)
	}
	sort.Strings(existing)
	// fmt.Printf("Existing: %v", existing)
	f.Close()
	go q.loop(existing)
	return &q, nil
}

func (q *Q) Close() {
	c := make(chan struct{})
	q.quit <- c
	<-c
}

func (q *Q) Enqueue(m string) {
	// TODO: if len(m) > maxMsgSize
	// fmt.Printf("Enq %v\n", m)
	q.enqueue <- m
}

func (q *Q) Dequeue() string {
	// TODO: on close
	// fmt.Printf("Deq\n")
	return <-q.dequeue
}

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
			continue
		}
		if readBatch == nil {
			// readBatch points to the oldest batch.
			readBatch = b
		}
		batches = append(batches, b.filename)
		count += uint(len(b.elems))
	}

	writeBatch := newBatch(q.prefix)
	if readBatch == nil {
		readBatch = writeBatch
	}
	var out chan string
	var nextUp string // Next element in the queue.
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
			if writeBatch.size > minBlockSize {
				if readBatch == writeBatch {
					// Don't write to disk, read is already reading from this
					// batch.
					// fmt.Printf("New writeBatch, not saving the old batch, it's already being read from.\n")
					writeBatch = newBatch(q.prefix)
					continue
				}
				_, err := writeBatch.saveToDisk(q.dir)
				if err != nil {
					log.Printf("error writing batch to disk: %v", err)
					writeBatch = newBatch(q.prefix)
					continue
				}
				batches = append(batches, writeBatch.filename)
				// fmt.Printf("Save %v, batches: %v\n", name, len(batches))
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
					// fmt.Printf("Next from batches: %v %v\n", batches[0], len(batches))
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
					// if readBatch != writeBatch {
					// fmt.Printf("Out of stored batches. Follow writeBatch\n")
					// }
					readBatch = writeBatch
				}
			}
		}
	}
}

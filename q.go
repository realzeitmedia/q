// TODO: save on quit.
// TODO: check write permissions on startup.
package q

import (
	"errors"
	"fmt"
	"log"
	"os"
)

const (
	minBlockSize = 2 * 1024 * 1024
	maxMsgSize   = 10 * 1024 * 1024
	magicNumber  = "QQ"
)

var (
	errMagicNumber = errors.New("file not a Q file.")
	errDataError   = errors.New("invalid file format")
)

type Q struct {
	dir, prefix string
	enqueue     chan string
	dequeue     chan string
	countReq    chan chan uint
	quit        chan chan struct{}
}

func NewQ(dir, prefix string) *Q {
	q := Q{
		dir:      dir,
		prefix:   prefix,
		enqueue:  make(chan string),
		dequeue:  make(chan string),
		countReq: make(chan chan uint),
		quit:     make(chan chan struct{}),
	}
	go q.loop()
	return &q
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

func (q *Q) loop() {
	var batches []string // batch file names.
	// TODO: read batches on startup.
	writeBatch := &batch{}
	readBatch := writeBatch
	count := uint(0)
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
			close(c)
			close(q.enqueue)
			close(q.dequeue)
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
					fmt.Printf("New writeBatch, not saving the old batch, it's already being read from.\n")
					writeBatch = &batch{}
					continue
				}
				name, err := writeBatch.saveToDisk(q.dir, q.prefix)
				if err != nil {
					log.Printf("error writing batch to disk: %v", err)
					writeBatch = &batch{}
					continue
				}
				batches = append(batches, name)
				fmt.Printf("Save %v, batches: %v\n", name, len(batches))
				writeBatch = &batch{}
			}
		case out <- nextUp:
			readBatch.dequeue()
			count--
			out = nil
			if readBatch.len() == 0 {
			AGAIN:
				// Finished with this batch. Open the next one, if any.
				if len(batches) > 0 {
					fmt.Printf("Next from batches: %v %v\n", batches[0], len(batches))
					var err error
					filename := batches[0]
					readBatch, err = openBatch(filename)
					batches = batches[1:]
					if err != nil {
						// Skip this file for this run. Don't delete it.
						log.Printf("open batch %v error: %v", filename, err)
						goto AGAIN
					}
					if err = os.Remove(filename); err != nil {
						log.Printf("can't remove batch: %v", err)
					}
				} else {
					// No batches on disk. Read from the one we're writing to.
					if readBatch != writeBatch {
						fmt.Printf("Out of stored batches. Follow writeBatch\n")
					}
					readBatch = writeBatch
				}
			}
		}
	}
}

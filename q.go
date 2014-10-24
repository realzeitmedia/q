// TODO: save on quit.
// TODO: check write permissions on startup.
package q

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"syscall"
	"time"
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

// batch is a cunck of elements, which might go to disk.
type batch struct {
	elems []string
	size  uint // byte size, without overhead.
}

func (b *batch) enqueue(m string) {
	b.elems = append(b.elems, m)
	b.size += uint(len(m))
}

// dequeue takes the left most element. batch can't be empty.
func (b *batch) dequeue() string {
	el := b.elems[0]
	b.size -= uint(len(el))
	b.elems = b.elems[1:]
	return el

}

func (b *batch) len() int {
	return len(b.elems)
}

// peek at the last one. batch can't be empty.
func (b *batch) peek() string {
	return b.elems[0]
}

func (b *batch) saveToDisk(dir, prefix string) (string, error) {
	filename := fmt.Sprintf("%s/%s-%020d.q", dir, prefix, time.Now().UnixNano())
	fh, err := os.OpenFile(filename, syscall.O_WRONLY|syscall.O_CREAT|syscall.O_EXCL, 0600)
	if err != nil {
		return filename, err
	}
	defer fh.Close()
	return filename, b.serialize(fh)
}

func (b *batch) serialize(w io.Writer) error {
	_, err := w.Write([]byte(magicNumber))
	if err != nil {
		return err
	}
	if err = binary.Write(w, binary.LittleEndian, uint32(b.len())); err != nil {
		return err
	}

	for _, e := range b.elems {
		if err = binary.Write(w, binary.LittleEndian, uint32(len(e))); err != nil {
			return err
		}
		_, err = w.Write([]byte(e))
		if err != nil {
			return err
		}
	}
	return nil
}

func openBatch(filename string) (*batch, error) {
	fh, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer fh.Close()
	return deserialize(bufio.NewReader(fh))
}

func deserialize(r io.Reader) (*batch, error) {
	b := &batch{}
	magic := make([]byte, len(magicNumber))
	if _, err := r.Read(magic); err != nil {
		return nil, err
	}
	if string(magic) != magicNumber {
		return nil, errMagicNumber
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	count := binary.LittleEndian.Uint32(buf)
	// fmt.Printf("Count: %d\n", count)
	for i := uint32(0); i < count; i++ {
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		size := binary.LittleEndian.Uint32(buf)
		if size > maxMsgSize {
			panic(fmt.Sprintf("Size too big: %d", size))
		}
		msg := make([]byte, size)
		if _, err := io.ReadFull(r, msg); err != nil {
			return nil, err
		}
		b.enqueue(string(msg))
	}
	// We expect to be at EOF now.
	n, err := io.ReadFull(r, buf)
	if n != 0 || err != io.EOF {
		return nil, errDataError
	}
	return b, nil
}

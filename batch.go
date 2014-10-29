package q

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path"
	"syscall"
	"time"
)

// batch is a chunk of elements, which might go to disk.
type batch struct {
	filename string // Need to be ordered alphabetically
	elems    []interface{}
}

func newBatch(prefix string) *batch {
	return &batch{
		filename: fmt.Sprintf("%s-%020d%s", prefix, time.Now().UnixNano(), fileExtension),
	}
}

func (b *batch) enqueue(m interface{}) {
	b.elems = append(b.elems, m)
}

// dequeue takes the left most element. batch can't be empty.
func (b *batch) dequeue() interface{} {
	el := b.elems[0]
	b.elems = b.elems[1:]
	return el

}

func (b *batch) len() uint {
	return uint(len(b.elems))
}

// peek at the last one. batch can't be empty.
func (b *batch) peek() interface{} {
	return b.elems[0]
}

// saveToDisk write the batch to disk. Returns the file size in bytes.
func (b *batch) saveToDisk(dir string) (int, error) {
	filename := dir + "/" + b.filename
	fh, err := os.OpenFile(filename, syscall.O_WRONLY|syscall.O_CREAT|syscall.O_EXCL, 0600)
	if err != nil {
		return 0, err
	}
	defer fh.Close()
	counter := newCountWriter(fh)
	err = b.serialize(counter)
	return counter.count, err
}

func (b *batch) serialize(w io.Writer) error {
	_, err := w.Write([]byte(magicNumber))
	if err != nil {
		return err
	}
	enc := gob.NewEncoder(w)
	if err = enc.Encode(b.elems); err != nil {
		return err
	}
	return nil
}

func openBatch(filename string) (*batch, error) {
	fh, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer fh.Close()
	b, err := deserialize(bufio.NewReader(fh))
	if err != nil {
		return nil, err
	}
	b.filename = path.Base(filename)
	return b, nil
}

func deserialize(r io.Reader) (*batch, error) {
	b := &batch{}
	magic := make([]byte, len(magicNumber))
	if _, err := io.ReadFull(r, magic); err != nil {
		return nil, err
	}
	if string(magic) != magicNumber {
		return nil, errMagicNumber
	}
	dec := gob.NewDecoder(r)
	var msgs []interface{}
	for {
		err := dec.Decode(&msgs)
		if err != nil {
			if err == io.EOF {
				return b, nil
			}
			return nil, err
		}
		for _, msg := range msgs {
			b.enqueue(msg)
		}
	}
}

type countWriter struct {
	f     io.Writer
	count int
}

func newCountWriter(f io.Writer) *countWriter {
	return &countWriter{
		f: f,
	}
}

func (c *countWriter) Write(b []byte) (int, error) {
	n, err := c.f.Write(b)
	c.count += n
	return n, err
}

package q

import (
	"bufio"
	"encoding/gob"
	"errors"
	"io"
	"os"
	"syscall"
)

var (
	errIncompleteWrite = errors.New("Not all bytes written")
	errMagicNumber     = errors.New("file not a Q file")
)

// batch is a chunk of elements, which might go to disk.
type batch struct {
	elems []string
}

func newBatch(q chan string) *batch {
	elems := make([]string, 0, len(q))
	for e := range q {
		elems = append(elems, e)
	}
	return &batch{
		elems: elems,
	}
}

func (b *batch) len() int {
	return len(b.elems)
}

// saveToDisk write the batch to disk. Returns the file size in bytes.
func (b *batch) saveToDisk(filename string) (int, error) {
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
	n, err := io.WriteString(w, magicNumber)
	if err != nil {
		return err
	}
	if n != len(magicNumber) {
		return errIncompleteWrite
	}
	enc := gob.NewEncoder(w)
	if err = enc.Encode(b.elems); err != nil {
		return err
	}
	return nil
}

func openBatch(filename string) (queuechunk, error) {
	fh, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer fh.Close()
	b, err := deserialize(bufio.NewReader(fh))
	if err != nil {
		return nil, err
	}

	q := make(queuechunk, b.len())
	for _, e := range b.elems {
		q <- e
	}
	close(q)
	return q, nil
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
	var elems []string
	for {
		err := dec.Decode(&elems)
		if err != nil {
			if err == io.EOF {
				b.elems = elems
				return b, nil
			}
			return nil, err
		}
	}
}

// countWriter counts the number of bytes written.
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

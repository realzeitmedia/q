package q

import (
	"bufio"
	"container/list"
	"encoding/binary"
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
	elems    *list.List
	size     uint // byte size, without overhead.
}

func newBatch(prefix string) *batch {
	return &batch{
		filename: fmt.Sprintf("%s-%020d%s", prefix, time.Now().UnixNano(), fileExtension),
		elems:    list.New(),
	}
}

func (b *batch) enqueue(m string) {
	b.elems.PushBack(m)
	b.size += uint(len(m))
}

// dequeue takes the left most element. batch can't be empty.
func (b *batch) dequeue() string {
	el := b.elems.Front()
	b.size -= uint(len(el.Value.(string)))
	b.elems.Remove(el)
	return el.Value.(string)

}

func (b *batch) len() int {
	return b.elems.Len()
}

// peek at the last one. batch can't be empty.
func (b *batch) peek() string {
	return b.elems.Front().Value.(string)
}

func (b *batch) saveToDisk(dir string) (string, error) {
	filename := dir + "/" + b.filename
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

	for e := b.elems.Front(); e != nil; e = e.Next() {
		if err = binary.Write(w, binary.LittleEndian, uint32(len(e.Value.(string)))); err != nil {
			return err
		}
		_, err = w.Write([]byte(e.Value.(string)))
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
	b, err := deserialize(bufio.NewReader(fh))
	if err != nil {
		return nil, err
	}
	b.filename = path.Base(filename)
	return b, nil
}

func deserialize(r io.Reader) (*batch, error) {
	b := newBatch("")
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

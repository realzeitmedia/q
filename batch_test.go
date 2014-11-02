package q

import (
	"bytes"
	"reflect"
	"testing"
)

func TestBatchSerialize(t *testing.T) {
	c := make(chan string, 3)
	c <- "first"
	c <- "second"
	c <- "third"
	close(c)
	b := newBatch(c)

	// batch to bytes...
	var buf bytes.Buffer
	err := b.serialize(&buf)
	if err != nil {
		t.Fatalf("unexpected serialize error: %v", err)
	}

	// ...and back again to batch.
	again, err := deserialize(bytes.NewBuffer(buf.Bytes()))
	if err != nil {
		t.Fatalf("unexpected deserialize error: %v", err)
	}
	if !reflect.DeepEqual(again, b) {
		t.Fatalf("deserialize not the same. Want %#v, got %#v", b, again)
	}
}

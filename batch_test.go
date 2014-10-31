package q

import (
	"bytes"
	"reflect"
	"testing"
)

func TestBatchSerialize(t *testing.T) {
	typ := reflect.TypeOf("")
	b := newBatch("")
	b.enqueue("first")
	b.enqueue("second")
	b.enqueue("third")

	// batch to bytes...
	var buf bytes.Buffer
	err := b.serialize(&buf)
	if err != nil {
		t.Fatalf("unexpected serialize error: %v", err)
	}

	// ...and back again to batch.
	again, err := deserialize(bytes.NewBuffer(buf.Bytes()), typ)
	if err != nil {
		t.Fatalf("unexpected deserialize error: %v", err)
	}
	if !reflect.DeepEqual(again.elems, b.elems) {
		t.Fatalf("deserialize not the same. Want %#v, got %#v", b, again)
	}
}

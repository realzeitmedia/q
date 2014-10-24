package q

import (
	"bytes"
	"reflect"
	"testing"
)

func TestBatchSerialize(t *testing.T) {
	b := &batch{}
	b.enqueue("first")
	b.enqueue("second")
	b.enqueue("third")

	// batch to bytes...
	var buf bytes.Buffer
	err := b.serialize(&buf)
	if err != nil {
		t.Fatalf("unexpected serialize error: %v", err)
	}
	want := "QQ\x03\x00\x00\x00\x05\x00\x00\x00first\x06\x00\x00\x00second\x05\x00\x00\x00third"
	got := buf.String()
	if got != want {
		t.Errorf("serialize error. Got \n%#v, want \n%#v", got, want)
	}

	// ...and back again to batch.
	again, err := deserialize(bytes.NewBufferString(got))
	if err != nil {
		t.Fatalf("unexpected deserialize error: %v", err)
	}
	if !reflect.DeepEqual(again, b) {
		t.Fatalf("deserialize not the same. Want %#v, got %#v", b, again)
	}
}

func TestBatchDeserialize(t *testing.T) {
	msg := "QQ\x03\x00\x00\x00\x05\x00\x00\x00first\x06\x00\x00\x00second\x05\x00\x00\x00third"
	_, err := deserialize(bytes.NewBufferString(msg))
	if err != nil {
		t.Fatalf("unexpected deserialize error: %v", err)
	}

	// Bogus trailing data
	_, err = deserialize(bytes.NewBufferString(msg + "trailing"))
	if err != errDataError {
		t.Fatalf("expected deserialize error: %v", err)
	}

	// Wrong magic.
	_, err = deserialize(bytes.NewBufferString(msg[10:]))
	if err != errMagicNumber {
		t.Fatalf("expected deserialize error: %v", err)
	}

	// Chopped msg.
	_, err = deserialize(bytes.NewBufferString(msg[:30]))
	if err == nil || err.Error() != "unexpected EOF" {
		t.Fatalf("expected deserialize error: %#v", err)
	}
}

package q_test

import (
	"fmt"

	"github.com/alicebob/q"
)

func ExampleSimple() {
	// Dir should be e.q. "/var/lib/myapp/queue".
	// It is /tmp/ to make this example compile.
	queue, err := q.NewQ("/tmp", "views")
	if err != nil {
		panic(err) // ...
	}
	defer queue.Close()
	go func() {
		for msg := range queue.Queue() {
			// Do something with msg.
			fmt.Println(msg.(string))
		}
	}()

	// Elsewhere:
	queue.Enqueue("Hello World!")
	queue.Enqueue("How's things?")
	//OUTPUT:
	// Hello World!
	// How's things?
}

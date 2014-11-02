package q_test

import (
	"fmt"

	"github.com/alicebob/q"
)

func Example() {
	queue, err := q.NewQ("/var/lib/myapp/queue/", "views")
	if err != nil {
		panic(err) // ...
	}
	defer queue.Close()

	// Set-up a single (or more) queue readers.
	go func() {
		for msg := range queue.Queue() {
			// Do something with msg which can potentially block.
			fmt.Println(msg)
		}
	}()

	// Elsewhere:
	queue.Enqueue("Hello World!")
	queue.Enqueue("How's things?")
}

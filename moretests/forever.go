// A queue which keeps filling and clearing, forever.
package main

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/alicebob/q"
)

const (
	payload = "Mary had a little lamb, Its fleece was white as snow.  Everywhere that Mary went The lamb was sure to go."
)

func main() {
	rand.Seed(time.Now().Unix())

	os.RemoveAll("./store")
	if err := os.Mkdir("./store/", 0700); err != nil {
		panic(fmt.Sprintf("Can't make ./store/: %v", err))
	}
	defer os.RemoveAll("./store")

	q, err := q.NewQ("./store", "q")
	if err != nil {
		panic(err)
	}

	for i := 0; i < 7; i++ {
		go func() {
			for {
				q.Enqueue(payload)
				time.Sleep(1 * time.Millisecond)
			}
		}()
	}

	for {
		// Empty the queue as quick as possible, then sleep a little.
		for q.Count() != 0 {
			if got := <-q.Queue(); got != payload {
				panic(fmt.Sprintf("Payload error. Got %#v", got))
			}
		}
		p := time.Duration(rand.Intn(22345)) * time.Millisecond
		fmt.Printf("Sleep %s\n", p)
		time.Sleep(p)
	}
}

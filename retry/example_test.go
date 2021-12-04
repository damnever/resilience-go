package retry

import (
	"context"
	"errors"
	"fmt"
	"time"
)

func Example() {
	r := New(ExponentialBackoffs(10, time.Second, time.Minute))

	counter := 0
	err := r.Run(context.Background(), func() error {
		counter++
		return Unrecoverable(errors.New("fatal error")) // Unrecoverable error makes it stop.
	})
	fmt.Println(counter, err)

	iter := r.Iterator() // The Iterator isn't thread-safe.
	counter = 0
	for iter.Next(context.Background()) {
		if counter++; counter == 2 {
			break
		}
	}
	fmt.Println(counter)

	// Output:
	// 1 fatal error
	// 2
}

package circuitbreaker

import (
	"fmt"
	"time"
)

func Example() {
	cb := New(DefaultConfig())

	func() {
		c := cb.Circuit() // DO NOT reuse it, create a new one each time.
		if c.IsInterrupted() {
			return // Not healthy, return immediately.
		}

		var err error
		defer func(startAt time.Time) { c.Observe(startAt, err != nil) }(time.Now())
		// err = doSth()
		fmt.Println(err)
	}()

	func() { // Use Run for "convenience".
		cb.Run(func() bool {
			fmt.Println("failed")
			return false // not ok
		})
	}()

	// Output:
	// <nil>
	// failed
}

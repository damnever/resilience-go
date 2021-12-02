package circuitbreaker

import (
	"fmt"
	"time"
)

func Example() {
	cb := New(DefaultConfig())

	func() {
		guard := cb.Guard() // DO NOT reuse it, create a new one each time.
		if !guard.Allow() {
			return // Not healthy, return immediately.
		}

		var err error
		defer func(startAt time.Time) { guard.Observe(startAt, err != nil) }(time.Now())
		// err = doSth()
		fmt.Println(err)
	}()

	func() { // Use Run for "convenience".
		_ = cb.Run(func() bool {
			fmt.Println("failed")
			return false // not ok
		}, func() {
			// fallback.
		})
	}()

	// Output:
	// <nil>
	// failed
}

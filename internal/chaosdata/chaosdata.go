// Package chaosdata defines the deterministic key/value pairs used by
// cmd/chaosworker and cmd/chaos, so both sides agree on exactly what a
// given write index means without duplicating the format in two places.
package chaosdata

import "fmt"

// Key returns the deterministic key for write index i.
func Key(i int) []byte { return []byte(fmt.Sprintf("key:%08d", i)) }

// Value returns the deterministic value for write index i.
func Value(i int) []byte { return []byte(fmt.Sprintf("val:%08d", i)) }

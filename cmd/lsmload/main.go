// Command lsmload is a test helper: it opens a DB at -dir and performs
// -count sequential Puts with deterministic keys/values, optionally
// pausing -delay between each. Used by the root package's kill-restart
// test to drive a real subprocess that gets SIGKILL'd mid-burst.
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	lsm "github.com/paarthsiphone/lsm-tree-storage"
)

func main() {
	dir := flag.String("dir", "", "data directory")
	count := flag.Int("count", 1000, "number of puts")
	delay := flag.Duration("delay", 0, "delay between puts")
	flag.Parse()

	if *dir == "" {
		fmt.Fprintln(os.Stderr, "lsmload: -dir is required")
		os.Exit(1)
	}

	db, err := lsm.Open(*dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "lsmload: Open:", err)
		os.Exit(1)
	}

	for i := 0; i < *count; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		value := []byte(fmt.Sprintf("val-%06d", i))
		if err := db.Put(key, value); err != nil {
			fmt.Fprintln(os.Stderr, "lsmload: Put:", err)
			os.Exit(1)
		}
		if *delay > 0 {
			time.Sleep(*delay)
		}
	}

	if err := db.Close(); err != nil {
		fmt.Fprintln(os.Stderr, "lsmload: Close:", err)
		os.Exit(1)
	}
}

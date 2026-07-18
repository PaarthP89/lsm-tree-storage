// Command chaosworker opens a DB and performs a burst of sequential,
// deterministic Puts, printing "ACK %08d" to stdout the instant each Put
// returns successfully. That ACK line is the harness's only source of
// truth for which writes were actually acknowledged (durably fsync'd)
// before a SIGKILL -- ground truth is derived from what this process
// reported, never from timing assumptions. See cmd/chaos.
package main

import (
	"flag"
	"fmt"
	"os"

	lsm "github.com/paarthsiphone/lsm-tree-storage"
	"github.com/paarthsiphone/lsm-tree-storage/internal/chaosdata"
)

func main() {
	dir := flag.String("dir", "", "data directory")
	count := flag.Int("count", 5000, "number of puts to attempt")
	flushThreshold := flag.Int("flushthreshold", 4096, "memtable flush threshold in bytes")
	flag.Parse()

	if *dir == "" {
		fmt.Fprintln(os.Stderr, "chaosworker: -dir is required")
		os.Exit(1)
	}

	db, err := lsm.Open(*dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "chaosworker: Open:", err)
		os.Exit(1)
	}
	db.SetFlushThreshold(*flushThreshold)

	for i := 0; i < *count; i++ {
		if err := db.Put(chaosdata.Key(i), chaosdata.Value(i)); err != nil {
			fmt.Fprintln(os.Stderr, "chaosworker: Put:", err)
			os.Exit(1)
		}
		// Printed only after Put has returned successfully -- WAL fsync
		// (and any synchronous flush triggered by this write) already
		// completed by this point, so this line is the durable-write
		// boundary, not an estimate of it.
		fmt.Printf("ACK %08d\n", i)
	}

	if err := db.Close(); err != nil {
		fmt.Fprintln(os.Stderr, "chaosworker: Close:", err)
		os.Exit(1)
	}
	fmt.Println("DONE")
}

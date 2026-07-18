package lsm

import (
	"github.com/paarthsiphone/lsm-tree-storage/memtable"
	"github.com/paarthsiphone/lsm-tree-storage/sstable"
	"github.com/paarthsiphone/lsm-tree-storage/wal"
)

// l0/l1/mem/w are test-only convenience accessors for the current dbState's
// fields (Phase 8c moved these behind db.state, an atomic.Pointer[dbState],
// so production code never accesses them as bare DB fields anymore -- see
// db.go). Tests want a quick, readable way to inspect "what's live right
// now" without threading db.state.Load() through every assertion.
func (db *DB) l0() []*sstable.SSTable  { return db.state.Load().l0 }
func (db *DB) l1() []*sstable.SSTable  { return db.state.Load().l1 }
func (db *DB) mem() *memtable.SkipList { return db.state.Load().mem }
func (db *DB) w() *wal.Writer          { return db.state.Load().w }

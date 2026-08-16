// Package sstable implements on-disk sorted string tables: flush from a
// memtable (or any sorted entry iterator, via FlushIterator), the
// sparse-index footer format, binary-search reads, and a full-table
// Iterator for sequential scans. Compaction's k-way merge itself lives in
// package compaction, built on top of Iterator and FlushIterator.
package sstable

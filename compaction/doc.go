// Package compaction implements single-level SSTable compaction: a k-way
// merge of every currently-live SSTable into one new file, dropping
// obsolete tombstones, written and installed with the same crash-safety
// discipline (temp file -> fsync -> atomic rename, then MANIFEST edits in
// ADDED-before-REMOVED order) as everything else in this codebase.
package compaction

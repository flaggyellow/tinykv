// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"reflect"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	first uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	hardState, _, _ := storage.InitialState()
	hi, _ := storage.LastIndex()
	lo, _ := storage.FirstIndex()
	entries, err := storage.Entries(lo, hi+1)
	if err != nil {
		entries = make([]pb.Entry, 0)
	}
	new_entries := make([]pb.Entry, len(entries))
	copy(new_entries, entries)
	return &RaftLog{
		storage:   storage,
		committed: hardState.Commit,
		applied:   lo - 1,
		stabled:   hi,
		entries:   new_entries,
		first:     lo,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return nil
	}
	if l.stabled+1 < l.first {
		log.Errorf("[unstableEntries]stabled %d smaller than first %d", l.stabled, l.first)
		return nil
	}
	if l.stabled > l.LastIndex() {
		log.Errorf("[unstableEntries]stabled %d larger than LastIndex %d", l.stabled, l.first)
		return nil
	}
	return l.entries[l.stabled-l.first+1:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return nil
	}
	if l.applied > l.committed || l.applied+1 < l.first || l.committed+1-l.first > uint64(len(l.entries)) {
		log.Errorf("Something went wrong in the log!")
		return nil
	}
	return l.entries[l.applied-l.first+1 : l.committed-l.first+1]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		snapShotIndex := uint64(0)
		if l.pendingSnapshot != nil {
			snapShotIndex = l.pendingSnapshot.Metadata.Index
		}
		lastIndex, err := l.storage.LastIndex()
		if err != nil {
			panic(err)
		}
		return max(lastIndex, snapShotIndex)
	}
	return l.entries[len(l.entries)-1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if len(l.entries) > 0 && i >= l.first {
		if i > l.LastIndex() {
			return 0, ErrUnavailable
		}
		return l.entries[i-l.first].Term, nil
	}
	// find it in the snapshot
	if l.pendingSnapshot != nil {
		snapshotMetadata := l.pendingSnapshot.Metadata
		if i == snapshotMetadata.Index {
			return snapshotMetadata.Term, nil
		}
	}
	// find it in the storage
	term, err := l.storage.Term(i)
	if err == ErrUnavailable && !IsEmptySnap(l.pendingSnapshot) {
		if i < l.pendingSnapshot.Metadata.Index {
			err = ErrCompacted
		}
	}
	return term, err
}

// Entries return the Entries in [left, right)
func (l *RaftLog) Entries(left uint64, right uint64) ([]pb.Entry, error) {
	firstIndexOfStorage, _ := l.storage.FirstIndex()
	if left < firstIndexOfStorage {
		return nil, ErrCompacted
	}
	if right > l.LastIndex()+1 {
		return nil, ErrUnavailable
	}
	var ents []pb.Entry
	if l.first == firstIndexOfStorage {
		ents = l.entries[left-l.first : right-l.first]
	} else {
		println("WARNING: unexpected situation - l.first != firstIndexOfStorage")
	}
	return ents, nil
}

// Append the entries
func (l *RaftLog) Append(index uint64, ents []pb.Entry) {
	if len(ents) == 0 {
		return
	}
	if len(l.entries) == 0 {
		l.entries = append(l.entries, ents...)
		if len(l.entries) == 0 {
			l.first = 0
			log.Warnf("after append the l.entries is empty!!")
			return
		}
		l.first = l.entries[0].Index
		l.stabled = l.first - 1
		return
	}
	l.first = l.entries[0].Index
	last := ents[0].Index + uint64(len(ents)) - 1
	// if append to old, ignore
	if last < l.first {
		return
	}
	if l.first > ents[0].Index {
		ents = ents[l.first-ents[0].Index:]
	}
	// start append
	if l.LastIndex()+1 < ents[0].Index {
		// something wrong happens
		log.Panicf("missing log entry [last: %d, append at: %d]",
			l.entries[len(l.entries)-1].Index, ents[0].Index)
	} else if l.LastIndex()+1 == ents[0].Index {
		l.entries = append(l.entries, ents...)
	} else {
		// need to check
		entries2Check, _ := l.Entries(index+1, l.LastIndex()+1)
		offset := 0
		for _, ent := range entries2Check {
			if offset >= len(ents) {
				// this means log is longer than entries to append, need to further check
				if l.entries[ent.Index-l.first].Term < ents[len(ents)-1].Term {
					l.entries = l.entries[:ent.Index-l.first]
					l.stabled = min(l.stabled, l.LastIndex())
				}
				return
			}
			if reflect.DeepEqual(ent, ents[offset]) {
				offset++
			} else {
				// delete all of the wrong entries
				l.entries = l.entries[:ent.Index-l.first]
				if ent.Index-l.first == 0 {
					l.entries = make([]pb.Entry, 0)
				}
				l.stabled = ent.Index - 1
				break
			}
		}
		// append entries
		for offset < len(ents) {
			l.entries = append(l.entries, ents[offset])
			offset++
		}
	}
	if len(l.entries) == 0 {
		l.first = 0
		log.Warnf("after append the l.entries is empty!!")
		return
	}
	l.first = l.entries[0].Index
}

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
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

	"github.com/pingcap-incubator/tinykv/log"

	"reflect"
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
	hi, herr := storage.LastIndex()
	lo, lerr := storage.FirstIndex()
	if herr != nil {
		panic(herr)
	}
	if lerr != nil {
		panic(lerr)
	}
	entries, err := storage.Entries(lo, hi+1)
	if err != nil {
		entries = make([]pb.Entry, 0)
	}
	return &RaftLog{
		storage:   storage,
		committed: lo - 1,
		applied:   lo - 1,
		stabled:   hi,
		entries:   entries,
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
	unstb := make([]pb.Entry, 0)
	if len(l.entries) > 0 {
		// if l.stabled+1 < l.first {
		// 	log.Infof("[unstableEntries]stabled %d smaller than first %d", l.stabled, l.first)
		// }
		if l.stabled < l.LastIndex() {
			unstb = l.entries[l.stabled-l.first+1:]
		}
	}
	return unstb
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		return l.entries[l.applied-l.first+1 : l.committed-l.first+1]
	}
	return nil
}

// 对entries的理解：entry的存储分为两个部分，一部分在storage中，一部分在内存中。内存中的entry
// 应该是与storage中的entry一致的（除了unstable后的部分和storage的快照部分）。storage中除了需要存储基本entry以外，还需要存储snapshot中
// 的最高index，因此第0号槽位保留，但内存中应该是不需要这样做的。目前认为内存中的entry是first - last。
// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		// maybe stabled == last, we need to check in storage. (i think this should not happen.)
		// lastIndex, _ := l.storage.LastIndex()
		// return lastIndex
		firstInd, _ := l.storage.FirstIndex()
		return firstInd - 1
	}
	return l.entries[len(l.entries)-1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	// fmt.Printf("lastIndex: %d\n", lastIndex)
	if i == 0 {
		return 0, nil
	}
	if i > l.LastIndex() {
		return 0, ErrUnavailable
	}
	firstIndexOfStorage, _ := l.storage.FirstIndex()
	if i < firstIndexOfStorage - 1 {
		// maybe in snapshot, don't know how to deal with it.
		// fmt.Printf("firstIndex: %d\n", firstIndexOfStorage)
		log.Debugf("Term() returned Errcompacted, the index: %d, firstIndexStorage: %d", i, firstIndexOfStorage)
		return 0, ErrCompacted
	}
	// if can be find in memory
	if len(l.entries) > 0 && i >= l.first {
		return l.entries[i-l.first].Term, nil
	}
	// fmt.Printf("len(l.entries): %d, l.first: %d\n", len(l.entries), l.first)
	return l.storage.Term(i)
}

func (l *RaftLog) Entries(lo, hi uint64) ([]pb.Entry, error) {
	firstIndexOfStorage, _ := l.storage.FirstIndex()
	if lo < firstIndexOfStorage {
		return nil, ErrCompacted
	}
	if hi > l.LastIndex()+1 {
		return nil, ErrUnavailable
	}
	var ents []pb.Entry
	if l.first == firstIndexOfStorage {
		ents = l.entries[lo-l.first : hi-l.first]
	} else {
		println("WARNING: unexpected situation - l.first != firstIndexOfStorage")
	}
	return ents, nil
}

func (l *RaftLog) Append(index uint64, entries []*pb.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	n := len(entries)
	i := 0
	if index < l.LastIndex() {
		// println("1")
		// need to check whether the entries are right
		entries2Check, _ := l.Entries(index+1, l.LastIndex()+1)
		for _, ent := range entries2Check {
			// println("2")
			if reflect.DeepEqual(ent, *entries[i]) {
				// println("3")
				i++
				if i >= n {
					if entries[i-1].Index < l.LastIndex() {
						// delete all of the outdated entries
						if l.entries[ent.Index-l.first+1].Term < entries[i-1].Term {
							l.entries = l.entries[:ent.Index-l.first+1]
						}
					}
					return nil
				}
			} else {
				// println("4")
				// delete all of the wrong entries
				l.entries = l.entries[:ent.Index-l.first]
				if ent.Index-l.first == 0 {
					l.entries = make([]pb.Entry, 0)
				}
				break
			}
		}
	}
	// append entries
	l.stabled = min(l.stabled, l.LastIndex())
	// fmt.Printf("stabled: %d\n", l.stabled)
	for i < n {
		// println("5")
		l.entries = append(l.entries, *entries[i])
		i++
	}
	return nil
}

func (l *RaftLog) Advance(ready Ready) {
	if len(ready.Entries) > 0 {
		l.stabled = ready.Entries[len(ready.Entries)-1].Index
	}
	if len(ready.CommittedEntries) > 0 {
		l.applied = ready.CommittedEntries[len(ready.CommittedEntries)-1].Index
		// if l.applied > l.committed {
		// 	log.Panicf("[Advance]applied index %d bigger than commit index %d", l.applied, l.committed)
		// }
	}
}

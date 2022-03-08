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
	"errors"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

	// "github.com/pingcap-incubator/tinykv/log"

	"math/rand"
	"sort"
	"time"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

// check the validity of the config
func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	// peer id (persistance)
	id uint64
	// currentTerm & votedFor (persistance)
	Term uint64
	Vote uint64

	// the log (persistance)
	RaftLog *RaftLog

	// log replication progress of each peers
	// the Progress include nextIndex & matchIndex (for LEADER)
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
	// others
	randomizedElectionTimeout int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	// RaftLog
	raftLog := newLog(c.Storage)
	// peers
	state, confState, _ := raftLog.storage.InitialState()
	var peers []uint64
	// -??- i can't understand here
	if len(c.peers) > 0 {
		peers = c.peers
	} else {
		peers = confState.Nodes
	}
	// initialize raft
	raft := &Raft{
		id:               c.ID,
		Term:             state.Term,
		Vote:             state.Vote,
		RaftLog:          raftLog,
		Prs:              make(map[uint64]*Progress, len(peers)),
		State:            StateFollower,
		votes:            make(map[uint64]bool, 0),
		Lead:             None,
		heartbeatElapsed: c.HeartbeatTick,
		heartbeatTimeout: c.HeartbeatTick,
		electionElapsed:  c.ElectionTick,
		electionTimeout:  c.ElectionTick,
	}
	// initialize Prs
	for _, i := range peers {
		raft.Prs[i] = &Progress{}
	}
	raft.RaftLog.committed = state.Commit
	if c.Applied > 0 {
		raft.RaftLog.applied = c.Applied
	}
	return raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	nextIndex := r.Prs[to].Next
	logTerm, err := r.RaftLog.Term(nextIndex - 1)
	if err != nil {
		if err == ErrCompacted {
			r.sendSnapshot(to)
			return true
		}
		// fmt.Printf("nextIndex: %d\n", nextIndex)
		panic(err)
	}
	lastIndex := r.RaftLog.LastIndex()
	entries := make([]*pb.Entry, 0)
	if nextIndex <= lastIndex {
		entsToAppend, _ := r.RaftLog.Entries(nextIndex, lastIndex+1)
		for _, entry := range entsToAppend {
			temp := entry
			entries = append(entries, &temp)
		}
	}
	message := pb.Message{
		From:    r.id,
		To:      to,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgAppend,
		Index:   nextIndex - 1,
		LogTerm: logTerm,
		Entries: entries,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, message)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	message := pb.Message{
		From:    r.id,
		To:      to,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgHeartbeat,
		Commit:  min(r.RaftLog.committed, r.Prs[to].Match),
	}
	r.msgs = append(r.msgs, message)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower, StateCandidate:
		r.electionElapsed++
		if r.electionElapsed >= r.randomizedElectionTimeout {
			r.electionElapsed = 0
			// message := pb.Message{
			// 	To:      r.id,
			// 	From:    r.id,
			// 	MsgType: pb.MessageType_MsgHup,
			// }
			// r.msgs = append(r.msgs, message)
			r.becomeCandidate()
			r.sendVoteMessage()
		}
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			// message := pb.Message{
			// 	To:      r.id,
			// 	From:    r.id,
			// 	MsgType: pb.MessageType_MsgBeat,
			// }
			// r.msgs = append(r.msgs, message)
			r.sendHeartbeatMessage()
		}
	}
}

// becomeFollower transform this peer's state to Follower
// WHEN: 1. starts up
// 2. leader -> follower: discover server with higher term
// 3. candidate -> follower: discover current leader or new term
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Lead = lead
	r.Vote = None
	r.Term = term
	r.electionElapsed = 0
	// i think the `electionTimeout` should be randomized here.
	ran := rand.New(rand.NewSource(time.Now().UnixNano()))
	r.randomizedElectionTimeout = ran.Intn(r.electionTimeout) + r.electionTimeout
}

// becomeCandidate transform this peer's state to candidate
// WHEN: 1. follower -> candidate: times out, start election
// 2. candidate -> candidate: times out, new election
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.Term++
	// if there's only one node...
	if len(r.Prs) <= 1 {
		r.becomeLeader()
		return
	}
	r.State = StateCandidate
	r.electionElapsed = 0
	r.Lead = None
	// initialize votes
	r.votes = make(map[uint64]bool, 0)
	// vote for itself
	r.votes[r.id] = true
	r.Vote = r.id
	// i think the `electionTimeout` should be randomized here.
	ran := rand.New(rand.NewSource(time.Now().UnixNano()))
	r.randomizedElectionTimeout = ran.Intn(r.electionTimeout) + r.electionTimeout
}

// becomeLeader transform this peer's state to leader
// WHEN: candidate -> leader: receives votes from majority of servers
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.heartbeatElapsed = 0
	r.State = StateLeader
	r.Lead = r.id
	r.Vote = None
	//propose no-op entry
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{Index: r.RaftLog.LastIndex() + 1, Term: r.Term})
	// what if there's only one node?
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
	}
	// initialize the progress and try to append new no-op entry to followers
	for id := range r.Prs {
		if id == r.id {
			r.Prs[id] = &Progress{r.RaftLog.LastIndex(), r.RaftLog.LastIndex() + 1}
		} else {
			r.Prs[id] = &Progress{0, r.RaftLog.LastIndex()}
			r.sendAppend(id)
		}
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
			r.sendVoteMessage()
		case pb.MessageType_MsgRequestVote:
			r.msgs = append(r.msgs, r.handleVoteRequest(m))
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
			r.sendVoteMessage()
		case pb.MessageType_MsgRequestVote:
			r.msgs = append(r.msgs, r.handleVoteRequest(m))
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleVoteResponse(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgBeat:
			r.sendHeartbeatMessage()
		case pb.MessageType_MsgRequestVote:
			r.msgs = append(r.msgs, r.handleVoteRequest(m))
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgHeartbeatResponse:
			r.handleHeartbeatResponse(m)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgPropose:
			r.handlePropose(m)
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendEntriesResponse(m)
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// fmt.Printf("peer%d get message: %v\n", r.id, m)
	// Your Code Here (2A).
	message := pb.Message{
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Index:   m.Index,
		Reject:  true,
		MsgType: pb.MessageType_MsgAppendResponse,
	}
	if m.Term < r.Term {
		r.msgs = append(r.msgs, message)
		return
	}
	// need to check if index & term match
	r.becomeFollower(m.Term, m.From)
	message.Term = r.Term
	myTerm, err := r.RaftLog.Term(m.Index)
	// if unmatch
	if err != nil && err != ErrCompacted {
		r.msgs = append(r.msgs, message)
		return
	}
	if err != ErrCompacted && myTerm != m.LogTerm {
		r.msgs = append(r.msgs, message)
		return
	}
	// it matches, should apply append
	message.Reject = false
	r.RaftLog.Append(m.Index, m.Entries)
	message.Index = r.RaftLog.LastIndex()
	// update commit
	if r.RaftLog.committed < m.Commit {
		if len(m.Entries) == 0 {
			r.RaftLog.committed = min(m.Commit, max(r.RaftLog.committed, m.Index))
		} else {
			r.RaftLog.committed = min(m.Commit, max(r.RaftLog.committed, m.Entries[len(m.Entries)-1].Index))
		}
	}
	r.msgs = append(r.msgs, message)
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		return
	}
	if m.Reject {
		// next--, try again
		r.Prs[m.From].Next--
		r.sendAppend(m.From)
		return
	}
	r.Prs[m.From].Match = max(r.Prs[m.From].Match, m.Index)
	r.Prs[m.From].Next = r.Prs[m.From].Match + 1
	// if there still have entries to send, do it
	if r.Prs[m.From].Match < r.RaftLog.LastIndex() {
		r.sendAppend(m.From)
	}
	// check if any entries can be committed
	var matchIndex []uint64
	for id := range r.Prs {
		matchIndex = append(matchIndex, r.Prs[id].Match)
	}
	sort.Slice(matchIndex, func(i, j int) bool {
		return matchIndex[i] < matchIndex[j]
	})
	half := (len(r.Prs)+1)/2 - 1
	halfTerm := mustTerm(r.RaftLog.Term(matchIndex[half]))
	if matchIndex[half] > r.RaftLog.committed && halfTerm >= r.Term {
		r.RaftLog.committed = matchIndex[half]
		for id := range r.Prs {
			if r.id != id {
				r.sendAppend(id)
			}
		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	message := pb.Message{
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  true,
		MsgType: pb.MessageType_MsgHeartbeatResponse,
	}
	// in this case we tell the sender that he is out of date.
	if m.Term < r.Term {
		r.msgs = append(r.msgs, message)
		return
	}
	// m.Term >= r.Term
	// reset electionElapsed
	r.becomeFollower(m.Term, m.From)
	message.Reject = false
	message.Term = r.Term
	commit := min(r.RaftLog.LastIndex(), m.Commit)
	r.RaftLog.committed = max(r.RaftLog.committed, commit)
	message.Index = r.RaftLog.committed
	r.msgs = append(r.msgs, message)
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		return
	}
	if m.Index < r.RaftLog.committed {
		r.sendAppend(m.From)
	}
}

// give the response message of the vote request
func (r *Raft) handleVoteRequest(m pb.Message) pb.Message {
	// fmt.Printf("peer%d get message: %v\n", r.id, m)
	message := pb.Message{
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  true,
		MsgType: pb.MessageType_MsgRequestVoteResponse,
	}
	if m.Term < r.Term {
		// fmt.Printf("peer%d response vote: 1\n", r.id)
		return message
	}
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		message.Term = r.Term
	}
	// if i have been voted:
	if m.From != r.Vote && r.Vote != None {
		// fmt.Printf("peer%d response vote: 2\n", r.id)
		return message
	}
	// vote available
	myIndex := r.RaftLog.LastIndex()
	myLogTerm := mustTerm(r.RaftLog.Term(myIndex))
	if m.LogTerm > myLogTerm || (m.LogTerm == myLogTerm && m.Index >= myIndex) {
		r.Vote = m.From
		message.Reject = false
		// fmt.Printf("peer%d response vote: 3\n", r.id)
		return message
	}
	// fmt.Printf("peer%d response vote: 4\n", r.id)
	return message
}

// handle the vote response
func (r *Raft) handleVoteResponse(m pb.Message) {
	// fmt.Printf("peer%d get message: %v\n", r.id, m)
	// should not happen (maybe the wondering message)
	if m.Term < r.Term {
		return
	}
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		return
	}
	// if m.term == r.Term
	_, voted := r.votes[m.From]
	if !voted {
		r.votes[m.From] = !m.Reject
		decided, isWin := r.winOrLose()
		if decided {
			if isWin {
				r.becomeLeader()
			} else {
				r.becomeFollower(r.Term, None)
			}
		}
	}
}

// handle the propose request
func (r *Raft) handlePropose(m pb.Message) {
	// the propose request only contain the entry data
	// need to fill the entries first
	lastIndex := r.RaftLog.LastIndex()
	for i, entry := range m.Entries {
		entry.Index = lastIndex + uint64(i) + 1
		entry.Term = r.Term
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}

	for id := range r.Prs {
		if id == r.id {
			r.Prs[id].Match = r.RaftLog.LastIndex()
			r.Prs[id].Next = r.Prs[id].Match + 1
			if len(r.Prs) == 1 {
				r.RaftLog.committed = r.Prs[id].Match
			}
			continue
		} else {
			r.sendAppend(id)
		}
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

//send vote request
func (r *Raft) sendVoteMessage() {
	lastIndex := r.RaftLog.LastIndex()
	logTerm, err := r.RaftLog.Term(lastIndex)
	if err != nil && err != ErrCompacted {
		panic(err)
	}
	for id := range r.Prs {
		if id != r.id {
			message := pb.Message{
				From:    r.id,
				To:      id,
				MsgType: pb.MessageType_MsgRequestVote,
				Term:    r.Term,
				Commit:  r.RaftLog.committed,
				Index:   lastIndex,
				LogTerm: logTerm,
			}
			r.msgs = append(r.msgs, message)
		}
	}
}

//send Heart beat message
func (r *Raft) sendHeartbeatMessage() {
	for id := range r.Prs {
		if id != r.id {
			r.sendHeartbeat(id)
		}
	}
}

func (r *Raft) winOrLose() (bool, bool) {
	var ticketCounts, antiTicketCounts int
	for _, v := range r.votes {
		if v {
			ticketCounts++
		} else {
			antiTicketCounts++
		}
	}
	if ticketCounts > len(r.Prs)/2 {
		return true, true
	}
	if antiTicketCounts > len(r.Prs)/2 {
		return true, false
	}
	return false, false
}

func (r *Raft) sendSnapshot(to uint64) {
	// first send snapshot, here we ignore it.
	nextIndex := r.RaftLog.first
	logTerm := mustTerm(r.RaftLog.Term(nextIndex))
	lastIndex := r.RaftLog.LastIndex()
	entries := make([]*pb.Entry, 0)
	if nextIndex <= lastIndex {
		entsToAppend, _ := r.RaftLog.Entries(nextIndex, lastIndex+1)
		for _, entry := range entsToAppend {
			temp := entry
			entries = append(entries, &temp)
		}
	}
	message := pb.Message{
		From:    r.id,
		To:      to,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgAppend,
		Index:   nextIndex - 1,
		LogTerm: logTerm,
		Entries: entries,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, message)
}

func (r *Raft) GetHardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}
func (r *Raft) GetSoftState() *SoftState {
	return &SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}

func (r *Raft) Advance(ready Ready) {
	r.RaftLog.Advance(ready)
}

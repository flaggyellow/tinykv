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
	"math/rand"
	"sort"
	"time"

	"github.com/pingcap-incubator/tinykv/log"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
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

// 通过map提供对StateType转换string的方法
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
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

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
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
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
	hardState, confState, _ := c.Storage.InitialState()
	// if the the peers in config is empty, we need to get it from confstate
	if c.peers == nil {
		c.peers = confState.Nodes
	}
	// initialize raft
	// NOTE: the electionTimeout here is only a basic timeout value, the true timeout is up to randomizedElectionTimeout
	raft := &Raft{
		id:                        c.ID,
		Term:                      hardState.Term,
		Vote:                      hardState.Vote,
		RaftLog:                   newLog(c.Storage),
		Prs:                       make(map[uint64]*Progress, len(c.peers)),
		State:                     StateFollower,
		votes:                     make(map[uint64]bool, 0),
		msgs:                      make([]pb.Message, 0),
		Lead:                      None,
		heartbeatElapsed:          0,
		heartbeatTimeout:          c.HeartbeatTick,
		electionElapsed:           0,
		electionTimeout:           c.ElectionTick,
		randomizedElectionTimeout: c.ElectionTick,
	}

	// for the initialization, the randomizedElectionTimeout need to be randomized.
	ran := rand.New(rand.NewSource(time.Now().UnixNano()))
	raft.randomizedElectionTimeout = ran.Intn(raft.electionTimeout) + raft.electionTimeout

	// apply the `applied` (restart case)
	if c.Applied > 0 {
		raft.RaftLog.applied = c.Applied
	}

	// initialize Prs
	for _, i := range c.peers {
		raft.Prs[i] = &Progress{}
	}
	return raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	// we will send append when:
	// 1. leader get new entry
	// 2. handling heart beat response
	// 3. handling append response

	// get the index and term according to the `next`
	nextIndex := r.Prs[to].Next
	logTerm, err := r.RaftLog.Term(nextIndex - 1)
	if err != nil {
		if err == ErrCompacted {
			r.sendSnapshot(to)
			return true
		}
		log.Errorf("Failed to get last log term in `sendAppend()`")
		panic(err)
	}

	lastIndex := r.RaftLog.LastIndex()
	entries := make([]*pb.Entry, 0)
	if nextIndex <= lastIndex {
		entsToAppend, _ := r.RaftLog.Entries(nextIndex, lastIndex+1)
		// fmt.Printf("sendAppend: %v\n", entsToAppend)
		for _, entry := range entsToAppend {
			temp := entry
			entries = append(entries, &temp)
		}
		// fmt.Printf("sendAppend#2: %v\n", entries)
	}
	// send append
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
	log.Debugf("peer%d(%s, term %d) send append to peer%d, with index%d, term%d, entries%v",
		r.id, r.State.String(), r.Term, to, message.Index, message.LogTerm, message.Entries)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	progress, ok := r.Prs[to]
	var commit uint64
	if ok {
		commit = min(r.RaftLog.committed, progress.Match)
	} else {
		log.Warnf("peer %d with state %s is trying to send a heartbeat with no Progress information of peer %d!", r.id, r.State.String(), to)
		commit = r.RaftLog.committed
	}
	message := pb.Message{
		From:    r.id,
		To:      to,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgHeartbeat,
		Commit:  commit,
	}
	r.msgs = append(r.msgs, message)
}

func (r *Raft) sendVoteRequest() {
	lastIndex := r.RaftLog.LastIndex()
	lastTerm, err := r.RaftLog.Term(lastIndex)
	if err != nil {
		log.Errorf("failed to get last term in local hup.")
	}
	for id := range r.Prs {
		if id != r.id {
			message := pb.Message{
				From:    r.id,
				To:      id,
				MsgType: pb.MessageType_MsgRequestVote,
				Term:    r.Term,
				Index:   lastIndex,
				LogTerm: lastTerm,
			}
			r.msgs = append(r.msgs, message)
		}
	}
}

func (r *Raft) sendSnapshot(to uint64) {

}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower, StateCandidate:
		r.electionElapsed++
		if r.electionElapsed >= r.randomizedElectionTimeout {
			r.electionElapsed = 0
			// every time we meet electionTimeout, we randomize it again.
			ran := rand.New(rand.NewSource(time.Now().UnixNano()))
			r.randomizedElectionTimeout = ran.Intn(r.electionTimeout) + r.electionTimeout
			message := pb.Message{
				To:      r.id,
				From:    r.id,
				MsgType: pb.MessageType_MsgHup,
			}
			// the local message shall not be passed into the message queue,
			// it shall be handled imidiately.
			r.Step(message)
		}
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			message := pb.Message{
				To:      r.id,
				From:    r.id,
				MsgType: pb.MessageType_MsgBeat,
			}
			r.Step(message)
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.Term = term
	r.Vote = None
	r.State = StateFollower
	r.votes = make(map[uint64]bool, 0)
	r.Lead = lead
	r.electionElapsed = 0
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.Term++
	// if there's only one node...
	if len(r.Prs) <= 1 {
		r.becomeLeader()
		return
	}
	// we shall not vote for myself directly, there might be problems in project3.
	// here we just set the state
	r.Vote = None
	r.State = StateCandidate
	r.votes = make(map[uint64]bool, 0)
	r.Lead = None
	r.electionElapsed = 0
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	log.Warnf("node %d becomeLeader at term %d with index %d", r.id, r.Term, r.RaftLog.LastIndex())
	// term would not change
	r.Vote = None
	r.State = StateLeader
	r.votes = make(map[uint64]bool, 0)
	r.Lead = r.id
	r.heartbeatElapsed = 0
	// initialize Prs
	for peer, prs := range r.Prs {
		if peer == r.id {
			prs.Match = r.RaftLog.LastIndex()
			prs.Next = r.RaftLog.LastIndex() + 1
		} else {

			prs.Match = 0
			prs.Next = r.RaftLog.LastIndex() + 1
		}
	}
	//propose no-op entry, reuse the MsgPropose type
	message := pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		To:      r.id,
		From:    r.id,
		Entries: append([]*pb.Entry{}, &pb.Entry{
			EntryType: pb.EntryType_EntryNormal,
			Data:      nil}),
	}
	r.Step(message)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
// NOTE: handle
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.handleLocalHupReq(m)
	case pb.MessageType_MsgBeat:
		r.handleLocalBeatReq(m)
	case pb.MessageType_MsgPropose:
		r.handleMsgPropose(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handelAppendResponse(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)
	case pb.MessageType_MsgTransferLeader:
		r.handleMsgTransferLeader(m)
	case pb.MessageType_MsgTimeoutNow:
		r.handleMsgTimeoutNow(m)
	default:
		panic("unknown message type!!")
	}
	return nil
}

// -*- handleLocalHupReq handles the local hang up request
// this might be called by both raft itself and upper applications
func (r *Raft) handleLocalHupReq(m pb.Message) {
	switch r.State {
	// leader will ignore this
	case StateFollower, StateCandidate:
		// first become candidate
		r.becomeCandidate()
		// vote for myself
		r.Vote = r.id
		r.votes[r.id] = true
		// send vote request
		r.sendVoteRequest()
	}
}

// -*- handleLocalBeatReq handles the local heatbeat request
// this might be called by both raft itself and upper applications
func (r *Raft) handleLocalBeatReq(m pb.Message) {
	// only leader will take this
	switch r.State {
	case StateLeader:
		for id := range r.Prs {
			if id != r.id {
				r.sendHeartbeat(id)
			}
		}
	}
}

// -*- handleMsgPropose handles MsgPropose
// this might be called by both raft itself and upper applications
func (r *Raft) handleMsgPropose(m pb.Message) {
	// check role
	if r.State == StateCandidate || r.State == StateFollower {
		log.Warnf("peer %d state %s recieve a propose", r.id, r.State.String())
		return
	}
	if len(m.Entries) != 1 {
		log.Errorf("Leader should only Propose Entry once a time!")
	}
	// start propose
	entry := *m.Entries[0]
	entry.Term = r.Term
	entry.Index = r.RaftLog.LastIndex() + 1
	r.RaftLog.entries = append(r.RaftLog.entries, entry)
	// if only one node
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
		return
	}
	// renew prs
	for id, pr := range r.Prs {
		if id == r.id {
			pr.Match = r.RaftLog.LastIndex()
			pr.Next = pr.Match + 1
		}
		if pr.Next == r.RaftLog.LastIndex()-1 {
			pr.Next = r.RaftLog.LastIndex()
		}
	}
	// sendappend
	for peer := range r.Prs {
		if peer != r.id {
			r.sendAppend(peer)
		}
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	// defalult message template
	message := pb.Message{
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  true,
		Index:   m.Index,
		MsgType: pb.MessageType_MsgAppendResponse,
	}
	//if his term smaller than me, refuse and tell him
	if m.Term < r.Term {
		r.msgs = append(r.msgs, message)
		log.Debugf("peer%d reject peer%d's append, because it's term smaller than him", r.id, m.From)
		return
	}
	// if his term larger, i am out of date, be a follower
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
		message.Term = r.Term
	} else {
		// m.Term == r.Term
		if r.State == StateLeader {
			panic("handleAppendEntries: there are two leaders in the raft group!")
		}
		// candidate should accept the new leader
		r.becomeFollower(m.Term, m.From)
	}

	// check whether index & term match
	lastIndex := r.RaftLog.LastIndex()
	// the local log is not long enough
	if lastIndex < m.Index {
		r.msgs = append(r.msgs, message)
		log.Debugf("peer%d reject peer%d's append, because log not match", r.id, m.From)
		return
	}
	// the local log is long enough
	myTerm, err := r.RaftLog.Term(m.Index)
	if err == ErrCompacted {
		// don't know how to handle this
		panic("when appending the entries, the index is Compacted")
	}
	if err != nil {
		panic("handleAppendEntries: unknown err")
	}
	// if the term is not right
	if myTerm != m.LogTerm {
		r.msgs = append(r.msgs, message)
		log.Debugf("peer%d reject peer%d's append, because log not match", r.id, m.From)
		return
	}
	// no err in term, and term matches, apply append
	message.Reject = false
	// convert the entries
	entries := make([]pb.Entry, len(m.Entries))
	for i := range entries {
		entries[i] = *m.Entries[i]
	}
	r.RaftLog.Append(m.Index, entries)
	message.Index = r.RaftLog.LastIndex()
	// update commit
	// now the log should be the same with the time when leader send this message
	// what if it's a old message?
	if r.RaftLog.committed < m.Commit {
		if m.Commit > m.Index+uint64(len(m.Entries)) {
			log.Errorf("m.Commit is large than the append entry, %d vs %d", m.Commit, m.Index+uint64(len(m.Entries)))
		}
		if m.Commit > r.RaftLog.LastIndex() {
			log.Errorf("m.Commit is large than peer %d 's lastlog, %d vs %d", r.id, m.Commit, r.RaftLog.LastIndex())
		}
		r.RaftLog.committed = min(min(m.Commit, m.Index+uint64(len(m.Entries))), r.RaftLog.LastIndex())
	}
	// safety check
	if r.RaftLog.committed > m.Commit {
		log.Warnf("peer %d handles an old append request with commit %d, it's own commit is %d",
			r.id, m.Commit, r.RaftLog.committed)
	}
	message.Commit = r.RaftLog.committed
	r.msgs = append(r.msgs, message)
	log.Debugf("peer%d accept peer%d's append", r.id, m.From)
}

func (r *Raft) handelAppendResponse(m pb.Message) {
	if m.Term < r.Term {
		// in this case, the message is useless
		return
	}
	if m.Term > r.Term {
		// i am out of date
		r.becomeFollower(m.Term, None)
		return
	}
	// m.Term == r.Term
	// there are two cases:
	// 1. not match
	// 2. matches
	// make sure i am leader
	if r.State != StateLeader {
		log.Warnf("peer %d is not a leader, but handling the append responce", r.id)
		return
	}
	// if not match:
	if m.Reject {
		// m.Index false to match
		// case 0: the message is fresh, m.Index == next-1
		// case 1: the message is old, m.Index < next-1
		// case 2: the message is old, m.Index >= next
		// case 3: the message is old, m.Index == next-1
		// if m.Index >= next, there's no more useful infomation, just ignore
		// if m.Index < next, we should treat it carefully, just send new append
		if m.Index <= r.Prs[m.From].Next-1 {
			r.Prs[m.From].Next = m.Index
			r.sendAppend(m.From)
		}
		return
	}
	// if matches, m.Index can show the logs that have been matched
	r.Prs[m.From].Match = m.Index
	// the message can be old, we have two choice
	// 1. every match, next = lastlog (optimistic)
	// 2. every match, next = match+1 (pessimistic)
	r.Prs[m.From].Next = r.RaftLog.LastIndex() + 1
	// try to enhance the commit
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

func (r *Raft) handleRequestVote(m pb.Message) {
	// default message template
	message := pb.Message{
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  true,
		MsgType: pb.MessageType_MsgRequestVoteResponse,
	}
	// his term smaller than me, refuse.
	if m.Term < r.Term {
		r.msgs = append(r.msgs, message)
		return
	}
	// his term bigger than me, time to vote, but something need to be checked
	// anyway i need to become a follower first
	if m.Term > r.Term {
		// do we need to reset the election timeout here?
		// no, we should reset the timeout only when we accept the vote request
		r.Term = m.Term
		r.Vote = None
		r.State = StateFollower
		r.votes = make(map[uint64]bool, 0)
		r.Lead = None
		message.Term = r.Term
	}
	// bigger term stands for nothing, he can still have older log
	// i need to check if his deserve to be a leader.
	if r.State == StateLeader || r.State == StateCandidate {
		// if i am a leader, his term must be equal to me, i refuse
		// if i am a candidate, i've vote for myself, i refuse
		r.msgs = append(r.msgs, message)
		return
	}
	// in this case i am a follower, if i have been voted:
	if m.From != r.Vote && r.Vote != None {
		r.msgs = append(r.msgs, message)
		return
	}
	// if i have been voted, i'll do the same decision
	if m.From == r.Vote && r.Vote != None {
		message.Reject = false
		r.msgs = append(r.msgs, message)
		return
	}
	// i haven't vote, i need to check the log
	myIndex := r.RaftLog.LastIndex()
	myLogTerm := mustTerm(r.RaftLog.Term(myIndex))
	if m.LogTerm > myLogTerm || (m.LogTerm == myLogTerm && m.Index >= myIndex) {
		r.Vote = m.From
		message.Reject = false
		r.msgs = append(r.msgs, message)
		// here reset the timeout
		r.electionElapsed = 0
		return
	}
	// the log is old, refuse
	r.msgs = append(r.msgs, message)
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	// if the term is smaller than me, the message should be out of date
	if m.Term < r.Term {
		return
	}
	if m.Term > r.Term {
		// someone's term bigger than me, i am out of date, be a follower
		r.becomeFollower(m.Term, None)
		return
	}
	// if i am not a candidate, this is no use for me, return
	if r.State != StateCandidate {
		return
	}
	// in this case m.term == r.Term
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

// first bool means whether the result is decided,
// second bool means whether i win or lose
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

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	// for the default case we reject the message
	message := pb.Message{
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  true,
		MsgType: pb.MessageType_MsgHeartbeatResponse,
	}
	if m.Term < r.Term {
		// in this case we tell the sender that he is out of date.
		r.msgs = append(r.msgs, message)
		return
	}
	if m.Term > r.Term {
		// in this case we must accept that we are out of date, just be follower!
		r.becomeFollower(m.Term, m.From)
	} else {
		// m.Term == r.Term
		// also it sound strange here, but maybe we should do nothing but become a follower
		if r.State == StateLeader {
			panic("handleheartBeat: there are more than two leaders in the raft group!")
		}
		r.becomeFollower(m.Term, m.From)
	}
	// deal with the `commit` in the message
	// NOTE: how to deal with this information?
	// in `becomeLeader()` the initial value of `match` is zero, and the `commit` here is possible to be zero, is this right?
	// as we all know, once we commit it, we shall not change it.
	commit := min(m.Commit, r.RaftLog.LastIndex())
	r.RaftLog.committed = max(commit, r.RaftLog.committed)
	// NOTE: when to change `commit`?
	// 1. leader has matched your log
	// 2. leader send the commit to you
	// 3. the commit is larger than your commit

	// reply : i accept
	message.Reject = false
	message.Term = r.Term
	message.Index = r.RaftLog.LastIndex()
	message.Commit = r.RaftLog.committed
	r.msgs = append(r.msgs, message)

	// THINK: what will happen if the message has wanderred in the internet for some time?
	// case 1: m.Term = r.Term
	// because the Term not change, so the leader not change, the only thing that may case problem is `commit`
	// just make sure the `commit` is always the newer.
	// case 2: m.Term < r.Term
	// no problem! just ignore it!
	// case 3: m.Term > r.Term
	// this peer may have been down for a long time, we can just accept it, and wait for true leader to make it correct.
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if m.Term < r.Term {
		// we shall just ignore it, it's no use any more
		return
	}
	if m.Term > r.Term {
		// this means i am out of date.
		// WARNING: the leader may not be the real one
		// * for leader, just be a follower
		// * for candidate, things may be like this:
		// leader --down--> follower --timeout--> candidate
		// and even though, it's out of date, just be a follower!
		// * for follower:
		// i've been a follower, and i'am out of date, the information of leader might not be correct, so be a follower again
		// anyway, be a follower!
		r.becomeFollower(m.Term, m.From)
		return
	}
	// in this case m.Term == r.Term
	// this means another peer accept me, actually it's no reason to be rejected
	if r.State != StateLeader {
		log.Errorf("peer %d get a validate heartbeat responce but is not the leader.", r.id)
		return
	}
	if m.Reject {
		log.Errorf("peer %d get a validate heartbeat responce but is rejected.", r.id)
	} else {
		// here we can't do much thing to match the log, just check the `committed` !
		// WARNING: If no new entry come, some entry will never be committed. (if only check `m.Commit`)
		// i notice that my `Match(m.From)` always <= LastIndex(m.From) and
		// my LastIndex always >= Match(m.From)
		// if the three are same, the log is same.
		if m.Commit < r.RaftLog.committed || m.Index > r.Prs[m.From].Match || m.Index < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
	}
}

func (r *Raft) softState() *SoftState {
	return &SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

func (r *Raft) handleMsgTransferLeader(m pb.Message) {

}

func (r *Raft) handleMsgTimeoutNow(m pb.Message) {

}

// 3A part
// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

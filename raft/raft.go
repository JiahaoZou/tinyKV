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
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"time"

	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// 是否开启log
const debug = false

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

func (s *StateType) StateInfo() string {
	switch *s {
	case StateFollower:
		return "follower"
	case StateCandidate:
		return "candidate"
	case StateLeader:
		return "leader"
	}
	return ""
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

// 用到了一个消息结构
// Msg 分为 Local Msg 和 Common Msg。前者是本地发起的 Msg，多为上层传递下来的，
// 比如提交条目、请求心跳、请求选举等等，这些 Msg 不会在节点之间传播，它们的 term 也相应的等于 0。
// 后者就是节点之间发送的 Msg，用来集群之间的同步。

type Raft struct {
	// 配置信息
	config Config
	// raft id
	id uint64
	// 任期
	Term uint64
	// 当前投票给的ID
	Vote uint64
	// the log
	// log管理器
	RaftLog *RaftLog

	// log replication progress of each peers
	// 即match数组和next数组
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
	transferElapsed  int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	r := &Raft{
		config:           *c,
		id:               c.ID,
		Prs:              make(map[uint64]*Progress),
		votes:            make(map[uint64]bool),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		RaftLog:          newLog(c.Storage),
	}
	// 取得已经保存的raft节点状态，用于raft节点的重启或是创建
	// 初始化的数据是由storage从配置文件里读的
	hardSt, confSt, _ := r.RaftLog.storage.InitialState()
	if c.peers == nil {
		c.peers = confSt.Nodes
	}
	lastIndex := r.RaftLog.LastIndex()
	for _, peer := range c.peers {
		if peer == r.id {
			r.Prs[peer] = &Progress{Next: lastIndex + 1, Match: lastIndex}
		} else {
			r.Prs[peer] = &Progress{Next: lastIndex + 1}
		}
	}
	r.becomeFollower(0, None)
	r.resetElectionTimeout()
	r.Term, r.Vote, r.RaftLog.committed = hardSt.GetTerm(), hardSt.GetVote(), hardSt.GetCommit()
	if c.Applied > 0 {
		r.RaftLog.applied = c.Applied
	}
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
// 所有的send方法都是将msg加入msgs队列，然后被step消费掉
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	r.Log("send append to %d", to)
	// 获取上次发的index，如果被压缩了，就转为发快照
	prevIndex := r.Prs[to].Next - 1
	// 注意，如果preindex是快照的最后一个log，是可以正常返回Term的，此时不会发快照
	prevLogTerm, err := r.RaftLog.Term(prevIndex)
	if err != nil {
		if err == ErrCompacted {
			// 发送snapshot
			r.sendSnapshot(to)
			return false
		}
		panic(err)
	}
	var entries []*pb.Entry
	n := len(r.RaftLog.entries)
	// 从上一个已发送的数据起，一直到最后一个log
	for i := r.RaftLog.toSliceIndex(prevIndex + 1); i < n; i++ {
		entries = append(entries, &r.RaftLog.entries[i])
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
		LogTerm: prevLogTerm,
		Index:   prevIndex,
		Entries: entries,
	}
	r.msgs = append(r.msgs, msg)
	return true
}

func (r *Raft) sendAppendResponse(to uint64, reject bool, term, index uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
		LogTerm: term,
		Index:   index,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendSnapshot(to uint64) {
	snapshot, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		return
	}
	msg := pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		From:     r.id,
		To:       to,
		Term:     r.Term,
		Snapshot: &snapshot,
	}
	r.msgs = append(r.msgs, msg)
	r.Prs[to].Next = snapshot.Metadata.Index + 1
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	})
}

func (r *Raft) sendHeartbeatResponse(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendRequestVote(to, index, term uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		LogTerm: term,
		Index:   index,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendRequestVoteResponse(to uint64, reject bool) {
	if reject {
		r.Log("sendVoteResponse with reject true")
	} else {
		r.Log("sendVoteResponse with reject false")
	}

	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.tickElection()
	case StateCandidate:
		r.tickElection()
	case StateLeader:
		if r.leadTransferee != None {
			r.tickTransfer()
		}
		r.tickHeartbeat()
	}
}

func (r *Raft) tickElection() {
	r.electionElapsed++
	if r.electionElapsed >= r.electionTimeout {
		r.electionElapsed = 0
		r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
	}
}

func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
	}
}

func (r *Raft) tickTransfer() {
	r.transferElapsed++
	if r.transferElapsed >= r.electionTimeout*2 {
		r.transferElapsed = 0
		r.leadTransferee = None
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	if term > r.Term {
		r.Log("becomeFollower: term:" + strconv.Itoa(int(term)) + " lead:" + strconv.Itoa(int(lead)))
	}
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.Vote = None
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.Log("becomeCandidate")
	r.State = StateCandidate
	r.Lead = None
	r.Term++
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.Log("becomeLeader")
	r.State = StateLeader
	// 初始化nextIndex和matchIndex
	for peer := range r.Prs {
		if peer == r.id {
			r.Prs[peer].Next = r.RaftLog.LastIndex() + 2
			r.Prs[peer].Match = r.RaftLog.LastIndex() + 1
		} else {
			r.Prs[peer].Next = r.RaftLog.LastIndex() + 1
		}
	}
	// append一条空日志
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{Term: r.Term, Index: r.RaftLog.LastIndex() + 1})
	r.Lead = r.id
	r.heartbeatElapsed = 0
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	}
	r.broadcastAppend()
}

func (r *Raft) broadcastAppend() {
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendAppend(peer)
	}
}

// Step() 作为驱动器，用来接收上层发来的 Msg，然后根据不同的角色和不同的 MsgType 进行不同的处理。
// 首先，通过 switch-case 将 Step() 按照角色分为三个函数，
// 分别为：FollowerStep() 、CandidateStep()、LeaderStep() 。
// 接着，按照不同的 MsgTaype，将每个 XXXStep() 分为 12 个部分，用来处理不同的 Msg。
// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	r.Log("handle step")
	// lab3中内容，现在不需要太在意
	if _, ok := r.Prs[r.id]; !ok && m.MsgType == pb.MessageType_MsgTimeoutNow {
		return nil
	}
	if m.Term > r.Term {
		// 根据论文，任何raft收到大term的消息都要变成follower
		r.leadTransferee = None
		r.becomeFollower(m.Term, None)
	}
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.doElection()
		case pb.MessageType_MsgBeat:
		case pb.MessageType_MsgPropose:
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgAppendResponse:
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgRequestVoteResponse:
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgHeartbeatResponse:
		case pb.MessageType_MsgTransferLeader:
			if r.Lead != None {
				m.To = r.Lead
				r.msgs = append(r.msgs, m)
			}
		case pb.MessageType_MsgTimeoutNow:
			r.doElection()
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.doElection()
		case pb.MessageType_MsgBeat:
		case pb.MessageType_MsgPropose:
		case pb.MessageType_MsgAppend:
			if m.Term == r.Term {
				r.becomeFollower(m.Term, m.From)
			}
			r.handleAppendEntries(m)
		case pb.MessageType_MsgAppendResponse:
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleRequestVoteResponse(m)
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		case pb.MessageType_MsgHeartbeat:
			if m.Term == r.Term {
				r.becomeFollower(m.Term, m.From)
			}
			r.handleHeartbeat(m)
		case pb.MessageType_MsgHeartbeatResponse:
		case pb.MessageType_MsgTransferLeader:
			if r.Lead != None {
				m.To = r.Lead
				r.msgs = append(r.msgs, m)
			}
		case pb.MessageType_MsgTimeoutNow:
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
		case pb.MessageType_MsgBeat:
			r.broadcastHeartbeat()
		case pb.MessageType_MsgPropose:
			if r.leadTransferee == None {
				r.appendEntries(m.Entries)
			}
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendEntriesResponse(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgRequestVoteResponse:
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgHeartbeatResponse:
			r.sendAppend(m.From)
		case pb.MessageType_MsgTransferLeader:
			r.handleTransferLeader(m)
		case pb.MessageType_MsgTimeoutNow:
		}
	}
	return nil
}

func (r *Raft) FollowerStep(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.doElection()
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
		if r.Lead != None {
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}
	case pb.MessageType_MsgTimeoutNow:
		r.doElection()
	}
}

func (r *Raft) CandidateStep(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.doElection()
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppend:
		if m.Term == r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		if m.Term == r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
		if r.Lead != None {
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}
	case pb.MessageType_MsgTimeoutNow:
	}
}

func (r *Raft) LeaderStep(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
	case pb.MessageType_MsgBeat:
		r.broadcastHeartbeat()
	case pb.MessageType_MsgPropose:
		if r.leadTransferee == None {
			r.appendEntries(m.Entries)
		}
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntriesResponse(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.sendAppend(m.From)
	case pb.MessageType_MsgTransferLeader:
		r.handleTransferLeader(m)
	case pb.MessageType_MsgTimeoutNow:
	}
}

func (r *Raft) doElection() {
	r.becomeCandidate()
	r.heartbeatElapsed = 0
	r.resetElectionTimeout()
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}
	lastIndex := r.RaftLog.LastIndex()
	lastLogTerm, _ := r.RaftLog.Term(lastIndex)
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendRequestVote(peer, lastIndex, lastLogTerm)
	}
}
func (r *Raft) doAppend(entries []*pb.Entry) {
	for i, entry := range entries {
		if entry.Index < r.RaftLog.FirstIndex {
			// 跳过已压缩的日志
			continue
		}
		if entry.Index <= r.RaftLog.LastIndex() {
			logTerm, err := r.RaftLog.Term(entry.Index)
			if err != nil {
				panic(err)
			}
			if logTerm != entry.Term {
				// 覆盖冲突的日志
				idx := r.RaftLog.toSliceIndex(entry.Index)
				r.RaftLog.entries[idx] = *entry
				r.RaftLog.entries = r.RaftLog.entries[:idx+1]
				r.RaftLog.stabled = min(r.RaftLog.stabled, entry.Index-1)
			}
		} else {
			// 追加日志
			n := len(entries)
			for j := i; j < n; j++ {
				r.RaftLog.entries = append(r.RaftLog.entries, *entries[j])
			}
			break
		}
	}
}

// 处理RPC请求的函数
// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	r.Log("handle append from %d", m.From)
	// 1.判断 Msg 的 Term 是否大于等于自己的 Term，是则更新，否则拒绝
	if m.Term != None && m.Term < r.Term {
		r.sendAppendResponse(m.From, true, None, None)
		return
	}
	// 不管是否更新logs，只要收到大于等于自己任期的包，就更新选举倒计时和leader
	r.electionElapsed = 0
	r.resetElectionTimeout()
	r.Lead = m.From
	lastIndex := r.RaftLog.LastIndex()
	// 2.拒绝，如果 prevLogIndex > r.RaftLog.LastIndex()。否则往下
	// 快速恢复，此时发来的日志index过大，无法连续append
	// 这里的m.index实际上是m中包含数据的前一个index即preindex，所以这里用等于而不用大于等于
	if m.Index > lastIndex {
		r.sendAppendResponse(m.From, true, None, lastIndex+1)
		return
	}
	// 3.拒绝，如果接收者日志中没有包含这样一个条目：即该条目的任期在 prevLogIndex 上能和 prevLogTerm 匹配上。否则往下；
	// 这里preIndex如果小于FirstIndex时没有做特殊处理，是因为这种情况没有必要对leader做特殊的回复，只要在自己实际doAppend中做好判断即可
	// 但在这里做这样一个判断是重要的，因为如果m.index<firstlog,term(m.index)是有可能返回不了值的
	// 生成了的快照一定是可以commit的数据，即一定不会发生冲突
	if m.Index >= r.RaftLog.FirstIndex {
		logTerm, err := r.RaftLog.Term(m.Index)
		if err != nil {
			panic(err)
		}
		if logTerm != m.LogTerm {
			// 快速恢复，此时出现了日志冲突，返回发生冲突的日志index
			// sort是go官方包
			// func Search(n int, f func(int) bool) int
			// Search uses binary search to find and return the smallest index i in [0, n) at which f(i) is true
			index := r.RaftLog.toEntryIndex(sort.Search(r.RaftLog.toSliceIndex(m.Index+1),
				func(i int) bool { return r.RaftLog.entries[i].Term == logTerm }))
			r.sendAppendResponse(m.From, true, logTerm, index)
			return
		}
	}

	// 追加日志
	r.doAppend(m.Entries)

	if m.Commit > r.RaftLog.committed {
		// 追赶leader的apply进度，leader已经apply的日志可能还没发过来，所以要比较大小
		r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
	}
	r.sendAppendResponse(m.From, false, None, r.RaftLog.LastIndex())
}

// handleHeartbeat handle Heartbeat RPC request
// 用于follower接到heartbit后的处理
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.Term != None && m.Term < r.Term {
		r.sendHeartbeatResponse(m.From, true)
		return
	}
	r.Lead = m.From
	r.electionElapsed = 0
	r.resetElectionTimeout()
	r.sendHeartbeatResponse(m.From, false)
}

// 为leader添加log
func (r *Raft) appendEntries(entries []*pb.Entry) {
	r.Log("handle propose")
	lastIndex := r.RaftLog.LastIndex()
	for i, entry := range entries {
		entry.Term = r.Term
		entry.Index = lastIndex + uint64(i) + 1
		if entry.EntryType == pb.EntryType_EntryConfChange {
			// 一次只能有一个ConfChange
			if r.PendingConfIndex != None {
				continue
			}
			r.PendingConfIndex = entry.Index
		}
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}
	// 更新自己的Match和Next？
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1
	// 添加了新log，广播一次append
	r.broadcastAppend()
	// 如果集群只有一个节点，就不需要走共识流程，最新的logindex就是commitindex
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	meta := m.Snapshot.Metadata

	if meta.Index <= r.RaftLog.committed {
		// 避免commit回退
		r.sendAppendResponse(m.From, false, None, r.RaftLog.committed)
		return
	}

	r.becomeFollower(max(r.Term, m.Term), m.From)
	first := meta.Index + 1

	// 既然leader发来了snapshot，说明已有的entries都没用
	if len(r.RaftLog.entries) > 0 {
		r.RaftLog.entries = nil
	}

	r.RaftLog.FirstIndex = first

	//重设commit信息于apply信息，这里在第一次写的时候没做
	r.RaftLog.applied = meta.Index
	r.RaftLog.committed = meta.Index
	r.RaftLog.stabled = meta.Index

	r.Prs = make(map[uint64]*Progress)
	for _, peer := range meta.ConfState.Nodes {
		r.Prs[peer] = &Progress{}
	}
	r.RaftLog.pendingSnapshot = m.Snapshot
	r.sendAppendResponse(m.From, false, None, r.RaftLog.LastIndex())
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	if _, ok := r.Prs[id]; !ok {
		r.Prs[id] = &Progress{Next: 1}
	}
	r.PendingConfIndex = None
	// 这里需要发送一个commitIndex为0的heartbeat，用于让其他raftStore创建新的peer，新的peer会将初始log的index和term都设为0，所以会立即
	// 触发leader的snapshot，新peer会根据snapshot带来的信息来更新自己的数据。这也是为什么创建store的时候所有raft的初始index为5
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      id,
		From:    r.id,
		Term:    r.Term,
		Commit:  util.RaftInvalidIndex,
	})
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	if _, ok := r.Prs[id]; ok {
		delete(r.Prs, id)
		if r.State == StateLeader {
			// 移除一个节点后可能会推进commit，尝试一下
			r.leaderCommit()
		}
	}
	r.PendingConfIndex = None
}

// follower调用 handleRequestVote 处理投票请求
func (r *Raft) handleRequestVote(m pb.Message) {
	r.Log("handleRequestVote")
	// 1.判断 Msg 的 Term 是否大于等于自己的 Term，是则更新，否则拒绝；
	if m.Term != None && m.Term < r.Term {
		r.sendRequestVoteResponse(m.From, true)
		return
	}
	// 2.如果 votedFor 不为空或者不等于 candidateID，则说明该节点以及投过票了，直接拒绝。否则往下；
	if r.Vote != None && r.Vote != m.From {
		r.sendRequestVoteResponse(m.From, true)
		return
	}

	lastIndex := r.RaftLog.LastIndex()
	lastLogTerm, _ := r.RaftLog.Term(lastIndex)
	// 3.Candidate 的日志至少和自己一样新，那么就给其投票，否者拒绝。新旧判断逻辑如下：
	// --如果两份日志最后的条目的任期号不同，那么任期号大的日志更加新
	// --如果两份日志最后的条目任期号相同，那么日志比较长的那个就更加新；
	if lastLogTerm > m.LogTerm ||
		lastLogTerm == m.LogTerm && lastIndex > m.Index {
		// 候选者日志落后于自己
		r.sendRequestVoteResponse(m.From, true)
		return
	}
	r.Vote = m.From
	r.electionElapsed = 0
	r.resetElectionTimeout()
	r.sendRequestVoteResponse(m.From, false)
}

// candidate接收follower返回的投票结果
func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	// 论文上没有写，但可以加上一个判断如果当前节点不是candidate了，就直接返回的逻辑

	// 如果这个返回是旧的，直接无视即可
	if m.Term != None && m.Term < r.Term {
		return
	}
	if m.Reject {
		r.Log("handleVoteResponse with reject true")
	} else {
		r.Log("handleVoteResponse with reject false")
	}
	// votes是一个map，在一个节点成为candidate时会初始化
	r.votes[m.From] = !m.Reject
	grant := 0
	votes := len(r.votes)
	threshold := len(r.Prs) / 2
	for _, g := range r.votes {
		if g {
			grant++
		}
	}
	r.Log("grant %d", grant)
	if grant > threshold {
		r.becomeLeader()
	} else if votes-grant > threshold {
		r.becomeFollower(r.Term, None)
	}

}

// leader处理follower传回的append结果，只有leader会处理，其它角色忽略即可
func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	r.Log("handler AppendEntriesResponse from %d", m.From)
	// 如果这个回复是因为各种原因而延迟到达的旧任期的回复，直接忽略即可
	if m.Term != None && m.Term < r.Term {
		return
	}
	// 1.如果被 reject 了，那么重置 next。重置规则为将旧的 next --，然后比较 m.Index + 1，
	// 最后取小的那个赋值给 next，然后重新进行日志 / 快照追加；
	if m.Reject {
		index := m.Index
		// follower拒绝了append后会返回期望的index
		if index == None {
			return
		}
		// 如果返回的任期不为none，说明发生了任期冲突，此时返回的index是不准的，需要我们根据任期找到相应的index
		if m.LogTerm != None {
			logTerm := m.LogTerm
			l := r.RaftLog
			sliceIndex := sort.Search(len(l.entries),
				func(i int) bool { return l.entries[i].Term > logTerm })
			// 如果leader有冲突term的日志，则可以从这个term下一个term的日志开始发送。例如上一次append的prevIndex处，follower的term为3，那么
			// leader在这里找到term为3的日志的最后一个index为sliceIndex-1，并从sliceIndex开始发送。
			if sliceIndex > 0 && l.entries[sliceIndex-1].Term == logTerm {
				index = l.toEntryIndex(sliceIndex)
			}
		}
		// 对于发生日志不连续的情况，需要设置连续的nextIndex；
		// 对于日志冲突的情况，会将nextIndex回退到冲突term的第一条日志的index或上一条注释所说明的情况
		// 重发
		r.Prs[m.From].Next = index
		r.sendAppend(m.From)
		return
	}
	// 若成功
	// 成功后要进行统计，看看是否可以更新commit
	if m.Index > r.Prs[m.From].Match {
		r.Prs[m.From].Match = m.Index
		r.Prs[m.From].Next = m.Index + 1
		r.leaderCommit()
		// 完成对leadTransferee的日志同步，leadTransferee可以当选leader
		if m.From == r.leadTransferee && m.Index == r.RaftLog.LastIndex() {
			r.sendTimeoutNow(m.From)
			r.leadTransferee = None
		}
	}
}

// 更新commitIndex
func (r *Raft) leaderCommit() {
	match := make(uint64Slice, len(r.Prs))
	i := 0
	for _, prs := range r.Prs {
		match[i] = prs.Match
		i++
	}
	sort.Sort(match)
	n := match[(len(r.Prs)-1)/2]

	if n > r.RaftLog.committed {
		logTerm, err := r.RaftLog.Term(n)
		if err != nil {
			panic(err)
		}
		if logTerm == r.Term {
			r.RaftLog.committed = n
			r.broadcastAppend()
		}
	}
}

func (r *Raft) handleTransferLeader(m pb.Message) {
	if m.From == r.id {
		return
	}
	if r.leadTransferee != None && r.leadTransferee == m.From {
		return
	}
	if _, ok := r.Prs[m.From]; !ok {
		return
	}
	r.leadTransferee = m.From
	r.transferElapsed = 0

	// 如果leadTransferee日志跟上了就立即让他发起选举，否则让它的日志跟上
	if r.Prs[m.From].Match == r.RaftLog.LastIndex() {
		r.sendTimeoutNow(m.From)
	} else {
		r.sendAppend(m.From)
	}
}

func (r *Raft) sendTimeoutNow(to uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgTimeoutNow,
		From:    r.id,
		To:      to,
	}
	r.msgs = append(r.msgs, msg)
}

// resetElectionTimeout 重置ElectionTimeout为随机数
func (r *Raft) resetElectionTimeout() {
	rand.Seed(time.Now().UnixNano())
	r.electionTimeout = r.config.ElectionTick + rand.Intn(r.config.ElectionTick)
}

func (r *Raft) GetId() uint64 {
	return r.id
}

func (r *Raft) Log(format string, args ...interface{}) {
	if debug {
		outputColor := 30 + r.id
		sprintf := fmt.Sprintf("%c[0;40;%dm id[%v]state[%v]term[%v]commited[%v]applied[%v]stabled[%d]: %c[0m",
			0x1B, outputColor, r.id, r.State.StateInfo(), r.Term, r.RaftLog.committed, r.RaftLog.applied, r.RaftLog.stabled, 0x1B)
		if len(args) != 0 {
			fmt.Printf(sprintf+format+"\n", args...)
		} else {
			fmt.Printf(sprintf + format + "\n")
		}
	}
}

func (r *Raft) broadcastHeartbeat() {
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendHeartbeat(peer)
	}
}

func (r *Raft) softState() *SoftState {
	return &SoftState{Lead: r.Lead, RaftState: r.State}
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

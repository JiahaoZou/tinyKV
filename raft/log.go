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

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	// 论文中的 committedIndex，即节点认为已经提交的日志 Index
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	// 论文中的 lastApplied，即节点最新应用的日志 Index
	// 初始时设置为还未生成快照的index-1
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	// 节点已经持久化的最后一条日志的 Index，初始时通过storage的lastIndex获取
	stabled uint64

	// all entries that have not yet compact.
	// 所有未被 compact 的 entry, 包括持久化与非持久化。
	// 初始时从storage读出来，只有持久化的数据
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	// 未被compact的第一条log
	FirstIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	// 新建一个节点，日志相关信息均要从上面传来的storage里取
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	// 返回还未生成快照的第一条log index
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	entries, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		panic(err)
	}
	raftLog := &RaftLog{
		storage:    storage,
		stabled:    lastIndex,
		entries:    entries,
		applied:    firstIndex - 1,
		FirstIndex: firstIndex,
	}
	return raftLog
}

// 将index转换成实际entrys数组的下标，用于产生了快照的情况
func (l *RaftLog) toSliceIndex(i uint64) int {
	idx := int(i - l.FirstIndex)
	if idx < 0 {
		panic("toSliceIndex: index < 0")
	}
	return idx
}

func (l *RaftLog) toEntryIndex(i int) uint64 {
	return uint64(i) + l.FirstIndex
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	first, _ := l.storage.FirstIndex()
	if first > l.FirstIndex {
		if len(l.entries) > 0 {
			entries := l.entries[l.toSliceIndex(first):]
			l.entries = make([]pb.Entry, len(entries))
			copy(l.entries, entries)
		}
		l.FirstIndex = first
	}
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	if l.entries == nil || len(l.entries) == 0 {
		return nil
	}
	return l.entries
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	// unstable: index > stabled
	if l.entries == nil || len(l.entries) == 0 {
		return nil
	}
	return l.entries[l.stabled-l.FirstIndex+1:]
}

// nextEnts returns all the committed but not applied entries
// 返回所有已经可以提交但没有应用的日志
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	// 返回索引在[applied+1, committed+1)
	if l.entries == nil || len(l.entries) == 0 {
		// 如果没有log，返回
		return nil
	}
	// firstIndex 对应 entries[0]， 返回的是左闭右开区间
	return l.entries[l.applied-l.FirstIndex+1 : l.committed-l.FirstIndex+1]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	var index uint64
	// 如果有快照，先把快照的最后一个index取到
	if !IsEmptySnap(l.pendingSnapshot) {
		index = l.pendingSnapshot.Metadata.Index
	}
	// 如果除了快照外还有新的log，就返回最新的index
	if len(l.entries) > 0 {
		return max(l.entries[len(l.entries)-1].Index, index)
	}
	// 为什么一定要从持久化的log中取lastindex做比较？
	i, _ := l.storage.LastIndex()
	return max(i, index)
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	// i不在快照中
	if len(l.entries) > 0 && i >= l.FirstIndex {
		return l.entries[i-l.FirstIndex].Term, nil
	}
	term, err := l.storage.Term(i)
	// i在快照中，如果刚好是快照的最后一个log，可以返回term
	// 如果不是最后一个log，则term信息已经丢失，返回一个错误码即可
	if err == ErrUnavailable && !IsEmptySnap(l.pendingSnapshot) {
		if i == l.pendingSnapshot.Metadata.Index {
			term = l.pendingSnapshot.Metadata.Term
			err = nil
		} else if i < l.pendingSnapshot.Metadata.Index {
			err = ErrCompacted
		}
	}
	return term, err
}

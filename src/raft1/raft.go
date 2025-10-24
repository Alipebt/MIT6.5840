package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	logs        Logs

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	applyCh chan raftapi.ApplyMsg

	heartBeatTicker *time.Ticker
	electionTicker  *time.Ticker
	status          Status

	lastIncludedIndex int
	lastIncludedTerm  int
	snapShot          []byte
}

type Logs []Log

func (rf *Raft) logsLen() int {
	return rf.lastIncludedIndex + len(rf.logs)
}

func (rf *Raft) logsSlice(start int, end int) Logs {
	start -= rf.lastIncludedIndex
	if start < 0 {
		start = 0
	}
	end -= rf.lastIncludedIndex
	if end < 0 {
		end = 0
	}
	if end >= 0 && start <= end {
		return rf.logs[start:end]
	}
	return nil
}

type Status int

const (
	UnknowStatus Status = iota
	Leader
	Candidater
	Follower
)

type Log struct {
	Command interface{}
	Term    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := rf.status == Leader
	//Debug(dInfo, "S%v T%v GetState %v", rf.me, rf.currentTerm, rf.status)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	ok := e.Encode(rf.currentTerm)
	if ok != nil {
		return
	}
	ok = e.Encode(rf.votedFor)
	if ok != nil {
		return
	}
	ok = e.Encode(rf.logs)
	if ok != nil {
		return
	}
	//ok = e.Encode(rf.snapShot)
	//if ok != nil {
	//	return
	//}
	ok = e.Encode(rf.lastIncludedIndex)
	if ok != nil {
		return
	}
	ok = e.Encode(rf.lastIncludedTerm)
	if ok != nil {
		return
	}
	raftState := w.Bytes()
	rf.persister.Save(raftState, rf.snapShot)
	//Debug(dPersist, "S%v T%v Logs:%v", rf.me, rf.currentTerm, rf.logs)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	currentTerm := -1
	voteFor := -1
	var logs Logs
	lastIncludedIndex := 0
	lastIncludedTerm := 0
	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil || d.Decode(&logs) != nil ||
		d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		//Debug(dError, "S%v RP error to read persist", rf.me)
		return
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = voteFor
		rf.logs = logs
		//rf.snapShot = snapShot
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.lastApplied = lastIncludedIndex
		//rf.commitIndex = lastIncludedIndex
	}

	//Debug(dError, "S%v T%v Reboot---", rf.me, rf.currentTerm)
	//Debug(dSnap, "S%v T%v #readPersist#:%v", rf.me, rf.currentTerm, rf.lastApplied)

}

func (rf *Raft) readSnapShot(data []byte) {
	if data == nil {
		return
	}
	rf.snapShot = data
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//Debug(dError, "S%v T%v Snapshot::", rf.me, rf.currentTerm)
	if index < rf.lastIncludedIndex {
		return
	}

	rf.lastIncludedTerm = rf.getLog(index).Term
	rf.logs = rf.logsSlice(index, rf.logsLen())
	rf.lastIncludedIndex = index
	rf.snapShot = snapshot
	//Debug(dSnap, "S%v T%v sIndex=%v sTrem:%v", rf.me, rf.currentTerm, rf.lastIncludedIndex, rf.lastIncludedTerm)
	//Debug(dSnap, "S%v T%v next log=%v", rf.me, rf.currentTerm, rf.logs)
	rf.persist()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果term < currentTerm返回 false
	// 如果 votedFor 为空或者为 candidateId，并且候选人的日志至少和自己一样新，那么就投票给他
	reply.VoteGranted = false

	//Debug(dError, "S%v T%v rf.getLog(-1):%v len(rf.logs):%v", rf.me, rf.currentTerm, rf.getLog(-1), len(rf.logs))

	if args.Term > rf.currentTerm {
		rf.updateNodeWithTerm(args.Term)
		reply.VoteGranted = true
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if args.LastLogTerm > rf.getLog(-1).Term {
			// 候选人最后一条Log条目的任期号大于本地最后一条Log条目的任期号；
			reply.VoteGranted = true
		} else if args.LastLogTerm == rf.getLog(-1).Term && args.LastLogIndex >= rf.logsLen() {
			// 或者，候选人最后一条Log条目的任期号等于本地最后一条Log条目的任期号，且候选人的Log记录长度大于等于本地Log记录的长度
			reply.VoteGranted = true
		} else {
			//Debug(dVote, "S%v T%v test %v %v", rf.me, rf.currentTerm, args.LastLogIndex, rf.logsLen())
			reply.VoteGranted = false
		}
	}

	reply.Term = rf.currentTerm

	if reply.VoteGranted {
		rf.votedFor = args.CandidateId
		rf.resetElectionTicker()
		//Debug(dTerm, "S%v T%v success vote to S%v", rf.me, rf.currentTerm, args.CandidateId)
		//Debug(dTerm, "S%v T%v args:%v", rf.me, rf.currentTerm, args)
		//Debug(dPersist, "S%v T%v rf.votedFor:%v args.LastLogTerm:%v", rf.me, rf.currentTerm, rf.votedFor, args.LastLogTerm)
		//Debug(dPersist, "S%v T%v rf.getLog(-1).Term:%v args.LastLogIndex:%v", rf.me, rf.currentTerm, rf.getLog(-1).Term, args.LastLogIndex)
		//Debug(dPersist, "S%v T%v len(rf.logs)-1:%v", rf.me, rf.currentTerm, rf.logsLen())
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapShot(server int, args *InstallSnapShotArgs, reply *InstallSnapShotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	return ok
}

type InstallSnapShotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapShotReply struct {
	Term int
}

func (rf *Raft) InstallSnapShot(args *InstallSnapShotArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm || rf.status == Leader {
		return
	}
	//Debug(dError, "S%v T%v InstallSnapshot::", rf.me, rf.currentTerm)
	var snapShot []byte
	for {
		if args.Offset == 0 {
			snapShot = nil
		}
		snapShot = append(snapShot[:args.Offset], args.Data...)
		if args.Done {
			break
		}
	}
	if rf.lastIncludedIndex >= args.LastIncludedIndex {
		return
	}

	oldLastIncludedIndex := rf.lastIncludedIndex

	//Debug(dSnap, "S%v T%v snapShot nil", rf.me, rf.currentTerm)
	rf.snapShot = nil
	rf.snapShot = append(rf.snapShot, snapShot...)
	//Debug(dSnap, "S%v T%v snapShot append:%v", rf.me, rf.currentTerm, len(snapShot))
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	//Debug(dSnap, "S%v T%v SIndex:%v STerm:%v", rf.me, rf.currentTerm, rf.lastIncludedIndex, rf.lastIncludedTerm)

	if args.LastIncludedIndex < oldLastIncludedIndex+len(rf.logs) && rf.getLog(args.LastIncludedIndex).Term == args.LastIncludedTerm {
		//Debug(dSnap, "S%v T%v in apply", rf.me, rf.currentTerm)
		rf.logsSlice(args.LastIncludedIndex, oldLastIncludedIndex+len(rf.logs))
	} else {
		rf.logs = nil
	}
	reply.Term = rf.currentTerm

	// 把快照发送给客户端，相当于commit到快照的index
	applyMsg := raftapi.ApplyMsg{
		CommandValid: false,
		Command:      nil,
		CommandIndex: 0,

		SnapshotValid: true,
		Snapshot:      rf.snapShot,
		SnapshotTerm:  rf.lastIncludedTerm,
		SnapshotIndex: rf.lastIncludedIndex,
	}
	rf.mu.Unlock()
	rf.applyCh <- applyMsg
	rf.mu.Lock()

	rf.commitIndex = rf.lastIncludedIndex
	if rf.lastApplied < rf.lastIncludedIndex {
		rf.lastApplied = rf.lastIncludedIndex
	}

	//Debug(dSnap, "S%v T%v now lenlog:%v", rf.me, rf.currentTerm, len(rf.logs))
	//Debug(dSnap, "S%v T%v #InstallSnapShot#:%v", rf.me, rf.currentTerm, rf.lastApplied)

	rf.persist()
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int

	PrevLogTerm int
	Entries     Logs

	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool

	// 冲突log对应的term
	XTerm int
	// 第一条Term==XTerm的log
	XIndex int
	// log长度
	XLen int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.handleHeartBeat(args)

	reply.Success = rf.handleAppendLog(args, reply)
	reply.Term = rf.currentTerm

	//Debug(dLeader, "S%v T%v reply:%v", rf.me, rf.currentTerm, reply)

	rf.commitMsg()

	rf.persist()

}

func (rf *Raft) handleAppendLog(args *AppendEntriesArgs, reply *AppendEntriesReply) bool {

	isSuccess := false

	if rf.status == Leader {
		return false
	}
	// 如果领导人的任期小于接收者的当前任期
	if args.Term < rf.currentTerm {
		return false
	}

	//if len(args.Entries) != 0 {
	//Debug(dLog, "S%v T%v From S%v T%v", rf.me, rf.currentTerm, args.LeaderId, args.Term)
	//Debug(dLog, "S%v T%v receive index:%v len(log):%v", rf.me, rf.currentTerm, args.PrevLogIndex+1, len(args.Entries))
	//Debug(dLog, "S%v T%v args i:%v t:%v", rf.me, rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm)
	//}

	// 找不到一个和 prevLogIndex 以及 prevLogTerm 一样的索引和任期的日志条目
	if rf.getLog(args.PrevLogIndex).Term == -1 {
		// index冲突(无此index)
		//Debug(dInfo, "S%v T%v index冲突(无此index)", rf.me, rf.currentTerm)
		reply.XTerm = -1
		if rf.lastIncludedIndex == args.PrevLogIndex+1 {
			isSuccess = true
		}
	} else if rf.getLog(args.PrevLogIndex).Term != args.PrevLogTerm {
		// Term冲突
		//Debug(dInfo, "S%v T%v Term冲突 rf.pTerm:%v args.PrevLogTerm:%v", rf.me, rf.currentTerm, rf.getLog(args.PrevLogIndex).Term, args.PrevLogTerm)
		reply.XTerm = rf.getLog(args.PrevLogIndex).Term
	} else {
		isSuccess = true
	}

	// term为XTerm的第一条log, 当XTerm=-1时,XIndex一直为0
	for index, log := range rf.logs {
		if log.Term == reply.XTerm {
			reply.XIndex = index + rf.lastIncludedIndex
			break
		}
	}

	// 如果一个已经存在的条目和刚刚接收到的日志条目发生了冲突（因为索引相同，任期不同），
	// 那么就删除这个已经存在的条目以及它之后的所有条目
	// 追加日志中尚未存在的任何新条目
	// L:   0   1   2   3   4
	// F:	0   1   2   5
	//				p   n
	// E:               3   4
	// args.Entries != nil排除心跳
	if isSuccess && args.Entries != nil {
		//Debug(dTerm, "S%v T%v cut log 0 ~ %v", rf.me, rf.currentTerm, args.PrevLogIndex+1)
		if len(rf.logs) > 0 {
			rf.logs = rf.logsSlice(0, args.PrevLogIndex+1)
		}
		//Debug(dTimer, "S%v T%v log=%v", rf.me, rf.currentTerm, rf.logs)
		rf.logs = append(rf.logs, args.Entries...)
		//Debug(dTerm, "S%v T%v add log %v ~ %v t:%v", rf.me, rf.currentTerm, args.PrevLogIndex+1, rf.logsLen()-1, rf.getLog(-1).Term)
		//Debug(dTerm, "S%v T%v now log=%v", rf.me, rf.currentTerm, rf.logs)
	}

	reply.XLen = rf.logsLen()

	// 已提交的最高日志条目的索引大于接收者的已知已提交最高日志条目的索引
	if isSuccess && args.LeaderCommit > rf.commitIndex {
		//  leaderCommit 或者是 上一个新条目的索引 取两者的最小值
		rf.commitIndex = min(args.LeaderCommit, rf.logsLen()-1)
		//Debug(dVote, "S%v T%v commitIndex:%v from S%v", rf.me, rf.commitIndex, rf.commitIndex, args.LeaderId)
	}

	return isSuccess
}

func (rf *Raft) GetStatus() Status {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.status
}

func (rf *Raft) createAppendEntriesArgs(entries Logs, peer int) *AppendEntriesArgs {

	args := &AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,

		// 本地中，peer已存在的最后一个index，将要添加log的位置的前一个位置
		PrevLogIndex: rf.nextIndex[peer] - 1,
		// PrevLogIndex位置log的term
		PrevLogTerm: rf.getLog(rf.nextIndex[peer] - 1).Term,
		Entries:     entries,

		LeaderCommit: rf.commitIndex,
	}
	return args
}

func (rf *Raft) createApplyMsg() raftapi.ApplyMsg {
	applyMsg := raftapi.ApplyMsg{
		CommandValid: true,
		Command:      rf.getLog(rf.lastApplied).Command,
		CommandIndex: rf.lastApplied,
	}
	return applyMsg
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.status != Leader {
		return -1, rf.currentTerm, false
	}

	entry := Log{
		Command: command,
		Term:    rf.currentTerm,
	}

	rf.logs = append(rf.logs, entry)
	rf.matchIndex[rf.me] = rf.logsLen() - 1

	//Debug(dClient, "S%v T%v len(Logs):%v new logs:%v", rf.me, rf.currentTerm, rf.logsLen(), entry)
	return rf.logsLen() - 1, rf.currentTerm, rf.status == Leader
}

func (rf *Raft) sendLogs() {
	//isCommited := false

	for i, _ := range rf.peers {

		if i == rf.me {
			continue
		}

		go func(server int) {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.killed() || rf.status != Leader {
				//Debug(dLeader, "S%v T%v 退出Leader处理流程 ", rf.me, rf.currentTerm)
				return
			}

			if rf.nextIndex[server] <= rf.lastIncludedIndex {
				snapShotArgs := &InstallSnapShotArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.lastIncludedIndex,
					LastIncludedTerm:  rf.lastIncludedTerm,
					Offset:            0,
					Data:              rf.snapShot,
					Done:              true,
				}
				snapShotReply := &InstallSnapShotReply{}
				rf.mu.Unlock()
				ok := rf.sendInstallSnapShot(server, snapShotArgs, snapShotReply)
				rf.mu.Lock()
				if !ok {
					return
				}
				rf.nextIndex[server] = rf.lastIncludedIndex
				rf.matchIndex[server] = rf.lastIncludedIndex
				//Debug(dInfo, "S%v T%v rf.lastIncludedIndex:%v", server, rf.currentTerm, rf.lastIncludedIndex)
			}

			//for rf.status == Leader {
			var entries Logs
			// entries=nil表示为单纯的心跳包
			// 判断在范围内有未发送的log
			if rf.nextIndex[server] < rf.logsLen() && rf.nextIndex[server] >= 0 {
				// 其中‘=’的结果是 append nil，表示已将所有日志发送完毕。
				// case 1:
				// local: 0   1   2   3   4   5
				// peer:  0   1   2   3
				//				          n=4
				//entries:				  4   5

				// case 2:
				// local: 0   1   2   3   4   5
				// peer:  0   1   2   3   4   5
				// nextIndex:		             n=6
				//entries:			nil
				if rf.nextIndex[server] == 0 && rf.logsLen() > 1 {
					// 不加logs[0]
					entries = append(entries, rf.getLog(rf.logsLen()))
				}
				entries = append(entries, rf.logsSlice(rf.nextIndex[server], rf.logsLen())...)
			}

			//Debug(dLog2, "S%v T%v log:%v", rf.me, rf.currentTerm, rf.logs)
			if entries != nil {
				//Debug(dTimer, "S%v T%v len %v to %v", rf.me, rf.currentTerm, rf.lastIncludedIndex, rf.logsLen())
				//Debug(dTimer, "S%v T%v add %v", rf.me, rf.currentTerm, rf.logs)
				//Debug(dTimer, "S%v T%v send %v to S%v", rf.me, rf.currentTerm, entries, server)
			}

			args := rf.createAppendEntriesArgs(entries, server)
			reply := &AppendEntriesReply{}

			//Debug(dLeader, "S%v T%v send %v~%v", rf.me, rf.currentTerm, rf.nextIndex[server], rf.logsLen())

			rf.mu.Unlock()
			ok := rf.sendAppendEntries(server, args, reply)
			rf.mu.Lock()
			if !ok {
				return
			}
			//Debug(dClient, "S%v T%v reply:%v from S%v", rf.me, rf.currentTerm, reply, server)

			if reply.Term > rf.currentTerm {
				//Debug(dLeader, "S%v T%v 过期的Leader,变为Follower ", rf.me, rf.currentTerm)
				// 过期的Leader,更新Node的状态
				rf.updateNodeWithTerm(reply.Term)
				return
			}

			if reply.Success {
				// 成功接收log
				// 由于发送了所有的新log，如果成功了则该node的下一个log位置为其原有log长度+entries长度
				// Debug(dLeader, "S%v T%v reply success from S%v", rf.me, rf.currentTerm, server)
				//Debug(dLog, "S%v T%v in S%v reply:%v", rf.me, rf.currentTerm, server, reply)
				rf.nextIndex[server] = reply.XLen
				rf.matchIndex[server] = reply.XLen - 1
				//Debug(dLog, "S%v T%v append log success in S%v,and entries:%v", rf.me, rf.currentTerm, server, len(entries))
			} else {
				//if rf.nextIndex[server] > rf.lastIncludedIndex {
				// 未成功接收，则一定返回有XTerm,Xindex,Xlen
				if reply.XTerm != -1 {
					// -1表示PrevLogIndex位置发生冲突，即nextIndex-1或len(log)位置。表示要插入的位置的前一个位置冲突

					// leader检查自身是否有Xterm的Term
					iXTerm := -1
					// 倒序遍历，提取最后一个Term=XTerm的index
					for iXTerm = rf.logsLen(); iXTerm >= 0; iXTerm-- {
						if rf.getLog(iXTerm).Term == reply.XTerm {
							break
						}
					}
					//Debug(dClient, "S%v T%v iXTerm:%v", rf.me, rf.currentTerm, iXTerm)

					if iXTerm != -1 {
						// 存在，设nextIndex为indexLastXTerm下一个。
						// 表示peer在XTerm期间的所有日志都已存在(对于peer，XTerm为最新Term)
						// L:	[0   1   2   3   4]
						// t:    1   1   1   2   2
						//                           n=5
						// F: 	[0   1   2   3   4]
						// t:	 1   1   1   1   1
						//                   n=3
						rf.nextIndex[server] = iXTerm + 1
					} else {
						// 不存在，设nextIndex为reply.XIndex
						// L:	[0   1   2   3   4]
						// t:	 1   1   1   2   2
						// 						     n=5
						// F:	[0   1   2   3   4]
						// t:	 1   1   1   1   1
						//					 n=3
						rf.nextIndex[server] = reply.XIndex
					}
				} else {
					// PrevLogIndex位置不存在log
					// L:	[0   1   2   3   4]
					// t:	 1   1   1   2   2
					// 				         p=4 n=5
					// F:	[0   1   2   3]
					// t:	 1   1   1   1
					//					     n=4

					rf.nextIndex[server] = reply.XLen
				}
				//Debug(dLog, "S%v T%v nextIndex处理结束 nextIndex[%v]:%v", rf.me, rf.currentTerm, server, rf.nextIndex[server])
				//} else {
				//
				//}
			}

			// 至此nextIndex处理结束。
			// 注：心跳也会进行处理，是否应该取消对nextIndex处理?

			// 心跳不参与后续判断和修改,但需要判断和修改nextIndex,
			// 否则导致心跳无法获取Follower log的最新状态，在节点恢复后发送错误位置的log
			if entries == nil {
				return
			}

			// 过半票决，成功为大多数添加日志
			for N := rf.logsLen() - 1; N > rf.commitIndex; N-- {
				agreement := 0
				for _, matchIndex := range rf.matchIndex {
					if matchIndex >= N && rf.getLog(N).Term == rf.currentTerm {
						agreement++
						//Debug(dCommit, "S%v T%v agreement++", rf.me, rf.currentTerm)
					}
				}
				//Debug(dCommit, "S%v T%v agreement=%v len(rf.peers)/2=%v", rf.me, rf.currentTerm, agreement, len(rf.peers)/2)
				if agreement > len(rf.peers)/2 {
					rf.commitIndex = N
					// Debug
					//var a, b, c, d, e, f, g int
					//for s, matchIndex := range rf.matchIndex {
					//	if s == 0 {
					//		a = matchIndex
					//	} else if s == 1 {
					//		b = matchIndex
					//	} else if s == 2 {
					//		c = matchIndex
					//	} else if s == 3 {
					//		d = matchIndex
					//	} else if s == 4 {
					//		e = matchIndex
					//	} else if s == 5 {
					//		f = matchIndex
					//	} else if s == 6 {
					//		g = matchIndex
					//	}
					//}
					//Debug(dError, "S%v T%v %v [%v %v %v %v %v %v %v]", rf.me, rf.currentTerm, N, a, b, c, d, e, f, g)
					//Debug(dLeader, "S%v T%v Leader commitIndex=%v", rf.me, rf.currentTerm, N)
					break
				}
			}
			//}
		}(i)
	}
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	rf.mu.Lock()
	rf.resetHeartBeatTicker()
	rf.resetElectionTicker()
	rf.mu.Unlock()
	defer rf.electionTicker.Stop()
	defer rf.heartBeatTicker.Stop()

	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.

		//只有在以下情况下才重置选举计时器：
		// 收到当前leader的AppendEntries RPC（心跳或日志复制）
		// 给其他候选人投票（在RequestVote响应中投票）
		// 开始新的选举（自己变成候选人时）
		select {
		case <-rf.electionTicker.C:
			if rf.GetStatus() != Leader {
				rf.Election()
			}
			rf.mu.Lock()
			rf.resetElectionTicker()
			rf.mu.Unlock()
		case <-rf.heartBeatTicker.C:
			if rf.GetStatus() == Leader {
				rf.HeartBeat()
			}
		}
	}
}

func (rf *Raft) getLog(index int) Log {
	log := Log{
		Command: nil,
		Term:    -1,
	}
	//Debug(dError, "S%v T%v #%v", rf.me, rf.currentTerm, rf.logsLen())
	//Debug(dError, "S%v T%v #%v %v", rf.me, rf.currentTerm, index, rf.lastIncludedIndex)
	if index <= -1 {
		index = rf.logsLen() + index
	}
	if rf.logsLen() > 1 {
		//// 存在log[0],len==1
		if index >= 0 && index < rf.logsLen() {
			// logs:	0  1  2  3
			// 			   1  2  3   len=4
			if index-rf.lastIncludedIndex >= 0 && index-rf.lastIncludedIndex < len(rf.logs) {
				log = rf.logs[index-rf.lastIncludedIndex]
			}
		}
	}
	if index == 0 && len(rf.logs) != 0 {
		log = rf.logs[0]
	}
	if log.Term == -1 {
		//Debug(dVote, "S%v T%v rf.logsLen:%v index:%v", rf.me, rf.currentTerm, rf.logsLen(), index)
	}
	return log
}

// election electe leader when ticker timeout
func (rf *Raft) Election() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentTerm++
	rf.status = Candidater

	votes := 0
	rvotes := 0
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.logsLen(),
		LastLogTerm:  rf.getLog(-1).Term,
	}

	//Debug(dInfo, "S%v T%v Election", rf.me, rf.currentTerm)

	for i, _ := range rf.peers {
		if i == rf.me {
			rf.votedFor = rf.me
			votes++
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.killed() {
				return
			}

			reply := &RequestVoteReply{}
			//Debug(dVote, "S%v T%v ---RequestVote---> S%v", rf.me, rf.currentTerm, server)

			rf.mu.Unlock()
			ok := rf.sendRequestVote(server, args, reply)
			rf.mu.Lock()

			if !ok {
				//Debug(dVote, "S%v T%v ---RequestVote---> S%v failed", rf.me, rf.currentTerm, server)
				return
			}

			if reply.Term > rf.currentTerm {
				rf.updateNodeWithTerm(reply.Term)
			}
			if reply.VoteGranted == false || reply.Term != rf.currentTerm {
				// reply.Term != rf.currentTerm 防止投票的Term与当前Term不同，但该节点收到了选票
				rvotes++
			} else if reply.VoteGranted == true {
				//Debug(dVote, "S%v T%v ---Vote---> S%v ", server, rf.currentTerm, rf.me)
				votes++
			}

			if votes > len(rf.peers)/2 && rf.status == Candidater {
				rf.initLeader()
				//Debug(dLeader, "S%v T%v become Leader ,commitIndex:%v", rf.me, rf.currentTerm, rf.commitIndex)
			} else if rvotes > len(rf.peers)/2 {
				rf.status = Follower
				rf.votedFor = -1
				//Debug(dLeader, "S%v T%v election failed", rf.me, rf.currentTerm)
			}

		}(i)
	}
}

func (rf *Raft) commitMsg() {
	// 参考killed()的注释
	//Debug(dClient, "S%v T%v lapplied:%v CIndex:%v", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex)
	for !rf.killed() && rf.lastApplied < rf.commitIndex {
		rf.lastApplied++

		applyMsg := rf.createApplyMsg()
		//if rf.lastApplied == rf.commitIndex {
		//Debug(dClient, "S%v T%v Commit:%v index:%v", rf.me, rf.currentTerm, rf.getLog(rf.lastApplied), rf.lastApplied)
		//}

		// 如果持有锁则可能产生死锁
		rf.mu.Unlock()
		rf.applyCh <- applyMsg
		rf.mu.Lock()

	}

}

// heartBeat send heartbeat to others from leader
func (rf *Raft) HeartBeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//Debug(dSnap, "S%v T%v #HeartBeat-1#:%v", rf.me, rf.currentTerm, rf.lastApplied)

	rf.sendLogs()
	//Debug(dSnap, "S%v T%v #HeartBeat0#:%v", rf.me, rf.currentTerm, rf.lastApplied)

	rf.commitMsg()
	//Debug(dSnap, "S%v T%v #HeartBeat1#:%v", rf.me, rf.currentTerm, rf.lastApplied)
	rf.persist()
	//Debug(dSnap, "S%v T%v #HeartBeat2s#:%v", rf.me, rf.currentTerm, rf.lastApplied)

}

func (rf *Raft) handleHeartBeat(args *AppendEntriesArgs) {
	if args.Term >= rf.currentTerm {
		// 新Term，新Leader的心跳
		rf.updateNodeWithTerm(args.Term)
		rf.votedFor = args.LeaderId
		rf.resetElectionTicker()
	}
}

// updateNodeWithTerm update the term and change node`s new term status
func (rf *Raft) updateNodeWithTerm(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.status = Follower
}

// resetElectionTicker set a random time to ticker
func (rf *Raft) resetElectionTicker() {
	ms := 150 + (rand.Int63() % 150)
	if rf.electionTicker == nil {
		rf.electionTicker = time.NewTicker(time.Duration(ms) * time.Millisecond)
	} else {
		rf.electionTicker.Reset(time.Duration(ms) * time.Millisecond)
	}
}

// resetHeartBeatTicker set a constant time to ticker or init it
func (rf *Raft) resetHeartBeatTicker() {
	if rf.heartBeatTicker == nil {
		rf.heartBeatTicker = time.NewTicker(50 * time.Millisecond)
	} else {
		rf.heartBeatTicker.Reset(50 * time.Millisecond)
	}
}

// initLeader change node`s status to leader and refresh volatile data or init it
func (rf *Raft) initLeader() {
	rf.status = Leader
	for s, _ := range rf.peers {
		if len(rf.nextIndex) < len(rf.peers) {
			rf.nextIndex = append(rf.nextIndex, len(rf.logs))
			rf.matchIndex = append(rf.matchIndex, 0)
		} else {
			rf.nextIndex[s] = rf.logsLen()
			rf.matchIndex[s] = 0
		}
		// Debug
		//var a, b, c, d, e, f, g int
		//for s, nextIndex := range rf.nextIndex {
		//	if s == 0 {
		//		a = nextIndex
		//	} else if s == 1 {
		//		b = nextIndex
		//	} else if s == 2 {
		//		c = nextIndex
		//	} else if s == 3 {
		//		d = nextIndex
		//	} else if s == 4 {
		//		e = nextIndex
		//	} else if s == 5 {
		//		f = nextIndex
		//	} else if s == 6 {
		//		g = nextIndex
		//	}
		//}
		//Debug(dError, "S%v T%v %v next [%v %v %v %v %v %v %v]", rf.me, rf.currentTerm, s, a, b, c, d, e, f, g)
		//Debug(dLeader, "S%v T%v Leader commitIndex=%v", rf.me, rf.currentTerm, s)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {

	rf := &Raft{
		mu:                sync.Mutex{},
		peers:             peers,
		persister:         persister,
		me:                me,
		dead:              0,
		currentTerm:       0,
		votedFor:          -1,
		logs:              make(Logs, 1),
		commitIndex:       0,
		lastApplied:       0,
		nextIndex:         nil,
		matchIndex:        nil,
		applyCh:           applyCh,
		status:            Follower,
		lastIncludedIndex: 0,
		lastIncludedTerm:  0,
	}

	// Your initialization code here (3A, 3B, 3C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapShot(persister.ReadSnapshot())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

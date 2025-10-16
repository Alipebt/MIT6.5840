package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

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
	logs        []Log

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	applyCh chan raftapi.ApplyMsg

	heartBeatTicker *time.Ticker
	electionTicker  *time.Ticker
	status          Status
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
	reply.Term = rf.currentTerm

	//Debug(dError, "S%v T%v rf.getLog(-1):%v len(rf.logs):%v", rf.me, rf.currentTerm, rf.getLog(-1), len(rf.logs))

	if args.Term > rf.currentTerm {
		rf.updateNodeWithTerm(args.Term)
		reply.VoteGranted = true
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if args.LastLogTerm > rf.getLog(-1).Term {
			// 候选人最后一条Log条目的任期号大于本地最后一条Log条目的任期号；
			reply.VoteGranted = true
		} else if args.LastLogTerm == rf.getLog(-1).Term && args.LastLogIndex >= len(rf.logs)-1 {
			// 或者，候选人最后一条Log条目的任期号等于本地最后一条Log条目的任期号，且候选人的Log记录长度大于等于本地Log记录的长度
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
	}

	if reply.VoteGranted {
		rf.votedFor = args.CandidateId
		rf.resetElectionTicker()
		//Debug(dTerm, "S%v T%v success vote to S%v", rf.me, rf.currentTerm, args.CandidateId)
		//Debug(dTerm, "S%v T%v args:%v", rf.me, rf.currentTerm, args)
		//Debug(dPersist, "S%v T%v rf.votedFor:%v args.LastLogTerm:%v", rf.me, rf.currentTerm, rf.votedFor, args.LastLogTerm)
		//Debug(dPersist, "S%v T%v rf.getLog(-1).Term:%v args.LastLogIndex:%v", rf.me, rf.currentTerm, rf.getLog(-1).Term, args.LastLogIndex)
		//Debug(dPersist, "S%v T%v len(rf.logs)-1:%v", rf.me, rf.currentTerm, len(rf.logs)-1)
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int

	PrevLogTerm int
	Entries     []Log

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

	Debug(dLeader, "S%v T%v reply:%v", rf.me, rf.currentTerm, reply)

	rf.commitMsg()

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
	//	Debug(dLog, "S%v T%v From S%v T%v", rf.me, rf.currentTerm, args.LeaderId, args.Term)
	//	Debug(dLog, "S%v T%v receive index:%v len(log):%v", rf.me, rf.currentTerm, args.PrevLogIndex+2, len(args.Entries))
	//}

	// 找不到一个和 prevLogIndex 以及 prevLogTerm 一样的索引和任期的日志条目
	if rf.getLog(args.PrevLogIndex).Term == -1 {
		// index冲突(无此index)
		//Debug(dInfo, "S%v T%v index冲突(无此index)", rf.me, rf.currentTerm)
		reply.XTerm = -1
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
			reply.XIndex = index
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
		rf.logs = rf.logs[:args.PrevLogIndex+1]
		rf.logs = append(rf.logs, args.Entries...)
		//Debug(dTerm, "S%v T%v add log %v ~ %v t:%v", rf.me, rf.currentTerm, args.PrevLogIndex+2, len(rf.logs), rf.getLog(-1).Term)
	}

	reply.XLen = len(rf.logs)

	// 已提交的最高日志条目的索引大于接收者的已知已提交最高日志条目的索引
	if isSuccess && args.LeaderCommit > rf.commitIndex {
		//  leaderCommit 或者是 上一个新条目的索引 取两者的最小值
		rf.commitIndex = min(args.LeaderCommit, len(rf.logs)-1)
	}

	return isSuccess
}

func (rf *Raft) GetStatus() Status {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.status
}

func (rf *Raft) createAppendEntriesArgs(entries []Log, peer int) *AppendEntriesArgs {

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
	term := rf.currentTerm
	isLeader := rf.status == Leader
	if !isLeader {
		return -1, term, isLeader
	}

	entry := Log{
		Command: command,
		Term:    rf.currentTerm,
	}

	rf.logs = append(rf.logs, entry)
	rf.matchIndex[rf.me] = len(rf.logs) - 1

	//Debug(dClient, "S%v T%v get new logs:%v", rf.me, rf.currentTerm, entry)
	//Debug(dCommit, "S%v T%v Leader len(Logs):%v Term:%v", rf.me, rf.currentTerm, len(rf.logs), rf.currentTerm)
	return len(rf.logs) - 1, term, isLeader
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

			if rf.status != Leader {
				//Debug(dLeader, "S%v T%v 退出Leader处理流程 ", rf.me, rf.currentTerm)
				return
			}

			//for rf.status == Leader {
			var entries []Log
			// entries=nil表示为单纯的心跳包
			// 判断在范围内有未发送的log
			if rf.nextIndex[server] < len(rf.logs) && rf.nextIndex[server] > 0 {
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
				entries = append(entries, rf.logs[rf.nextIndex[server]:]...)
			}

			//Debug(dLog2, "S%v T%v log:%v", rf.me, rf.currentTerm, rf.logs)
			//if entries != nil {
			//	Debug(dTimer, "S%v T%v send %v to S%v", rf.me, rf.currentTerm, rf.getLog(-1), server)
			//}

			args := rf.createAppendEntriesArgs(entries, server)
			reply := &AppendEntriesReply{}

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
				rf.nextIndex[server] = reply.XLen
				rf.matchIndex[server] = reply.XLen - 1
				//if len(entries) != 0 {
				//	Debug(dLog, "S%v T%v append log success in S%v,and entries:%v", rf.me, rf.currentTerm, server, len(entries))
				//}
			} else {
				// 未成功接收，则一定返回有XTerm,Xindex,Xlen
				if reply.XTerm != -1 {
					// -1表示PrevLogIndex位置发生冲突，即nextIndex-1或len(log)位置。表示要插入的位置的前一个位置冲突

					// leader检查自身是否有Xterm的Term
					iXTerm := -1
					// 倒序遍历，提取最后一个Term=XTerm的index
					for iXTerm = len(rf.logs) - 1; iXTerm >= 0; iXTerm-- {
						if rf.logs[iXTerm].Term == reply.XTerm {
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
			}

			// 至此nextIndex处理结束。
			// 注：心跳也会进行处理，是否应该取消对nextIndex处理?

			// 心跳不参与后续判断和修改,但需要判断和修改nextIndex,
			// 否则导致心跳无法获取Follower log的最新状态，在节点恢复后发送错误位置的log
			if entries == nil {
				return
			}

			// 过半票决，成功为大多数添加日志
			for N := rf.commitIndex + 1; N < len(rf.logs); N++ {
				agreement := 0
				for _, matchIndex := range rf.matchIndex {
					if matchIndex >= N && rf.logs[N].Term == rf.currentTerm {
						agreement++
						//Debug(dCommit, "S%v T%v agreement++", rf.me, rf.currentTerm)
					}
				}
				//Debug(dCommit, "S%v T%v agreement=%v len(rf.peers)/2=%v", rf.me, rf.currentTerm, agreement, len(rf.peers)/2)
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
				//Debug(dCommit, "S%v T%v %v [%v %v %v %v %v %v %v]", rf.me, rf.currentTerm, N, a, b, c, d, e, f, g)
				if agreement > len(rf.peers)/2 {
					rf.commitIndex = N
					//Debug(dCommit, "S%v T%v Leader commitIndex=%v", rf.me, rf.currentTerm, N)
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

	if len(rf.logs) > 1 {
		// 存在log[0],len==1
		if index >= 1 && index < len(rf.logs) {
			// logs:	0  1  2  3
			// 			   1  2  3   len=4
			log = rf.logs[index]
		} else if index <= -1 && index > -len(rf.logs) {
			// logs:	0  1  2  3
			// 		      -3 -2 -1	 len=4
			log = rf.logs[len(rf.logs)+index]
		}
	}
	if index == 0 {
		log = rf.logs[0]
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
		LastLogIndex: len(rf.logs) - 1,
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
			if reply.VoteGranted == false {
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
	if rf.lastApplied < rf.commitIndex {
		rf.lastApplied++

		applyMsg := rf.createApplyMsg()

		rf.mu.Unlock()
		rf.applyCh <- applyMsg
		rf.mu.Lock()

		//Debug(dClient, "S%v T%v Commit:%v", rf.me, rf.currentTerm, rf.getLog(rf.lastApplied))
		//Debug(dClient, "S%v T%v Commit:%v", rf.me, rf.currentTerm, rf.lastApplied)

	}
}

// heartBeat send heartbeat to others from leader
func (rf *Raft) HeartBeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.sendLogs()

	rf.commitMsg()

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
	for range rf.peers {
		rf.nextIndex = append(rf.nextIndex, len(rf.logs))
		rf.matchIndex = append(rf.matchIndex, 0)
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
		mu:          sync.Mutex{},
		peers:       peers,
		persister:   persister,
		me:          me,
		dead:        0,
		currentTerm: 0,
		votedFor:    -1,
		logs:        make([]Log, 1),
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   nil,
		matchIndex:  nil,
		applyCh:     applyCh,
		status:      Follower,
	}

	// Your initialization code here (3A, 3B, 3C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	//go rf.commiter()

	return rf
}

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
	// TODO
	currentTerm int
	votedFor    int
	logs        map[int]Log

	commitIndex int
	lastApplied int

	nextIndex  map[int]int
	matchIndex map[int]int

	applyCh chan raftapi.ApplyMsg
	wg      sync.WaitGroup

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
	Debug(dInfo, "S%v T%v GetState %v", rf.me, rf.currentTerm, rf.status)
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
	// TODO
	Term        int
	CandidateId int
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
	// TODO
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm || args.Term == rf.currentTerm && rf.votedFor != -1 {
		// term is lower than mine, or the same term but already voted return false
		reply.VoteGranted = false
		Debug(dVote, "S%v T%v ---Vote---> S%v refused", rf.me, rf.currentTerm, args.CandidateId)
	} else if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) || args.Term > rf.currentTerm {
		// not voted or higher term, return true
		rf.updateNodeWithNewTerm(args.Term)
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		Debug(dVote, "S%v T%v ---Vote---> S%v", rf.me, rf.currentTerm, args.CandidateId)
	}
	// reply this node`s term
	reply.Term = rf.currentTerm
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
	Entries     map[int]Log

	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term >= rf.currentTerm {
		if rf.status == Leader {
			Debug(dLeader, "S%v T%v <---heartbeat--- S%v, become Follower", rf.me, rf.currentTerm, args.LeaderId)
		} else if args.Entries == nil {
			Debug(dTimer, "S%v T%v <---heartbeat--- S%v", rf.me, rf.currentTerm, args.LeaderId)
		}
		rf.updateNodeWithNewTerm(args.Term)
		rf.resetElectionTicker()
	}
	reply.Term = rf.currentTerm

	if args.LeaderCommit > rf.commitIndex {
		if len(rf.logs) < args.LeaderCommit {
			rf.commitIndex = len(rf.logs)
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		Debug(dCommit, "S%v T%v Commit Command and Index:%v %v", rf.me, rf.currentTerm, rf.logs[rf.commitIndex].Command, rf.commitIndex)
		applyMsg := rf.createApplyMsg()

		rf.mu.Unlock()
		rf.applyCh <- applyMsg
		rf.mu.Lock()
	}

	if args.Entries == nil {
		return
	}

	// TODO: 3B
	Debug(dCommit, "S%v T%v start handle command", rf.me, rf.currentTerm)

	success := true

	Debug(dSnap, "S%v T%v len(rf.logs):%v PrevLogIndex:%v", rf.me, rf.currentTerm, len(rf.logs), args.PrevLogIndex)

	if len(rf.logs) > args.PrevLogIndex {
		success = false
		if args.PrevLogIndex >= 0 {
			i := args.PrevLogIndex
			for i = range rf.logs {
				delete(rf.logs, i)
			}
			Debug(dLog, "S%v T%v delete logs because no commited", rf.me, rf.currentTerm, args.LeaderId)
		}
		Debug(dError, "S%v T%v <---failed to AppendEntries--- %v  1", rf.me, rf.currentTerm, args.LeaderId)
	} else if len(rf.logs) == args.PrevLogIndex {
		if args.PrevLogIndex != 0 {
			if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
				success = false
				Debug(dError, "S%v T%v <---failed to AppendEntries--- %v  2", rf.me, rf.currentTerm, args.LeaderId)
			}
		}
	} else if len(rf.logs) < args.PrevLogIndex {
		// TODO
		Debug(dError, "S%v T%v <---AppendEntries--- %v  3", rf.me, rf.currentTerm, args.LeaderId)
	}

	if success == true {
		rf.entriesToLogs(args.Entries)
		Debug(dCommit, "S%v T%v new logs:%v", rf.me, rf.currentTerm, args.Entries)
	}

	reply.Success = success
	//Debug(dTrace, "S%v T%v Log:%v", rf.me, rf.currentTerm, rf.logs)

}

func (rf *Raft) GetStatus() Status {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.status
}

func (rf *Raft) entriesToLogs(entries map[int]Log) {
	for _, item := range entries {
		rf.logs[len(rf.logs)+1] = item
	}
}

func (rf *Raft) createAppendEntriesArgs() *AppendEntriesArgs {
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: len(rf.logs) - 1,

		PrevLogTerm: 0,
		Entries:     nil,

		LeaderCommit: rf.commitIndex,
	}
	if len(rf.logs) > 0 {
		args.PrevLogTerm = rf.logs[len(rf.logs)].Term
	}
	return args
}

func (rf *Raft) createApplyMsg() raftapi.ApplyMsg {
	applyMsg := raftapi.ApplyMsg{
		CommandValid: true,
		Command:      rf.logs[rf.commitIndex].Command,
		CommandIndex: rf.commitIndex,
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
	// TODO
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := rf.status == Leader
	if !isLeader {
		return index, term, isLeader
	}

	agreement := 0
	isCommited := false
	entries := make(map[int]Log)
	entry := Log{
		Command: command,
		Term:    rf.currentTerm,
	}
	entries[0] = entry

	rf.entriesToLogs(entries)

	Debug(dCommit, "S%v T%v new logs:%v", rf.me, rf.currentTerm, entry)

	for i, _ := range rf.peers {

		if i == rf.me {
			agreement++
			continue
		}

		rf.wg.Add(1)
		go func(server int) {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			defer rf.wg.Done()

			args := rf.createAppendEntriesArgs()
			args.Entries = entries

			reply := &AppendEntriesReply{}

			rf.mu.Unlock()
			ok := rf.sendAppendEntries(server, args, reply)
			rf.mu.Lock()

			if !ok {
				Debug(dTimer, "S%v T%v ---AppendEntries---> S%v failed", rf.me, rf.currentTerm, server)
				return
			}
			if !reply.Success {
				if rf.nextIndex[server] > 0 {
					rf.nextIndex[server]--
				}
				Debug(dLog, "S%v T%v ---unsuccess agree---> S%v", rf.me, rf.currentTerm, args.LeaderId)
			} else {
				rf.nextIndex[server]++
				agreement++
				Debug(dLog, "S%v T%v nextIndex in S%v = %v", rf.me, rf.currentTerm, server, rf.nextIndex[server])
			}

			if agreement > len(rf.peers)/2 && !isCommited {
				isCommited = true
				rf.commitIndex++
				Debug(dClient, "S%v T%v Success commit commend and Index:%v %v",
					rf.me, rf.currentTerm, rf.logs[rf.commitIndex].Command, rf.commitIndex)
				// call commit
				applyMsg := rf.createApplyMsg()

				rf.mu.Unlock()
				rf.applyCh <- applyMsg
				rf.mu.Lock()

				index = rf.commitIndex
			}
		}(i)
	}
	rf.mu.Unlock()
	rf.wg.Wait()
	rf.mu.Lock()
	return index, term, isLeader
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

// election electe leader when ticker timeout
func (rf *Raft) Election() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentTerm++
	rf.status = Candidater

	votes := 0
	rvotes := 0
	args := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}

	Debug(dInfo, "S%v T%v Election", rf.me, rf.currentTerm)
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
			Debug(dVote, "S%v T%v ---RequestVote---> S%v", rf.me, rf.currentTerm, server)

			rf.mu.Unlock()
			ok := rf.sendRequestVote(server, args, reply)
			rf.mu.Lock()

			if !ok {
				Debug(dVote, "S%v T%v ---RequestVote---> S%v failed", rf.me, rf.currentTerm, server)
				return
			}
			if reply.Term > rf.currentTerm {
				rf.updateNodeWithNewTerm(reply.Term)
			}
			if reply.VoteGranted == false {
				rvotes++
			} else if reply.VoteGranted == true {
				votes++
			}

			if votes > len(rf.peers)/2 && rf.status == Candidater {
				rf.initLeader()
				Debug(dLeader, "S%v T%v become Leader", rf.me, rf.currentTerm)
			} else if rvotes > len(rf.peers)/2 {
				rf.status = Follower
				Debug(dLeader, "S%v T%v election failed", rf.me, rf.currentTerm)
			}

		}(i)
	}
}

// heartBeat send heartbeat to others from leader
func (rf *Raft) HeartBeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	args := rf.createAppendEntriesArgs()

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			reply := &AppendEntriesReply{}

			rf.mu.Unlock()
			ok := rf.sendAppendEntries(server, args, reply)
			rf.mu.Lock()

			if !ok {
				Debug(dTimer, "S%v T%v ---HeartBeat---> S%v failed", rf.me, rf.currentTerm, server)
				return
			}
			if reply.Term > rf.currentTerm {
				Debug(dLeader, "S%v T%v old term, become Follower", rf.me, rf.currentTerm)
				rf.updateNodeWithNewTerm(reply.Term)
			}

		}(i)

	}
}

// updateNodeWithNewTerm update the term and change node`s new term status
func (rf *Raft) updateNodeWithNewTerm(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.status = Follower
}

// resetElectionTicker set a random time to ticker
func (rf *Raft) resetElectionTicker() {
	ms := 500 + (rand.Int63() % 500)
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
	for i, _ := range rf.peers {
		rf.nextIndex[i] = rf.commitIndex
		rf.matchIndex[i] = 0
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
		logs:        make(map[int]Log),
		commitIndex: 0,
		lastApplied: -1,
		nextIndex:   make(map[int]int),
		matchIndex:  make(map[int]int),
		applyCh:     applyCh,
		wg:          sync.WaitGroup{},
		status:      Follower,
	}

	// Your initialization code here (3A, 3B, 3C).
	// TODO
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	//go rf.commiter()

	return rf
}

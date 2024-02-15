package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
const (
	Candidate = iota
	Follower
	Leader
)
const ElectionTimeout = 500 * time.Millisecond

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	currentTerm     int
	votedFor        int
	logs            []LogEntry
	status          int // Candidate Follower Leader
	LeaderHeartBeat bool
	voteResult      chan int // 若能提前得知竞选结果，则立刻通知

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var leader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	leader = rf.status == Leader
	rf.mu.Unlock()
	return term, leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
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
	// Your code here (2C).
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type LogEntry struct {
	LogIndex int
	Content  string

	Term     int
	LeaderId int
}

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term        int
	LeaderId    int
	PreLogIndex int
	PreLogTerm  int

	Logs         []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int
	Success bool
}

// 发送心跳 / 日志更新消息
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if server != rf.me {
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.status = Follower
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
		}
		return ok
	}
	return true
}

// 选举成功 / Leader心跳 / 日志更新消息 handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.currentTerm > args.Term { //直接拒绝
		//注意这里的心跳可能是过期leader发来的心跳，要发出最新的term撤回其leader
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	} else if rf.currentTerm == args.Term {
		if len(args.Logs) == 0 {
			//心跳 更新计时
			rf.mu.Lock()
			if rf.status == Candidate { // 选举过程中收到心跳则立刻掐死该次选举
				rf.mu.Unlock()
				rf.status = Follower
				rf.votedFor = -1
				rf.voteResult <- 1
			} else {
				rf.mu.Unlock()
			}
			//fmt.Printf("%v received heart beat from %v\n", rf.me, args.LeaderId)
			rf.LeaderHeartBeat = true
			reply.Success = true
			reply.Term = args.Term
		}
		//TODO 接受日志更新
	} else { // 选举成功/脑裂合并
		//t := rf.currentTerm
		//fmt.Printf("%v update term from %v to %v\n", rf.me, t, args.Term)
		rf.mu.Lock()
		rf.LeaderHeartBeat = true
		rf.votedFor = -1
		rf.currentTerm = args.Term
		if rf.status == Candidate { // 选举过程中收到心跳则立刻掐死该次选举
			rf.status = Follower
			rf.voteResult <- 1
		}
		rf.status = Follower
		rf.mu.Unlock()
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 选举失败信息
	if args.Term == -1 {
		rf.mu.Lock()
		if args.CandidateId == rf.votedFor {
			rf.votedFor = -1
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
		}
		return
	}
	rf.mu.Lock()
	if rf.currentTerm > args.Term { // term不够
		rf.mu.Unlock()
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	} else if rf.currentTerm == args.Term {
		if rf.votedFor == -1 && rf.status == Follower {
			rf.votedFor = args.CandidateId
			rf.mu.Unlock()
			//fmt.Printf("%v voted for %v, now term %v\n", rf.me, args.CandidateId, rf.currentTerm)
			reply.VoteGranted = true
			reply.Term = args.Term
		} else {
			rf.mu.Unlock()
		}
	} else { // rf.currentTerm < args.Term 此时必须同意
		rf.status = Follower
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.mu.Unlock()
		reply.VoteGranted = true
		reply.Term = args.Term

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, mu *sync.Mutex, ch chan int, voted *int) bool {
	if server != rf.me {
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		if reply.VoteGranted == true {
			mu.Lock()
			*voted++
			if *voted > len(rf.peers)/2 {
				ch <- 1
			}
			mu.Unlock()
		}
		return ok
	}
	return true
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
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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

func (rf *Raft) HeartBeat() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.status == Leader {
			rf.mu.Unlock()
			//fmt.Printf("%v send heart beat\n", rf.me)
			args := AppendEntriesArgs{rf.currentTerm, rf.me, 0, 0, nil, 0}
			for i, _ := range rf.peers {
				reply := AppendEntriesReply{}
				go rf.sendAppendEntries(i, &args, &reply)

			}
		} else {
			rf.mu.Unlock()
			return
		}

		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		ms := 150 + (rand.Int63() % 200)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		// Your code here (2A)
		// Check if a leader election should be started.
		// 若sleep过程中没有接收到心跳，则发起选举
		rf.mu.Lock()
		if rf.LeaderHeartBeat == false && rf.status == Follower && rf.votedFor == -1 {
			rf.mu.Unlock()
			//fmt.Printf("%d receive no heart beat,begin electron,now term: %v\n", rf.me, rf.currentTerm)
			voted := 1
			rf.mu.Lock()
			rf.status = Candidate
			rf.votedFor = rf.me
			rf.mu.Unlock()
			mu := sync.Mutex{}
			rf.voteResult = make(chan int)

			args := RequestVoteArgs{rf.currentTerm, rf.me, 0, 0}
			for i, _ := range rf.peers {
				reply := RequestVoteReply{}
				go rf.sendRequestVote(i, &args, &reply, &mu, rf.voteResult, &voted)
			}
			// 有些可能故障了回应不了，不用等，只要有半数回应就通过channel通知
			// 等不到半数就一直等
			// 除非已经有别的candidate选举成功为leader把当前candidate改为了follower，或者老leader复活等情况
			// 导致当前选举一定会失败
			// 使用select监听ch
			select {
			case <-rf.voteResult:
				rf.mu.Lock()
				// 如果被选中为leader
				if voted > len(rf.peers)/2 && rf.status == Candidate {
					rf.status = Leader
					rf.currentTerm++
					rf.votedFor = -1
					rf.mu.Unlock()
					//fmt.Printf("%d begin leader,now term %v \n", rf.me, rf.currentTerm)
					// 选举成功之后立马宣布
					go rf.HeartBeat()
				} else {
					//fmt.Printf("%v elect failure", rf.me)
					rf.status = Follower
					rf.votedFor = -1
					rf.mu.Unlock()
				}
				//如果超过了选举时间，则此次选举失败
				//选举失败的话，需要告诉给这位投过票的人
			case <-time.After(time.Duration(rand.Int63()%500)*time.Millisecond + ElectionTimeout):
				//fmt.Printf("%v elect failure", rf.me)
				rf.mu.Lock()
				rf.status = Follower
				rf.votedFor = -1
				rf.mu.Unlock()
				args := RequestVoteArgs{-1, rf.me, 0, 0}
				for i, _ := range rf.peers {
					reply := RequestVoteReply{}
					go rf.sendRequestVote(i, &args, &reply, &mu, rf.voteResult, &voted)
				}

			}
		} else {
			rf.LeaderHeartBeat = false
			rf.mu.Unlock()
		}
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.status = Follower
	rf.votedFor = -1

	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

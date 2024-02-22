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
	"fmt"
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
	Command interface{}
	Term    int
}

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term     int
	LeaderId int

	PreLogIndex  int //commit老的，同时request新的
	PreLogTerm   int
	Logs         []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	NextIndex int
	Term      int
	Success   bool
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	term            int
	votedFor        int
	logs            []LogEntry
	status          int // Candidate Follower Leader
	LeaderHeartBeat bool
	voteResult      chan int // 若能提前得知竞选结果，则立刻通知

	commitIndex int //commitIdx>=lastApplied
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	applyCh     chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

func min(x int, y int) int {
	if x > y {
		return y
	} else {
		return x
	}
}

func max(x int, y int) int {
	if x > y {
		return x
	} else {
		return y
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

// return term and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var leader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.term
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

// 发送心跳 / 日志更新消息
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, mu *sync.Mutex, ch chan int, agreed *int) bool {
	if server != rf.me {
		//cnt := 1
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		for !ok {
			return false
			//cnt++
			//time.Sleep(20 * time.Millisecond)
			//ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
			//if cnt == 5 {
			//	return false
			//}
		}
		rf.mu.Lock()
		if reply.Term > rf.term {
			rf.term = reply.Term
			rf.status = Follower
		}
		rf.mu.Unlock()

		if mu != nil {
			if reply.Success == true {
				mu.Lock()
				*agreed++
				fmt.Printf("svr%v agree on %v\n", server, args.Logs)
				if *agreed == len(rf.peers)/2+1 {
					ch <- 1
				}
				mu.Unlock()
			}
			rf.nextIndex[server] = reply.NextIndex
			fmt.Printf("svr%v nextIndex now is %v\n", server, rf.nextIndex[server])
		}
		return true
	}
	return true
}

// 选举成功 / Leader心跳 / 日志更新消息 handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.term > args.Term { //直接拒绝
		//注意这里的心跳可能是过期leader发来的心跳，要发出最新的term撤回其leader
		reply.Success = false
		reply.Term = rf.term
	} else if rf.term == args.Term { //心跳 / 日志更新
		rf.LeaderHeartBeat = true
		//fmt.Printf("svr%v args.PreLogIndex:%v  len(rf.logs)-1):%v  args.LeaderCommit:%v  rf.commitIndex:%v\n", rf.me, args.PreLogIndex, len(rf.logs)-1, args.LeaderCommit, rf.commitIndex)
		c1 := !((args.PreLogIndex == len(rf.logs)-1) && (args.PreLogTerm == rf.logs[args.PreLogIndex].Term)) && rf.commitIndex <= args.LeaderCommit
		c2 := args.LeaderCommit == -1
		if c1 || c2 {
			if c2 {
				t := len(rf.logs) - 1
				rf.logs = rf.logs[:t]
				reply.NextIndex = args.PreLogIndex + 1 // 上一个commit失败了，要撤回，nextIndex--
			} else {
				reply.NextIndex = args.PreLogIndex // 不匹配，nextIndex--
			}
			reply.Success = false
			reply.Term = args.Term
			return
		}

		if rf.commitIndex <= args.LeaderCommit {
			// 及时把已经committed的logs告诉给Foller
			for i := rf.commitIndex + 1; i <= min(args.LeaderCommit, len(rf.logs)-1); i++ {
				fmt.Printf("svr%v applying %v\n", rf.me, rf.logs[i].Command)
				rf.applyCh <- ApplyMsg{true, rf.logs[i].Command, i, false, nil, 0, 0}
			}
			rf.commitIndex = min(args.LeaderCommit, len(rf.logs)-1)
			if len(args.Logs) == 0 { // 心跳
				if rf.status == Candidate {
					// 选举过程中收到心跳则立刻掐死该次选举
					rf.status = Follower
					rf.votedFor = -1
					rf.voteResult <- 1
				}
			} else { //日志更新
				// 捎带确认，commit老的，append新的
				for i := 0; i < len(args.Logs); i++ {
					fmt.Printf("svr%v append %v\n", rf.me, args.Logs[i].Command)
					rf.logs = append(rf.logs, args.Logs[i])
				}
			}
		}
		//fmt.Printf("%v received heart beat from %v\n", rf.me, args.LeaderId)
		reply.Success = true
		reply.Term = args.Term
		reply.NextIndex = len(rf.logs)
	} else { // 选举成功/脑裂合并
		//t := rf.term
		//fmt.Printf("%v update term from %v to %v\n", rf.me, t, args.Term)
		rf.LeaderHeartBeat = true
		rf.votedFor = -1
		rf.term = args.Term
		if rf.status == Candidate { // 选举过程中收到心跳则立刻掐死该次选举
			rf.status = Follower
			rf.voteResult <- 1
		}
		rf.status = Follower
	}
}

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
	if rf.term > args.Term { // term不够
		rf.mu.Unlock()
		reply.VoteGranted = false
		reply.Term = rf.term
	} else if rf.term == args.Term {
		if rf.votedFor == -1 && rf.status == Follower {
			rf.votedFor = args.CandidateId
			rf.mu.Unlock()
			//fmt.Printf("%v voted for %v, now term %v\n", rf.me, args.CandidateId, rf.term)
			reply.VoteGranted = true
			reply.Term = args.Term
		} else {
			rf.mu.Unlock()
		}
	} else { // rf.term < args.Term 此时必须同意
		rf.status = Follower
		rf.term = args.Term
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
		for !ok {
			time.Sleep(10 * time.Millisecond)
			ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
		}
		if reply.VoteGranted == true {
			mu.Lock()
			*voted++
			if *voted == len(rf.peers)/2+1 {
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
	// Your code here (2B).
	index := -1
	term := -1
	isLeader := false
	rf.mu.Lock()
	if rf.status != Leader {
		rf.mu.Unlock()
		return index, term, isLeader
	} else {
		rf.mu.Unlock()
	}
	isLeader = true

	fmt.Printf("leader%v want to commit %v\n", rf.me, command)
	rf.logs = append(rf.logs, LogEntry{command, rf.term})
	index = len(rf.logs) - 1
	mu := sync.Mutex{}
	ch := make(chan int)
	agreed := 1

	for i, _ := range rf.peers {
		args := AppendEntriesArgs{
			rf.term, rf.me, rf.nextIndex[i] - 1, rf.logs[rf.nextIndex[i]-1].Term, rf.logs[rf.nextIndex[i]:], rf.commitIndex}
		reply := AppendEntriesReply{}
		go rf.sendAppendEntries(i, &args, &reply, &mu, ch, &agreed)
	}

	select { // 批准该commit
	case <-ch:
		rf.commitIndex++
		fmt.Printf("committing %v\n", rf.commitIndex)
		rf.applyCh <- ApplyMsg{
			CommandValid: true, Command: command, CommandIndex: rf.commitIndex, SnapshotValid: false, Snapshot: nil, SnapshotTerm: 0, SnapshotIndex: 0}

	case <-time.After(time.Duration(1000) * time.Millisecond):
		fmt.Printf("commit timeout: %v\n", rf.commitIndex+1)
		//超时的就不commit了,且需要把log也给删掉，但是部分Follower可能已经将其加入到logs里面了,此时需要删除掉本次log
		rf.logs = rf.logs[:rf.commitIndex+1]
		for i, _ := range rf.peers {
			rf.nextIndex[i] = min(rf.nextIndex[i], rf.commitIndex+1)
			args := AppendEntriesArgs{
				rf.term, rf.me, rf.nextIndex[i] - 1, rf.logs[rf.nextIndex[i]-1].Term, rf.logs[rf.nextIndex[i]:], -1}
			reply := AppendEntriesReply{}
			go rf.sendAppendEntries(i, &args, &reply, &mu, ch, &agreed)
		}
	}

	return index, term, isLeader
}

func (rf *Raft) HeartBeat() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.status == Leader {
			rf.mu.Unlock()
			fmt.Printf("%v send heart beat\n", rf.me)
			for i, _ := range rf.peers {
				args := AppendEntriesArgs{rf.term, rf.me, rf.nextIndex[i] - 1, rf.logs[rf.nextIndex[i]-1].Term, rf.logs[rf.nextIndex[i]:], rf.commitIndex}
				reply := AppendEntriesReply{}
				go rf.sendAppendEntries(i, &args, &reply, nil, nil, nil)
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
			fmt.Printf("%d receive no heart beat,begin electron,now term: %v\n", rf.me, rf.term)
			voted := 1
			rf.mu.Lock()
			rf.status = Candidate
			rf.votedFor = rf.me
			rf.mu.Unlock()
			mu := sync.Mutex{}
			rf.voteResult = make(chan int)

			args := RequestVoteArgs{rf.term, rf.me, 0, 0}
			for i, _ := range rf.peers {
				reply := RequestVoteReply{}
				go rf.sendRequestVote(i, &args, &reply, &mu, rf.voteResult, &voted)
			}
			// 有些可能故障了回应不了，不用等，只要有半数回应就通过channel通知
			// 等不到半数就一直等
			// 除非已经有别的candidate选举成功为leader把当前candidate改为了follower，或者老leader复活等情况
			// 导致当前选举一定会失败
			select {
			case <-rf.voteResult:
				rf.mu.Lock()
				// 如果被选中为leader
				if voted > len(rf.peers)/2 && rf.status == Candidate {
					rf.status = Leader
					rf.term++
					rf.votedFor = -1
					l := len(rf.logs)
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = l
						rf.matchIndex[i] = 0
					}
					rf.mu.Unlock()
					fmt.Printf("%d begin leader,now term %v \n", rf.me, rf.term)
					// 选举成功之后立马宣布
					go rf.HeartBeat()
				} else {
					fmt.Printf("%v elect failure", rf.me)
					rf.status = Follower
					rf.votedFor = -1
					rf.mu.Unlock()
				}
				//如果超过了选举时间，则此次选举失败
				//选举失败的话，需要告诉给这位投过票的人
			case <-time.After(time.Duration(rand.Int63()%800+200) * time.Millisecond):
				fmt.Printf("%v elect failure", rf.me)
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
	rf.applyCh = applyCh
	rf.commitIndex = 0
	rf.logs = []LogEntry{{nil, 0}}
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

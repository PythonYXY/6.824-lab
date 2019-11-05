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
	"6.824/labgob"
	"bytes"
	"fmt"
	"sync"
	"time"
)
import "6.824/labrpc"

// import "bytes"
// import "labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type State string
const (
	Follower State = "follower"
	Candidate State = "candidate"
	Leader State = "leader"
)

type Entry struct {
	Term int
	Cmd interface{}
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	timeout int            // 超时时间，单位毫秒
	applyCh chan ApplyMsg

	// 服务器上持久存在的(Updated on stable storage before responding to RPCs)
	currentTerm int         // 服务器最后一次知道的任期号（初始化为 0，持续递增）
	votedFor int            // 在当前任期获得选票的候选人的 Id, 默认值为-1
	log []Entry             // 日志条目集；每一个条目包含一个用户状态机执行的指令，和收到时的任期号(第一个索引值是1!!!)

	// 在服务器上经常变的
	currentState State
	commitIndex int         // 已知的最大的已经被提交的日志条目的索引值
	lastApplied int         // 最后被应用到状态机的日志条目索引值（初始化为 0，持续递增）
	lastUpdatedTime time.Time

	// 在领导人里经常变的
	nextIndex []int         // 对于每一个服务器，需要发送给他的下一个日志条目的索引值（初始化为领导人最后索引值加一）
	matchIndex []int        // 对于每一个服务器，已经复制给他的日志的最高索引值
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.currentState == Leader
	rf.mu.Unlock()

	return term, isleader
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	_ = e.Encode(rf.currentTerm)
	_ = e.Encode(rf.votedFor)
	_ = e.Encode(rf.log)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var log []Entry

	if d.Decode(&currentTerm) != nil ||
	   d.Decode(&votedFor) != nil ||
	   d.Decode(&log) != nil {
		fmt.Println("Decode Error!")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}


}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int             // 候选人的任期号
	CandidateId int		 // 请求选票的候选人的 Id
	LastLogIndex int     // 候选人的最后日志条目的索引值
	LastLogTerm int      // 候选人最后日志条目的任期号
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int 		    // 当前任期号，以便于候选人去更新自己的任期号
	VoteGranted bool    // 候选人赢得了此选票时为真
}

type AppendEntries struct {
	Term int              // 领导人的任期号
	LeaderId int          // 领导人的 Id，以便于跟随者重定向请求
	PrevLogIndex int      // 新的日志条目紧随之前的索引值
	PrevLogTerm int       // prevLogIndex 条目的任期号
	Entries []Entry       // 准备存储的日志条目（表示心跳时为空；一次性发送多个是为了提高效率）
	LeaderCommit int      // 领导人已经提交的日志的索引值
}

type AppendEntriesReply struct {
	Term int              // 当前的任期号，用于领导人去更新自己
	Success bool          // 跟随者包含了匹配上 prevLogIndex 和 prevLogTerm 的日志时为真

	ConflictTerm int      // Follower的与Leader不一致的Term
	ConflictIndex int     // Follower希望Leader重新发送的LogIndex
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()


	// Your code here (2A, 2B).
	reply.Term = rf.currentTerm // 千万不要改变reply指针
	reply.VoteGranted = false

	if args.Term < rf.currentTerm { return }

	// 如果接收到的 RPC 请求或响应中，任期号T > currentTerm，那么就令 currentTerm 等于 T
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.persist()
	}

	// 按照选票先到先服务的原则
	if rf.currentState == Follower && rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		return
	}

	// 确保候选人的日志信息至少和自己一样新
	if len(rf.log) > 0 && (
		args.LastLogTerm < rf.log[len(rf.log) - 1].Term ||
		(args.LastLogTerm ==  rf.log[len(rf.log) - 1].Term && args.LastLogIndex < len(rf.log) - 1)) {
		return
	}

	reply.VoteGranted = true

	rf.update(Follower, args.Term, args.CandidateId, time.Now().UTC())
	rf.persist()
	// fmt.Println("Current term: " + strconv.Itoa(rf.currentTerm) + ", " + strconv.Itoa(rf.me) + " voted for " + strconv.Itoa(args.CandidateId))
	return
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntries, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.HandleAppendEntries", args, reply)
	return ok
}

func (rf *Raft) HandleAppendEntries(args *AppendEntries, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	if rf.currentTerm > args.Term {
		return
	}

	if args.PrevLogIndex > len(rf.log) - 1 {
		reply.ConflictIndex = len(rf.log)
		reply.ConflictTerm = -1

		rf.update(Follower, args.Term, -1, time.Now().UTC())
		rf.persist()
		return
	}

	if args.PrevLogIndex < 0 || args.PrevLogTerm == rf.log[args.PrevLogIndex].Term {
		reply.Success = true

		if args.Entries != nil {
			rf.log = append(rf.log[:args.PrevLogIndex + 1], args.Entries...)
		}

		if args.LeaderCommit > rf.commitIndex {
			go func(commitIndex int, applyCh chan ApplyMsg, log []Entry, leaderCommit int) {
				for i := commitIndex + 1; i <= leaderCommit; i++ {
					applyCh <- ApplyMsg{
						CommandValid: true,
						Command:      log[i].Cmd,
						CommandIndex: i + 1, // log的索引从1开始
					}
				}
			}(rf.commitIndex, rf.applyCh, rf.log, args.LeaderCommit)

			rf.commitIndex = args.LeaderCommit
			rf.lastApplied = rf.commitIndex
		}

		rf.update(Follower, args.Term, -1, time.Now().UTC())
		rf.persist()
		return
	}

	// rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm
	reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
	conflictIndex := args.PrevLogIndex

	for conflictIndex > 0 && rf.log[conflictIndex - 1].Term == reply.ConflictTerm {
		conflictIndex--
	}
	reply.ConflictIndex = conflictIndex
	rf.update(Follower, args.Term, -1, time.Now().UTC())
	rf.persist()

	return
}

//
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
//
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	index = -1
	term = -1
	isLeader = true

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here (2B).
	if rf.currentState != Leader {
		return
	}

	rf.log = append(rf.log, Entry{
		Term: rf.currentTerm,
		Cmd:  command,
	})

	rf.matchIndex[rf.me] = len(rf.log) - 1 // 后续需要根据matchIndex判断日志是否已经复制给大多数服务器了（包括Leader本身）

	term = rf.currentTerm
	index = len(rf.log)  // log的索引从1开始
	rf.persist()

	return
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.timeout = randInt(300, 400)
	rf.applyCh = applyCh
	rf.currentState = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]Entry, 0)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.commitIndex = -1
	rf.lastApplied = -1

	rf.lastUpdatedTime = time.Now().UTC()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.handleTimeout(rf.timeout)

	return rf
}

func (rf *Raft) handleTimeout(timeout int) {
	for {
		time.Sleep(time.Duration(timeout) * time.Millisecond)
		rf.mu.Lock()
		if rf.currentState != Leader && (time.Since(rf.lastUpdatedTime).Nanoseconds() / 1e6 >= int64(timeout)) { //
			rf.update(Candidate, rf.currentTerm + 1, -1, time.Now().UTC())
			rf.persist()
			rf.startElection()  // 在startElection中会释放掉锁
			continue
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) startElection() {
	//fmt.Printf("%v start election with term %v\n", rf.me, rf.currentTerm)
	var counterLock sync.Mutex
	counter := 1
	currentTerm := rf.currentTerm
	request := &RequestVoteArgs{
		currentTerm,
		rf.me,
		len(rf.log) - 1,
		-1,
	}

	if len(rf.log) != 0 {
		request.LastLogTerm = rf.log[len(rf.log) - 1].Term
	}

	for index := 0; index < len(rf.peers); index++ {
		if index != rf.me {
			go func(index int) {
				reply := &RequestVoteReply{}
				ok := rf.sendRequestVote(index, request, reply)
				if ok && reply.VoteGranted {
					counterLock.Lock()
					counter += 1
					currentValue := counter
					counterLock.Unlock()

					rf.mu.Lock()
					if rf.currentState == Candidate && currentTerm == rf.currentTerm && currentValue > (len(rf.peers) / 2) {
						//fmt.Printf("%v became leader! With log: %v\n ", rf.me, rf.log)
						rf.currentState = Leader
						rf.persist()

						// 成为Leader后的初始化工作
						for i, _ := range rf.nextIndex {
							rf.nextIndex[i] = len(rf.log)
							rf.matchIndex[i] = -1
						}
						go rf.sendHeartbeats()
					}
					rf.mu.Unlock()
				}
			}(index)
		}
	}
	rf.mu.Unlock()
}

// 每120ms发送一次心跳信息
func (rf *Raft) sendHeartbeats() {
	for {
		rf.mu.Lock()
		if rf.currentState != Leader {
			rf.mu.Unlock()
			return
		}

		for serverIndex := 0; serverIndex < len(rf.peers); serverIndex++ {
			if serverIndex != rf.me {
				appendEntries := &AppendEntries{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[serverIndex] - 1,
					PrevLogTerm:  -1,
					Entries:      nil,
					LeaderCommit: rf.commitIndex,
				}

				if appendEntries.PrevLogIndex >= 0 { appendEntries.PrevLogTerm = rf.log[appendEntries.PrevLogIndex].Term }

				if len(rf.log) > rf.nextIndex[serverIndex] {
					appendEntries.Entries = make([]Entry, 0)
					start := rf.nextIndex[serverIndex]
					if start == -1 { start = 0 }
					appendEntries.Entries = append(appendEntries.Entries, rf.log[start:]...)
				}

				go func(serverIndex int) {
					reply := &AppendEntriesReply{}
					ok := rf.sendAppendEntries(serverIndex, appendEntries, reply)
					rf.mu.Lock()
					if ok && rf.currentState == Leader { rf.handleHeartbeatsReply(serverIndex, reply, appendEntries) }
					rf.mu.Unlock()
				}(serverIndex)
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(120) * time.Millisecond)
	}
}

func (rf *Raft) handleHeartbeatsReply(serverIndex int, reply *AppendEntriesReply, appendEntries *AppendEntries) {
	if reply.Success {
		// note
		rf.matchIndex[serverIndex] = appendEntries.PrevLogIndex + len(appendEntries.Entries)
		rf.nextIndex[serverIndex] = rf.matchIndex[serverIndex] + 1

		// 如果存在一个满足N > commitIndex的N，并且大多数的matchIndex[i] ≥ N成立，并且log[N].term == currentTerm成立，那么令 commitIndex 等于这个 N
		N := rf.matchIndex[serverIndex]

		if N >=0 && N < len(rf.log) && rf.log[N].Term == rf.currentTerm {
			counter := 0

			for i := 0; i < len(rf.peers); i++ {
				if rf.matchIndex[i] >= N { counter++ }
			}

			if counter > len(rf.peers) / 2 {
				go func(commitIndex int, applyCh chan ApplyMsg, log []Entry) {
					for i := commitIndex + 1; i <= N; i++ {
						applyCh <- ApplyMsg{
							CommandValid: true,
							Command:      log[i].Cmd,
							CommandIndex: i + 1, // log的索引从1开始
						}
					}
				}(rf.commitIndex, rf.applyCh, rf.log)
				rf.commitIndex = N
			}
		}
		return
	}

	if reply.Term > rf.currentTerm {
		rf.update(Follower, reply.Term, -1, time.Now().UTC())
		rf.persist()
		return
	}

	rf.nextIndex[serverIndex]--

	rf.nextIndex[serverIndex] = reply.ConflictIndex

	if reply.ConflictTerm != -1 {
		for i := appendEntries.PrevLogIndex; i >= 1; i-- {
			if rf.log[i-1].Term == reply.ConflictTerm {
				rf.nextIndex[serverIndex] = i
			}
		}
	}
}



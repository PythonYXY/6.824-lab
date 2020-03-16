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
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)
import "../labrpc"
import "../labgob"



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
	UseSnapShot  bool
	SnapShot     []byte
}

//
// A Go object implementing a single Raft peer.
//
type State int
const (
	Follower State = iota
	Candidate
	Leader
)

const NULL int = -1

type Log struct {
	Term int  // 从领导人处收到的任期号
	Command interface{} // 状态机的要执行的指令
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state State

	// 在所有服务器上持久存在的
	currentTerm int // 服务器最后知道的任期号
	votedFor int // 在当前任期内收到选票的候选人id（如果没有就为null)
	log []Log // 日志条目

	// 在所有服务器上不稳定存在的
	commitIndex int // 已知的被提交的最大日志条目的索引值 （从0开始递增）
	lastApplied int // 被状态机执行的最大的日志条目的索引值 （从0开始递增）

	// 在领导人服务器上不稳定存在的（在选举之后初始化的）
	nextIndex []int  // 对于每一个服务器，记录需要发给他的下一个日志条目的索引值
	matchIndex []int //对于每一个服务器，记录已经复制到该服务器的日志的最高索引值（从0开始递增）

	applyCh chan ApplyMsg

	// handle RPC
	voteCh chan bool
	appendLogCh chan bool

	killCh chan bool

	lastIncludedIndex int
	lastIncludedTerm int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
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

	rf.persister.SaveRaftState(rf.encodeRaftState())
}

func (rf *Raft) encodeRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	_ = e.Encode(rf.currentTerm)
	_ = e.Encode(rf.votedFor)
	_ = e.Encode(rf.log)
	_ = e.Encode(rf.lastIncludedIndex)
	_ = e.Encode(rf.lastIncludedTerm)

	return w.Bytes()
}

func (rf *Raft) persistWithSnapShot(snapshot []byte) {
	rf.persister.SaveStateAndSnapshot(rf.encodeRaftState(), snapshot)
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
	var log []Log
	var lastIncludedIndex int
	var lastIncludedTerm int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		fmt.Printf("read persist error for server %v\n", rf.me)
	} else {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedTerm
		rf.lastIncludedTerm = lastIncludedTerm
		rf.commitIndex = rf.lastIncludedIndex
		rf.lastApplied = rf.lastIncludedIndex
		rf.mu.Unlock()
	}
}

func (rf *Raft) DoSnapShot(curIdx int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if curIdx <= rf.lastIncludedIndex {
		return
	}

	newLog := make([]Log, 0)
	newLog = append(newLog, rf.log[curIdx - rf.lastIncludedIndex:]...)

	rf.lastIncludedIndex = curIdx
	rf.lastIncludedTerm = rf.getLog(curIdx).Term
	rf.log = newLog

	rf.persistWithSnapShot(snapshot)
}


type InstallSnapShotArgs struct {
	Term int
	LeaderId int
	LastIncludedIndex int
	LastIncludedTerm int
	Data []byte
}

type InstallSnapShotReply struct {
	Term int
}

func (rf *Raft) sendInstallSnapShot(server int, args *InstallSnapShotArgs, reply *InstallSnapShotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapShot(args *InstallSnapShotArgs, reply *InstallSnapShotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.beFollower(args.Term)
	}
	send(rf.appendLogCh) // 防止跟随者一段时间内没有接收到心跳信息而超时
	if args.LastIncludedIndex < rf.lastIncludedIndex {
		return
	}

	// TODO
	applyMsg := ApplyMsg{
		CommandValid: false,
		UseSnapShot:  true,
		SnapShot:     args.Data,
	}

	if args.LastIncludedIndex < rf.logLen()-1 {
		rf.log = append(make([]Log,0),rf.log[args.LastIncludedIndex -rf.lastIncludedIndex:]...)
	}else {
		rf.log = []Log{{args.LastIncludedTerm, nil},}
	}

	rf.lastIncludedIndex, rf.lastIncludedTerm = args.LastIncludedIndex, args.LastIncludedTerm
	rf.persistWithSnapShot(args.Data)
	rf.commitIndex = Max(rf.commitIndex,rf.lastIncludedIndex)
	rf.lastApplied = Max(rf.lastApplied, rf.lastIncludedIndex)
	if rf.lastApplied > rf.lastIncludedIndex { return }
	rf.applyCh <- applyMsg
}

func Max(index int, index2 int) int {
	if index > index2 { return index }
	return index2
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

type AppendEntriesArgs struct {
	Term int              // 领导人的任期号
	LeaderId int          // 领导人的 Id，以便于跟随者重定向请求
	PrevLogIndex int      // 新的日志条目紧随之前的索引值
	PrevLogTerm int       // prevLogIndex 条目的任期号
	Entries []Log       // 准备存储的日志条目（表示心跳时为空；一次性发送多个是为了提高效率）
	LeaderCommit int      // 领导人已经提交的日志的索引值
}

type AppendEntriesReply struct {
	Term int              // 当前的任期号，用于领导人去更新自己
	Success bool          // 跟随者包含了匹配上 prevLogIndex 和 prevLogTerm 的日志时为真
	ConflictIndex int     // 冲突任期的最早的索引
	ConflictTerm int      // 冲突日志条目的任期号
}


func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.beFollower(args.Term)
		send(rf.voteCh)
	}
	success := false
	if args.Term < rf.currentTerm {
		// return false
	} else if rf.votedFor != NULL && rf.votedFor != args.CandidateId {
		// return false
	} else if args.LastLogTerm < rf.getLastLogTerm() {
		// candidate log is not new
	} else if args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex < rf.getLastLogIndex() {
		// candidate log is not new
	} else {
		rf.votedFor = args.CandidateId
		success = true
		rf.state = Follower
		rf.persist()
		send(rf.voteCh)
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = success
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func Min(x, y int) int {
	if x < y  { return x}
	return y
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer send(rf.appendLogCh)

	if args.Term > rf.currentTerm {
		rf.beFollower(args.Term)
	}

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictIndex = 0
	reply.ConflictTerm = NULL

	PrevLogIndexTerm := -1
	logSize := rf.logLen()
	if args.PrevLogIndex >= rf.lastIncludedIndex && args.PrevLogIndex < rf.logLen() {
		PrevLogIndexTerm = rf.getLog(args.PrevLogIndex).Term
	}
	if PrevLogIndexTerm != args.PrevLogTerm {
		reply.ConflictIndex = logSize
		if PrevLogIndexTerm == -1 {
			// if a follower does not have prevLogIndex in its log,
			// it should return with conflictIndex = rf.logLen() and conflictTerm = None.
		} else {
			reply.ConflictTerm = PrevLogIndexTerm
			i := rf.lastIncludedIndex
			for ; i < logSize; i++ {
				if rf.getLog(i).Term == reply.ConflictTerm {
					reply.ConflictIndex = i
					break
				}
			}
		}
		return
	}

	if args.Term < rf.currentTerm {
		return
	}

	index := args.PrevLogIndex
	for i := 0; i < len(args.Entries); i++ {
		index++
		if index >= rf.logLen() {
			rf.log = append(rf.log, args.Entries[i:]...)
			rf.persist()
			break
		}

		if rf.getLog(index).Term != args.Entries[i].Term {
			rf.log = rf.getLogBefore(index)
			rf.log = append(rf.log, args.Entries[i:]...)
			rf.persist()
			break
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, rf.getLastLogIndex())
		rf.updateLastApplied()
	}

	reply.Success = true
}

func (rf *Raft) updateLastApplied() {
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		curLog := rf.getLog(rf.lastApplied)
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      curLog.Command,
			CommandIndex: rf.lastApplied,
		}
	}
}

func (rf *Raft) updateCommitIndex() {
	rf.matchIndex[rf.me] = rf.logLen() - 1
	copyMatchIndex := make([]int, len(rf.matchIndex))
	copy(copyMatchIndex, rf.matchIndex)
	sort.Sort(sort.Reverse(sort.IntSlice(copyMatchIndex)))
	N := copyMatchIndex[len(copyMatchIndex) / 2]
	if N > rf.commitIndex && rf.getLog(N).Term == rf.currentTerm {
		rf.commitIndex = N
		rf.updateLastApplied()
	}
}


func (rf *Raft) startAppendLog() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(idx int) {
			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}
			if rf.nextIndex[idx] - rf.lastIncludedIndex < 1 {
				rf.sendSnapShot(idx)
				return
			}

			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.getPrevLogIdx(idx),
				PrevLogTerm:  rf.getPrevLogTerm(idx),
				Entries:      append([]Log{}, rf.getLogAfter(rf.nextIndex[idx])...),
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()
			reply := &AppendEntriesReply{}

			ret := rf.sendAppendEntries(idx, &args, reply)

			rf.mu.Lock()
			if !ret || rf.state != Leader || rf.currentTerm != args.Term {
				rf.mu.Unlock()
				return
			}
			if reply.Term > rf.currentTerm {
				rf.beFollower(reply.Term)
				rf.mu.Unlock()
				return
			}

			if reply.Success {
				rf.matchIndex[idx] = args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[idx] = rf.matchIndex[idx] + 1
				rf.updateCommitIndex()
				rf.mu.Unlock()
			} else {
				tarIndex := reply.ConflictIndex
				if reply.ConflictTerm != NULL {
					logSize := rf.logLen()
					for i := rf.lastIncludedIndex; i < logSize; i++ {
						if rf.getLog(i).Term != reply.ConflictTerm { continue }
						for i < logSize && rf.getLog(i).Term == reply.ConflictTerm {i++ }
						tarIndex = i
					}
				}
				rf.nextIndex[idx] = tarIndex
				rf.mu.Unlock()
			}
		}(i)
	}
}

func (rf *Raft) sendSnapShot(server int) {
	args := InstallSnapShotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}

	rf.mu.Unlock()
	reply := InstallSnapShotReply{}
	ret := rf.sendInstallSnapShot(server, &args, &reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ret || rf.state != Leader || rf.currentTerm != args.Term {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.beFollower(reply.Term)
		return
	}

	rf.updateNextMatchIdx(server, rf.lastIncludedIndex)
}

func (rf *Raft) updateNextMatchIdx(server int, matchIdx int) {
	rf.matchIndex[server] = matchIdx
	rf.nextIndex[server] = matchIdx + 1
	rf.updateCommitIndex()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index = -1
	term = rf.currentTerm
	isLeader = rf.state == Leader

	if isLeader {
		index = rf.getLastLogIndex() + 1
		newLog := Log{
			rf.currentTerm,
			command,
		}
		rf.log = append(rf.log, newLog)
		rf.persist()
		rf.startAppendLog()
	}
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {

	// Your code here, if desired.
	send(rf.killCh)
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
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = NULL
	rf.log = make([]Log, 1)

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.applyCh = applyCh
	rf.voteCh = make(chan bool, 1)
	rf.appendLogCh = make(chan bool, 1)
	rf.killCh = make(chan bool, 1)

	rf.readPersist(persister.ReadRaftState())

	heartBeatTime := time.Duration(100) * time.Millisecond

	go func() {
		for {
			select {
			case <-rf.killCh:
				return
			default:
			}
			electionTime := time.Duration(rand.Intn(100) + 300) * time.Millisecond

			rf.mu.Lock()
			state := rf.state
			rf.mu.Unlock()

			switch state {
			case Follower, Candidate:
				select {
				case <- rf.voteCh:
				case <- rf.appendLogCh:
				case <- time.After(electionTime):
					rf.mu.Lock()
					rf.beCandidate()
					rf.mu.Unlock()
				}
			case Leader:
				rf.startAppendLog()
				time.Sleep(heartBeatTime)
			}
		}
	}()

	return rf
}

func (rf *Raft) getLog(i int) Log {
	return rf.log[i - rf.lastIncludedIndex]
}

func (rf *Raft) getLogAfter(i int) []Log {
	return rf.log[(i - rf.lastIncludedIndex):]
}

func (rf *Raft) getLogBefore(i int) []Log {
	return rf.log[:(i - rf.lastIncludedIndex)]
}

func (rf *Raft) logLen() int {
	return len(rf.log) + rf.lastIncludedIndex
}

func (rf *Raft) getLastLogIndex() int {
	return rf.logLen() - 1
}

func (rf * Raft) getLastLogTerm() int {
	idx := rf.getLastLogIndex()
	if idx < rf.lastIncludedIndex { return -1 }
	return rf.getLog(idx).Term
}

func (rf *Raft) getPrevLogIdx(i int) int {
	return rf.nextIndex[i] - 1
}

func (rf *Raft) getPrevLogTerm(i int) int {
	prevLogIdx := rf.getPrevLogIdx(i)
	if prevLogIdx < rf.lastIncludedIndex {
		return -1
	}
	return rf.getLog(prevLogIdx).Term
}

func (rf *Raft) beCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me // vote myself
	rf.persist()
	go rf.startElection()
}

func (rf *Raft) beFollower(term int) {
	rf.state = Follower
	rf.votedFor = NULL
	rf.currentTerm = term
	rf.persist()
}

func (rf *Raft) beLeader() {
	if rf.state != Candidate {
		return
	}
	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
	}
}


func send(ch chan bool) {
	select {
	case <- ch:
	default:
	}
	ch <- true
}

func (rf *Raft) startElection() {

	rf.mu.Lock()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	rf.mu.Unlock()

	var votes int32 = 1

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(idx int) {
			reply := &RequestVoteReply{}
			ret := rf.sendRequestVote(idx, &args, reply)
			if ret {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					rf.beFollower(reply.Term)
					send(rf.voteCh)
					return
				}
				if rf.state != Candidate || rf.currentTerm != args.Term { return }
				if reply.VoteGranted {
					atomic.AddInt32(&votes, 1)
				}

				if atomic.LoadInt32(&votes) > int32(len(rf.peers) / 2) {
					rf.beLeader()
					send(rf.voteCh) //在成为Leader之后立即发送心跳信息
				}
			}
		}(i)
	}
}


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
	"labrpc"
	"log"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "labgob"

const (
	LEADER    = int32(0)
	FOLLOWER  = int32(1)
	CANDIDATE = int32(2)
)

var (
	// debugLock = false
	debugLock = true
	// mu        = &sync.Mutex{}
	heartBeatInterval = 80 * time.Millisecond
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)
}

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

type Entry struct {
	Command string
	Term    int
	Index   int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	CurrentTerm int
	VotedFor    int
	Log         []Entry

	CommitIndex int
	LastApplied int

	NextIndex  []int
	MatchIndex []int

	Role             int32
	ElectionTimeout  time.Duration
	VoteCount        int
	Voters           []int
	ValidRpcReceived bool
	ApplyCH          chan ApplyMsg

	Timeout         *time.Timer
	TimeoutTicker   *time.Ticker
	HeartBeat       *time.Timer
	HeartBeatTicker *time.Ticker

	ElectionSuccess chan bool
}

func (rf *Raft) Lock() {
	if debugLock {
		_, _, line, _ := runtime.Caller(1)
		log.Printf("%v acquire lock at line %d", rf, line)
	}
	rf.mu.Lock()
	if debugLock {
		_, _, line, _ := runtime.Caller(1)
		log.Printf("%v acquire lock success at line %d", rf, line)
	}
}

func (rf *Raft) Unlock() {
	if debugLock {
		_, _, line, _ := runtime.Caller(1)
		log.Printf("%v release lock at line %d", rf, line)
	}
	rf.mu.Unlock()
	if debugLock {
		_, _, line, _ := runtime.Caller(1)
		log.Printf("%v release lock success at line %d", rf, line)
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.Lock()
	defer rf.Unlock()

	term = rf.CurrentTerm
	if rf.Role == LEADER {
		isleader = true
	}

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
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (arg *RequestVoteArgs) String() string {
	s := StructToString(arg)
	return s
}

func (arg *RequestVoteReply) String() string {
	s := StructToString(arg)
	return s
}

func (arg *AppendEntriesArgs) String() string {
	s := StructToString(arg)
	return s
}

func (arg *AppendEntriesReply) String() string {
	s := StructToString(arg)
	return s
}

// func (arg *AppendEntriesArgs) String() string {
// 	s := StructToString(arg)
// 	return s
// }

// func (arg *AppendEntriesReply) String() string {
// 	s := StructToString(arg)
// 	return s
// }

func (rf *Raft) String() string {
	return fmt.Sprintf("raft instance %d", rf.me)
}

func (rf *Raft) GetRole() int32 {
	return atomic.LoadInt32(&rf.Role)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.Lock()
	defer rf.Unlock()

	log.Printf("%v receive %v, rf attr: %v", rf, args, StructToString(rf))
	// rf.ValidRpcReceived = true
	if args.Term < rf.CurrentTerm {
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.CurrentTerm {
		reply.VoteGranted = true
		reply.Term = rf.CurrentTerm
		return
	}

	if rf.VotedFor == -1 || rf.VotedFor == args.CandidateID {
		reply.VoteGranted = true
		rf.VotedFor = args.CandidateID
		return
	}

	reply.VoteGranted = false
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
	log.Printf("%v send %v to [%v]", rf, args, server)

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	// log
	LogRPC(rf.me, server, RPCKindRequestVote, args, reply)

	log.Printf("%v receive reply: %v", rf, reply)

	rf.Lock()
	defer rf.Unlock()

	if reply.Term > rf.CurrentTerm {
		rf.CurrentTerm = reply.Term
		rf.ConvertToFollower()
	}

	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.Lock()
	defer rf.Unlock()
	log.Printf("%v receive %v, rf attr: %v", rf, args, StructToString(rf))

	if args.Term < rf.CurrentTerm {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		return
	}

	rf.CurrentTerm = args.Term

	rf.ValidRpcReceived = true
	rf.VotedFor = -1
	rf.Voters = rf.Voters[:0]

	rf.ResetTimeout("append entry")

	rf.ConvertToFollower()

	reply.Success = true
	reply.Term = rf.CurrentTerm

	if len(args.Entries) < 1 {
		reply.Success = true
		return
	}

	for _, e := range args.Entries {
		rf.CommitIndex++
		rf.Log = append(rf.Log, e)
		c, err := strconv.Atoi(e.Command)
		if err != nil {
			panic(err)
		}
		am := ApplyMsg{
			CommandValid: true,
			Command:      c,
			CommandIndex: rf.CommitIndex,
		}
		rf.ApplyCH <- am
	}

	log.Printf("%v append entries finished, attr: %v", rf, StructToString(rf))
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if atomic.LoadInt32(&rf.Role) != LEADER {
		return false
	}

	rf.Lock()
	if rf.MatchIndex[server] < rf.CommitIndex {
		startIndex := rf.NextIndex[server]
		endIndex := rf.CommitIndex + 1
		log.Printf("peer: %v, start index: %v, end index: %v, match index: %v, next index: %v", server, startIndex, endIndex, rf.MatchIndex, rf.NextIndex)
		args.Entries = append(rf.Log[startIndex:endIndex], args.Entries...)
	}
	log.Printf("%v send %v to [%v], rf attr: %v", rf, args, server, StructToString(rf))
	rf.Unlock()

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.Lock()
	defer rf.Unlock()

	// log
	if len(args.Entries) < 1 {
		LogRPC(rf.me, server, RPCKindHeartbeat, args, reply)
	} else {
		LogRPC(rf.me, server, RPCKindAppendEntry, args, reply)
	}

	log.Printf("[ok: %v]%v get return value: %v", ok, rf, reply)
	if reply.Term > rf.CurrentTerm {
		rf.CurrentTerm = reply.Term
		rf.ConvertToFollower()
	}
	if !ok {
		return ok
	}

	if len(args.Entries) < 1 {
		return ok
	}

	rf.MatchIndex[server]++
	rf.NextIndex[server] += len(args.Entries)
	log.Printf("match index: %v", rf.MatchIndex)
	log.Printf("next index: %v", rf.NextIndex)
	log.Printf("rf attr: %v", StructToString(rf))

	if getMaxCommited(rf.MatchIndex) > rf.CommitIndex {
		// commit success
		for _, e := range args.Entries {
			rf.CommitIndex++
			if rf.CommitIndex > rf.LastApplied {
				rf.LastApplied++
			}
			rf.Log = append(rf.Log, e)
			c, err := strconv.Atoi(e.Command)
			if err != nil {
				panic(err)
			}
			am := ApplyMsg{
				CommandValid: true,
				Command:      c,
				CommandIndex: rf.CommitIndex,
			}
			rf.ApplyCH <- am
		}
	}

	return ok
}

func (rf *Raft) ConvertToLeader() {
	LogRoleChange(rf.me, RoleMap[rf.Role], RoleLeader)

	log.Printf("%v grant %d votes from %v", rf, len(rf.Voters), rf.Voters)
	atomic.StoreInt32(&rf.Role, LEADER)
}

func (rf *Raft) ConvertToCandidate() {
	LogRoleChange(rf.me, RoleMap[rf.Role], RoleCandidate)

	atomic.StoreInt32(&rf.Role, CANDIDATE)

	log.Printf("%v convert to candidate, attr: %v", rf, StructToString(rf))
}

func (rf *Raft) ConvertToFollower() {
	// rf.Lock()
	// defer rf.Unlock()

	LogRoleChange(rf.me, RoleMap[rf.Role], RoleFollower)

	atomic.StoreInt32(&rf.Role, FOLLOWER)

	log.Printf("%v convert to follower, attr: %v", rf, StructToString(rf))
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	_, b := rf.GetState()

	rf.Lock()
	defer rf.Unlock()

	if !b {
		return index, term, b
	}

	isLeader = true
	index = rf.CommitIndex + 1
	term = rf.CurrentTerm

	lastIndex := rf.CommitIndex
	lastTerm := rf.Log[lastIndex].Term
	entry := Entry{
		Command: fmt.Sprint(command),
		Term:    rf.CurrentTerm,
		Index:   lastIndex + 1,
	}
	log.Printf("start a commit, entry: %v", entry)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		args := AppendEntriesArgs{
			Term:         rf.CurrentTerm,
			LeaderID:     rf.me,
			PrevLogIndex: lastIndex,
			PrevLogTerm:  lastTerm,
			Entries:      []Entry{entry},
			LeaderCommit: lastIndex + 1,
		}
		go rf.sendAppendEntries(i, &args, &AppendEntriesReply{})
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
}

func (rf *Raft) NewElection() {
	rf.Lock()
	rf.Voters = []int{rf.me}
	rf.VotedFor = rf.me
	rf.CurrentTerm++

	args := RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateID:  rf.me,
		LastLogIndex: rf.CommitIndex,
	}
	rf.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			reply := RequestVoteReply{}
			rf.sendRequestVote(i, &args, &reply)

			if reply.VoteGranted {
				rf.Lock()
				log.Printf("%v grant %d votes from %v", rf, len(rf.Voters), rf.Voters)
				rf.Voters = append(rf.Voters, i)

				electionSuccess := false
				if atomic.LoadInt32(&rf.Role) != LEADER {
					if len(rf.Voters) > len(rf.peers)/2 {
						electionSuccess = true
						LogRoleChange(rf.me, RoleMap[rf.Role], RoleLeader)
						atomic.StoreInt32(&rf.Role, LEADER)
					}
				}
				rf.Unlock()

				if electionSuccess {
					go rf.SendHeartBeat()
					rf.ElectionSuccess <- true
				}
			}
		}(i)
	}
}

func (rf *Raft) ResetTimeout(reason string) {
	log.Printf("%v reset timeout, reason: %v", rf, reason)
	rf.Timeout.Stop()
	select {
	case <-rf.Timeout.C:
	default:
	}
	rf.Timeout.Reset(rf.ElectionTimeout)
	// rf.Timeout = time.NewTimer(rf.ElectionTimeout)
}

func (rf *Raft) Loop() {
	loopIndex := 0
	for {
		loopIndex++
		log.Printf("loop %v, %v start loop, role: %s", loopIndex, rf, RoleMap[atomic.LoadInt32(&rf.Role)])
		switch atomic.LoadInt32(&rf.Role) {
		case LEADER:
			// t := <-rf.HeartBeatTicker.C
			// log.Printf("loop %v, %v heart beat tick at %v", loopIndex, rf, t)
			time.Sleep(heartBeatInterval)
			rf.SendHeartBeat()
		case FOLLOWER:
			rf.Lock()
			rf.ResetTimeout(fmt.Sprintf("main loop %d", loopIndex))
			rf.Unlock()
			t := <-rf.Timeout.C
			log.Printf("loop %v, %v election timeout at %v", loopIndex, rf, t)
			rf.Lock()
			rf.ConvertToCandidate()
			log.Printf("loop %v, %v convert to candidate", loopIndex, rf)
			rf.Unlock()
		case CANDIDATE:
			rf.NewElection()

			// rf.Lock()
			// timeout := rf.ElectionTimeout
			// rf.Unlock()
			// time.Sleep(timeout)

			rf.Lock()
			rf.ResetTimeout(fmt.Sprintf("main loop %d", loopIndex))
			rf.Unlock()
			select {
			case <-rf.ElectionSuccess:
			case <-rf.Timeout.C:
			}
		}
	}
}

func (rf *Raft) SendHeartBeat() {
	rf.Lock()
	peers := rf.peers
	me := rf.me
	currentTerm := rf.CurrentTerm
	rf.Unlock()

	for i := range peers {
		if i == me {
			continue
		}

		args := AppendEntriesArgs{
			Term:     currentTerm,
			LeaderID: me,
		}

		// rf.Lock()
		// Add backlogged logs
		// server := i
		// if rf.MatchIndex[server] < rf.CommitIndex {
		// 	startIndex := rf.NextIndex[server]
		// 	endIndex := rf.CommitIndex + 1
		// 	log.Printf("peer: %v, start index: %v, end index: %v, match index: %v, next index: %v", i, startIndex, endIndex, rf.MatchIndex, rf.NextIndex)
		// 	args.Entries = append(rf.Log[startIndex:endIndex], args.Entries...)
		// }
		// rf.Unlock()

		go rf.sendAppendEntries(i, &args, &AppendEntriesReply{})
	}
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

	rf.Log = []Entry{Entry{}}
	rf.CurrentTerm = 0
	rf.ElectionTimeout = time.Duration(RandomInt(150, 300)) * time.Millisecond
	rf.Role = FOLLOWER
	rf.VotedFor = -1
	for range peers {
		rf.NextIndex = append(rf.NextIndex, 1)
		rf.MatchIndex = append(rf.MatchIndex, 0)
	}
	rf.ApplyCH = applyCh

	rf.Timeout = time.NewTimer(rf.ElectionTimeout)
	rf.TimeoutTicker = time.NewTicker(rf.ElectionTimeout)
	rf.HeartBeat = time.NewTimer(heartBeatInterval)
	rf.HeartBeatTicker = time.NewTicker(heartBeatInterval)

	rf.ElectionSuccess = make(chan bool)

	log.Printf("%v init", rf)
	log.Printf("instance attr: %v", StructToString(rf))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// log
	LogRunnerStart(rf.me)

	go rf.Loop()
	// go rf.LeaderAppendEntries()

	return rf
}

func getMaxCommited(a []int) int {
	l := make([]int, len(a))
	copy(l, a)
	sort.Ints(l)
	mid := len(l)/2 + 1
	log.Printf("mid: %v, v: %v", mid, l[mid])
	return l[mid]
}

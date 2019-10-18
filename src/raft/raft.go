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
	// "math"
	// "github.com/huandu/goroutine"
	// goroutine "github.com/huandu/go-tls"
	// "github.com/XiaochenCui/goroutine"
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

	round uint32
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

	Role            int32
	ElectionTimeout time.Duration
	VoteCount       int
	Voters          []int

	// A server remains in follower state as long as it receives valid RPCs from a leader or candidate
	ValidRpcReceived chan bool

	ApplyCH chan ApplyMsg

	Timeout         *time.Timer
	TimeoutTicker   *time.Ticker
	HeartBeat       *time.Timer
	HeartBeatTicker *time.Ticker

	ElectionSuccess chan bool
	RoleChanged     chan bool
	// ValidAppendEntryReceived chan bool
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

	// optimized to reduce the number of rejected AppendEntries RPCs
	ConflictTerm  int
	ConflictIndex int
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
		reply.Term = rf.CurrentTerm
		return
	}

	if args.Term > rf.CurrentTerm {
		rf.ConvertToFollower()
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		reply.Term = rf.CurrentTerm
	}

	if rf.VotedFor == -1 || rf.VotedFor == args.CandidateID {
		lastLog := rf.Log[rf.CommitIndex]
		log.Printf("%v last log: %v", rf, StructToString(lastLog))
		if args.LastLogTerm >= lastLog.Term && args.LastLogIndex >= lastLog.Index {
			// rf.ValidRpcReceived <- true
			unblockSend(rf.ValidRpcReceived)

			rf.ConvertToFollower()
			reply.VoteGranted = true
			rf.VotedFor = args.CandidateID
		}
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
	log.Printf("%v send RequestVote to [%v], args: %v", rf, server, args)

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	// log
	if ok {
		LogRPC(rf.me, server, RPCKindRequestVote, args, reply)
	}

	log.Printf("[ok: %v]%v get RequestVote from %v, reply: %v", ok, rf, server, reply)

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

	reply.Term = rf.CurrentTerm
	reply.Success = false

	// Reply false if term < currentTerm
	if args.Term < rf.CurrentTerm {
		return
	}

	rf.CurrentTerm = args.Term

	unblockSend(rf.ValidRpcReceived)

	rf.VotedFor = -1
	rf.Voters = rf.Voters[:0]
	rf.ConvertToFollower()

	prevLog := rf.Log[len(rf.Log)-1]
	if args.PrevLogTerm < prevLog.Term {
		redundantIndex := 1
		for i := prevLog.Index; i >= 1; i-- {
			if rf.Log[i].Term <= args.PrevLogTerm {
				redundantIndex = i + 1
				break
			}
		}
		rf.Log = rf.Log[:redundantIndex]
		rf.CommitIndex = redundantIndex - 1
	}

	// update prevLog
	prevLog = rf.Log[len(rf.Log)-1]
	if args.PrevLogIndex != prevLog.Index || args.PrevLogTerm != prevLog.Term {
		// The term of the conflicting entry
		reply.ConflictTerm = prevLog.Term
		// The first index it stores for the conflict term
		for i := 1; i < len(rf.Log); i++ {
			entry := rf.Log[i]
			if entry.Term == prevLog.Term {
				reply.ConflictIndex = entry.Index
				break
			}
		}

		return
	}

	// if len(rf.Log) < args.PrevLogIndex+1 {
	// 	return
	// }

	// prevLog := rf.Log[args.PrevLogIndex]
	// if prevLog.Term != args.PrevLogTerm {
	// 	reply.ConflictTerm = prevLog.Term
	// 	for i := 1; i < len(rf.Log); i++ {
	// 		entry := rf.Log[i]
	// 		if entry.Term == reply.ConflictTerm {
	// 			reply.ConflictIndex = i
	// 			break
	// 		}
	// 	}
	// 	return
	// }

	reply.Success = true
	reply.Term = rf.CurrentTerm

	// update commit index
	rf.commitEntries(args.Entries)
	// if rf.CommitIndex < args.LeaderCommit {
	// 	rf.CommitIndex = args.LeaderCommit
	// }

	// for _, e := range args.Entries {
	// 	if len(rf.Log)-1 < e.Index {
	// 		rf.Log = append(rf.Log, e)

	// 		// rf.CommitIndex++
	// 		rf.UpdateCommitIndex(rf.CommitIndex + 1)
	// 		continue
	// 	}

	// 	rf.Log[e.Index] = e
	// }

	applyTarget := rf.CommitIndex
	if rf.CommitIndex > args.LeaderCommit {
		applyTarget = args.LeaderCommit
	}

	if rf.LastApplied < applyTarget {
		log.Printf("%v going to apply logs, current: %v, aim: %v", rf, rf.LastApplied, applyTarget)
		go rf.applyEntries(rf.LastApplied+1, applyTarget)
	}

	// for rf.LastApplied < applyTarget {
	// 	i := rf.LastApplied + 1
	// 	e := rf.Log[i]
	// 	c, err := strconv.Atoi(e.Command)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	am := ApplyMsg{
	// 		CommandValid: true,
	// 		Command:      c,
	// 		CommandIndex: e.Index,
	// 	}
	// 	log.Printf("%v apply entry : %v", rf, StructToString(am))
	// 	rf.ApplyCH <- am

	// 	rf.LastApplied++
	// }

	if len(args.Entries) > 0 {
		log.Printf("%v append entries finished, attr: %v", rf, StructToString(rf))
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if atomic.LoadInt32(&rf.Role) != LEADER {
		return false
	}

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.Lock()
	defer rf.Unlock()

	// log
	if ok {
		if len(args.Entries) < 1 {
			LogRPC(rf.me, server, RPCKindHeartbeat, args, reply)
		} else {
			LogRPC(rf.me, server, RPCKindAppendEntry, args, reply)
		}
	}

	log.Printf("[ok: %v]%v get AppendEntry from %v, reply: %v", ok, rf, server, reply)
	if reply.Term > rf.CurrentTerm {
		rf.CurrentTerm = reply.Term
		rf.ConvertToFollower()
		// rf.RoleChanged <- true
		unblockSend(rf.RoleChanged)
	}
	if !ok {
		return ok
	}

	if !reply.Success {
		log.Printf("[%v -> %v] conflict term: %v, conflict index: %v", rf.me, server, reply.ConflictTerm, reply.ConflictIndex)

		// conflict entry found, degrade next index to resend some previous entries
		if len(rf.Log)-1 >= reply.ConflictIndex {
			conflictEntry := rf.Log[reply.ConflictIndex]
			if conflictEntry.Term == reply.ConflictTerm {
				rf.NextIndex[server] = reply.ConflictIndex
			} else {
				for i := reply.ConflictIndex; i >= 1; i-- {
					if rf.Log[i].Term <= reply.ConflictTerm {
						rf.NextIndex[server] = i + 1
					}
				}
			}
		}

		// prevent the NextIndex from degrade to less than 1
		if rf.NextIndex[server] < 1 {
			rf.NextIndex[server] = 1
		}

		return ok
	}

	if len(args.Entries) < 1 {
		return ok
	}

	lastLog := args.Entries[len(args.Entries)-1]
	rf.MatchIndex[server] = lastLog.Index
	rf.NextIndex[server] = lastLog.Index + 1
	log.Printf("match index: %v", rf.MatchIndex)
	log.Printf("next index: %v", rf.NextIndex)
	log.Printf("rf attr: %v", StructToString(rf))

	originCommitIndex := rf.CommitIndex
	rf.CommitIndex = getMaxCommited(rf.MatchIndex)
	if rf.CommitIndex > rf.LastApplied {
		// commit success
		// rf.CommitIndex = rf.MatchIndex
		go rf.applyEntries(rf.LastApplied+1, rf.CommitIndex)

		if originCommitIndex < rf.CommitIndex {
			indexes := []int{originCommitIndex, rf.CommitIndex}
			s := indexesToStr(indexes)
			LogEventNew(Event{
				ID:      rf.me,
				Name:    "commit index update",
				Content: s,
			})
		}

		// for _, e := range args.Entries {
		// 	// rf.CommitIndex++
		// 	rf.UpdateCommitIndex(rf.CommitIndex + 1)
		// 	if rf.CommitIndex > rf.LastApplied {
		// 		rf.LastApplied++
		// 	}
		// 	rf.Log = append(rf.Log, e)
		// 	c, err := strconv.Atoi(e.Command)
		// 	if err != nil {
		// 		panic(err)
		// 	}
		// 	am := ApplyMsg{
		// 		CommandValid: true,
		// 		Command:      c,
		// 		CommandIndex: rf.CommitIndex,
		// 	}
		// 	rf.ApplyCH <- am
		// }
	}

	return ok
}

func (rf *Raft) commitEntries(es []Entry) {
	if len(es) == 0 {
		return
	}

	indexes := []int{rf.CommitIndex}

	for _, e := range es {
		if len(rf.Log)-1 < e.Index {
			rf.Log = append(rf.Log, e)

			// rf.CommitIndex++
			// rf.UpdateCommitIndex(e.Index)
			rf.CommitIndex = e.Index
			indexes = append(indexes, e.Index)
			continue
		}

		rf.Log[e.Index] = e
	}

	log.Print(indexes)
	// panic(nil)
	s := indexesToStr(indexes)

	LogEventNew(Event{
		ID:      rf.me,
		Name:    "commit index update",
		Content: s,
	})
}

func indexesToStr(indexes []int) string {
	result := []int{}
	for i, index := range indexes {
		if i == 0 || i == len(indexes)-1 {
			result = append(result, index)
			continue
		}
		if index*2 == indexes[i-1]+indexes[i+1] {
			continue
		}
		result = append(result, index)
	}

	s := ""
	for _, n := range result {
		s += fmt.Sprintf("%d...", n)
	}
	return s[:len(s)-3]
}

func (rf *Raft) applyEntries(start, end int) {
	rf.Lock()
	defer rf.Unlock()

	// logs := rf.Log
	// current := rf.LastApplied
	// rf.Unlock()

	if rf.LastApplied >= end {
		return
	}

	LogApply(rf.me, start, end)

	for i := start; i <= end; i++ {
		e := rf.Log[i]
		c, err := strconv.Atoi(e.Command)
		if err != nil {
			panic(err)
		}

		am := ApplyMsg{
			CommandValid: true,
			Command:      c,
			CommandIndex: e.Index,
		}
		log.Printf("%v apply entry : %v", rf, StructToString(am))
		rf.ApplyCH <- am
		rf.LastApplied++
	}

	// rf.Lock()
	// rf.LastApplied += end - start + 1
	// rf.Unlock()
}

// func (rf *Raft) UpdateCommitIndex(i int) {
// 	// LogAttrChange(rf.me, "CommitIndex", rf.CommitIndex, i)
// 	rf.CommitIndex = i
// }

func (rf *Raft) ConvertToLeader() {
	LogRoleChange(rf.me, RoleMap[rf.Role], RoleLeader)

	log.Printf("%v become leader, grant %d votes from %v", rf, len(rf.Voters), rf.Voters)
	atomic.StoreInt32(&rf.Role, LEADER)
	rf.VotedFor = -1

	for i := range rf.peers {
		rf.NextIndex[i] = rf.CommitIndex + 1
		rf.MatchIndex[i] = 0
	}

	// init ticker
	rf.HeartBeatTicker = time.NewTicker(heartBeatInterval)
}

func (rf *Raft) ConvertToCandidate() {
	LogRoleChange(rf.me, RoleMap[rf.Role], RoleCandidate)

	atomic.StoreInt32(&rf.Role, CANDIDATE)

	log.Printf("%v convert to candidate, attr: %v", rf, StructToString(rf))
}

func (rf *Raft) ConvertToFollower() {
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

	if !b {
		return index, term, b
	}

	rf.Lock()
	defer rf.Unlock()
	// defer rf.Unlock()

	isLeader = true
	index = len(rf.Log)
	term = rf.CurrentTerm

	// lastIndex := rf.CommitIndex
	// lastTerm := rf.Log[lastIndex].Term
	log.Printf("start a commit, command: %v, term: %v, index: %v", command, rf.CurrentTerm, index)

	entry := Entry{
		Command: fmt.Sprint(command),
		Term:    term,
		Index:   index,
	}
	rf.Log = append(rf.Log, entry)

	// rf.commitEntries([]Entry{entry})
	// rf.Unlock()

	// for i := range peers {
	// 	entry := Entry{
	// 		Command: fmt.Sprint(command),
	// 		Term:    term,
	// 		Index:   lastIndex + 1,
	// 	}

	// 	if i == me {
	// 		continue
	// 	}
	// 	args := AppendEntriesArgs{
	// 		Term:         term,
	// 		LeaderID:     me,
	// 		PrevLogIndex: lastIndex,
	// 		PrevLogTerm:  lastTerm,
	// 		Entries:      []Entry{entry},
	// 		LeaderCommit: lastIndex + 1,
	// 	}
	// 	go rf.sendAppendEntries(i, &args, &AppendEntriesReply{})
	// }

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

	if rf.Role != CANDIDATE {
		rf.Unlock()
		return
	}

	LogTermUp(rf.me, rf.CurrentTerm, rf.CurrentTerm+1)

	rf.Voters = []int{rf.me}
	rf.VotedFor = rf.me
	rf.CurrentTerm++

	lastLog := rf.Log[rf.CommitIndex]
	log.Printf("%v last log: %v", rf, StructToString(lastLog))

	args := RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateID:  rf.me,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
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
						// LogRoleChange(rf.me, RoleMap[rf.Role], RoleLeader)
						// atomic.StoreInt32(&rf.Role, LEADER)
						rf.ConvertToLeader()
					}
				}
				rf.Unlock()

				if electionSuccess {
					go rf.startAppendEntries()
					// rf.ElectionSuccess <- true
					unblockSend(rf.ElectionSuccess)
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

func (rf *Raft) loop() {
	loopIndex := 0
	for {
		loopIndex++
		log.Printf("loop %v, %v start loop, role: %s", loopIndex, rf, RoleMap[atomic.LoadInt32(&rf.Role)])
		switch atomic.LoadInt32(&rf.Role) {
		case LEADER:
			select {
			// case <-time.After(heartBeatInterval):
			// 	rf.startAppendEntries()
			// case <-rf.RoleChanged:
			// 	continue
			// case <-rf.ValidRpcReceived:
			// 	continue

			case <-rf.HeartBeatTicker.C:
				rf.startAppendEntries()
			case <-rf.RoleChanged:
				rf.HeartBeatTicker.Stop()
				// continue
			case <-rf.ValidRpcReceived:
				rf.HeartBeatTicker.Stop()
				// continue
			}
		case FOLLOWER:
			// rf.Lock()
			// rf.ResetTimeout(fmt.Sprintf("main loop %d", loopIndex))
			// rf.Unlock()

			select {
			case <-time.After(rf.ElectionTimeout):
				rf.Lock()
				select {
				case <-rf.ValidRpcReceived:
					log.Printf("%v receive valid rpc, keep follow", rf)
					rf.Unlock()
					continue
				default:
				}
				rf.ConvertToCandidate()
				log.Printf("loop %v, %v convert to candidate", loopIndex, rf)
				rf.Unlock()
			case <-rf.ValidRpcReceived:
				rf.Lock()
				log.Printf("%v receive valid rpc, keep follow", rf)
				rf.Unlock()
				// continue
			case <-rf.RoleChanged:
			}

		case CANDIDATE:
			rf.NewElection()

			// rf.Lock()
			// rf.ResetTimeout(fmt.Sprintf("main loop %d", loopIndex))
			// rf.Unlock()
			select {
			case <-rf.ElectionSuccess:
			case <-rf.ValidRpcReceived:
			case <-time.After(rf.ElectionTimeout):
			}
		}
	}
}

func (rf *Raft) startAppendEntries() {
	rf.Lock()
	log.Printf("%v start a AppendEntry round", rf)
	peers := rf.peers
	me := rf.me
	rf.Unlock()

	for i := range peers {
		if i == me {
			continue
		}

		rf.Lock()
		// init args
		args := AppendEntriesArgs{
			Term:         rf.CurrentTerm,
			LeaderID:     rf.me,
			LeaderCommit: rf.CommitIndex,
		}

		// get correspond next index
		log.Printf("%v debug next index %v", rf, rf.NextIndex)
		nextIndex := rf.NextIndex[i]

		// set previous logs' index and term
		prevLog := rf.Log[nextIndex-1]
		args.PrevLogIndex = prevLog.Index
		args.PrevLogTerm = prevLog.Term

		// fill up entries
		args.Entries = append(args.Entries, rf.Log[nextIndex:]...)

		// emit
		log.Printf("%v send AppendEntry to %v, args: %v, rf attr: %v", rf, i, StructToString(args), StructToString(rf))
		rf.Unlock()

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
	rf.CurrentTerm = 1
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
	// rf.HeartBeatTicker = time.NewTicker(heartBeatInterval)

	rf.ElectionSuccess = make(chan bool, 1)
	rf.RoleChanged = make(chan bool, 1)
	rf.ValidRpcReceived = make(chan bool, 1)

	log.Printf("%v init", rf)
	log.Printf("instance attr: %v", StructToString(rf))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// log
	LogRunnerStart(rf.me)

	go rf.loop()
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

func unblockSend(c chan bool) {
	select {
	case c <- true:
	default:
	}
}
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
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

var (
	debugLock = true
	// mu        = &sync.Mutex{}
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
	Log         []interface{}

	CommitIndex int
	LastApplied int

	NextIndex  []int
	MatchIndex []int

	Role             string // leader, follower, candidate
	ElectionTimeout  time.Duration
	VoteCount        int
	Voters           []int
	ValidRpcReceived bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	if debugLock {
		log.Printf("%v going to get lock", rf)
	}
	rf.mu.Lock()
	term = rf.CurrentTerm
	if rf.Role == "leader" {
		isleader = true
	}
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
	Entries      []interface{}
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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	if debugLock {
		log.Printf("%v going to get lock", rf)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("%v receive %v, rf attr: %v", rf, args, StructToString(rf))
	// rf.ValidRpcReceived = true
	if args.Term < rf.CurrentTerm {
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.CurrentTerm {
		reply.VoteGranted = true
		rf.VotedFor = args.CandidateID
		return
	}

	if rf.VotedFor == -1 || rf.VotedFor == args.CandidateID {
		reply.VoteGranted = true
		rf.VotedFor = args.CandidateID
		return
	}
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

	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if debugLock {
		log.Printf("%v going to get lock", rf)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Printf("%v receive %v, rf attr: %v", rf, args, StructToString(rf))

	if args.Term < rf.CurrentTerm {
		reply.Success = false
		return
	}

	rf.CurrentTerm = args.Term

	reply.Success = true
	reply.Term = rf.CurrentTerm

	rf.ValidRpcReceived = true

	rf.VotedFor = -1
	rf.VoteCount = 0

	rf.ConvertToFollower()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	log.Printf("%v send %v to [%v]", rf, args, server)

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if debugLock {
		log.Printf("%v going to get lock", rf)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// log
	LogRPC(rf.me, server, RPCKindHeartbeat, args, reply)

	log.Printf("%v get return value: %v", rf, reply)
	if !ok {
		if reply.Term > rf.CurrentTerm {
			rf.CurrentTerm = reply.Term
		}
	}

	return ok
}

func (rf *Raft) ConvertToLeader() {
	if debugLock {
		log.Printf("%v going to get lock", rf)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// log
	LogRoleChange(rf.me, rf.Role, "leader")

	log.Printf("%v grant %v votes, candidate routine finished", rf, rf.VoteCount)
	log.Printf("voters: %v", rf.Voters)
	rf.Role = RoleLeader
}

func (rf *Raft) ConvertToCandidate() {
	if debugLock {
		log.Printf("%v going to get lock", rf)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// log
	LogRoleChange(rf.me, rf.Role, RoleCandidate)
	rf.Role = RoleCandidate

	rf.CurrentTerm++
	rf.VoteCount = 0

	log.Printf("%v convert to candidate, attr: %v", rf, StructToString(rf))
}

func (rf *Raft) ConvertToFollower() {
	if debugLock {
		log.Printf("%v going to get lock", rf)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// log
	LogRoleChange(rf.me, rf.Role, RoleFollower)
	rf.Role = RoleFollower
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

func (rf *Raft) StartElection() {
	// Check if candidating should go on
	if debugLock {
		log.Printf("%v going to get lock", rf)
	}
	rf.mu.Lock()

	if rf.Role == "leader" {
		rf.mu.Unlock()
		return
	}

	rf.mu.Unlock()

	rf.ConvertToCandidate()

	args := RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateID:  rf.me,
		LastLogIndex: rf.CommitIndex,
	}
	for i := range rf.peers {
		if i == rf.me {
			if debugLock {
				log.Printf("%v going to get lock", rf)
			}
			rf.mu.Lock()
			rf.VoteCount++
			rf.VotedFor = i
			rf.mu.Unlock()
			continue
		}
		go func(i int) {
			reply := RequestVoteReply{}
			rf.sendRequestVote(i, &args, &reply)

			if debugLock {
				log.Printf("%v going to get lock", rf)
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.VoteGranted {
				rf.VoteCount++
				rf.Voters = append(rf.Voters, i)

				if rf.VoteCount > len(rf.peers)/2 {
					rf.ConvertToLeader()
					return
				}
			}
		}(i)
	}
}

func (rf *Raft) Loop() {
	for {
		if debugLock {
			log.Printf("%v going to get lock", rf)
		}
		rf.mu.Lock()
		rf.ValidRpcReceived = false
		rf.mu.Unlock()

		time.Sleep(rf.ElectionTimeout)

		if debugLock {
			log.Printf("%v going to get lock", rf)
		}
		rf.mu.Lock()
		log.Printf("%v loop start, attr: %v", rf, StructToString(rf))
		rf.mu.Unlock()
		if !rf.ValidRpcReceived {
			go rf.StartElection()
		}
	}
}

func (rf *Raft) LeaderAppendEntries() {
	for {
		_, b := rf.GetState()
		// log.Printf("%v [leader: %v] ready to send append entries rpc", rf, b)
		if b {
			for i := range rf.peers {
				if i == rf.me {
					continue
				}

				args := AppendEntriesArgs{
					Term:     rf.CurrentTerm,
					LeaderID: rf.me,
				}
				go rf.sendAppendEntries(i, &args, &AppendEntriesReply{})
			}
		}
		time.Sleep(50 * time.Millisecond)
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

	rf.mu = sync.Mutex{}

	rf.CurrentTerm = 0
	rf.ElectionTimeout = time.Duration(RandomInt(150, 300)) * time.Millisecond
	rf.Role = "follower"
	rf.VotedFor = -1
	log.Printf("%v init", rf)
	log.Printf("instance attr: %v", StructToString(rf))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// log
	LogRunnerStart(rf.me)

	go rf.Loop()
	go rf.LeaderAppendEntries()

	return rf
}

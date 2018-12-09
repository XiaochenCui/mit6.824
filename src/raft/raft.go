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
	"labrpc"
	"log"
	"math/rand"
	"sync"
	"time"
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

type Log struct {
	command []byte // Command for state machine
	term    int    // When entry was received by leader
	index   int    // First index is 1
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
	currentTerm int
	votedFor    int   // candidateId that received vote in current term (-1 indicates none)
	logs        []Log // Log entries

	state int // 1: leader, 2: candidate, 3: follower
	votes int

	electionTimeout int
	lastCommanded   int // Last time the peer is commanded
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	if rf.state == 1 {
		isleader = true
	} else {
		isleader = false
	}

	return term, isleader
}

func (rf *Raft) setLeader() {
	log.Printf("[%d] become leader", rf.me)
	rf.state = 1
}

func (rf *Raft) setCandidate() {
	log.Printf("[%d] become candidate", rf.me)
	rf.state = 2
}

func (rf *Raft) setFollower() {
	log.Printf("[%d] become follower", rf.me)
	rf.state = 3
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

type AppendEntryArgs struct {
	Term     int // Leader's term
	LeaderID int // So follower can redirect clients
}

type AppendEntryReply struct {
}

// AppendEntry
func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	if args.LeaderID == rf.me {
		return
	}

	if args.Term < rf.currentTerm {
		return
	}

	rf.currentTerm = args.Term
	rf.setFollower()

	log.Printf("[%d] Receive AppendEntry calling from leader", rf.me)

	rf.votedFor = -1
	rf.resetTimeout()
	return
}

func (rf *Raft) resetTimeout() {
	rf.mu.Lock()
	now := int(time.Now().UnixNano())
	log.Printf("[%d] reset timeout [%d]", rf.me, now)
	rf.lastCommanded = now
	rf.mu.Unlock()
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // Candidate's term
	CandidateId  int // Candidate requesting vote
	LastLogIndex int // Index of candidate's last log entry
	LastLogTerm  int // Term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//rf.mu.Lock()
	//defer rf.mu.Unlock()

	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	if args.CandidateId == rf.me {
		reply.VoteGranted = true
		return
	}

	if rf.state == 3 {
		log.Printf("%d denies vote request from %d", rf.me, args.CandidateId)
		return
	}

	if args.Term < rf.currentTerm {
		log.Printf("%d denies vote request from %d", rf.me, args.CandidateId)
		return
	}

	var selfLastLogTerm int
	var selfLastLogIndex int
	if len(rf.logs) > 0 {
		selfLastLogTerm = rf.logs[len(rf.logs)-1].term
		selfLastLogIndex = rf.logs[len(rf.logs)-1].index
	} else {
		selfLastLogTerm = 0
		selfLastLogIndex = 0
	}

	log.Printf(`[%d]Log comparison: candidate.LastLogTerm: %d, selfLastLogTerm: %d,
				candidate.LastLogIndex: %d, selfLastLogIndex: %d, rf.votedFor: %d`,
		rf.me,
		args.LastLogTerm, selfLastLogTerm, args.LastLogIndex, selfLastLogIndex,
		rf.votedFor)
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if args.LastLogTerm > selfLastLogTerm {
			reply.VoteGranted = true
		}
		if args.LastLogTerm == selfLastLogTerm {
			if args.LastLogIndex >= selfLastLogIndex {
				reply.VoteGranted = true
			}
		}
	}
	if reply.VoteGranted {
		log.Printf("%d approve vote request from %d, reply: %v", rf.me, args.CandidateId, reply)
		rf.votedFor = args.CandidateId
		rf.resetTimeout()
	} else {
		log.Printf("%d denies vote request from %d", rf.me, args.CandidateId)
	}
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
	rand.Seed(time.Now().UnixNano())
	min := 500 * 1000000
	max := 1000 * 1000000
	rf.electionTimeout = int(rand.Intn(max-min) + min)
	log.Printf("%d's electionTimeout is: %d", rf.me, rf.electionTimeout)

	rf.lastCommanded = int(time.Now().UnixNano())
	rf.votedFor = -1

	go func() {
		for {
			time.Sleep(10 * time.Millisecond)

			_, isleader := rf.GetState()
			if isleader {
				continue
			}

			now := int(time.Now().UnixNano())

			//log.Printf("[%d] now: %d, lastCommanded: %d", rf.me, now, rf.lastCommanded)
			if now-rf.lastCommanded < rf.electionTimeout {
				continue
			}

			// Start election
			log.Printf("%v ready to request vote...\n", rf.me)

			rf.resetTimeout()

			rf.setCandidate()
			rf.currentTerm++

			var lastLogIndex int
			var lastLogTerm int
			if len(rf.logs) > 0 {
				lastLogIndex = rf.logs[len(rf.logs)-1].index
				lastLogTerm = rf.logs[len(rf.logs)-1].term
			} else {
				lastLogIndex = 0
				lastLogTerm = 0
			}

			requestVoteArgs := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}

			// Issue RequestVote RPC's in parallel
			for i, peer := range peers {
				go func(i int, peer *labrpc.ClientEnd) {
					reply := RequestVoteReply{}
					log.Printf("[%d] ready to send RequestVote to %d", rf.me, i)
					ok := peer.Call("Raft.RequestVote", &requestVoteArgs, &reply)
					if !ok {
						log.Printf("Rpc calling failed")
					}
					log.Printf("[%d] reply from %d: %v", rf.me, i, reply)
					if reply.VoteGranted {
						rf.votes++

						if rf.votes > len(peers)/2 {
							// Become Leader
							_, isleader := rf.GetState()
							if !isleader {
								rf.setLeader()
							}
						}
					}
				}(i, peer)
			}
		}
	}()

	go func() {
		for {
			_, isleader := rf.GetState()
			if isleader {
				for _, peer := range peers {
					args := AppendEntryArgs{
						Term:     rf.currentTerm,
						LeaderID: rf.me,
					}
					go func(peer *labrpc.ClientEnd) {
						_ = peer.Call("Raft.AppendEntry", &args, &AppendEntryReply{})
					}(peer)
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

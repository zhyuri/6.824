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
	"log"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "labgob"

const (
	// enable debug output
	debugEnabled = false
	// timeout base for re-election
	voteTimeOutBase = 350 * time.Millisecond
	// heartbeat ticker interval
	heartbeatInterval = 150 * time.Millisecond
)

// timestamp in NanoSeconds when the application started, for debug output purpose
var startTimestamp = time.Now().UnixNano()

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
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	debug           bool
	term            term
	electionTimer   *time.Timer
	heartbeatTicker *time.Ticker
	log             []interface{}
}

//
// Raft Term
//
type term struct {
	id       int
	leader   int
	role     role
	votedFor int // this peer voted for who to be the leader
}

// role of the node
type role uint

const (
	Follower  role = iota
	Candidate role = iota
	Leader    role = iota
)

// for debug output
func (e role) String() string {
	switch e {
	case Follower:
		return "\x1b[32mF\x1b[0m"
	case Candidate:
		return "\x1b[34mC\x1b[0m"
	case Leader:
		return "\x1b[91mL\x1b[0m"
	default:
		return fmt.Sprintf("Unknown role: %d", int(e))
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.term.id, rf.term.role == Leader
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
	// Term id
	TermID int
	// id of the requester
	CandidateId int
	// index of candidate’s last log entry
	LastLogIndex int
	// term of candidate’s last log entry
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	From int
	// Term id
	TermID int
	// Vote to id
	VoteGranted bool
}

// debug only
func (r RequestVoteReply) String() string {
	return fmt.Sprintf("{From %d; Term: %d; VoteGranted: %v}", r.From, r.TermID, r.VoteGranted)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.RLock()
	reply.From = rf.me
	currentTerm := rf.term
	rf.mu.RUnlock()
	if args.TermID < currentTerm.id {
		// already in the newer term
		rf.debugf("Current term %v, deny stale request term %v", currentTerm.id, args.TermID)
		reply.TermID = currentTerm.id
		reply.VoteGranted = false
		return
	}
	if currentTerm.role != Candidate {
		// servers disregard RequestVote RPCs when they believe a current leader exists.
		rf.debugf("Still have leader %v, reject vote request from %v", currentTerm.leader, args.CandidateId)
		reply.TermID = currentTerm.id
		reply.VoteGranted = false
		return
	}
	if args.TermID == currentTerm.id && currentTerm.votedFor != args.CandidateId {
		// have voted in this term
		rf.debugf("Have already voted for %v, sorry %v", currentTerm.votedFor, args.CandidateId)
		reply.TermID = args.TermID
		reply.VoteGranted = false
		return
	}
	// If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	rf.mu.Lock()
	rf.term = term{
		id:       args.TermID,
		role:     Follower,
		votedFor: args.CandidateId,
		// haven't known new leader at this point
		leader: -1,
	}
	rf.mu.Unlock()
	rf.debugf("New term %v, vote for %v", args.TermID, args.CandidateId)
	reply.TermID = args.TermID
	reply.VoteGranted = true
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
	rf.mu.RLock()
	term := rf.term.id
	isLeader := rf.term.role == Leader
	rf.mu.RUnlock()

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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.heartbeatTicker.Stop()
	rf.electionTimer.Stop()
	rf.debug = false
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
	rf.debug = debugEnabled
	rf.term = term{
		id:       0,
		role:     Follower,
		votedFor: -1,
		leader:   -1,
	}
	rf.electionTimer = time.NewTimer(time.Millisecond)
	rf.heartbeatTicker = time.NewTicker(heartbeatInterval)

	go rf.electionDaemon()
	go rf.heartbeatDaemon()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) electionDaemon() {
	for {
		rf.mu.Lock()
		rf.electionTimer.Reset(voteTimeOutBase)
		rf.mu.Unlock()

		<-rf.electionTimer.C
		rf.mu.RLock()
		term := rf.term
		rf.mu.RUnlock()
		if term.role == Leader {
			continue
		}
		// Raft uses randomized election timeouts to ensure that split votes are rare and that they are resolved quickly.
		waitTime := time.Duration(rand.Intn(100)) * time.Millisecond
		rf.debugf("Election timer expired, waiting %v to request new election", waitTime)
		time.Sleep(waitTime)
		rf.requestVote()
	}
}

func (rf *Raft) requestVote() {
	// update self term struct first
	rf.mu.Lock()
	myVoteTermID := rf.term.id + 1
	rf.term.role = Candidate
	rf.term.votedFor = rf.me
	rf.term.leader = -1
	rf.mu.Unlock()

	rf.debugf("Still don't have the leader, request for re-election")
	args := RequestVoteArgs{
		CandidateId: rf.me,
		TermID:      myVoteTermID,
	}
	// send out request in parallel and collect reply in a channel, so that we can promote as soon as have majority votes
	replies := make(chan RequestVoteReply, len(rf.peers)-1)
	go func() {
		rf.broadcast(func(id int, c *labrpc.ClientEnd) bool {
			reply := RequestVoteReply{}
			rf.debugf("Send vote request to %d", id)
			result := c.Call("Raft.RequestVote", &args, &reply)
			rf.debugf("Receive vote reply %v result %v", reply, result)
			if result {
				replies <- reply
			}
			return result
		})
		close(replies)
	}()
	// A candidate wins an election if it receives votes from a majority of the servers
	// in **the full cluster** for the same term.
	threshold := float64(len(rf.peers)) / 2.0
	rf.debugf("Threshold %f, counting", threshold)
	count := 1.0
	for reply := range replies {
		// no longer the Candidate anymore
		rf.mu.RLock()
		t := rf.term
		rf.mu.RUnlock()
		if t.role != Candidate {
			rf.debugf("Convert to follower of leader %v", t.leader)
			return
		}
		// A candidate wins an election if it receives votes from a majority of the servers
		// in the full cluster for **the same term**.
		if reply.TermID > myVoteTermID {
			rf.debugf("Failed to promote in term %v, some node already in term %v", myVoteTermID, reply.TermID)
			rf.mu.Lock()
			rf.term = term{
				id:   reply.TermID,
				role: Follower,
				// set -1 for now, waiting for heart beat from leader
				leader:   -1,
				votedFor: -1,
			}
			rf.mu.Unlock()
			return
		}
		if reply.VoteGranted {
			count++
		}
		// A candidate wins an election if it receives votes from **a majority** of the servers
		// in the full cluster for the same term.
		if count > threshold {
			// I am the new leader!
			rf.mu.Lock()
			rf.term = term{
				id:       myVoteTermID,
				role:     Leader,
				leader:   rf.me,
				votedFor: rf.me,
			}
			rf.mu.Unlock()
			rf.debugf("Promote to Leader with vote %v > %v", count, threshold)
			// send out heart beat immediately to announce the promotion
			rf.heartbeat()
			return
		}
	}
	// Failed to promote
	rf.debugf("Failed to promote, count %v <= %v", count, threshold)
}

func (rf *Raft) heartbeatDaemon() {
	for range rf.heartbeatTicker.C {
		go rf.heartbeat()
	}
}

func (rf *Raft) heartbeat() {
	if _, isLeader := rf.GetState(); !isLeader {
		// current node isn't leader
		return
	}
	rf.mu.RLock()
	args := AppendEntries{
		TermID:   rf.term.id,
		LeaderID: rf.me,
	}
	rf.mu.RUnlock()
	rf.broadcast(func(_ int, c *labrpc.ClientEnd) bool {
		reply := AppendReply{}
		result := c.Call("Raft.Append", &args, &reply)
		return result && reply.Success
	})
}

func (rf *Raft) broadcast(invoke func(id int, c *labrpc.ClientEnd) bool) []*labrpc.ClientEnd {
	failedClient := make(chan *labrpc.ClientEnd, len(rf.peers)-1)
	wg := sync.WaitGroup{}
	wg.Add(len(rf.peers) - 1)
	for i, client := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(id int, c *labrpc.ClientEnd) {
			defer wg.Done()
			result := invoke(id, c)
			if !result {
				failedClient <- c
			}
		}(i, client)
	}
	wg.Wait()
	close(failedClient)
	result := make([]*labrpc.ClientEnd, 0, len(failedClient))
	for c := range failedClient {
		result = append(result, c)
	}
	return result
}

type AppendEntries struct {
	TermID       int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []interface{}
	LeaderCommit int
}

type AppendReply struct {
	TermID  int
	Success bool
}

func (rf *Raft) Append(entry *AppendEntries, reply *AppendReply) {
	rf.mu.RLock()
	t := rf.term
	rf.mu.RUnlock()
	if entry.TermID < t.id {
		rf.debugf("Deny append from leader %v due to term %v < %v", entry.LeaderID, entry.TermID, t.id)
		reply.TermID = entry.TermID
		reply.Success = false
		return
	}
	rf.mu.Lock()
	rf.electionTimer.Reset(voteTimeOutBase)
	rf.term = term{
		id:       entry.TermID,
		role:     Follower,
		leader:   entry.LeaderID,
		votedFor: entry.LeaderID,
	}
	rf.mu.Unlock()
	newLeader := entry.LeaderID != t.leader
	if newLeader {
		rf.debugf("Receive Append from new leader %v.", entry.LeaderID)
	} else if len(entry.Entries) <= 0 {
		rf.debugf("Receive heart beat from leader %v", entry.LeaderID)
	} else {
		rf.debugf("Receive Append request from leader %v with entity count %d", entry.LeaderID, len(entry.Entries))
	}
	reply.TermID = t.id
	reply.Success = true
}

func (rf *Raft) debugf(format string, a ...interface{}) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	if !rf.debug {
		return
	}
	meColor := []int{35, 36, 92, 93, 95}
	me := fmt.Sprintf("\x1b[%dm%d\x1b[0m", meColor[rf.me%5], rf.me)
	leader := "\x1b[91m_\x1b[0m"
	if rf.term.leader >= 0 {
		l := meColor[rf.term.leader%5]
		leader = fmt.Sprintf("\x1b[%dm%d\x1b[0m", l, rf.term.leader)
	}
	log.Printf(fmt.Sprintf("+%.2f %d %s [%s -> %s]\t",
		float64(time.Now().UnixNano()-startTimestamp)/1000000000,
		rf.term.id, rf.term.role, me, leader)+
		format+"\n", a...)
}

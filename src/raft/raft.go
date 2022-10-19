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
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
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

type serverState int

const (
	Follower serverState = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state on all servers
	serverState     serverState
	electionTimeout time.Time
	commitIndex     int // index of the highest log entry konwon to be commited
	lastApplied     int // index of highest log entry applied to state machine

	// Volatile state on leaders
	nextIndex  []int // for each server, idx of the next log entry to send to that server
	matchIndex []int // fot each server, index of highest log entry knwon to be replicated on server

	// Consts
	applyChannel  chan ApplyMsg
	commitTrigger chan bool
}

type LogEntry struct {
	Index   int
	Command interface{} // command for state machine
	Term    int         // when the entry was received by the leader
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
	isleader = rf.serverState == Leader

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.stepDown(args.Term)
	}

	canVote := (rf.votedFor == -1 || rf.votedFor == args.CandidateId)

	if canVote && rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	}
}

func (rf *Raft) isLogUpToDate(index int, term int) bool {
	lastLog := rf.log[len(rf.log)-1]

	if lastLog.Term != term {
		return term >= lastLog.Term
	}

	return index >= lastLog.Index
}

type AppendEntiresArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry // log entries to store (empty for heartbeat)
	LeaderCommit int        // leader's commit idx
}

type AppendEntiresReply struct {
	Term    int
	Success bool
}

func (rf *Raft) getAppendEntriesArgs(destinationPeer int) *AppendEntiresArgs {
	prevLogIndex := rf.nextIndex[destinationPeer] - 1
	prevLogTerm := rf.log[prevLogIndex].Term
	var entries []LogEntry

	if len(rf.log) > 0 {
		entries = rf.log[rf.nextIndex[destinationPeer]:]
	}

	return &AppendEntiresArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
}

func (rf *Raft) AppendEntires(args *AppendEntiresArgs, reply *AppendEntiresReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	//
	if args.Term < rf.currentTerm {
		return
	}

	rf.electionTimeout = generateNextElectionTimeout()

	//
	if rf.log[len(rf.log)-1].Index < args.PrevLogIndex {
		return
	}

	//
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return
	}

	reply.Success = true

	if args.Term > rf.currentTerm {
		rf.stepDown(args.Term)
	}

	if rf.serverState == Candidate && args.Term >= rf.currentTerm {
		rf.stepDown(args.Term)
	}

	if len(args.Entries) > 0 {
		// If an existing entry conflicts with a new one (same index but different terms),
		// delete the existing entry and all that follow it
		conflictingEntries := rf.log[args.PrevLogIndex+1:]
		var i int
		for i = 0; i < min(len(conflictingEntries), len(args.Entries)); i++ {
			if conflictingEntries[i].Term != args.Entries[i].Term {
				rf.log = rf.log[:args.PrevLogIndex+1+i]
				break
			}
		}
		if i < len(args.Entries) {
			// Append any new entries not already in the log
			rf.log = append(rf.log, args.Entries[i:]...)
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.log[len(rf.log)-1].Index)
		go func() { rf.commitTrigger <- true }()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntiresArgs, reply *AppendEntiresReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntires", args, reply)
	return ok
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
	rf.mu.Lock()
	isLeader := rf.serverState == Leader

	if !isLeader {
		rf.mu.Unlock()
		return -1, -1, isLeader
	}

	index := rf.nextIndex[rf.me]
	term := rf.currentTerm

	rf.log = append(rf.log, LogEntry{
		Index:   index,
		Term:    term,
		Command: command,
	})

	rf.nextIndex[rf.me] += 1
	rf.matchIndex[rf.me] = index

	rf.mu.Unlock()

	commitChannel := make(chan bool)

	go rf.applyCommit(index, commitChannel)

	for peer := range rf.peers {
		if peer != rf.me {
			go rf.issueAppendEntries(peer, commitChannel)
		}
	}

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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()

		var timeToNextAction time.Duration

		// println(rf.me, rf.serverState, rf.votedFor, rf.currentTerm)

		if rf.serverState == Leader {
			rf.electionTimeout = generateNextElectionTimeout()
			timeToNextAction = time.Until(rf.electionTimeout)
		} else {
			if rf.electionTimeout.After((time.Now())) {
				timeToNextAction = time.Until(rf.electionTimeout)
			} else {
				go rf.startElection()

				rf.electionTimeout = generateNextElectionTimeout()
				timeToNextAction = time.Until(rf.electionTimeout)
			}
		}

		rf.mu.Unlock()
		time.Sleep(timeToNextAction)
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.serverState = Candidate
	rf.votedFor = rf.me

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log[len(rf.log)-1].Index,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}

	rf.mu.Unlock()

	votingChannel := make(chan bool)

	for peer := range rf.peers {
		if peer != rf.me {
			go rf.askForVote(peer, votingChannel, &args)
		}
	}

	votesGranted := 1
	electionWon := false

	for i := 0; i < len(rf.peers)-1; i++ {
		voteGranted := <-votingChannel

		if electionWon {
			continue
		}

		if voteGranted {
			votesGranted += 1
		}

		if rf.majorityReached(votesGranted) {
			electionWon = true
			rf.mu.Lock()

			if rf.serverState == Candidate && rf.currentTerm == args.Term {
				rf.serverState = Leader
				lastLogIdx := 0

				if len(rf.log) > 0 {
					lastLogIdx = rf.log[len(rf.log)-1].Index
				}

				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))

				for peer := range rf.peers {
					rf.nextIndex[peer] = lastLogIdx + 1
					rf.matchIndex[peer] = 0
				}

				rf.matchIndex[rf.me] = lastLogIdx

				go rf.scheduleHeartbeat()
			}

			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) askForVote(peer int, votingChannel chan<- bool, args *RequestVoteArgs) {
	reply := &RequestVoteReply{}
	response := rf.sendRequestVote(peer, args, reply)
	voteGranted := false

	if response {
		rf.mu.Lock()

		electionStillOngoing := rf.serverState == Candidate && args.Term == rf.currentTerm

		if electionStillOngoing {
			if reply.Term > rf.currentTerm {
				rf.stepDown(reply.Term)
			} else {
				voteGranted = reply.VoteGranted
			}
		}

		rf.mu.Unlock()
	}

	votingChannel <- voteGranted
}

func (rf *Raft) scheduleHeartbeat() {
	heartbeatInterval := time.Duration(100) * time.Millisecond

	for !rf.killed() {
		rf.mu.Lock()

		if rf.serverState != Leader {
			rf.mu.Unlock()
			return
		}

		rf.mu.Unlock()

		for peer := range rf.peers {
			if peer != rf.me {
				go rf.issueAppendEntries(peer, nil)
			}

		}

		time.Sleep(heartbeatInterval)
	}
}

func (rf *Raft) issueAppendEntries(peer int, commitChannel chan<- bool) {
	rf.mu.Lock()
	args := rf.getAppendEntriesArgs(peer)
	reply := &AppendEntiresReply{}
	rf.mu.Unlock()

	response := rf.sendAppendEntries(peer, args, reply)

	if response {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.Term > rf.currentTerm {
			rf.stepDown(reply.Term)
			return
		}

		if rf.serverState != Leader || args.Term != rf.currentTerm {
			return
		}

		// if appendEntries wasn't a heartbeat
		if commitChannel != nil {
			if reply.Success {
				newNextIndex := args.PrevLogIndex + len(args.Entries) + 1
				newMatchIndex := args.PrevLogIndex + len(args.Entries)

				if rf.nextIndex[peer] < newNextIndex {
					rf.nextIndex[peer] = newNextIndex
				}

				if rf.matchIndex[peer] < newMatchIndex {
					rf.matchIndex[peer] = newMatchIndex
				}

				go func() { commitChannel <- true }()
			} else {
				// reply fails when peer's log is inconsistent
				// so we need to lower the nextIndex till we find common point
				rf.nextIndex[peer] -= 1

				go rf.issueAppendEntries(peer, commitChannel)
			}
		}
	}
}

func (rf *Raft) applyCommit(logIndex int, commitChannel chan bool) {
	commitApplied := false
	serversWithEntry := 1

	for i := 0; i < len(rf.peers)-1; i++ {
		entryReplicated := <-commitChannel

		if commitApplied {
			continue
		}

		if entryReplicated {
			serversWithEntry += 1
		}

		if rf.majorityReached(serversWithEntry) {
			commitApplied = true
			rf.mu.Lock()

			if rf.serverState != Leader || logIndex <= rf.commitIndex {
				rf.mu.Unlock()
				continue
			}

			rf.commitIndex = logIndex

			rf.mu.Unlock()

			rf.commitTrigger <- true
		}

	}
}

func (rf *Raft) stepDown(term int) {
	rf.serverState = Follower
	rf.electionTimeout = generateNextElectionTimeout()
	rf.currentTerm = term
	rf.votedFor = -1
}

func generateNextElectionTimeout() time.Time {
	return time.Now().Add(time.Duration(500+rand.Intn(500)) * time.Millisecond)
}

func (rf *Raft) majorityReached(count int) bool {
	return count > (len(rf.peers) / 2)
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

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.serverState = Follower
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.electionTimeout = generateNextElectionTimeout()
	rf.nextIndex = nil
	rf.matchIndex = nil
	rf.log = []LogEntry{{Index: 0, Term: 0}}
	rf.applyChannel = applyCh
	rf.commitTrigger = make(chan bool)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.messageApplier()
	go rf.ticker()

	return rf
}

func (rf *Raft) messageApplier() {
	for !rf.killed() {
		<-rf.commitTrigger

		rf.mu.Lock()

		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied += 1
			nextLogEntryToApply := rf.log[rf.lastApplied]

			rf.mu.Unlock()

			rf.applyChannel <- ApplyMsg{
				Command:      nextLogEntryToApply.Command,
				CommandIndex: nextLogEntryToApply.Index,
				CommandValid: true,
			}

			rf.mu.Lock()
		}

		rf.mu.Unlock()
	}
}

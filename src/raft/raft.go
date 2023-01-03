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

import "time"
import "math/rand"
import "sync"
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"



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

const (
	sleepUnit int = 100

	electionTimeoutMin int = 1000
	electionTimeoutMax int = 1500
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Term	int
	Command	interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	isleader	bool
	votedFor	int
	currentTerm	int
	log 		[]LogEntry
	commitIndex	int
	lastApplied	int
	nextIndex	[]int
	matchIndex	[]int
	lastHeartbeat	time.Time
	applyCh 	chan ApplyMsg

	granted		int

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
	isleader = rf.isleader
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
	Term			int
	CandidateId		int
	LastLogIndex	int
	LastLogTerm		int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term 			int
	VotedGranted	bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = args.Term

	if (args.Term < rf.currentTerm) {
		reply.VotedGranted = false
		return
	} 

	if ((args.Term > rf.currentTerm) || (args.Term == rf.currentTerm) && (rf.votedFor == -1 || rf.votedFor == args.CandidateId)) && (len(rf.log) == 0 || args.LastLogTerm >= rf.log[len(rf.log) - 1].Term) {
		reply.VotedGranted = true
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		rf.isleader = false
		rf.lastHeartbeat = time.Now()
	} else {
		reply.VotedGranted = false
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

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term			int
	LeaderId		int
	PrevLogIndex	int
	PrevLogTerm		int
	Entries			[]LogEntry
	LeaderCommit	int
}

type AppendEntriesReply struct {
	// Your data here (2A).
	Term 			int
	Success			bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = args.Term

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	rf.lastHeartbeat = time.Now()
	rf.currentTerm = args.Term
	rf.isleader = false

	if len(rf.log) == 0 && args.PrevLogIndex > 0 {
		reply.Success = false
		return
	}

	if len(rf.log) > 0 && args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex - 1].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	reply.Success = true
	
	if args.PrevLogIndex > 0 && args.PrevLogIndex < len(rf.log) {
		rf.log = rf.log[0:args.PrevLogIndex]
	}
	for i := range args.Entries {
		rf.log = append(rf.log, args.Entries[i])
	}
	if args.LeaderCommit <= len(rf.log) && args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
	}

	return
	
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) countRequestVoteReply(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.sendRequestVote(server, args, reply)
	if !ok {
		return
	}
	if reply.VotedGranted {

		rf.mu.Lock()
		defer rf.mu.Unlock()

		rf.granted++
		if !rf.isleader && rf.granted >= len(rf.peers) / 2 + 1 {
			rf.isleader = true
			rf.currentTerm++
			rf.nextIndex = make([]int, len(rf.peers))
			for i := range rf.nextIndex {
				rf.nextIndex[i] = len(rf.log) + 1
			}

			rf.matchIndex = make([]int, len(rf.peers))
			for i := range rf.matchIndex {
				rf.matchIndex[i] = 0
			}
		}
	}
}

func (rf *Raft) doElection() {
	rf.votedFor = rf.me
	//rf.currentTerm++
	rf.granted = 1
	rf.lastHeartbeat = time.Now()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		lastLogIndex := 0
		lastLogTerm := 0
		if len(rf.log) > 0 {
			lastLogIndex = len(rf.log)
			lastLogTerm = rf.log[len(rf.log) - 1].Term
		}

		args := RequestVoteArgs {
			Term:			rf.currentTerm + 1,
			CandidateId:	rf.me,
			LastLogIndex:	lastLogIndex,
			LastLogTerm:	lastLogTerm,
		}

		reply := RequestVoteReply {}
		
		go rf.countRequestVoteReply(i, &args, &reply)
	}
}

func (rf *Raft) countReplicationReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.sendAppendEntries(server, args, reply)
	if !ok {
		return
	}
	if reply.Success {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		successfulIndex := args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = successfulIndex + 1
		rf.matchIndex[server] = successfulIndex
		for i := len(rf.log); i > rf.commitIndex; i-- {
			count := 1
			for j := range rf.matchIndex {
				if rf.matchIndex[j] >= i {
					count++
				}
			}

			if count >= len(rf.peers) / 2 + 1 {
				rf.commitIndex = i
				break
			}
		}
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.nextIndex[server]--
	}
}

func (rf *Raft) sendHeartbeat() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		prevLogIndex := rf.nextIndex[i] - 1
		prevLogTerm := 0
		entries := make([]LogEntry, 0)
		if prevLogIndex > 0 {
			prevLogTerm = rf.log[prevLogIndex - 1].Term
			entries = rf.log[prevLogIndex:]
		} else {
			entries = rf.log
		}

		args := AppendEntriesArgs {
			Term:			rf.currentTerm,
			LeaderId:		rf.me,
			PrevLogIndex:	prevLogIndex,
			PrevLogTerm:	prevLogTerm,
			Entries:		entries,
			LeaderCommit:	rf.commitIndex,
		}

		reply := AppendEntriesReply {}

		go rf.countReplicationReply(i, &args, &reply)
	}
}

func (rf *Raft) checkLeaderAlive() {
	for {
		rf.mu.Lock()

		electionTimeOut := time.Duration(electionTimeoutMin + rand.Intn(electionTimeoutMax - electionTimeoutMin)) * time.Millisecond
		if rf.isleader {
			rf.sendHeartbeat()
		} else if time.Now().After(rf.lastHeartbeat.Add(electionTimeOut)) {
			rf.doElection()
		}

		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i - 1].Command,
				CommandIndex: i,
			}
			rf.applyCh <- applyMsg
		}
		rf.lastApplied = rf.commitIndex
		
		rf.mu.Unlock()
		time.Sleep(time.Duration(sleepUnit) * time.Millisecond)
	}
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
	rf.mu.Lock()
	index := len(rf.log) + 1
	term := rf.currentTerm
	isLeader := rf.isleader

	// Your code here (2B).

	if isLeader {
		logEntry := LogEntry {
			Command:	command,
			Term:		term,
		}
		rf.log = append(rf.log, logEntry)
	}

	rf.mu.Unlock()

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
	rf.votedFor = -1
	rf.isleader = false
	rf.lastHeartbeat = time.Now()
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.granted = 0
	rf.log = make([]LogEntry, 0)
	rf.applyCh = applyCh

	rand.Seed(time.Now().UnixNano())
	go rf.checkLeaderAlive()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}

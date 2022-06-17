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
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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
	electionTimeout int64
	lastHeartBeat   int64
	LogState        LogState

	state      StateType
	leaderChan chan int
	applyChan  chan int
	applyFunc  func(msg ApplyMsg)
	message    message
}

type StateType uint64

const (
	StateTypeFollower StateType = iota
	StateTypeLeader
	StateTypeCandidate
)

type LogState struct {
	CurrentTerm int
	VotedFor    *int
	Log         []Entry

	votes int
}

type Entry struct {
	Index   int
	Term    int
	Command interface{}
}

type message struct {
	LogIndex    int
	CommitIndex int // index of highest log entry known to be committed
	LastApplied int // index of highest log entry applied to state machine

	nextIndex  []int
	matchIndex []int
}

type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	LeaderCommit int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
}

type AppendEntryReply struct {
	Term    int
	Success bool

	PrevLogIndex int
	PrevLogTerm  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.LogState.CurrentTerm, rf.state == StateTypeLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.LogState.CurrentTerm)
	e.Encode(rf.message.LastApplied)
	e.Encode(rf.message.CommitIndex)
	e.Encode(rf.message.LogIndex)
	e.Encode(rf.LogState.Log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.LogState = LogState{CurrentTerm: 0}
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var log []Entry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&rf.message.LastApplied) != nil ||
		d.Decode(&rf.message.CommitIndex) != nil ||
		d.Decode(&rf.message.LogIndex) != nil ||
		d.Decode(&log) != nil {
		panic("failed to read persistent data")
	} else {
		rf.LogState = LogState{
			CurrentTerm: currentTerm,
			Log:         log,
		}
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	if args.Term < rf.LogState.CurrentTerm {
		reply.Success = false
		reply.Term = rf.LogState.CurrentTerm
		return
	}

	defer func() {
		go rf.persist()
	}()

	rf.mu.Lock()
	rf.becomeFollowerLocked(args.Term, &args.LeaderId)
	rf.mu.Unlock()

	valid := true
	if args.PrevLogIndex > 0 {
		if rf.message.LogIndex < args.PrevLogIndex {
			//fmt.Printf("%d found inconsistent log index. local=%d, assumed=%d\n", rf.me, rf.message.LogIndex, args.PrevLogIndex)
			reply.PrevLogIndex = rf.message.LogIndex
			if rf.message.LogIndex > 0 {
				reply.PrevLogTerm = rf.LogState.Log[rf.message.LogIndex-1].Term
			}
			valid = false
		}

		// if the term is mismatched, returns with the first index of my term
		if valid && rf.LogState.Log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
			//fmt.Printf("%d found inconsistent term %d at index %d\n", rf.me, args.PrevLogTerm, args.PrevLogIndex)
			term := rf.LogState.Log[args.PrevLogIndex-1].Term
			index := args.PrevLogIndex + 1
			for j := args.PrevLogIndex - 1; j > rf.message.LastApplied-1; j-- {
				if rf.LogState.Log[j].Term == term {
					index--
				} else {
					break
				}
			}
			reply.PrevLogIndex = index
			reply.PrevLogTerm = term
			valid = false
		}
	}

	if !valid {
		reply.Success = false
		reply.Term = rf.LogState.CurrentTerm
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if len(args.Entries) > 0 {
		rf.localAppendEntriesLocked(args.PrevLogIndex, args.Term, args.Entries)
	}
	if args.LeaderCommit > 0 {
		rf.localApplyEntriesLocked(args.LeaderCommit)
	}
	reply.Success = true
	reply.Term = rf.LogState.CurrentTerm
}

func (rf *Raft) localAppendEntriesLocked(prevLogIndex, term int, newEntries []Entry) {
	logIndex := prevLogIndex
	i := 0
	for ; logIndex < len(rf.LogState.Log) && i < len(newEntries); i++ {
		entry := rf.LogState.Log[logIndex]
		if entry.Term != term {
			rf.LogState.Log[logIndex] = newEntries[i]
		}
		logIndex++
	}
	for ; i < len(newEntries); i++ {
		rf.LogState.Log = append(rf.LogState.Log, newEntries[i])
		logIndex++
	}
	//fmt.Printf("%d is appending logs from index %d to %d\n", rf.me, prevLogIndex, logIndex)
	rf.message.LogIndex = logIndex
}

func (rf *Raft) localApplyEntriesLocked(commitIndex int) {
	if commitIndex > rf.message.CommitIndex {
		rf.message.CommitIndex = commitIndex
		rf.applyChan <- commitIndex
	}
}

func (rf *Raft) becomeFollowerLocked(term int, votedFor *int) {
	rf.state = StateTypeFollower
	if votedFor != nil {
		rf.lastHeartBeat = time.Now().UnixMilli()
	}
	rf.LogState.CurrentTerm = term
	rf.LogState.VotedFor = votedFor
}

func (rf *Raft) becomeCandidate() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.isLeaseExpired() {
		return rf.LogState.CurrentTerm, false
	}

	rf.state = StateTypeCandidate
	rf.lastHeartBeat = time.Now().UnixMilli()
	rf.LogState.CurrentTerm = rf.LogState.CurrentTerm + 1
	rf.LogState.VotedFor = &rf.me
	rf.LogState.votes = 1

	return rf.LogState.CurrentTerm, true
}

func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	if rf.state == StateTypeCandidate {
		//fmt.Printf("%d becomes leader for term %d, current index is %d.\n", rf.me, rf.LogState.CurrentTerm, rf.message.LogIndex)
		for i := 0; i < len(rf.peers); i++ {
			rf.message.nextIndex[i] = rf.message.LogIndex + 1
			rf.message.matchIndex[i] = 0
		}
	}
	rf.state = StateTypeLeader
	rf.mu.Unlock()
	rf.heartbeat()
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	grantVote := true
	if args.Term < rf.LogState.CurrentTerm {
		grantVote = false
	}
	if args.Term == rf.LogState.CurrentTerm && rf.LogState.VotedFor != nil && *rf.LogState.VotedFor != args.CandidateId {
		//fmt.Printf("%d rejects vote because the vote has been granted to %d\n", rf.me, *rf.LogState.VotedFor)
		grantVote = false
	}

	if grantVote && rf.message.LogIndex > 0 {
		lastEntry := rf.LogState.Log[rf.message.LogIndex-1]
		if args.LastLogTerm < lastEntry.Term || (args.LastLogTerm == lastEntry.Term && args.LastLogIndex < lastEntry.Index) {
			//fmt.Printf("%d rejects vote from %d because my log is more up-to-date.\n", rf.me, args.CandidateId)
			grantVote = false
		}
	}

	if grantVote {
		rf.becomeFollowerLocked(args.Term, &args.CandidateId)
		go rf.persist()
	} else if rf.LogState.CurrentTerm < args.Term {
		rf.becomeFollowerLocked(args.Term, nil)
		go rf.persist()
	}

	reply.VoteGranted = grantVote
	reply.Term = rf.LogState.CurrentTerm
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

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != StateTypeLeader {
		return index, term, false
	}
	index = rf.message.LogIndex + 1
	term = rf.LogState.CurrentTerm

	if index > len(rf.LogState.Log) {
		rf.LogState.Log = append(rf.LogState.Log, Entry{
			Index:   index,
			Term:    term,
			Command: command,
		})
	} else {
		rf.LogState.Log[index-1] = Entry{
			Index:   index,
			Term:    term,
			Command: command,
		}
	}

	rf.message.LogIndex = index
	rf.message.matchIndex[rf.me] = index
	rf.message.nextIndex[rf.me] = index + 1

	go rf.persist()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		i := i
		go rf.appendEntriesToPeer(i)
	}

	return index, term, true
}

func (rf *Raft) appendEntriesToPeer(i int) bool {
	prevLogIndex := 0
	prevLogTerm := 0
	if rf.message.nextIndex[i] > 1 {
		//current LogIndex at peer
		prevLogIndex = rf.message.nextIndex[i] - 1
		prevLogTerm = rf.LogState.Log[prevLogIndex-1].Term
	}

	index := rf.message.LogIndex
	term := rf.LogState.CurrentTerm
	var appendEntries []Entry
	for k := prevLogIndex; k < index; k++ {
		appendEntries = append(appendEntries, rf.LogState.Log[k])
	}
	args := &AppendEntryArgs{
		Term:         term,
		LeaderId:     rf.me,
		LeaderCommit: rf.message.CommitIndex,

		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      appendEntries,
	}
	reply := &AppendEntryReply{}
	rf.sendAppendEntries(i, args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Success {
		rf.message.matchIndex[i] = index
		rf.message.nextIndex[i] = index + 1

		rf.applyChan <- index
	} else if reply.Term <= term {
		adjustedNextIndex := rf.message.nextIndex[i] - 1
		if reply.PrevLogTerm > 0 {
			adjustedNextIndex = reply.PrevLogIndex + 1
			if reply.PrevLogTerm != rf.LogState.Log[reply.PrevLogIndex-1].Term {
				// if term is mismatched, decrement the index and sync again
				if rf.LogState.Log[reply.PrevLogIndex-1].Term != reply.PrevLogTerm {
					adjustedNextIndex--
				}
			}
		} else {
			adjustedNextIndex = 1
		}
		rf.message.nextIndex[i] = adjustedNextIndex
		return false
	}

	return true
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	epsilon := rand.Int63n(50)
	interval := (rf.electionTimeout)/2 + epsilon

	for rf.killed() == false {

		time.Sleep(time.Duration(interval) * time.Millisecond)

		if rf.state == StateTypeLeader {
			rf.heartbeat()
		} else {
			if rf.isLeaseExpired() {
				rf.electLeaders()
			}
		}
	}
}

func (rf Raft) isLeaseExpired() bool {
	return time.Now().UnixMilli()-rf.lastHeartBeat > rf.electionTimeout
}

func (rf *Raft) listenStateChange() {
	for rf.killed() == false {
		select {
		case <-rf.leaderChan:
			rf.becomeLeader()
		case logIndex := <-rf.applyChan:
			rf.handleUpdateIndex(logIndex)
		}
	}
}

func (rf *Raft) handleUpdateIndex(logIndex int) {
	nextCommitId := rf.message.CommitIndex

	if rf.state == StateTypeLeader && rf.message.CommitIndex < logIndex {
		votes := 0
		for i := range rf.peers {
			if rf.message.matchIndex[i] >= logIndex {
				votes++
			}
			if votes > len(rf.peers)/2 {
				nextCommitId = logIndex
				rf.message.CommitIndex = nextCommitId
			}
		}
	}
	if nextCommitId > rf.message.LastApplied {
		//fmt.Printf("%d is applying logs from index %d to %d\n", rf.me, rf.message.LastApplied, rf.message.CommitIndex)
		i := rf.message.LastApplied
		for ; i < rf.message.CommitIndex && i < len(rf.LogState.Log); i++ {
			rf.applyFunc(ApplyMsg{
				CommandValid: true,
				Command:      rf.LogState.Log[i].Command,
				CommandIndex: rf.LogState.Log[i].Index,
			})
		}
		rf.message.LastApplied = i
	}

	go rf.persist()
}

func (rf *Raft) heartbeat() {
	for i := range rf.peers {
		if i == rf.me {
			rf.lastHeartBeat = time.Now().UnixMilli()
			continue
		}
		if rf.message.matchIndex[i] < rf.message.LogIndex {
			go rf.appendEntriesToPeer(i)
		} else {
			args := &AppendEntryArgs{
				Term:         rf.LogState.CurrentTerm,
				LeaderId:     rf.me,
				LeaderCommit: rf.message.CommitIndex,
			}
			reply := &AppendEntryReply{}
			go rf.sendAppendEntries(i, args, reply)
		}
	}

}
func (rf *Raft) electLeaders() {
	nextTerm, success := rf.becomeCandidate()
	if !success {
		return
	}

	lastLogIndex := rf.message.LogIndex
	lastLogTerm := 0
	if lastLogIndex > 0 {
		lastEntry := &rf.LogState.Log[rf.message.LogIndex-1]
		lastLogTerm = lastEntry.Term
	}
	arg := &RequestVoteArgs{
		Term:         nextTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		i := i
		go func() {
			reply := &RequestVoteReply{}
			rf.sendRequestVote(i, arg, reply)
			if reply.Term != rf.LogState.CurrentTerm {
				return
			}
			if reply.VoteGranted {
				if rf.state == StateTypeCandidate {
					rf.LogState.votes++
					if rf.LogState.votes > len(rf.peers)/2 {
						rf.leaderChan <- nextTerm
					}
				}
			} else {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > nextTerm {
					rf.becomeFollowerLocked(reply.Term, nil)
				}
			}
		}()
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
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,

		electionTimeout: 200,
		leaderChan:      make(chan int),
		applyChan:       make(chan int),
		applyFunc: func(msg ApplyMsg) {
			applyCh <- msg
		},
		state: StateTypeFollower,

		message: message{
			nextIndex:  make([]int, len(peers)),
			matchIndex: make([]int, len(peers)),
		},
	}

	// Your initialization code here (2A, 2B, 2C).

	//select {
	//case msg := <-applyCh:
	//	rf.apply(msg)
	//}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.listenStateChange()

	return rf
}

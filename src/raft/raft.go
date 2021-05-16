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
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

// It doesn't look like they have this in stdlib.
// go is trash.
func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ApplyMsg is used by the raft library to communicate with the state machine.
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	Logs         [](*RLog)
	processedCh  chan bool
}

const (
	follower  = iota
	candidate = iota
	leader    = iota
)

const (
	// Not sure how long 1 election will take?
	// Hopefully less than [timerLow]ms.
	heartBeat = 110
	timerLow  = 300
	timerHigh = 700
)

// RLog is a raft log entry.
type RLog struct {
	Command    interface{}
	AppendTerm int // This is the term a log is INITIALLY appended to a leader
}

// We'll start off with an empty [SnapShot].
// We need to keep the snapshot and the remaining log in sync.
type SnapShot struct {

	// [LogTerm], [LogIndex], [Kvs] must only be updated when taking a new snapshot.
	LogTerm  int // Term of log entry at [LogIndex].
	LogIndex int // The index applied by the kvs, right before Snapshotting.

	// Kvs is getting persisted on every persist. Might have to hack around that.
	// Kvs        map[string]string // kvs state at the time of the snapshot.
	KVEncoding []byte
}

type RaftPersistent struct {
	CurrentTerm int       // Starts out at 0
	VotedFor    int       // This is just [peers] index. -1 if haven't voted for anyone.
	RaftLog     [](*RLog) // Logs right after the snapshot.
}

// Raft is a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	ch        chan ApplyMsg

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state         int           // Starts out as follower
	lastContact   time.Time     // Don't need to sync time for these labs.
	timerDuration time.Duration // Keeps track of how long the current election timer is.

	// Invariant: [commitIndex] and [lastApplied] are within the nodes truncated log including
	// the snapshot [LogIndex].
	commitIndex int // Index known by this node to be committed
	lastApplied int // Index for log which was lastApplied by this node.

	PersistentState *RaftPersistent // raft persistent state.
	SnapShot        *SnapShot       // raft snapshot

	// This state is only valid for leaders once they
	// win an election and is only init after they win.
	// Invariant: [nextIndex] must always be greater than match index.
	// TODO: [matchIndex], [nextIndex] can be out of range. So, we need to make sure that
	// using them to access the log, doesn't result in an error.
	nextIndex  map[int]int // Index of the next log entry to send to a server.
	matchIndex map[int]int // Index of the highest log entry known to be replicated on a server.
}

// RequestVoteArgs is send to the peer we send [RequestVote] RPC to.
type RequestVoteArgs struct {
	CandidateTerm int
	CandidateID   int
	LastLogIndex  int
	LastLogTerm   int
}

// RequestVoteReply is filled in by the peer which we send [RequestVote] RPC to.
type RequestVoteReply struct {
	PeerTerm    int  // This is the term of the peer which candidate sent [RequestVote] to.
	VoteGranted bool // Whether the peer voted for the candidate.
}

// AppendEntriesArgs is sent to the peer in the
// [AppendEntries] RPC to.
// Note that only the leader should send these out.
// However, there are cases when previous leaders could
// potentially send these out. In that case, we reject
// the leader.
type AppendEntriesArgs struct {
	LeaderTerm        int
	LeaderID          int
	PrevLogIndex      int
	PrevLogTerm       int
	Entries           [](*RLog)
	LeaderCommitIndex int
	SnapShot          *SnapShot
}

// AppendEntriesReply returns some data
// from the peer we sent an [AppendEntries] RPC to.
type AppendEntriesReply struct {
	PeerTerm int
	Success  bool
}

// Basic check to see if current node has majority.
func (rf *Raft) hasMajority(n int) bool {
	return 2*n > len(rf.peers)
}

func (rf *Raft) Me() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.me
}

// Invariant: The slice and the kvs passed should have no other references
// which can be modified.
func (rf *Raft) sendSnapshot(ch chan bool, snap *SnapShot, logs [](*RLog)) {
	// Sends the latest snapshot up through append entries.
	rf.ch <- ApplyMsg{
		false,
		snap,
		-1,
		logs,
		ch,
	}
}

// Returns [true] if the kvserver should equip the log.
// Invariant: The slice and kvs passed in should have no other references which
// will be modified.
func (rf *Raft) MaybeInstallSnapshot(msg ApplyMsg) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	snap := msg.Command.(*SnapShot)

	if snap.LogIndex <= rf.SnapShot.LogIndex {
		// We've already installed this snapshot.
		return false
	}
	rf.SnapShot = &SnapShot{
		snap.LogTerm,
		snap.LogIndex,
		snap.KVEncoding,
	}
	rf.PersistentState.RaftLog = msg.Logs
	rf.lastApplied = snap.LogIndex
	rf.commitIndex = snap.LogIndex
	rf.persistWithSnap()

	defer func() {
		// Allow some other thread to reply success to the leader.
		msg.processedCh <- true
	}()

	return true
}

// Atomically takes a snap, modifies log and persists a snapshot to the persister.
// This function will copy the map, so it doesn't need to be copied by caller.
// Invariant: Acquire lock first.
// [appliedIndex] is 1-based.
func (rf *Raft) TakeSnapShot(kvEncoding []byte, appliedIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Covert to 0-based indexing.
	appliedIndex--

	// Note if, appliedIndex is behind the current snapshot, then
	// we can just return. Although, that should never happen.
	// SnapShots must always proceed forward in time.
	if rf.SnapShot.LogIndex >= appliedIndex {
		panic("New Snapshot position is not increasing.")
	}

	if rf.lastLogIndex() < appliedIndex {
		// We don't have the log entry to get the term.
		// But that log entry must've been committed and at one
		// point and we must've had it.
		// We've already applied beyond our log.
		// This shouldn't happen.
		// We should never lose committed entries.
		// So, this isn't possible.
		panic("We don't have this log entry. So, we can't snapshot.")
	}

	// [appliedIndex] is in range of the truncated log.
	rf.SnapShot = &SnapShot{
		rf.indexLog(appliedIndex).AppendTerm,
		appliedIndex,
		kvEncoding,
	}
	rf.PersistentState.RaftLog = rf.sliceLog(appliedIndex, rf.lastLogIndex()+1)
	rf.persistWithSnap()
}

// Converts a 0-based index in the overall log
// to an index in truncated log.
// Index must be in-range.
// Invariant: Acquire lock first
func (rf *Raft) baseIndex() int {
	return rf.SnapShot.LogIndex + 1
}

// Converts an index in the log to an index in the
// truncated log.
func (rf *Raft) indexInLog(n int) int {
	return n - rf.baseIndex()
}

// Slices the log [i, j). i, j are log
// indices in the complete log, but they are inrange
// of the truncated log once coverted.
func (rf *Raft) sliceLog(i, j int) [](*RLog) {
	return rf.PersistentState.RaftLog[rf.indexInLog(i):rf.indexInLog(j)]
}

// Get the log entry at the index.
// index is 0 based.
// index must be in-range.
// Invariant: Acquire lock first.
func (rf *Raft) indexLog(n int) *RLog {
	base := rf.baseIndex()
	return rf.PersistentState.RaftLog[n-base]
}

// Invariant: Acquire lock before calling this.
// Note: will only return -1, when no logs have been added.
func (rf *Raft) lastLogIndex() int {
	return rf.baseIndex() + len(rf.PersistentState.RaftLog) - 1
}

// TODO: for the following two functions, we need to
// consider the cases where we might get -1.
// Invariant: Acquire lock before calling this.
func (rf *Raft) aLogTerm(n int) int {
	base := rf.baseIndex()
	if n < base-1 || n > rf.lastLogIndex() {
		return -1
	} else if n == base-1 {
		return rf.SnapShot.LogTerm
	}
	return rf.indexLog(n).AppendTerm
}

// Invariant: Acquire lock before calling this.
// Note that this will only return -1 for the case
// where there have been no logs added.
func (rf *Raft) lastLogTerm() int {
	return rf.aLogTerm(rf.lastLogIndex())
}

// Checks periodically if timer has expired.
// This loop also ensures that we don't exit
// the program through main and kill all the procs.
func (rf *Raft) electionTimer() {
	for {
		if rf.killed() {
			return
		}
		// Check every 30 milliseconds if timer should go off.
		time.Sleep(time.Millisecond * time.Duration(30))
		go rf.startElection()
	}
}

// Inits the raft state for a leader just after
// it wins an election.
// Invariant: Acquire lock before calling this.
func (rf *Raft) initLeaderState() {
	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.lastLogIndex() + 1
		rf.matchIndex[i] = -1

		if i == rf.me {
			// But we have replicated everything in our own server.
			rf.matchIndex[i] = rf.lastLogIndex()
		}
	}
}

// Note that election is only allowed to commit its results
// if the term when it started is equal to the term when it gets
// the results back.
func (rf *Raft) startElection() {
	rf.mu.Lock()
	if time.Since(rf.lastContact) < rf.timerDuration || rf.state == leader {
		// Either the timer hasn't expired, or we became leader already.
		// This can happen if this is the second time the timer went off, but
		// just as the timer went off, we finished the old election.
		// The paper doesn't have a leader -> candidate transition anyway,
		// so I'm sure this is alright.
		rf.mu.Unlock()
		return
	}

	rf.PersistentState.CurrentTerm++
	electionTerm := rf.PersistentState.CurrentTerm
	rf.state = candidate
	rf.PersistentState.VotedFor = rf.me
	rf.persist()
	rf.resetTimer()

	replies := make(map[int](*RequestVoteReply))
	replyCh := make(chan int)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			// Don't want to send RPC to ourself.
			continue
		}
		req := &RequestVoteArgs{electionTerm, rf.me, rf.lastLogIndex(), rf.lastLogTerm()}
		rep := &RequestVoteReply{}
		replies[i] = rep
		go rf.sendRequestVote(replyCh, i, req, rep)
	}

	// Defining this here before unlocking so
	// that the race detector doesn't complain.
	wait := len(rf.peers) - 1
	rf.mu.Unlock()

	// Start off with 1 vote.
	numVotes := 1
	justReturn := false
	for wait > 0 {
		// Should get exactly len(peers) - 1 replies
		replyFrom := <-replyCh
		rf.mu.Lock()
		if !justReturn {
			if rf.state != candidate || rf.PersistentState.CurrentTerm > electionTerm {
				// We're no longer a candidate or we started another election
				// eitherway, this election no longer matters.
				justReturn = true
			} else if replies[replyFrom].PeerTerm > electionTerm {
				rf.PersistentState.CurrentTerm = replies[replyFrom].PeerTerm
				rf.state = follower
				rf.PersistentState.VotedFor = -1
				justReturn = true
				rf.persist()
			} else if replies[replyFrom].VoteGranted {
				numVotes++
			}

			if !justReturn && rf.hasMajority(numVotes) {
				// Majority voted us in.
				rf.state = leader
				rf.initLeaderState()
				go rf.sendAppendEntries()
				// Majority voted us in. I don't think we need to
				// care about other votes for this election.
				justReturn = true

				// fmt.Println("have become leader", rf.me)
			}
		}
		rf.mu.Unlock()
		wait--
	}
}

// Not sure if the same func will be used to send
// log append entries too. Right now, just using
// this to send heartbeat.
func (rf *Raft) sendAppendEntries() {
	for {
		if rf.killed() {
			return
		}
		time.Sleep(time.Duration(heartBeat) * time.Millisecond)
		go rf.sendHearts()
	}
}

func copyMap(mm map[string]string) map[string]string {
	nm := make(map[string]string)
	for k, v := range nm {
		nm[k] = v
	}
	return nm
}

// Returns the appendEntry structs to send to the peer.
// Invariant: acquire lock before calling
func (rf *Raft) appendEntryStruct(i int) (*AppendEntriesArgs, *AppendEntriesReply) {
	prevLogIndex := rf.nextIndex[i] - 1
	var prevLogTerm int
	var req *AppendEntriesArgs
	rep := &AppendEntriesReply{}
	if prevLogIndex < rf.SnapShot.LogIndex {
		prevLogTerm = rf.SnapShot.LogTerm
		prevLogIndex = rf.SnapShot.LogIndex
		snap := &SnapShot{
			LogTerm:    prevLogTerm,
			LogIndex:   prevLogIndex,
			KVEncoding: rf.SnapShot.KVEncoding,
		}
		var entries [](*RLog)
		log := make([]*RLog, len(rf.PersistentState.RaftLog))
		copy(log, rf.PersistentState.RaftLog)
		entries = log
		req = &AppendEntriesArgs{
			rf.PersistentState.CurrentTerm, rf.me, prevLogIndex, prevLogTerm, entries, rf.commitIndex, snap}
	} else {
		prevLogTerm = rf.aLogTerm(prevLogIndex)
		var entries [](*RLog)
		if rf.lastLogIndex() >= rf.nextIndex[i] {
			log := make([]*RLog, rf.lastLogIndex()+1-rf.nextIndex[i])
			copy(log, rf.sliceLog(rf.nextIndex[i], rf.lastLogIndex()+1))
			entries = log
		}
		req = &AppendEntriesArgs{
			rf.PersistentState.CurrentTerm, rf.me, prevLogIndex, prevLogTerm, entries, rf.commitIndex, nil}
	}
	return req, rep
}

// TODO: fix this
func (rf *Raft) sendHearts() {
	rf.mu.Lock()
	if rf.state != leader {
		// If we're not the leader, then
		// we have no business sending these.
		rf.mu.Unlock()
		return
	}

	ch := make(chan int)
	replies := make(map[int](*AppendEntriesReply))
	allArgs := make(map[int](*AppendEntriesArgs))
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		req, rep := rf.appendEntryStruct(i)
		replies[i] = rep
		allArgs[i] = req
		go rf.sendEntries(ch, i, req, rep)
	}

	wait := len(rf.peers) - 1
	leaderTerm := rf.PersistentState.CurrentTerm
	rf.mu.Unlock()
	justReturn := false
	for wait > 0 {
		// We got a reply from a peer.
		replyFrom := <-ch
		rf.mu.Lock()

		if rf.state != leader || rf.PersistentState.CurrentTerm != leaderTerm {
			justReturn = true
		}
		if !justReturn && replies[replyFrom].PeerTerm > rf.PersistentState.CurrentTerm {
			// Discovered a new term once again.
			rf.PersistentState.VotedFor = -1
			rf.state = follower
			rf.PersistentState.CurrentTerm = replies[replyFrom].PeerTerm
			justReturn = true
			rf.persist()
		}
		if !justReturn {
			// We're still the leader and we got a success from
			// a follower, so we need to update the match and commit
			// index of the follower.
			args := allArgs[replyFrom]
			if replies[replyFrom].Success {
				rf.matchIndex[replyFrom] = max(rf.matchIndex[replyFrom], args.PrevLogIndex+len(args.Entries))
				rf.nextIndex[replyFrom] = max(rf.nextIndex[replyFrom], rf.matchIndex[replyFrom]+1)
			} else {
				// If this didn't match, then try to fix it.
				// We could potentially try a binary search here.
				// newNext := args.PrevLogIndex
				lo := rf.matchIndex[replyFrom] + 1
				hi := args.PrevLogIndex
				if lo <= hi {
					newNext := lo + (hi-lo)/2
					// newNext := rf.matchIndex[replyFrom] + 1
					if newNext > rf.matchIndex[replyFrom] {
						// Just in case we got success from a different RPC.
						rf.nextIndex[replyFrom] = newNext
					}
				}
			}
		}
		rf.mu.Unlock()
		wait--
	}

}

// Resets the election timer.
// Invariant: This should only be called after
// acquiring the lock.
func (rf *Raft) resetTimer() {
	rf.lastContact = time.Now()
	sleepFor := timerLow + rand.Int31n(timerHigh-timerLow)
	rf.timerDuration = time.Millisecond * time.Duration(sleepFor)
}

// AppendEntries is the handler for the peer which RECEIVES an append entry rpc.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.PeerTerm = rf.PersistentState.CurrentTerm
	if args.LeaderTerm < rf.PersistentState.CurrentTerm {
		// We can't consider this guy the current
		// leader.
		return
	}

	if args.LeaderTerm > rf.PersistentState.CurrentTerm {
		// We discovered a new term, so reset VotedFor.
		rf.PersistentState.VotedFor = -1
		rf.persist()
	}

	// Okay, this guy is the leader for us.
	if rf.state == candidate {
		// We received an append entry from a node
		// which has the same or > term number. I think
		// we should accept this dude as the leader.
		if args.LeaderTerm >= rf.PersistentState.CurrentTerm {
			rf.state = follower
		}
	} else if rf.state == leader {
		// We received an append entry from a node
		// which has a term >= leader.
		// Can we receive an append entry from a node
		// with the same term as us?
		// If that node sends out ae, then it think it's the leader
		// for that term. But we got voted in for that term for sure.
		// So, it can't be voted in, and it can't send out ae.
		// This is hacky, cause we're basically copying over
		// the entire log. Don't want to send RPCs one entry
		// at a time either, so implement a faster method.
		if args.LeaderTerm > rf.PersistentState.CurrentTerm {
			rf.state = follower
		} else if args.LeaderTerm == rf.PersistentState.CurrentTerm {
			fmt.Println("huh", args, rf)
			panic("Another node send ae to leader with same term.")
		}
	}
	rf.PersistentState.CurrentTerm = args.LeaderTerm
	rf.persist()
	// At this point, we accept a valid leader, so we need
	// to reset the timer.
	if args.SnapShot != nil {
		installCh := make(chan bool)
		go rf.sendSnapshot(installCh, args.SnapShot, args.Entries)
		// At this point we either took over the snapshot or not.
		reply.Success = <-installCh
	} else {
		reply.Success = rf.updateAfterHearbeat(args)
	}
	rf.resetTimer()
}

// Invariant: Hold lock.
// Note that the leader does have the authority to do whatever it wants with our
// logs.
func (rf *Raft) updateAfterHearbeat(args *AppendEntriesArgs) bool {
	if args.PrevLogIndex < rf.SnapShot.LogIndex || args.PrevLogIndex > rf.lastLogIndex() {
		// We've truncated the log so we can't compare terms.
		return false
	}

	// At this point, we know that [args.PrevLogIndex] is within
	// the truncated log.
	if rf.aLogTerm(args.PrevLogIndex) != args.PrevLogTerm {
		// No match.
		return false
	}

	// PrevLogTerm was a match, so the Logs are in sync upto
	// that point.
	for i := 0; i < len(args.Entries); i++ {
		ii := args.PrevLogIndex + 1 + i
		if rf.indexInLog(ii) < rf.lastLogIndex()+1 {
			if rf.aLogTerm(ii) != args.Entries[i].AppendTerm {
				// We're not in sync.
				rf.PersistentState.RaftLog = rf.sliceLog(rf.baseIndex(), ii)
				rf.PersistentState.RaftLog = append(rf.PersistentState.RaftLog, args.Entries[i])
			}
			// There's a match, so the log must be the same.
		} else {
			rf.PersistentState.RaftLog = append(rf.PersistentState.RaftLog, args.Entries[i])
		}
	}
	rf.persist()
	if args.LeaderCommitIndex > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommitIndex, args.PrevLogIndex+len(args.Entries))
	}
	return true
}

func (rf *Raft) sendRequestVote(
	replyCh chan<- int, server int, args *RequestVoteArgs, reply *RequestVoteReply) {

	_ = rf.peers[server].Call("Raft.RequestVote", args, reply)
	replyCh <- server
}

func (rf *Raft) sendEntries(
	replyCh chan<- int, server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	_ = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	replyCh <- server
}

// RequestVote is the handler for the peer which RECEIVES
// a request vote.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.PeerTerm = rf.PersistentState.CurrentTerm
	if args.CandidateTerm < rf.PersistentState.CurrentTerm {
		reply.VoteGranted = false
		return
	}

	// A new election implies that old leader could
	// potentially be useless. We don't want to take requests
	// from that leader anymore.
	if args.CandidateTerm > rf.PersistentState.CurrentTerm {
		// We ran into someone with a higher term.
		// So, we're guaranteed to accept this as a new term.
		// So, we reset VotedFor.
		rf.state = follower
		rf.PersistentState.VotedFor = -1
	}

	rf.PersistentState.CurrentTerm = args.CandidateTerm
	rf.persist()
	notAlreadyVoted := rf.PersistentState.VotedFor == -1 || rf.PersistentState.VotedFor == args.CandidateID
	canVote := rf.lastLogTerm() == args.LastLogTerm && rf.lastLogIndex() <= args.LastLogIndex
	canVote = canVote || rf.lastLogTerm() < args.LastLogTerm
	reply.VoteGranted = notAlreadyVoted && canVote
	if reply.VoteGranted {
		rf.PersistentState.VotedFor = args.CandidateID
		rf.resetTimer()
	}
}

// Start is called when a kvserver wants to start
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != leader {
		return -1, -1, false
	}

	// We kinda just let this be taken to the other nodes
	// along with a heartbeat. We don't want to do extra shit!
	// TODO: Might need to send heatbeats from here for speedup.
	newLog := &RLog{command, rf.PersistentState.CurrentTerm}
	rf.PersistentState.RaftLog = append(rf.PersistentState.RaftLog, newLog)
	rf.persist()

	// We also keep track of matchIndex for ourselves.
	rf.matchIndex[rf.me]++
	// Note that we're using 0 based index, but the client expects
	// 1 based indexing.
	go rf.sendHearts()
	return rf.lastLogIndex() + 1, rf.PersistentState.CurrentTerm, true
}

func (rf *Raft) periodicallyApply(ch chan ApplyMsg) {
	for {
		if rf.killed() {
			return
		}
		// TODO: Check if the sleep constant needs to be tuned.
		time.Sleep(time.Millisecond * time.Duration(30))

		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex {
			// Once incremented, [lastApplied] should be within truncated log,
			// acc to the invariants.
			rf.lastApplied++
			toSend := ApplyMsg{
				true,
				rf.indexLog(rf.lastApplied).Command,
				rf.lastApplied + 1,
				nil,
				nil,
			}

			// Again, we need to expose index + 1 for the tests
			// since it expects 1 based indexing.
			rf.mu.Unlock()
			ch <- toSend
			rf.mu.Lock()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) periodicallyUpdateCommitIndex() {
	for {
		if rf.killed() {
			return
		}
		// TODO: Check if the sleep constant needs to be tuned.
		time.Sleep(time.Millisecond * time.Duration(30))
		rf.mu.Lock()
		if rf.state == leader {
			var indices []int

			// Basically stores log index in leader ->
			// number of nodes which the leader thinks
			// have that log entry.
			// We only care for log indices which are greater
			// than commitIndex.
			var accumIndices []int
			for i := 0; i < len(rf.PersistentState.RaftLog); i++ {
				indices = append(indices, 0)
				accumIndices = append(accumIndices, 0)
			}

			// matchIndex[i] might not be within the truncated log.
			// It can't be greater than the last log index however.
			for i := 0; i < len(rf.matchIndex); i++ {
				// If [matchIndex[i]] is > [commitIndex], and we know that
				// the [matchIndex[i]] is within the log.
				if rf.matchIndex[i] > rf.commitIndex {
					indices[rf.indexInLog(rf.matchIndex[i])]++
				}
			}

			// TODO: Make sure that matchIndex is also upto date for the leader.
			// Iterate from the back to get the first valid log index
			// which is the largest.
			for i := len(indices) - 1; i >= 0; i-- {
				if i == len(indices)-1 {
					accumIndices[i] = indices[i]
				} else {
					accumIndices[i] = accumIndices[i+1] + indices[i]
				}
				if rf.hasMajority(accumIndices[i]) &&
					rf.indexLog(i+rf.baseIndex()).AppendTerm == rf.PersistentState.CurrentTerm {
					// This is the largest index where the leader can be sure that
					// it exists on all the servers.
					rf.commitIndex = i + rf.baseIndex()
					break
				}
			}
		}
		rf.mu.Unlock()
	}
}

// Kill long running go routines
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// GetState returns current term and whether the server
// thinks it's the leader. I guess this is used by
// the test library.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.PersistentState.CurrentTerm, (rf.state == leader)
}

func (rf *Raft) persistentEncode() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.PersistentState)
	return w.Bytes()
}

// Invariant: [data] is non-nil.
func (rf *Raft) persistentDecode(data []byte) *RaftPersistent {
	// will have to decode/encode the byte slice.
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var PersistentState *RaftPersistent
	d.Decode(&PersistentState)
	return PersistentState
}

func (rf *Raft) snapEncode() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.SnapShot)
	return w.Bytes()
}

func (rf *Raft) snapDecode(data []byte) *SnapShot {
	// will have to decode/encode the byte slice.
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var SnapShot *SnapShot
	d.Decode(&SnapShot)
	return SnapShot
}

func (rf *Raft) LoadSnapshot() *SnapShot {
	return rf.readSnap(rf.persister.ReadSnapshot())
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// Invariant: Acquire lock before calling this.
func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.persistentEncode())
}

// Invariant: Acquire lock.
func (rf *Raft) persistWithSnap() {
	rf.persister.SaveStateAndSnapshot(rf.persistentEncode(), rf.snapEncode())
}

// restore previously persisted state.
// We do this before any threads are launched so
// it's safe to not acquire locks here.
func (rf *Raft) setPersistent(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	rf.PersistentState = rf.persistentDecode(data)
}

func (rf *Raft) readSnap(data []byte) *SnapShot {
	if data == nil || len(data) < 1 {
		return nil
	}
	return rf.snapDecode(data)
}

// We do this before any threads are launched so
// it's safe to not acquire locks here.
func (rf *Raft) setSnap(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	rf.SnapShot = rf.snapDecode(data)
}

// Make sure that the SnapShot is set.
func initPersistent(rf *Raft) {
	rf.PersistentState = &RaftPersistent{
		CurrentTerm: 0,
		VotedFor:    -1,
	}
	rf.SnapShot = &SnapShot{
		LogTerm:    -1,
		LogIndex:   -1,
		KVEncoding: []byte{},
	}

	// initialize from state persisted before a crash
	rf.setPersistent(rf.persister.ReadRaftState())

	// TODO: We're loading the snapshot here, but we need to make sure
	// that the kvserver has already installed the snapshot, before processing
	// any commands.
	rf.setSnap(rf.persister.ReadSnapshot())

	// Commit index and lastApplied while not persistent,
	// depend on persistent state.
	rf.commitIndex = rf.SnapShot.LogIndex
	rf.lastApplied = rf.SnapShot.LogIndex
}

func initNonPersistent(rf *Raft) {
	rf.state = follower
}

// Make is used by the service or tester wants to create a Raft server. the ports
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

	// Init the invariant state.
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.ch = applyCh

	initNonPersistent(rf)
	initPersistent(rf)

	// We need to start the election timer here.
	rf.resetTimer()
	go rf.electionTimer()
	go rf.periodicallyApply(applyCh)
	go rf.periodicallyUpdateCommitIndex()
	return rf
}

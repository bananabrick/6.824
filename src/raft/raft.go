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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

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

const (
	follower  = iota
	candidate = iota
	leader    = iota
)

const (
	// Not sure how long 1 election will take?
	// Hopefully less than [timerLow]ms.
	heartBeat = 150
	timerLow  = 450
	timerHigh = 700
)

type raftLog struct {
	Command    interface{}
	AppendTerm int // This is the term a log is INITIALLY appended to a leader
}

// Raft is a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm   int           // Starts out at 0
	votedFor      int           // This is just [peers] index. -1 if haven't voted for anyone.
	state         int           // Starts out as follower
	lastContact   time.Time     // Don't need to sync time for these labs.
	timerDuration time.Duration // Keeps track of how long the current election timer is.
	logs          [](*raftLog)
	commitIndex   int // Index known by this node to be committed
	lastApplied   int // Index for log which was lastApplied by this node.

	// This state is only valid for leaders once they
	// win an election and is only init after they win.
	// Invariant: nextIndex must always be greater than match index.
	// Except for the leader in which case nextIndex is useless.
	nextIndex  map[int]int // Index of the next log entry to send to a server.
	matchIndex map[int]int // Index of the highest log entry known to be replicated on a server.
}

// Me is used for some whack debugging.
func (rf *Raft) Me() int {
	return rf.me
}

// Basic check to see if current node has majority.
func (rf *Raft) hasMajority(n int) bool {
	return 2*n > len(rf.peers)
}

// GetState returns current term and whether the server
// thinks it's the leader. I guess this is used by
// the test library.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, (rf.state == leader)
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

// RequestVoteArgs is send to the peer we send [RequestVote] RPC to.
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandidateTerm int
	CandidateID   int
	LastLogIndex  int
	LastLogTerm   int
}

// Invariant: Acquire lock before calling this.
func (rf *Raft) lastLogIndex() int {
	return len(rf.logs) - 1
}

// Invariant: Acquire lock before calling this.
func (rf *Raft) aLogTerm(n int) int {
	if n < 0 || n >= len(rf.logs) {
		return -1
	}
	return rf.logs[n].AppendTerm
}

// Invariant: Acquire lock before calling this.
func (rf *Raft) lastLogTerm() int {
	return rf.aLogTerm(len(rf.logs) - 1)
}

// RequestVoteReply is filled in by the peer which we send [RequestVote] RPC to.
type RequestVoteReply struct {
	// Your data here (2A).
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
	Entries           [](*raftLog)
	LeaderCommitIndex int
}

// AppendEntriesReply returns some data
// from the peer we sent an [AppendEntries] RPC to.
type AppendEntriesReply struct {
	PeerTerm int
	Success  bool
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
func (rf *Raft) initLeaderState() {
	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.lastLogIndex() + 1

		// Assume nothing is replicated on any server.
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
	if time.Now().Sub(rf.lastContact) < rf.timerDuration || rf.state == leader {
		// Either the timer hasn't expired, or we became leader already.
		// This can happen if this is the second time the timer went off, but
		// just as the timer went off, we finished the old election.
		// The paper doesn't have a leader -> candidate transition anyway,
		// so I'm sure this is alright.
		rf.mu.Unlock()
		return
	}

	rf.currentTerm++
	electionTerm := rf.currentTerm
	rf.state = candidate
	rf.votedFor = rf.me
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
			if rf.state != candidate || rf.currentTerm > electionTerm {
				// We're no longer a candidate or we started another election
				// eitherway, this election no longer matters.
				justReturn = true
			} else if replies[replyFrom].PeerTerm > electionTerm {
				rf.currentTerm = replies[replyFrom].PeerTerm
				rf.state = follower
				rf.votedFor = -1
				justReturn = true
			} else if replies[replyFrom].VoteGranted == true {
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
		prevLogIndex := rf.nextIndex[i] - 1
		prevLogTerm := rf.aLogTerm(prevLogIndex)
		var entries [](*raftLog)
		if rf.lastLogIndex() >= rf.nextIndex[i] {
			entries = rf.logs[rf.nextIndex[i]:]
		}
		req := &AppendEntriesArgs{
			rf.currentTerm, rf.me, prevLogIndex, prevLogTerm, entries, rf.commitIndex}
		rep := &AppendEntriesReply{}
		replies[i] = rep
		allArgs[i] = req
		go rf.sendEntries(ch, i, req, rep)
	}

	wait := len(rf.peers) - 1
	rf.mu.Unlock()
	justReturn := false
	for wait > 0 {
		// We got a reply from a peer.
		replyFrom := <-ch
		rf.mu.Lock()
		// TODO: this might be potentially, buggy.
		if rf.state != leader {
			justReturn = true
		}
		if !justReturn && replies[replyFrom].PeerTerm > rf.currentTerm {
			// Discovered a new term once again.
			rf.votedFor = -1
			rf.state = follower
			rf.currentTerm = replies[replyFrom].PeerTerm
			justReturn = true
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
				newNext := args.PrevLogIndex
				if newNext > rf.matchIndex[replyFrom] {
					// Just in case we got success from a different RPC.
					rf.nextIndex[replyFrom] = newNext
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

	reply.PeerTerm = rf.currentTerm
	if args.LeaderTerm < rf.currentTerm {
		// We can't consider this guy the current
		// leader.
		return
	}

	if args.LeaderTerm > rf.currentTerm {
		// We discovered a new term, so reset votedFor.
		rf.votedFor = -1
	}

	// Okay, this guy is the leader for us.
	if rf.state == candidate {
		// We received an append entry from a node
		// which has the same or > term number. I think
		// we should accept this dude as the leader.
		if args.LeaderTerm >= rf.currentTerm {
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
		if args.LeaderTerm > rf.currentTerm {
			rf.state = follower
		} else if args.LeaderTerm == rf.currentTerm {
			fmt.Println("huh", args, rf)
			panic("Another node send ae to leader with same term.")
		}
	}
	rf.currentTerm = args.LeaderTerm
	// At this point, we accept a valid leader, so we don't need
	// to reset the heartbeat.
	reply.Success = rf.updateAfterHearbeat(args)
	rf.resetTimer()
}

// Invariant: Hold lock.
func (rf *Raft) updateAfterHearbeat(args *AppendEntriesArgs) bool {
	if rf.aLogTerm(args.PrevLogIndex) != args.PrevLogTerm {
		// No match.
		return false
	}

	// PrevLogTerm was a match, so the logs are in sync upto
	// that point.
	for i := 0; i < len(args.Entries); i++ {
		ii := args.PrevLogIndex + 1 + i
		if ii < len(rf.logs) {
			if rf.aLogTerm(ii) != args.Entries[i].AppendTerm {
				// We're not in sync.
				rf.logs = rf.logs[:ii]
				rf.logs = append(rf.logs, args.Entries[i])
			}
			// There's a match, so the log must be the same.
		} else {
			rf.logs = append(rf.logs, args.Entries[i])
		}
	}

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
	reply.PeerTerm = rf.currentTerm
	if args.CandidateTerm < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	// A new election implies that old leader could
	// potentially be useless. We don't want to take requests
	// from that leader anymore.
	if args.CandidateTerm > rf.currentTerm {
		// We ran into someone with a higher term.
		// So, we're guaranteed to accept this as a new term.
		// So, we reset votedFor.
		rf.state = follower
		rf.votedFor = -1
	}

	rf.currentTerm = args.CandidateTerm
	notAlreadyVoted := rf.votedFor == -1 || rf.votedFor == args.CandidateID
	canVote := rf.lastLogTerm() == args.LastLogTerm && rf.lastLogIndex() <= args.LastLogIndex
	canVote = canVote || rf.lastLogTerm() < args.LastLogTerm
	reply.VoteGranted = notAlreadyVoted && canVote
	if reply.VoteGranted {
		rf.votedFor = args.CandidateID
		rf.resetTimer()
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
	defer rf.mu.Unlock()

	if rf.state != leader {
		return -1, -1, false
	}

	// We kinda just let this be taken to the other nodes
	// along with a heartbeat. We don't want to do extra shit!
	newLog := &raftLog{command, rf.currentTerm}
	rf.logs = append(rf.logs, newLog)

	// We also keep track of matchIndex for ourselves.
	rf.matchIndex[rf.me]++
	// Note that we're using 0 based index, but the client expects
	// 1 based indexing.
	return len(rf.logs), rf.currentTerm, true
}

func (rf *Raft) periodicallyApply(ch chan ApplyMsg) {
	for {
		if rf.killed() {
			return
		}
		// TODO: Check if the sleep constant needs to be tuned.
		time.Sleep(time.Millisecond * time.Duration(30))
		rf.mu.Lock()
		if rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			// Again, we need to expose index + 1 for the tests
			// since it expects 1 based indexing.
			ch <- ApplyMsg{true, rf.logs[rf.lastApplied].Command, rf.lastApplied + 1}
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
			for i := 0; i < len(rf.logs); i++ {
				indices = append(indices, 0)
				accumIndices = append(accumIndices, 0)
			}

			// matchIndex is either -1, or it points to
			// an item in the log. So, it should fit within
			// the bounds of the indices array.
			for i := 0; i < len(rf.matchIndex); i++ {
				if rf.matchIndex[i] > rf.commitIndex {
					indices[rf.matchIndex[i]]++
				}
			}

			// TODO: Make sure that matchIndex is also upto date for the leader.
			// Iterate from the back to get the firs valid log index
			// which is the largest.
			for i := len(indices) - 1; i >= 0; i-- {
				if i == len(indices)-1 {
					accumIndices[i] = indices[i]
				} else {
					accumIndices[i] = accumIndices[i+1] + indices[i]
				}
				if rf.hasMajority(accumIndices[i]) && rf.logs[i].AppendTerm == rf.currentTerm {
					// This is the largest index where the leader can be sure that
					// it exists on all the servers.
					rf.commitIndex = i
					break
				}
			}
		}
		rf.mu.Unlock()
	}
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = follower
	rf.commitIndex = -1
	rf.lastApplied = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// We need to start the election timer here.
	rf.resetTimer()
	go rf.electionTimer()
	go rf.periodicallyApply(applyCh)
	go rf.periodicallyUpdateCommitIndex()
	return rf
}

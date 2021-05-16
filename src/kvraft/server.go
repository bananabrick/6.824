package kvraft

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"

	"../labgob"
	"../labrpc"
	"../raft"
)

type commandT string
type clientIDT int64

const (
	putCommand    commandT = "put"
	appendCommand commandT = "append"
	noCommand     commandT = "no"
)

// Op is the command sent to the raft library.
type Op struct {
	CommandOp   commandT
	Key         string
	Value       string
	SequenceNum int // This is just the id used by the [Clerk] so that we can deduplicate requests.
	ClientID    clientIDT
}

type pendingCommit struct {
	expectedIndex int
	clientID      clientIDT
	sequenceNum   int
	// Send true if the applyMsg matches this, otherwise send false.
	waitCh chan bool
}

type SnapshottableState struct {
	// This is the core key-value store state.
	kvs           map[string]string
	lastProcessed int // 1-based command in the log which has been processed or/and applied.
	// Highest sequence number from the client which has been applied.
	// Note that since the client only makes one request at a time,
	// the sequence numbers which are returned from the [applyCh] are
	// non-decreasing. For that reason, we can just ignore sequence nums
	// which are lower than sequence applied.
	sequenceApplied map[clientIDT]int
}

// KVServer runs on top of a single raft instance. It processes
// requests and responses.
type KVServer struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	maxraftstate int   // snapshot if log grows this big

	// If a commit is in here, then we know that we've started agreement, but not
	// yet processed a reply that SOMETHING got committed at the expected index.
	pendingCommits     map[int]*pendingCommit
	SnapshottableState *SnapshottableState
}

// func (rf *Raft) snapEncode() []byte {
// 	w := new(bytes.Buffer)
// 	e := labgob.NewEncoder(w)
// 	e.Encode(rf.SnapShot)
// 	return w.Bytes()
// }

// func (rf *Raft) snapDecode(data []byte) *SnapShot {
// 	// will have to decode/encode the byte slice.
// 	r := bytes.NewBuffer(data)
// 	d := labgob.NewDecoder(r)
// 	var SnapShot *SnapShot
// 	d.Decode(&SnapShot)
// 	return SnapShot
// }

func (kv *KVServer) encodeSnapshottable() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.SnapshottableState)
	return w.Bytes()
}

// Assumes data is non-empty.
func (kv *KVServer) decodeSnapshottable(data []byte) *SnapshottableState {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var SnapshottableState *SnapshottableState
	err := d.Decode(&SnapshottableState)
	if err != nil {
		panic("can't decode snapshottable")
	}
	return SnapshottableState
}

// Invariant: acquire lock first.
func (kv *KVServer) KVSnapshot() {
	data := kv.encodeSnapshottable()
	kv.rf.TakeSnapShot(data, kv.SnapshottableState.lastProcessed)
}

func (kv *KVServer) maybeLoadFromSnap() {
	snapshot := kv.rf.LoadSnapshot()
	if snapshot == nil {
		return
	}
	kv.SnapshottableState = kv.decodeSnapshottable(snapshot.KVEncoding)
}

func (kv *KVServer) initSnapshottable() {
	kv.SnapshottableState = &SnapshottableState{}
	kv.SnapshottableState.kvs = make(map[string]string)
	kv.SnapshottableState.lastProcessed = 0 // haven't processed any.
	kv.SnapshottableState.sequenceApplied = make(map[clientIDT]int)

	kv.maybeLoadFromSnap()
}

// StartKVServer is used to start a kvserver.
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.pendingCommits = make(map[int]*pendingCommit)

	kv.initSnapshottable()

	go kv.readFromApplyCh()
	return kv
}

// Invariant: pendingCommit != nil, op != nil
func commitSame(pending *pendingCommit, op *Op) bool {
	same := pending.clientID == op.ClientID
	same = same && pending.sequenceNum == op.SequenceNum
	return same
}

func (kv *KVServer) handleCommand(commitedMsg *raft.ApplyMsg) {
	opCommitted := (commitedMsg.Command).(Op)
	kv.mu.Lock()
	if commitedMsg.CommandIndex != kv.SnapshottableState.lastProcessed+1 {
		// if [CommandIndex] is less, then we've already processed this entry.
		// if [CommandIndex] is greater, then at least one log entry was skipped.
		// It could potentially happen if a snapshot is installed.
		kv.mu.Unlock()
		return
	}

	expectedCommit := kv.pendingCommits[commitedMsg.CommandIndex]
	if kv.SnapshottableState.sequenceApplied[opCommitted.ClientID] < opCommitted.SequenceNum {
		// Not in pending ack, and has never been removed from pending ack.
		// We know that it hasn't been removed from pending ack, cause we remove
		// from pending ack iff SequenceNum <= highest akced sequence num.
		// So, we can be sure that this has never been applied to state machine.
		if opCommitted.CommandOp == putCommand {
			// Just put it.
			kv.SnapshottableState.kvs[opCommitted.Key] = opCommitted.Value
		} else if opCommitted.CommandOp == appendCommand {
			value, ok := kv.SnapshottableState.kvs[opCommitted.Key]
			if ok {
				// Key already exists.
				kv.SnapshottableState.kvs[opCommitted.Key] = value + opCommitted.Value
			} else {
				kv.SnapshottableState.kvs[opCommitted.Key] = opCommitted.Value
			}
		}
		kv.SnapshottableState.sequenceApplied[opCommitted.ClientID] = opCommitted.SequenceNum
	}

	kv.SnapshottableState.lastProcessed = commitedMsg.CommandIndex

	// TODO: Remove from expectedCommit.
	if expectedCommit == nil {
		// [expectedCommit] was nil. So, we weren't expecting this either
		// due to a failure, or due to a different kvserver committing this.
		// Either way, we can't send a response.
		// Client may retry, but that request won't get double committed.
		kv.mu.Unlock()
	} else if commitSame(expectedCommit, &opCommitted) {
		// This commit originated at this node, so send response back to
		// [Clerk] that this is committed. No other node will send this response back.
		// This could be a double send to the [Clerk]. But that's fine.
		delete(kv.pendingCommits, commitedMsg.CommandIndex)
		kv.mu.Unlock()
		expectedCommit.waitCh <- true
	} else {
		// Commit doesn't match the expected commit, so some other node
		// must have committed this.
		// It's that nodes responsibility to reply to the client.
		delete(kv.pendingCommits, commitedMsg.CommandIndex)
		kv.mu.Unlock()
		expectedCommit.waitCh <- false
	}
}

func (kv *KVServer) handleRaftSentSnap(commitedMsg *raft.ApplyMsg) {

}

func (kv *KVServer) readFromApplyCh() {
	for {
		if kv.killed() {
			return
		}

		// TODO: try to install snapshots, and ignore commands <= lastApplied to the kvserver.

		// This isn't a datarace since applyCh var is only ever
		// read from.
		commitedMsg := <-kv.applyCh
		if !commitedMsg.CommandValid {
			// We've received a snapshot from raft.
			// We can ask raft if we should upload this.
			panic("shouldn't be sent.")
			kv.handleRaftSentSnap(&commitedMsg)
		} else {
			kv.handleCommand(&commitedMsg)
		}
	}
}

// Get RPC handler
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()

	// We're going to try and commit a no-op so that we can
	// be sure that the leader is at least up to date with all of the
	// responses which could've been sent back to the client before
	// this request was made.
	newOp := Op{
		CommandOp:   noCommand,
		Key:         args.Key,
		Value:       "",
		SequenceNum: args.SequenceNum,
		ClientID:    clientIDT(args.ClientID),
	}

	cid := clientIDT(args.ClientID)

	expIndex, _, isLeader := kv.rf.Start(newOp)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.Value = ""
		kv.mu.Unlock()
		return
	}

	pendingCommit := &pendingCommit{
		expectedIndex: expIndex,
		clientID:      cid,
		sequenceNum:   args.SequenceNum,
		waitCh:        make(chan bool),
	}

	// Could there be another pendingCommit at [expIndex]?
	// Assume yes. Then, it must've been added at some previous request.
	// The fact that it hasn't been removed implies that, we didn't get a resp
	// from apply ch yet for ANY commit, but since the log just added another op
	// at the index, the old op will never get committed.
	// So, we just write [false] to the channel.

	prevOp, ok := kv.pendingCommits[expIndex]
	if ok {
		// There must be a goroutine waiting to read this, cause otherwise
		// we already wrote to this channel. But if we write to the channel
		// we also remove the entry from the map.
		prevOp.waitCh <- false
	}

	kv.pendingCommits[expIndex] = pendingCommit
	kv.mu.Unlock()

	worked := <-pendingCommit.waitCh

	kv.mu.Lock()
	defer kv.mu.Unlock()
	if worked {
		// The op worked, so we can do a read.
		value, ok := kv.SnapshottableState.kvs[args.Key]
		if ok {
			reply.Err = OK
			reply.Value = value
		} else {
			reply.Err = ErrNoKey
			reply.Value = ""
		}
	} else {
		// The op didn't get committed. The person will retry.
		reply.Err = ErrWrongLeader
		reply.Value = ""
	}
}

// PutAppend handles put and append RPCs.
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()

	// We're going to try and commit a no-op so that we can
	// be sure that the leader is at least up to date with all of the
	// responses which could've been sent back to the client before
	// this request was made.
	var command commandT
	if args.Op == putOp {
		command = putCommand
	} else if args.Op == appendOp {
		command = appendCommand
	} else {
		panic(fmt.Sprintf("unknown op for putAppend RPC %s", args.Op))
	}

	newOp := Op{
		CommandOp:   command,
		Key:         args.Key,
		Value:       args.Value,
		SequenceNum: args.SequenceNum,
		ClientID:    clientIDT(args.ClientID),
	}

	cid := clientIDT(args.ClientID)

	expIndex, _, isLeader := kv.rf.Start(newOp)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	// Is it okay to init the maps after starting agreement?
	// Yep, any state update must acquire a lock.
	pendingCommit := &pendingCommit{
		expectedIndex: expIndex,
		clientID:      cid,
		sequenceNum:   args.SequenceNum,
		waitCh:        make(chan bool),
	}

	// Could there be another pendingCommit at [expIndex]?
	// Assume yes. Then, it must've been added at some previous request.
	// The fact that it hasn't been removed implies that, we didn't get a resp
	// from apply ch yet for ANY commit, but since the log just added another op
	// at the index, the old op will never get committed.
	// So, we just write [false] to the channel.

	prevOp, ok := kv.pendingCommits[expIndex]
	if ok {
		// There must be a goroutine waiting to read this, cause otherwise
		// we already wrote to this channel. But if we write to the channel
		// we also remove the entry from the map.
		// TODO: make sure writing on this channel removes it from the map.
		prevOp.waitCh <- false
	}

	kv.pendingCommits[expIndex] = pendingCommit
	kv.mu.Unlock()
	worked := <-pendingCommit.waitCh
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if worked {
		reply.Err = OK
	} else {
		// The op didn't get committed. The [Clerk] will retry.
		reply.Err = ErrWrongLeader
	}
}

// Kill is called by the tested when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Kills the underlying raft instance.
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

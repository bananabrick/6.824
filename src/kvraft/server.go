package kvraft

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

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
	ExpectedIndex int
	ClientID      clientIDT
	SequenceNum   int
	// Send true if the applyMsg matches this, otherwise send false.
	WaitCh chan bool
}

type SnapshottableState struct {
	// This is the core key-value store state.
	Kvs           map[string]string
	LastProcessed int // 1-based command in the log which has been processed or/and applied.
	// Highest sequence number from the client which has been applied.
	// Note that since the client only makes one request at a time,
	// the sequence numbers which are returned from the [applyCh] are
	// non-decreasing. For that reason, we can just ignore sequence nums
	// which are lower than sequence applied.
	SequenceApplied map[clientIDT]int
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
	SnapshottableState *SnapshottableState
	PendingCommits     map[int]*pendingCommit
}

func (kv *KVServer) encodeSnapshottable() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.SnapshottableState)
	return w.Bytes()
}

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
	kv.rf.TakeSnapShot(data, kv.SnapshottableState.LastProcessed)
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
	kv.SnapshottableState.Kvs = make(map[string]string)
	kv.SnapshottableState.LastProcessed = 0 // haven't processed any.
	kv.SnapshottableState.SequenceApplied = make(map[clientIDT]int)
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
	kv.PendingCommits = make(map[int]*pendingCommit)

	kv.initSnapshottable()

	go kv.readFromApplyCh()
	go kv.takeSnaps()
	return kv
}

func (kv *KVServer) takeSnaps() {
	for {
		if kv.killed() {
			return
		}

		time.Sleep(time.Second * 2)
		kv.mu.Lock()
		fmt.Println("periodic server state", "kvme", kv.me, kv.SnapshottableState.LastProcessed)
		fmt.Println("taking snapshot", "kvme", kv.me, kv.rf.Leader())
		kv.rf.TakeSnapShot(kv.encodeSnapshottable(), kv.SnapshottableState.LastProcessed)
		fmt.Println("took snapshot", "kvme", kv.me, kv.rf.Leader())
		kv.mu.Unlock()
	}
}

// Invariant: pendingCommit != nil, op != nil
func commitSame(pending *pendingCommit, op *Op) bool {
	same := pending.ClientID == op.ClientID
	same = same && pending.SequenceNum == op.SequenceNum
	return same
}

func (kv *KVServer) handleCommand(commitedMsg *raft.ApplyMsg) {
	opCommitted := (commitedMsg.Command).(Op)
	kv.mu.Lock()
	fmt.Println("kvserver received command", "kvme", kv.me, kv.rf.Leader(), commitedMsg.CommandIndex)
	if commitedMsg.CommandIndex != kv.SnapshottableState.LastProcessed+1 {
		// if [CommandIndex] is less, then we've already processed this entry.
		// if [CommandIndex] is greater, then at least one log entry was skipped.
		// It could potentially happen if a snapshot is installed.
		kv.mu.Unlock()
		return
	}
	fmt.Println("kvserver processing command", "kvme", kv.me, kv.rf.Leader(), commitedMsg.CommandIndex)
	expectedCommit := kv.PendingCommits[commitedMsg.CommandIndex]
	// fmt.Println("stuff", commitedMsg.CommandIndex, kv.SnapshottableState.LastProcessed+1, expectedCommit)
	if kv.SnapshottableState.SequenceApplied[opCommitted.ClientID] < opCommitted.SequenceNum {
		// Not in pending ack, and has never been removed from pending ack.
		// We know that it hasn't been removed from pending ack, cause we remove
		// from pending ack iff SequenceNum <= highest akced sequence num.
		// So, we can be sure that this has never been applied to state machine.
		if opCommitted.CommandOp == putCommand {
			// Just put it.
			kv.SnapshottableState.Kvs[opCommitted.Key] = opCommitted.Value
		} else if opCommitted.CommandOp == appendCommand {
			value, ok := kv.SnapshottableState.Kvs[opCommitted.Key]
			if ok {
				// Key already exists.
				kv.SnapshottableState.Kvs[opCommitted.Key] = value + opCommitted.Value
			} else {
				kv.SnapshottableState.Kvs[opCommitted.Key] = opCommitted.Value
			}
		}
		kv.SnapshottableState.SequenceApplied[opCommitted.ClientID] = opCommitted.SequenceNum
	}

	if commitedMsg.CommandIndex == kv.SnapshottableState.LastProcessed+1 {
		kv.SnapshottableState.LastProcessed = commitedMsg.CommandIndex
	}

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
		delete(kv.PendingCommits, commitedMsg.CommandIndex)
		kv.mu.Unlock()
		expectedCommit.WaitCh <- true
	} else {
		// Commit doesn't match the expected commit, so some other node
		// must have committed this.
		// It's that nodes responsibility to reply to the client.
		delete(kv.PendingCommits, commitedMsg.CommandIndex)
		kv.mu.Unlock()
		expectedCommit.WaitCh <- false
	}
}

func (kv *KVServer) handleRaftSentSnap(commitedMsg *raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	fmt.Println("kvserver received snap", "kvme", kv.me, kv.rf.Leader())
	snapshot := (commitedMsg.Command).(*raft.SnapShot)
	snapshottable := kv.decodeSnapshottable(snapshot.KVEncoding)
	if kv.rf.MaybeInstallSnapshot(*commitedMsg) {
		fmt.Println("kv server acquiring snap", "kvme", kv.me, kv.rf.Leader())
		kv.SnapshottableState = snapshottable
	}
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
		fmt.Println("kvserver received from applych", commitedMsg, "kvme", kv.me, kv.rf.Leader())
		kv.mu.Lock()
		fmt.Println("kvserver received from applych acquire lock", commitedMsg, "kvme", kv.me, kv.rf.Leader())
		kv.mu.Unlock()
		if !commitedMsg.CommandValid {
			// We've received a snapshot from raft.
			// We can ask raft if we should upload this.
			// panic("what")
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
		ExpectedIndex: expIndex,
		ClientID:      cid,
		SequenceNum:   args.SequenceNum,
		WaitCh:        make(chan bool),
	}

	// Could there be another pendingCommit at [expIndex]?
	// Assume yes. Then, it must've been added at some previous request.
	// The fact that it hasn't been removed implies that, we didn't get a resp
	// from apply ch yet for ANY commit, but since the log just added another op
	// at the index, the old op will never get committed.
	// So, we just write [false] to the channel.

	prevOp, ok := kv.PendingCommits[expIndex]
	if ok {
		// There must be a goroutine waiting to read this, cause otherwise
		// we already wrote to this channel. But if we write to the channel
		// we also remove the entry from the map.
		prevOp.WaitCh <- false
	}

	kv.PendingCommits[expIndex] = pendingCommit
	fmt.Println("get", pendingCommit, newOp, "kvme", kv.me, "leader", kv.rf.Leader())
	kv.mu.Unlock()

	worked := <-pendingCommit.WaitCh

	kv.mu.Lock()
	defer kv.mu.Unlock()
	fmt.Println("get1", pendingCommit, worked, kv.SnapshottableState.Kvs[args.Key], "leader", kv.rf.Leader())
	if worked {
		// The op worked, so we can do a read.
		value, ok := kv.SnapshottableState.Kvs[args.Key]
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
		ExpectedIndex: expIndex,
		ClientID:      cid,
		SequenceNum:   args.SequenceNum,
		WaitCh:        make(chan bool),
	}

	// Could there be another pendingCommit at [expIndex]?
	// Assume yes. Then, it must've been added at some previous request.
	// The fact that it hasn't been removed implies that, we didn't get a resp
	// from apply ch yet for ANY commit, but since the log just added another op
	// at the index, the old op will never get committed.
	// So, we just write [false] to the channel.

	prevOp, ok := kv.PendingCommits[expIndex]
	if ok {
		// There must be a goroutine waiting to read this, cause otherwise
		// we already wrote to this channel. But if we write to the channel
		// we also remove the entry from the map.
		// TODO: make sure writing on this channel removes it from the map.
		prevOp.WaitCh <- false
	}

	kv.PendingCommits[expIndex] = pendingCommit
	fmt.Println("put", pendingCommit, newOp, "kvme", kv.me, "leader", kv.rf.Leader())
	kv.mu.Unlock()

	worked := <-pendingCommit.WaitCh
	kv.mu.Lock()
	defer kv.mu.Unlock()
	fmt.Println("put1", pendingCommit, worked, newOp, "leader", kv.rf.Leader())
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

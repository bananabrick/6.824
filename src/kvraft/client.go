package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"../labrpc"
)

// Clerk is used by the client to talk to the server.
// It manages RPC interactions with the server.
// Note that the client will only call any [Clerk]
// methods one at a time.
// NOTE: Users of [Clerk] should see a linearizable history of requests
// and responses.
type Clerk struct {
	servers      []*labrpc.ClientEnd
	currLeader   int   // this is the current suspected leader.
	clientID     int64 // id associated with the client
	seqNum       int
	seenResponse int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func (ck *Clerk) nextSeqNum() int {
	curr := ck.seqNum
	ck.seqNum++
	return curr
}

// MakeClerk is used by a kvserver client to manage requests
// made to the kvserver. Basically handles retry/some duplicate logic.
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers

	// Assumes there is at least one server and that the first one
	// is the leader.
	ck.currLeader = 0
	ck.clientID = nrand()
	ck.seqNum = 1
	ck.seenResponse = 0

	return ck
}

func (ck *Clerk) setNewLeader() {
	ck.currLeader = (ck.currLeader + 1) % len(ck.servers)
}

// Get the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	nextSeqNum := ck.nextSeqNum()
	args := GetArgs{
		Key:         key,
		ClientID:    ck.clientID,
		SequenceNum: nextSeqNum,
		// If we're making this request, that means
		// we've seen the response to the previous request.
		SeenSeqUntil: nextSeqNum - 1,
	}
	reply := GetReply{}
	for {
		dprintln("client sending get to", ck.currLeader, ck.clientID, nextSeqNum)
		ok, rep := ck.makeRPCWithTimeoutGet(ck.currLeader, "KVServer.Get", args, reply, 200)
		dprintln("client_g", ok, args, rep, ck.currLeader)
		if ok {
			// Request went through.
			if rep.Err == OK {
				ck.seenResponse = max(ck.seenResponse, args.SequenceNum)
				return rep.Value
			} else if rep.Err == ErrNoKey {
				ck.seenResponse = max(ck.seenResponse, args.SequenceNum)
				return ""
			}
		}

		// If we're here, then the previous request didn't work for some reason.
		// Try a new leader.
		ck.setNewLeader()
	}
}

func (ck *Clerk) makeRPCWithTimeoutGet(leader int, name string, args GetArgs, reply GetReply, timeout int) (bool, *GetReply) {
	waitCh := make(chan bool)
	go func() {
		var ok bool
		ok = ck.servers[leader].Call(name, &args, &reply)
		waitCh <- ok
	}()

	var ok bool
	select {
	case ok = <-waitCh:
		return ok, &reply
	case <-time.After(time.Millisecond * time.Duration(timeout)):
		return false, nil
	}
}

// PutAppend shared by Put and Append. Append acts like put if no key.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
// Note: Client considers this function returning to be indicative
// of the request completing successfully.
func (ck *Clerk) PutAppend(key string, value string, op clientOp) {
	nextSeqNum := ck.nextSeqNum()
	args := PutAppendArgs{
		Key:         key,
		Value:       value,
		Op:          op,
		ClientID:    ck.clientID,
		SequenceNum: nextSeqNum,
		// If we're making this request, that means
		// we've seen the response to the previous request.
		SeenSeqUntil: nextSeqNum - 1,
	}
	reply := PutAppendReply{}
	for {
		dprintln("client sending update to", ck.currLeader, ck.clientID, nextSeqNum)
		ok, rep := ck.makeRPCWithTimeoutPut(ck.currLeader, "KVServer.PutAppend", args, reply, 200)
		dprintln("client_p", ok, args, rep, ck.currLeader)
		if ok && rep.Err == OK {
			// Request went through.
			ck.seenResponse = max(ck.seenResponse, args.SequenceNum)
			return
		}
		// If we're here, then the previous request didn't work for some reason.
		// Try a new leader.
		ck.setNewLeader()
	}
}

func (ck *Clerk) makeRPCWithTimeoutPut(leader int, name string, args PutAppendArgs, reply PutAppendReply, timeout int) (bool, *PutAppendReply) {
	waitCh := make(chan bool)
	go func() {
		var ok bool
		ok = ck.servers[leader].Call(name, &args, &reply)
		waitCh <- ok
	}()

	var ok bool
	select {
	case ok = <-waitCh:
		return ok, &reply
	case <-time.After(time.Millisecond * time.Duration(timeout)):
		return false, nil
	}
}

// Put is used by the client to update or put a key value pair.
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, putOp)
}

// Append is used by a client to append to an existing kv pair.
// Behaves like put otherwise.
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, appendOp)
}

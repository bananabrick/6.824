package kvraft

import (
	"log"
)

// Err associated with RPCs to the
// kvservers.
type Err string

const (
	// OK resp
	OK Err = "OK"
	// ErrNoKey signifies a key not found response
	ErrNoKey Err = "ErrNoKey"
	// ErrWrongLeader signifies that we contacted the wrong leader
	ErrWrongLeader Err = "ErrWrongLeader"
)

type clientOp string

const (
	putOp    clientOp = "Put"    // update or set value associated with a key
	appendOp clientOp = "Append" // Append value to the key string
)

const debug = 0

func dprintln(a ...interface{}) (n int, err error) {
	if debug > 0 {
		log.Println(a...)
	}
	return
}

// PutAppendArgs does a Put RPC or an Append RPC
type PutAppendArgs struct {
	Key         string
	Value       string
	Op          clientOp
	ClientID    int64
	SequenceNum int
	// Got back reply for SeqNum <= [SeenSeqUntil]
	SeenSeqUntil int
}

// PutAppendReply is the reply to the PutAppendArgs RPC
type PutAppendReply struct {
	Err Err
}

// GetArgs RPC
type GetArgs struct {
	Key         string
	ClientID    int64
	SequenceNum int

	// Got back reply for SeqNum <= [SeenSeqUntil]
	SeenSeqUntil int
}

// GetReply is the reply to the GetArgs RPC
type GetReply struct {
	Err   Err
	Value string
}

func max(a, b int) int {
	if a >= b {
		return a
	}
	return b
}

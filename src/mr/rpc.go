package mr

// RPC definitions.

import (
	"os"
	"strconv"
)

// TaskReq is used to ask the
// master for a single map or reduce
// task.
type TaskReq struct{}

// TaskReply has its fields set by the
// master and then read by a worker.
type TaskReply struct {
	// No task given. The server
	// is asking the worker to wait.
	Wait bool

	// If task isn't map it's reduce.
	IsMap bool

	MapFile string

	// Names of all the files. For the map
	// task, this will be one file.
	// For the reduce task, this will be
	// a number of files.
	ReduceFiles []string

	// Num paritions as stated by the map
	// reduce paper.
	R int

	// Which map task this task corresponds to.
	// Only useful for map tasks.
	// Only really need for naming intermediate files.
	TaskNum int
}

// TaskDoneReq is used by the worker to signal to
// the master that the task is done.
type TaskDoneReq struct {
	IsMap   bool
	TaskNum int

	// This is the final output reduce filename.
	ReduceFile string

	// This is a map from reduce parition
	// num to the intermediate filename.
	MapFiles map[int]string
}

// TaskDoneReply is required by Go but the client
// doesn't get a reply back from the master.
type TaskDoneReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

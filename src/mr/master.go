package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	stateIdle      = iota
	stateAssigned  = iota
	stateCompleted = iota
)

const (
	// We give the workers a 12 second completion
	// window. In the actual map reduce, they use heartbeats.
	// But we assume machine failed if the task isn't done
	// in 10 seconds.
	completionWindow = 12
)

// Invariant for state in both mapTask
// and reduceTask, once a task is marked
// as `stateCompleted`, it's NEVER reversed.

// Basically some enums for task state.
type mapTask struct {
	filename string
	state    int
}

// Basically, we have num map tasks
// files for a given nReduce bucket.
// So, we need all of those before
// we can start up a reduce task.
type reduceTask struct {
	filenames []string
	state     int
}

// Master is returned to mrmaster which
// can call MakeMaster and Done.
type Master struct {
	// map task number to the task.
	mapTasks map[int](*mapTask)

	// reduce task number to the task.
	reduceTasks map[int](*reduceTask)

	// So, why do we need this?
	// Assigning duplicate tasks is fine and is
	// how we recover from failures, but we don't
	// want to do that if there isn't a failure.
	mut sync.Mutex

	// Basically, the number of paritions.
	R int

	// Basically number of map tasks which
	// need to be done.
	M int
}

// GetTask is used in an rpc call from the client to retrieve
// a new map or reduce task.
// Note that reduce tasks will only be served once all map
// tasks are finished.
// This function may be run on multiple go routines to
// server multiple clients.
func (m *Master) GetTask(req *TaskReq, reply *TaskReply) error {
	m.mut.Lock()
	defer m.mut.Unlock()

	// Look for a map task.
	taskAcquired := acquireMapTask(m, reply)
	if taskAcquired {
		return nil
	}

	// No map tasks were idle. But this doesn't
	// mean that we can actually do a reduce task.
	taskAcquired = acquireReduceTask(m, reply)
	reply.Wait = !taskAcquired
	return nil
}

// SendComplete is used by the client to indicate that
// This function may be run on multiple go routines to
// serve multiple clients.
func (m *Master) SendComplete(req *TaskDoneReq, reply *TaskDoneReply) error {
	m.mut.Lock()
	defer m.mut.Unlock()
	if req.IsMap {
		handleMapDone(m, req)
	} else {
		handleReduceDone(m, req)
	}
	return nil
}

// Invariant: Only call this after acquiring lock
// This function may be run on multiple go routines to
// serve multiple clients.
func handleMapDone(m *Master, req *TaskDoneReq) {
	// Other threads could also complete this task later.
	// However, that's fine because intermediate file is being renamed
	// atomically, so it will always be available to read for
	// any thread.
	if m.mapTasks[req.TaskNum].state == stateCompleted {
		return
	}

	// Only one of the workers which completes the task
	// will be able to append to reduceTasks list.
	m.mapTasks[req.TaskNum].state = stateCompleted
	for r, file := range req.MapFiles {
		m.reduceTasks[r].filenames = append(m.reduceTasks[r].filenames, file)
	}
}

// Invariant: Only call this after acquiring lock
// This function may be run on multiple go routines to
// serve multiple clients.
func handleReduceDone(m *Master, req *TaskDoneReq) {
	m.reduceTasks[req.TaskNum].state = stateCompleted
}

// Invariant: Only call this after acquiring lock
// This function may be run on multiple go routines to
// serve multiple clients.
func acquireReduceTask(m *Master, reply *TaskReply) bool {
	for r, task := range m.reduceTasks {
		if task.state == stateIdle && len(task.filenames) == m.M {
			// So, all of the intermediate map files are ready.
			reply.Wait = false
			reply.IsMap = false
			reply.ReduceFiles = task.filenames
			reply.TaskNum = r
			task.state = stateAssigned

			// Assume worker fails if it doesn't
			// complete the task fast enough.
			go func(m *Master, t *reduceTask) {
				time.Sleep(time.Second * completionWindow)
				m.mut.Lock()
				if t.state != stateCompleted {
					t.state = stateIdle
				}
				m.mut.Unlock()
			}(m, task)
			return true

		}
	}
	return false
}

// Invariant: Only call this after acquiring lock
// This function may be run on multiple go routines to
// serve multiple clients.
func acquireMapTask(m *Master, reply *TaskReply) bool {
	for i, task := range m.mapTasks {
		if task.state == stateIdle {
			// So, we have a task which is idle.
			// Note that a task may be idle even if it was assigned.
			// This happens when we assume that a machine failed.
			reply.Wait = false
			reply.IsMap = true
			reply.MapFile = task.filename
			reply.R = m.R
			reply.TaskNum = i
			task.state = stateAssigned

			// Worker fails if it's too slow.
			go func(m *Master, t *mapTask) {
				time.Sleep(time.Second * completionWindow)
				m.mut.Lock()
				if t.state != stateCompleted {
					t.state = stateIdle
				}
				m.mut.Unlock()
			}(m, task)
			return true
		}
	}
	return false
}

// start a thread that listens for RPCs from worker.go.
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done is called by main/mrmaster.go to periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	m.mut.Lock()
	defer m.mut.Unlock()

	for _, task := range m.reduceTasks {
		if task.state != stateCompleted {
			return false
		}
	}
	return true
}

// MakeMaster is called by mrmaster.go. It starts
// a master server.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.mapTasks = make(map[int](*mapTask))
	m.reduceTasks = make(map[int](*reduceTask))
	m.R = nReduce
	m.M = len(files)

	// Generate map tasks.
	for i, path := range files {
		// each file is basically a map task.
		m.mapTasks[i] = &mapTask{path, stateIdle}
	}

	// Generate reduce tasks.
	for r := 0; r < nReduce; r++ {
		m.reduceTasks[r] = &reduceTask{}
		m.reduceTasks[r].state = stateIdle
	}

	m.server()
	return &m
}

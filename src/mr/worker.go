package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// KeyValue slice is returned by Map functions.
type KeyValue struct {
	Key   string
	Value string
}

// Worker is called by main/mrworker.go calls this function.
// It basically sets up a client which will ask the server
// for a task.
// The server can respond with either a map or a reduce task.
// or the server can respond with nothing in which
// case the client will keep waiting.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		time.Sleep(time.Second)
		task, success := getTask()
		if !success {
			// If we couldn't fetch task for ANY reason,
			// we assume the server is dead, so we shutdown
			// client. Kind of broad assumption, but works
			// for this lab.
			log.Println("ByeBye")
			os.Exit(0)
		}

		if task.Wait {
			continue
		}

		if task.IsMap {
			success = handleMap(mapf, task)
		} else {
			success = handleReduce(reducef, task)
		}

		if !success {
			log.Println()
		}
	}
}

func intermediateFileName(m, r int) string {
	return fmt.Sprintf("./mr_%d_%d_i", m, r)
}

// Returns value lets the program know if it's time to quit.
func handleMap(mapf func(string, string) []KeyValue, task *TaskReply) bool {
	filename := task.MapFile
	// It should be ok if multiple threads
	// open these files or read from them at the
	// same time.
	file, err := os.Open(filename)
	if err != nil {
		log.Printf("Invalid file name %v", filename)
		return false
	}
	defer file.Close()

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Printf("cannot read %v", filename)
		return false
	}

	// Intermediate key values are split up by hash value.
	kva := mapf(filename, string(content))
	splitUp := make(map[int][]KeyValue)
	for _, kv := range kva {
		splitUp[ihash(kv.Key)%task.R] = append(splitUp[ihash(kv.Key)%task.R], kv)
	}

	intFiles := make(map[int]string)
	for r, kvs := range splitUp {
		// We're going to create a temp file, and then
		// write the kvs to it. Note that, we don't want
		// to just write to the actual intermediate file
		// cause we don't want multiple threads writing
		// to it at once.
		tempFile, err := ioutil.TempFile("./", "m_temp")
		if err != nil {
			log.Println("Couldn't create temp file for map task")
			return false
		}

		enc := json.NewEncoder(tempFile)
		for _, kv := range kvs {
			enc.Encode(&kv)
		}
		intFiles[r] = intermediateFileName(task.TaskNum, r)
		os.Rename(tempFile.Name(), intFiles[r])
		tempFile.Close()
	}

	// So, if we're here we wrote the down the kvs in an intermediate file correctly.
	// So, let's notify master that we finished map task number
	req := TaskDoneReq{true, task.TaskNum, "", intFiles}
	reply := TaskReply{}
	return call("Master.SendComplete", &req, &reply)

}

// ByKey is used for custom sorting.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func finalFileName(r int) string {
	return fmt.Sprintf("./mr-out-%d", r)
}

// Return value lets the program know it's time to quit.
func handleReduce(reducef func(string, []string) string, task *TaskReply) bool {
	var kva []KeyValue
	for _, filename := range task.ReduceFiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Printf("Invalid file name %v", filename)
			return false
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			// TODO: For now we assume that failure means
			// that we finished the task successfully, not
			// that we couldn't decode all values.
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(kva))
	tempFile, err := ioutil.TempFile("./", "mo_temp")
	if err != nil {
		return false
	}
	defer tempFile.Close()

	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	finalName := finalFileName(task.TaskNum)
	os.Rename(tempFile.Name(), finalName)
	req := TaskDoneReq{false, task.TaskNum, finalName, nil}
	reply := TaskReply{}
	return call("Master.SendComplete", &req, &reply)
}

// Returns nil, false on failures.
// Returns pointer, true on success.
func getTask() (*TaskReply, bool) {
	req := TaskReq{}
	reply := TaskReply{}
	success := call("Master.GetTask", &req, &reply)
	if !success {
		return nil, false
	}
	return &reply, true
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

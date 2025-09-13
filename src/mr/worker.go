package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	for {
		time.Sleep(100 * time.Millisecond)

		args := Args{}
		reply := Reply{}

		args.Task.TaskType = UnknowTaskType
		ok := call("Coordinator.RPCHandler", &args, &reply)
		if !ok {
			fmt.Printf("call failed!\n")
			return
		}
		if reply.Task.TaskType == MapTask {
			// Map
			DoMap(mapf, &args, &reply)
		} else if reply.Task.TaskType == ReduceTask {
			// Reduce
			DoReduce(reducef, &args, &reply)
		} else if reply.Task.TaskType == UnknowTaskType {
			//fmt.Printf("No tasks obtained.\n")
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func DoMap(mapf func(string, string) []KeyValue, args *Args, reply *Reply) {

	intermediate := ByKey{}
	file2result := map[string]ByKey{}

	task := reply.Task

	// Read and process
	for _, filename := range task.ProcessFile {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()

		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}

	// Get file name and result
	for i := 0; i < len(intermediate); i++ {

		kv := KeyValue{}
		kv.Key = intermediate[i].Key
		kv.Value = intermediate[i].Value

		reducerID := ihash(kv.Key) % reply.NReduce
		name := fmt.Sprintf("mr-%v-%v", reply.Task.Id, reducerID)
		file2result[name] = append(file2result[name], kv)

		record := false
		for _, item := range task.Result {
			if name == item {
				record = true
			}
		}
		if record == false {
			task.Result = append(task.Result, name)
		}
	}

	// Store
	for name, kva := range file2result {

		tmpfile, err := os.CreateTemp("./", "temp_*")
		if err != nil {
			log.Fatal(err)
		}

		enc := json.NewEncoder(tmpfile)
		for _, kv := range kva {
			err = enc.Encode(&kv)
			if err != nil {
				log.Fatal(err)
			}
		}
		tmpfile.Close()

		err = os.Rename(tmpfile.Name(), name)
		if err != nil {
			log.Fatal(err)
		}
		os.Remove(tmpfile.Name())
	}

	// finish map and call server
	task.Status = TaskCompleted
	args.Task = task
	ok := call("Coordinator.RPCHandler", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
		return
	}
}

func DoReduce(reducef func(string, []string) string, args *Args, reply *Reply) {

	file2result := map[string]ByKey{}
	intermediate := []KeyValue{}

	task := reply.Task

	for _, filename := range task.ProcessFile {
		file, err := os.OpenFile(filename, os.O_RDONLY|os.O_CREATE, 0644)
		if err != nil {
			log.Fatal(err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err = dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	sort.Sort(ByKey(intermediate))

	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[i].Key == intermediate[j].Key {
			j++
		}

		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		kv := KeyValue{}
		kv.Key = intermediate[i].Key
		kv.Value = reducef(kv.Key, values)

		name := fmt.Sprintf("mr-out-%v", task.Id)
		file2result[name] = append(file2result[name], kv)
		task.Result = append(task.Result, name)

		i = j
	}

	// Store
	for name, kva := range file2result {
		tmpfile, err := os.CreateTemp("./", "temp_*")
		if err != nil {
			log.Fatal(err)
		}
		for _, kv := range kva {
			fmt.Fprintf(tmpfile, "%v %v\n", kv.Key, kv.Value)
		}
		tmpfile.Close()

		err = os.Rename(tmpfile.Name(), name)
		if err != nil {
			log.Fatal(err)
		}
		os.Remove(tmpfile.Name())
	}

	// Finish Reduce
	task.Status = TaskCompleted
	args.Task = task
	ok := call("Coordinator.RPCHandler", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
		return
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

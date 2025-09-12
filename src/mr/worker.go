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
		time.Sleep(1000 * time.Millisecond)

		fmt.Printf("CALL\n")

		args := Args{}
		reply := Reply{}

		args.Status = 0
		args.MapperID = -1
		args.ReducerID = -1
		ok := call("Coordinator.RPCHandler", &args, &reply)
		if !ok {
			fmt.Printf("call failed!\n")
			return
		}
		if reply.TaskType == 0 {
			// Map
			args.TaskType = 0
			args.MapperID = reply.MapperID

			name2kv := map[string]ByKey{}

			fmt.Printf("Map%v\t: begin work\n", args.MapperID)

			for _, filename := range reply.Files {
				fmt.Printf("Map%v\t: map file:%v\n", args.MapperID, filename)

				intermediate := []KeyValue{}

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

				i := 0
				for i < len(intermediate) {
					kv := KeyValue{}
					kv.Key = intermediate[i].Key
					kv.Value = intermediate[i].Value
					reducerID := ihash(kv.Key) % reply.NReduce

					name := fmt.Sprintf("mr-%v-%v", reply.MapperID, reducerID)

					exit := false
					for _, ifilename := range args.IntermediateFiles {
						if ifilename == name {
							exit = true
							break
						}
					}
					if exit == false {
						args.IntermediateFiles = append(args.IntermediateFiles, name)
					}

					name2kv[name] = append(name2kv[name], kv)

					//i = j
					i++
				}
			}
			// store
			fmt.Printf("Map%v\t: store\n", args.MapperID)

			for name, kva := range name2kv {

				tmpfile, errCf := os.CreateTemp("./", "temp_*")
				if errCf != nil {
					log.Fatal(errCf)
				}

				enc := json.NewEncoder(tmpfile)
				for _, kv := range kva {
					errEc := enc.Encode(&kv)
					if errEc != nil {
						log.Fatal(errEc)
					}
				}
				tmpfile.Close()

				err := os.Rename(tmpfile.Name(), name)
				if err != nil {
					log.Fatal(err)
				}
				os.Remove(tmpfile.Name())
			}
			// finish map
			fmt.Printf("Map%v\t: finish work\n", args.MapperID)

			args.Status = 2

			ok = call("Coordinator.RPCHandler", &args, &reply)
			if !ok {
				fmt.Printf("call failed!\n")
				return
			}
		} else {
			//reduce
			args.TaskType = 1
			args.ReducerID = reply.ReducerID

			name2kv := map[string]ByKey{}

			if reply.Files == nil {
				continue
			}

			fmt.Printf("Reduce%v\t: begin\n", args.ReducerID)

			for _, filename := range reply.Files {

				intermediate := []KeyValue{}

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

				sort.Sort(ByKey(intermediate))

				i := 0
				for i < len(intermediate) {
					j := i + 1
					for j < len(intermediate) && intermediate[i].Key == intermediate[j].Key {
						j++
					}

					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, intermediate[i].Value)
					}

					kv := KeyValue{}
					kv.Key = intermediate[i].Key
					kv.Value = reducef(kv.Key, values)

					name := fmt.Sprintf("mr-out-%v", reply.ReducerID)

					name2kv[name] = append(name2kv[name], kv)

					i = j
				}
			}
			// store
			fmt.Printf("Reduce%v\t: store\n", args.ReducerID)

			for name, kva := range name2kv {
				tmpfile, errCf := os.CreateTemp("./", "temp_*")
				if errCf != nil {
					log.Fatal(errCf)
				}
				for _, kv := range kva {
					fmt.Fprintf(tmpfile, "%v %v\n", kv.Key, kv.Value)
				}
				tmpfile.Close()

				errRn := os.Rename(tmpfile.Name(), name)
				if errRn != nil {
					log.Fatal(errRn)
				}
				os.Remove(tmpfile.Name())
			}

			// finish reduce
			fmt.Printf("Reduce%v\t: finish\n", args.ReducerID)

			args.Status = 2
			ok = call("Coordinator.RPCHandler", &args, &reply)
			if !ok {
				fmt.Printf("call failed!\n")
				return
			}

		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

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

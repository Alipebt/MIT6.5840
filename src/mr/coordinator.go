package mr

import (
	"log"
	"strconv"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	files             []string
	intermediateFiles []string
	assignedFiles     map[string]bool
	mappedData        map[string]bool

	mapperID      int
	reducerID     int
	nReduce       int
	mapperStatus  map[int]int
	reducerStatus map[int]int
	mapFinish     bool
	reduceFinish  bool

	mu_Mapper  sync.Mutex
	mu_Reducer sync.Mutex

	mu_RFinish sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) RPCHandler(args *Args, reply *Reply) error {
	if args.Status == 0 {
		// ready
		if !c.mapFinish {
			// map
			c.mu_Mapper.Lock()
			reply.TaskType = 0
			if args.MapperID == -1 {
				reply.MapperID = c.mapperID
			} else {
				reply.MapperID = args.MapperID
			}

			//TODO： 不需要bool
			assigned := make(map[string]bool)
			for _, item := range c.files {
				if c.assignedFiles[item] == false {
					reply.Files = append(reply.Files, item)
					c.assignedFiles[item] = true
					assigned[item] = true
					break
				}
			}
			reply.NReduce = c.nReduce

			c.mapperStatus[c.mapperID] = 1
			c.mapperID++
			c.mu_Mapper.Unlock()

		} else {
			// reduce
			c.mu_Reducer.Lock()
			reply.TaskType = 1

			for id, status := range c.reducerStatus {
				if status != 2 {
					c.reducerID = id
					break
				}
			}

			if args.ReducerID == -1 {
				reply.ReducerID = c.reducerID
			} else {
				reply.ReducerID = args.ReducerID
			}
			for _, filename := range c.intermediateFiles {
				tail, _ := strconv.Atoi(filename[len(filename)-1:])
				if tail == c.reducerID {
					reply.Files = append(reply.Files, filename)
				}
			}
			c.reducerStatus[c.reducerID] = 1
			c.mu_Reducer.Unlock()

		}
	} else if args.Status == 2 {
		// finish
		if args.TaskType == 0 {
			// map
			c.mu_Mapper.Lock()
			c.mapperStatus[args.MapperID] = 2
			for _, file := range args.IntermediateFiles {
				c.intermediateFiles = append(c.intermediateFiles, file)
			}
			for _, file := range args.MappedData {
				c.mappedData[file] = true
			}

			c.mapFinish = true
			for _, ok := range c.assignedFiles {
				if !ok {
					c.mapFinish = false
					break
				}
			}
			for _, item := range c.mappedData {
				if item != true {
					c.mapFinish = false
					break
				}
			}
			c.mu_Mapper.Unlock()
		} else {
			// reduce
			c.mu_Reducer.Lock()
			c.reducerStatus[args.ReducerID] = 2
			ifFinish := true
			for _, st := range c.reducerStatus {
				if st == 1 || st == -1 {
					ifFinish = false
					break
				}
			}
			c.mu_RFinish.Lock()
			c.reduceFinish = ifFinish
			c.mu_RFinish.Unlock()
			c.mu_Reducer.Unlock()
		}
	}

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mu_RFinish.Lock()
	if c.reduceFinish == true {
		ret = true
	}
	c.mu_RFinish.Unlock()

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = files
	c.intermediateFiles = make([]string, 0)

	c.mapperID = 0
	c.reducerID = 0
	c.nReduce = nReduce
	c.mapFinish = false
	c.reduceFinish = false

	c.reducerStatus = make(map[int]int)
	for i := 0; i < nReduce; i++ {
		c.reducerStatus[i] = -1
	}
	c.mapperStatus = make(map[int]int)

	c.assignedFiles = make(map[string]bool)
	for _, file := range files {
		c.assignedFiles[file] = false
	}
	c.mappedData = make(map[string]bool)
	for _, file := range files {
		c.mappedData[file] = false
	}

	c.server()
	return &c
}
